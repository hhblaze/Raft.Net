using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DBreeze.Utils;

namespace Raft.Transport.UdpServer
{
    internal class UdpSocketListener:IDisposable
    {

        public Socket m_ListenSocket=null;
        public IPEndPoint EndPoint { get; set; }
        SocketAsyncEventArgs Sae = new SocketAsyncEventArgs();
        public int Port = 40000;
        byte[] buffer = null;

        /// <summary>
        /// ChunkSize to avoid fragmentation due to UDP documentation
        /// https://ru.wikipedia.org/wiki/UDP
        /// 1432 byte 
        /// </summary>
        public int UdpChunkSize = 1412; // 1432 - 20;   


        /// <summary>
        /// Must be supplied from outside, a function to process received data from different EndPoints
        /// </summary>
        public Action<IPEndPoint, byte[]> DataReceived = null;

        public UdpDataSender UdpSender = null;
        public UdpDataReceiver UdpReceiver = null;
        public TimeMaster TM = null;
        public IWarningLog Log = null;

        public UdpSocketListener(TimeMaster timeMaster,IWarningLog log)
        {
            buffer = new byte[UdpChunkSize + 20]; //due to reliable protocol header          

            Log = log;
            TM = timeMaster;
            UdpReceiver = new UdpDataReceiver(this);           
            UdpSender = new UdpDataSender(this);
        }

        //!!!!!!!!!!!!!!!!!!!!!!!!!!!  TIME MASTER FOR SENDER

        public void Dispose()
        {
            //!!!!!!!!!!!!!!!!!!!!!!!!!!
            try
            {
                if (UdpReceiver != null)
                {
                    UdpReceiver.Dispose();
                    UdpReceiver = null;
                }

                if (UdpSender != null)
                {
                    UdpSender.Dispose();
                    UdpSender = null;
                }
            }
            catch
            { }
        }


     // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   MAKE FOR EVERY RECEIVER ITS OWN
       
        public bool Start(int port)
        {
            //SaeState saeState = null;
            Port = port;
            try
            {                
                m_ListenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                m_ListenSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                this.EndPoint = new IPEndPoint(IPAddress.Any, Port);
                m_ListenSocket.Bind(this.EndPoint);
                               
                Sae.UserToken = 0;
                Sae.Completed += new EventHandler<SocketAsyncEventArgs>(eventArgs_Completed);
                Sae.RemoteEndPoint = this.EndPoint;

                Sae.SetBuffer(buffer, 0, buffer.Length);

                if (!m_ListenSocket.ReceiveFromAsync(Sae))
                    eventArgs_Completed(this, Sae);

                this.Log.LogError(new WarningLogEntry
                {
                    LogType = WarningLogEntry.eLogType.INFORMATION,
                    Description = $"UdpSocketListener started listen on {port}"
                });           

                return true;
            }
            catch (Exception e)
            {
                if (Sae != null)
                {
                    Sae.Completed -= new EventHandler<SocketAsyncEventArgs>(eventArgs_Completed);
                    Sae.Dispose();
                    Sae = null;
                }

                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void LogTo(string data)
        {
            //Console.WriteLine(String.Format("{0}> {1}: {2}", DateTime.Now.ToString("HH:mm:ss.ms"), this.EndPoint.ToString(), data));
        }
        
        public void LogTo1(string data)
        {
            // Console.WriteLine(String.Format("{0}> {1}: {2}", DateTime.Now.ToString("HH:mm:ss.ms"),this.EndPoint.ToString(), data));
        }

        public void LogTo2(string data)
        {
            Console.WriteLine(String.Format("{0}> {1}: {2}", DateTime.Now.ToString("HH:mm:ss.ms"), this.EndPoint.ToString(), data));
        }

        void ResetSae()
        {
            try
            {
                if (Sae != null)
                {
                    Sae.Completed -= new EventHandler<SocketAsyncEventArgs>(eventArgs_Completed);
                    Sae.Dispose();
                    Sae = null;
                }
            }
            catch
            {
            }

            try
            {
                Sae = new SocketAsyncEventArgs();
                Sae.UserToken = 0;
                Sae.Completed += new EventHandler<SocketAsyncEventArgs>(eventArgs_Completed);
                Sae.RemoteEndPoint = this.EndPoint;

                Sae.SetBuffer(buffer, 0, buffer.Length);

                if (!m_ListenSocket.ReceiveFromAsync(Sae))
                    eventArgs_Completed(this, Sae);   
            }
            catch(System.Exception ex)
            {
            }
           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void eventArgs_Completed(object sender, SocketAsyncEventArgs e)
        {
           // e.Completed -= new EventHandler<SocketAsyncEventArgs>(eventArgs_Completed);

            if (e.SocketError != SocketError.Success)
            {
                var errorCode = (int)e.SocketError;

                //The listen socket was closed
                if (errorCode == 995 || errorCode == 10004 || errorCode == 10038)
                {
                    //throw new Exception("UdpSocketListener eventArgs_Completed: the listen socket was closed");
                   // return;
                }

                //OnError(new SocketException(errorCode));    
                this.ResetSae();
                return;
            }

            if (e.LastOperation == SocketAsyncOperation.ReceiveFrom)
            {
                try
                {
                    //e.RemoteEndPoint
                    //e.Buffer,e.Offset,e.BytesTransferred
                    //var remoteEndPoint = e.RemoteEndPoint as IPEndPoint;
                    //var receivedData = new ArraySegment<byte>(e.Buffer, e.Offset, e.BytesTransferred);
                    // OnNewClientAccepted(m_ListenSocket, e);

                    this.LogTo(String.Format("received from {0}  {1} bytes ", e.RemoteEndPoint.ToString(), e.BytesTransferred));

                    if (e.BytesTransferred > 0)
                    {
                        byte[] tmp = e.Buffer.Substring(e.Offset, e.BytesTransferred);

                        try
                        {
                            if (!m_ListenSocket.ReceiveFromAsync(Sae))
                            {
                                this.UdpReceiver.Received((IPEndPoint)e.RemoteEndPoint, tmp);
                                eventArgs_Completed(this, Sae);
                                return;
                            }
                        }
                        catch
                        {
                            //!!!
                            this.UdpReceiver.Received((IPEndPoint)e.RemoteEndPoint, tmp);
                            eventArgs_Completed(this, Sae);
                            return;
                        }

                        this.UdpReceiver.Received((IPEndPoint)e.RemoteEndPoint, tmp);

                    }
                    else
                    {
                        if (!m_ListenSocket.ReceiveFromAsync(Sae))
                        {                          
                            eventArgs_Completed(this, Sae);
                            return;
                        }
                    }
                }
                catch
                {
                    ResetSae();
                    return;
                }
            }
            else
            {                
                ResetSae();
                return;
            }
        }

        public void Stop()
        {
            if (m_ListenSocket == null)
                return;

            lock (this)
            {
                try
                {
                    if(Sae != null)
                        Sae.Completed -= new EventHandler<SocketAsyncEventArgs>(eventArgs_Completed);
                }
                catch
                {}

                if (m_ListenSocket == null)
                    return;

                try
                {
                    m_ListenSocket.Shutdown(SocketShutdown.Both);
                }
                catch { }

                try
                {
                    m_ListenSocket.Close();
                }
                catch { }
                finally
                {
                    m_ListenSocket = null;
                }
            }

            //OnStopped();
        }


        ///// <summary>
        ///// For tests
        ///// </summary>
        ///// <param name="data"></param>
        ///// <param name="port"></param>
        public void SendTo(byte[] data, int port)
        {
            SocketAsyncEventArgs ss = new SocketAsyncEventArgs();
            ss.RemoteEndPoint = new IPEndPoint(IPAddress.Parse("192.168.101.38"), port);
            ss.Completed += ss_Completed;
            ss.SetBuffer(data, 0, data.Length);

            m_ListenSocket.SendToAsync(ss);
        }

        void ss_Completed(object sender, SocketAsyncEventArgs e)
        {
            Console.WriteLine(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss.ms") + " OK");
        }


        //ReaderWriterLockSlim _sync_senderBuffer = new ReaderWriterLockSlim();
        //Dictionary<string, SocketAsyncEventArgs> senderBuffer = new Dictionary<string, SocketAsyncEventArgs>();

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ipEndPoint"></param>
        ///// <param name="data"></param>
        //public void SendTo(IPEndPoint ipEndPoint, byte[] data)
        //{
        //    if (ipEndPoint == null || data == null || data.Length < 1)
        //        return;

        //    SocketAsyncEventArgs ss=null;

        //    try
        //    {
        //        _sync_senderBuffer.EnterUpgradeableReadLock();
        //        try
        //        {
        //            if (!senderBuffer.TryGetValue(ipEndPoint.ToString(), out ss))
        //            {
        //                _sync_senderBuffer.EnterWriteLock();
        //                try
        //                {
        //                    if (!senderBuffer.TryGetValue(ipEndPoint.ToString(), out ss))
        //                    {
        //                        ss = new SocketAsyncEventArgs();
        //                        ss.RemoteEndPoint = ipEndPoint;
        //                        ss.Completed += ss_Completed;
        //                        //ss.SetBuffer(new byte[10000], 0, 10000);
        //                        senderBuffer.Add(ipEndPoint.ToString(), ss);
        //                    }
        //                }
        //                finally
        //                {
        //                    _sync_senderBuffer.ExitWriteLock();
        //                }
        //            }
        //        }
        //        finally
        //        {
        //            _sync_senderBuffer.ExitUpgradeableReadLock();
        //        }

                
        //        ss.SetBuffer(data, 0, data.Length);
                
        //        if (!m_ListenSocket.SendToAsync(ss))
        //        {
        //            ss.Completed -= ss_Completed;
        //            ss.Dispose();

        //            _sync_senderBuffer.EnterWriteLock();
        //            try
        //            {
        //                ss = new SocketAsyncEventArgs();
        //                ss.RemoteEndPoint = ipEndPoint;
        //                ss.Completed += ss_Completed;

        //                senderBuffer[ipEndPoint.ToString()] = ss;
        //            }
        //            finally
        //            {
        //                _sync_senderBuffer.ExitWriteLock();
        //            }

        //            ss.SetBuffer(data, 0, data.Length);

        //            if (!m_ListenSocket.SendToAsync(ss))
        //            {
        //                Console.WriteLine(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Encountered mistake");
        //            }

        //            Console.WriteLine("********************Encountered mistake");
        //        }
        //        //m_ListenSocket.BeginSendTo(data, 0, data.Length, SocketFlags.None, ipEndPoint, null, null);
              
        //    }
        //    catch (Exception ex)
        //    {
        //        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //    }
           
        //}

       





    }
}
