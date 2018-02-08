using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Raft.Transport.UdpServer
{
    /// <summary>
    /// We send over this class
    /// 
    /// http://msdn.microsoft.com/en-us/library/tst0kwb1(v=vs.110).aspx
    /// </summary>
    internal class UdpDataSender:IDisposable
    {
        //From here we can send udp packages to different endpoints, that's why for every endpoint we store our own queue.
        //We will never delete queue, because we think that only limited quantity of endpoints are possible.
        //For internal server side server clusters 

        ReaderWriterLockSlim _sync_qu = new ReaderWriterLockSlim();
        /// <summary>
        /// Holds endpoints as Keys and what to send as value
        /// </summary>
        Dictionary<string, UdpDataQueue> qu = new Dictionary<string, UdpDataQueue>();
        /// <summary>
        /// All operations via this socket
        /// </summary>
        UdpSocketListener UdpSocket = null;
        /// <summary>
        /// 
        /// </summary>
        object lock_SentEndPoint = new object();
        /// <summary>
        /// Works with lock_SentEndPoint.
        /// Key is IPEndPoint, Value is sent DateTime
        /// Only DataProtocol (not operational protocol data can be registered here)
        /// </summary>
        Dictionary<string, DateTime> qSentEndPoints = new Dictionary<string, DateTime>();
       
     
        public UdpDataSender(UdpSocketListener udpSocket)
        {
            UdpSocket = udpSocket;
            
            //this.UdpSocket.TM.FireEventAfter(10000, 
            //    (o) => {

                  
            //        KeyValuePair<string, DateTime> ep;
            //        UdpDataQueue dq = null;

            //        for (int i = 0; i < 100; i++)
            //        {
            //            lock (lock_SentEndPoint)
            //            {
            //                if (qSentEndPoints.Count() == 0)
            //                    return;

            //                ep = qSentEndPoints.FirstOrDefault();                           

            //                //early to resend    
            //                if (DateTime.UtcNow.Subtract(ep.Value).TotalSeconds < 3)
            //                    return;
            //                //Removing from Dictionary. and resending again
            //                qSentEndPoints.Remove(ep.Key);

            //            }
                    
            //            _sync_qu.EnterReadLock();
            //            try
            //            {
            //                qu.TryGetValue(ep.Key, out dq);
            //            }
            //            finally
            //            {
            //                _sync_qu.ExitReadLock();
            //            }

            //            //Sending data from that queue (stimulating to send it right now)
            //            if (dq != null)
            //                SendData(dq,false);
            //        }

            //        return;            
            //}, null, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="endPoint"></param>
        /// <param name="arrivedDataBlockId"></param>
        /// <param name="chunkId"></param>
        public void ChunkReceived(IPEndPoint endPoint, ulong arrivedDataBlockId, uint chunkId)
        {
            UdpSocket.LogTo(String.Format("ChunkReceived> DB {0} Chunk {1}",arrivedDataBlockId,chunkId));
                

            string ep = endPoint.ToString();
            UdpDataQueue dq = null;
            
            _sync_qu.EnterReadLock();
            try
            {
                if (qu.TryGetValue(ep, out dq))
                {
                    dq.ChunkReceived(arrivedDataBlockId, chunkId);
                }
            }
            finally
            {
                _sync_qu.ExitReadLock();
            }

            lock (lock_SentEndPoint)
            {
                //Removing from qSentEndPoints
                qSentEndPoints.Remove(ep);
            }

            //After we have received a chunk we want to send new one           
            if (dq != null)
                this.SendData(dq,true);     //Ignoring timer check
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="endPoint"></param>
        /// <param name="arrivedDataBlockId"></param>
        public void StartDataBlockFromFirstChunk(IPEndPoint endPoint)
        {
            string ep = endPoint.ToString();
            UdpDataQueue dq = null;

            _sync_qu.EnterReadLock();
            try
            {
                if (qu.TryGetValue(ep, out dq))
                {
                    dq.StartDataBlockAgain();
                }
            }
            finally
            {
                _sync_qu.ExitReadLock();
            }

            lock (lock_SentEndPoint)
            {
                //Removing from qSentEndPoints
                qSentEndPoints.Remove(ep);
            }

            //After we have received a chunk we want to send new one           
            if (dq != null)
                this.SendData(dq, false); 
        }

        public enum eSendType
        {
             /// <summary>
            /// Standard data for the other host
            /// </summary>
            ProtocolData = 0,
            /// <summary>
            /// Answer of the receiver
            /// </summary>
            ProtocolAnswer_ChunkHasArrived = 1,
            /// <summary>
            /// Answer for the sender to try to start from the first chunk of the current datablock
            /// </summary>
            ProtocolAnswer_StartDataBlockAgain = 2,
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="endPoint"></param>
        public void Send(eSendType sendType, byte[] data, IPEndPoint endPoint)
        {
            if (data == null || data.Length < 1)
                return;

            UdpSocket.LogTo(String.Format("Sending to {0} type {1} of {2} bytes", endPoint.ToString(), sendType.ToString(), data.Length.ToString()));

            string ep = endPoint.ToString();
            UdpDataQueue dq = null;

            _sync_qu.EnterUpgradeableReadLock();
            try
            {
                if (!qu.TryGetValue(ep, out dq))
                {
                    _sync_qu.EnterWriteLock();
                    try
                    {
                        if (!qu.TryGetValue(ep, out dq))
                        {
                            dq = new UdpDataQueue(endPoint,this.UdpSocket);
                            qu[ep] = dq;
                        }
                    }
                    finally
                    {
                        _sync_qu.ExitWriteLock();
                    }
                }

                //Filling data queue
                dq.Add(sendType, data);
            }
            finally
            {
                _sync_qu.ExitUpgradeableReadLock();
            }

            //Sending data from that queue (stimulating to send it right now)
            SendData(dq,false);
 
        }

       
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dq"></param>
        void SendData(UdpDataQueue dq, bool dontCheckTimer)
        {
            UdpDataQueue.ChunkToBeSend chunk = null;

            lock (lock_SentEndPoint)
            {
                //By Automatic Sending Timer the value first will be removed from qSentEndPoints
                if (qSentEndPoints.ContainsKey(dq.EndPoint.ToString()))
                {
                    UdpSocket.LogTo(String.Format("SendData ret 2"));
                    return;
                }

                chunk = dq.GetNewChunkToBeSent();

                if (chunk.ChunkContent == null)
                {
                    UdpSocket.LogTo(String.Format("SendData ret 3 (no chunk to be send)"));
                    return;
                }

                //Setting sent time
                qSentEndPoints[dq.EndPoint.ToString()] = dq.LastSentDateTime;
            }

         

            TechSend(dq, chunk);
           
          
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dq"></param>
        /// <param name="chunk"></param>
        void TechSend(UdpDataQueue dq, UdpDataQueue.ChunkToBeSend chunk)
        {
            try
            {
                if (chunk.SendTryNumber > 10)
                    return;

                if (dq.ss == null)
                {
                    dq.ss = new SocketAsyncEventArgs();
                    dq.ss.RemoteEndPoint = dq.EndPoint;
                    dq.ss.Completed += dq.ss_Completed;
                }

                try
                {
                    dq.ss.SetBuffer(chunk.ChunkContent, 0, chunk.ChunkContent.Length);
                }
                catch (Exception ex)
                {
                    //Time to reassign SocketAsyncEventArgs
                    UdpSocket.LogTo(String.Format("TechSend SendToAsync failed 2"));

                    dq.ss.Dispose();
                    dq.ss = null;
                    dq.SocketInProgress = false;

                    chunk.SendTryNumber += 1;

                    TechSend(dq, chunk);

                    return;
                }
                

                //Putting into

                lock (lock_SentEndPoint)
                {
                    dq.SocketInProgress = true;
                    if (!UdpSocket.m_ListenSocket.SendToAsync(dq.ss))
                    {
                        //Time to reassign SocketAsyncEventArgs
                        UdpSocket.LogTo(String.Format("TechSend SendToAsync failed"));

                        dq.ss.Dispose();
                        dq.ss = null;
                        dq.SocketInProgress = false;

                        chunk.SendTryNumber += 1;

                        TechSend(dq, chunk);

                        return;
                    }
                    else
                    {
                        UdpSocket.LogTo(String.Format("TechSend sent {0} bytes to {1}", chunk.ChunkContent.Length, dq.EndPoint.ToString()));

                        //Sent was done succesfully                         
                        dq.LastSentDateTime = DateTime.UtcNow;
                        //dq.SendTryNumber = 0;

                        //Adding to sent only if it's a Data Chunk
                        if (chunk.TypeOfBlock == eSendType.ProtocolData)
                        {
                            //Resetting sent time
                            qSentEndPoints[dq.EndPoint.ToString()] = dq.LastSentDateTime;
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                UdpSocket.LogTo(String.Format("TechSend ret {0}", ex.ToString()));
            }
        }

                
        public void Dispose()
        {
            //throw new NotImplementedException();
            

            try
            {
                _sync_qu.EnterWriteLock();
                try
                {
                    foreach (var q in qu.Values)
                    {
                        q.Dispose();
                    }
                }
                finally
                {
                    _sync_qu.ExitWriteLock();
                }
            }
            catch
            { }
        }
    }
}
