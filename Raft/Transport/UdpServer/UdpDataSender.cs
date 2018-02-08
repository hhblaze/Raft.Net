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
    public enum eUdpPackageType
    {
        /// <summary>
        /// Standard data for the other host
        /// </summary>
        ProtocolData = 0,

        /// <summary>
        /// Answer of the receiver
        /// </summary>
        ProtocolAnswer_ChunkHasArrived = 1,

        ///// <summary>
        ///// Answer for the sender to try to start from the first chunk of the current datablock
        ///// </summary>
        //ProtocolAnswer_StartDataBlockAgain = 2,
        ///// <summary>
        ///// Receiver sends information that complete DataBlock has arrived 
        ///// </summary>
        //ProtocolAnswer_DataBlockArrived =3,

        /// <summary>
        /// Receiver asks sender to repeat transmission starting from specified sequence number
        /// </summary>
        ProtocolAnswer_StartFromSequenceNumber = 4,
    }

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
        Dictionary<string, UdpEndPoint> qu = new Dictionary<string, UdpEndPoint>();

        /// <summary>
        /// All operations via this socket
        /// </summary>
        UdpSocketListener UdpSocket = null;

        /// <summary>
        /// 
        /// </summary>
        object lock_PendingSend_EndPoints = new object();
        /// <summary>
        /// Works with lock_SentEndPoint.
        /// Key is IPEndPoint, Value is sent DateTime
        /// Only DataProtocol (not operational protocol data can be registered here)
        /// </summary>
        Dictionary<string, DateTime> q_PendingSend_EndPoints = new Dictionary<string, DateTime>();
        bool PendingSendTimerInProgress = false;
     
        public UdpDataSender(UdpSocketListener udpSocket)
        {
            UdpSocket = udpSocket;
            
            this.UdpSocket.TM.FireEventAfter(10000, PendingSend_TimerElapsed, null, false);
        }


        #region "Lost chunks handler"

        /// <summary>
        /// 
        /// </summary>
        void PendingSend_TimerElapsed(object userToken)
        {
            try
            {

                List<string> eps = new List<string>();
                int i = 0;

                lock (lock_PendingSend_EndPoints)
                {
                    if (PendingSendTimerInProgress)
                        return;

                    PendingSendTimerInProgress = true;

                    foreach (var ep in q_PendingSend_EndPoints)
                    {
                        if (i > 100)
                            break;

                        if (DateTime.UtcNow.Subtract(ep.Value).TotalSeconds > 3)
                        {
                            eps.Add(ep.Key);
                            i++;
                        }                       
                    }

                    if (eps.Count() < 1)
                    {
                        PendingSendTimerInProgress = false;
                        return;
                    }

                    foreach (var ep1 in eps)
                        q_PendingSend_EndPoints.Remove(ep1);
                
                }

                               
                UdpEndPoint dq = null;                               
                 
                foreach(var ep1 in eps)
                {                  

                    _sync_qu.EnterReadLock();
                    try
                    {
                        qu.TryGetValue(ep1, out dq);
                    }
                    finally
                    {
                        _sync_qu.ExitReadLock();
                    }

                    //Sending data from that queue (stimulating to send it right now)
                    if (dq != null)
                        dq.ResendTimerElapsed();
                }

            }
            catch (Exception ex)
            {

            }

            lock (lock_PendingSend_EndPoints)
            {             
                PendingSendTimerInProgress = false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="endPoint"></param>
        /// <param name="utc"></param>
        public void AddTo_PendingSend_EndPoint(string endPoint, DateTime utc)     
        {
            lock (lock_PendingSend_EndPoints)
            {
                q_PendingSend_EndPoints[endPoint] = utc;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="endPoint"></param>
        public void RemoveFrom_PendingSend_EndPoint(string endPoint)     
        {
            lock (lock_PendingSend_EndPoints)
            {
                q_PendingSend_EndPoints.Remove(endPoint);
            }
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="endPoint"></param>
        /// <param name="arrivedDataBlockId"></param>
        /// <param name="chunkId"></param>
        public void ChunkReceived(IPEndPoint endPoint, ulong dataBlockId, uint chunkId)
        {
            UdpSocket.LogTo(String.Format("ChunkReceived> DB {0} Chunk {1}", dataBlockId, chunkId));
            
            string ep = endPoint.ToString();
            UdpEndPoint dq = null;

            _sync_qu.EnterReadLock();
            try
            {
                qu.TryGetValue(ep, out dq);              
            }
            finally
            {
                _sync_qu.ExitReadLock();
            }

            if(dq != null)
                dq.ChunkReceived(dataBlockId, chunkId);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="endPoint"></param>
        /// <param name="arrivedDataBlockId"></param>
        /// <param name="chunkId"></param>
        public void StartDataBlockFromChunkNumber(IPEndPoint endPoint, ulong dataBlockId, uint chunkId)
        {
            UdpSocket.LogTo(String.Format("StartDataBlockFromChunkNumber> DB {0} Chunk {1}", dataBlockId, chunkId));

            string ep = endPoint.ToString();
            UdpEndPoint dq = null;

            _sync_qu.EnterReadLock();
            try
            {
                qu.TryGetValue(ep, out dq);
            }
            finally
            {
                _sync_qu.ExitReadLock();
            }

            if (dq != null)
                dq.StartDataBlockFromChunkNumber(dataBlockId, chunkId);
        }
      

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="endPoint"></param>
        ///// <param name="arrivedDataBlockId"></param>
        //public void StartDataBlockFromFirstChunk(IPEndPoint endPoint)
        //{
        //    string ep = endPoint.ToString();
        //    UdpDataQueue dq = null;

        //    _sync_qu.EnterReadLock();
        //    try
        //    {
        //        if (qu.TryGetValue(ep, out dq))
        //        {
        //            dq.StartDataBlockAgain();
        //        }
        //    }
        //    finally
        //    {
        //        _sync_qu.ExitReadLock();
        //    }

        //    lock (lock_SentEndPoint)
        //    {
        //        //Removing from qSentEndPoints
        //        qSentEndPoints.Remove(ep);
        //    }

        //    //After we have received a chunk we want to send new one           
        //    if (dq != null)
        //        this.SendData(dq, false); 
        //}

       

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="endPoint"></param>
        public void Send(eUdpPackageType udpPackageType, byte[] data, IPEndPoint endPoint)
        {
            if (data == null || data.Length < 1)
                return;

            UdpSocket.LogTo(String.Format("Sending to {0} type {1} of {2} bytes", endPoint.ToString(), udpPackageType.ToString(), data.Length.ToString()));

            string ep = endPoint.ToString();
            UdpEndPoint dq = null;

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
                            dq = new UdpEndPoint(endPoint, this.UdpSocket);
                            qu[ep] = dq;
                        }
                    }
                    finally
                    {
                        _sync_qu.ExitWriteLock();
                    }
                }

                //Filling data queue
                dq.Add(udpPackageType, data);
            }
            finally
            {
                _sync_qu.ExitUpgradeableReadLock();
            }

            //Sending data from that queue (stimulating to send it right now)
            if (dq != null)
                dq.Send();

           
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
