using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DBreeze.Utils;
using System.Net;
using System.Net.Sockets;

namespace Raft.Transport.UdpServer
{
    /// <summary>
    /// Represents a sent queue per IPEndPoint
    /// </summary>
    internal class UdpEndPoint:IDisposable
    {
        //https://ru.wikipedia.org/wiki/UDP

        /// <summary>
        /// Serves cache
        /// </summary>
        ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();

        /// <summary>
        /// Every Add monotonically increases this id
        /// </summary>
        ulong DataBlockId = 0;
        /// <summary>
        /// Key: dataChunkId (data chunk is a portion od data to be send)
        /// Value: 
        ///     Key: DataBlockId
        ///     Value: SplittedDataBlock
        /// </summary>      
        SortedDictionary<ulong, SplittedDataBlock> cache = new SortedDictionary<ulong, SplittedDataBlock>();
        /// <summary>
        /// 
        /// </summary>
        public IPEndPoint EndPoint = null;
        /// <summary>
        /// 
        /// </summary>
        public DateTime LastSentDateTime = DateTime.UtcNow.AddHours(-1);
        /// <summary>
        /// All operations via this socket
        /// </summary>
        UdpSocketListener UdpSocket = null;

        /// <summary>
        /// 
        /// </summary>
        public SocketAsyncEventArgs SocketAsync = null;

        public UdpEndPoint(IPEndPoint endPoint, UdpSocketListener udpSocket)
        {
            EndPoint = endPoint;
            UdpSocket = udpSocket;
        }

        /// <summary>
        /// 
        /// </summary>
        class SplittedDataBlock
        {
            public eUdpPackageType UdpPackageType = eUdpPackageType.ProtocolData;
            /// <summary>
            /// ChunkId
            /// </summary>
            public Dictionary<uint, byte[]> Chunks = new Dictionary<uint, byte[]>();
        }

        /*
         * https://ru.wikipedia.org/wiki/UDP
         * 1432 byte
         * 
            DataBlock - one logical and finished data block which must be resent to the Recepient
         * DataBlockNumber
         * DataBlockChunk - 1-N, the receiver will be able to ignore complete DataBlock if it's sent not from the beginning
         * 2 bytes chunk size
         * 
         * Dbn(8bytes) + Cn(4bytes) + Cs(2bytes) = 14
         * 1432 - 20 = 1412
         
         */

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sendType"></param>
        /// <param name="data"></param>
        public void Add(eUdpPackageType sendType, byte[] data)
        {
            if (sendType != eUdpPackageType.ProtocolData)
            {
                AddTechnicalData(sendType, data);
                return;
            }

            _sync.EnterWriteLock();
            try
            {
                DataBlockId++;

                this.UdpSocket.LogTo(String.Format("UdpEndPoint.Add {0} DB:{2} -> {1} bytes", sendType.ToString(), data.Length.ToString(),DataBlockId.ToString()));

                //Splitting dataBlock on chunk size
                Dictionary<uint, byte[]> blocks = new Dictionary<uint, byte[]>();
                int i = 0;
                uint j = 1;
                byte[] chunk = null;

                while (true)
                {

                    chunk = data.Substring(i, this.UdpSocket.UdpChunkSize);
                   
                    if (chunk == null || chunk.Length < 1)
                        break;

                    
                    byte[] prot = ((ushort)sendType).To_2_bytes_array_BigEndian();

                    blocks.Add(j,
                        prot
                        .ConcatMany(
                            DataBlockId.To_8_bytes_array_BigEndian(),
                            data.Length.To_4_bytes_array_BigEndian(),
                            j.To_4_bytes_array_BigEndian(),
                            ((ushort)chunk.Length).To_2_bytes_array_BigEndian(),
                            chunk)
                            );

                    i += this.UdpSocket.UdpChunkSize;
                    j++;
                }
                
                //Storing ready blocks in cache                
                cache.Add(DataBlockId, new SplittedDataBlock() { UdpPackageType =sendType, Chunks = blocks });
            }
            finally
            {
                _sync.ExitWriteLock();
            }

        }

        object lock_TechnicalChunks = new object();
        Queue<byte[]> TechnicalChunks = new Queue<byte[]>();

        /// <summary>
        /// chunk.UdpPackageType != eUdpPackageType.ProtocolData
        /// </summary>
        /// <param name="sendType"></param>
        /// <param name="data"></param>
        void AddTechnicalData(eUdpPackageType sendType, byte[] data)
        {
            if (data.Length > this.UdpSocket.UdpChunkSize)
                throw new Exception("AddTechnicalData: technical dataBlock can't be bigger then UdpChunkSize");
                       
            try
            {
              
                this.UdpSocket.LogTo(String.Format("UdpEndPoint.AddTechnicalData {0} DB:{2} -> {1} bytes", sendType.ToString(), data.Length.ToString(), DataBlockId.ToString()));

                //Splitting dataBlock on chunk size
                uint j = 1;

                byte[] prot = ((ushort)sendType).To_2_bytes_array_BigEndian();

                byte[] chunk =
                    prot
                    .ConcatMany(
                        DataBlockId.To_8_bytes_array_BigEndian(),
                        data.Length.To_4_bytes_array_BigEndian(),
                        j.To_4_bytes_array_BigEndian(),
                        ((ushort)data.Length).To_2_bytes_array_BigEndian(),
                        data);

                //Storing ready blocks in cache   
                lock (lock_TechnicalChunks)
                {
                    TechnicalChunks.Enqueue(chunk);
                }
            
            }           
            finally
            {
              
            }

        }

        int chunksInDataBlock = 0;
        ulong DataBlockIdToBeSent = 1;
        uint ChunkIdToBeSent = 1;
        object lock_sendState = new object();
        bool ElementIsSent = false;

        /// <summary>
        /// Initiates send procedure of the next data block
        /// </summary>
        public void Send()
        {
            lock (lock_sendState)
            {
                if (ElementIsSent)
                    return;

                ElementIsSent = true;
            }

            ChunkToBeSend chunk = GetChunkToBeSent();

            if (chunk == null)
            {                
                lock (lock_sendState)
                {                    
                    ElementIsSent = false;
                }

                //Here we can remove from check timer list
                this.UdpSocket.UdpSender.RemoveFrom_PendingSend_EndPoint(this.EndPoint.ToString());

                return;
            }

            TechnicalSend(chunk);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="chunk"></param>
        void TechnicalSend(ChunkToBeSend chunk)
        {
            if (chunk.SendTryNumber > 10)
            {
                lock (lock_sendState)
                    ElementIsSent = false;
                return;
            }

            //In any case we setup last send date
            this.LastSentDateTime = DateTime.UtcNow;

            if (chunk.UdpPackageType == eUdpPackageType.ProtocolData)
            {
                //Here we can add to check timer list
                this.UdpSocket.UdpSender.AddTo_PendingSend_EndPoint(this.EndPoint.ToString(), this.LastSentDateTime);
            }

            try
            {
                //socketAsync


                if (SocketAsync == null)
                {
                    SocketAsync = new SocketAsyncEventArgs();
                    SocketAsync.RemoteEndPoint = this.EndPoint;
                    SocketAsync.Completed += SocketAsync_Completed;
                }

                try
                {
                    SocketAsync.SetBuffer(chunk.ChunkContent, 0, chunk.ChunkContent.Length);
                }
                catch (Exception ex)
                {
                    //Time to reassign SocketAsyncEventArgs
                    UdpSocket.LogTo(String.Format("TechnicalSend failed 1"));

                    SocketAsync.Completed -= SocketAsync_Completed;
                    SocketAsync.Dispose();
                    SocketAsync = null;

                    chunk.SendTryNumber += 1;

                    TechnicalSend(chunk);

                    return;
                }
                               
                

                if (!UdpSocket.m_ListenSocket.SendToAsync(SocketAsync))
                {
                    //Time to reassign SocketAsyncEventArgs
                    UdpSocket.LogTo(String.Format("TechnicalSend failed 2"));

                    SocketAsync.Completed -= SocketAsync_Completed;
                    SocketAsync.Dispose();
                    SocketAsync = null;

                    chunk.SendTryNumber += 1;

                    TechnicalSend(chunk);

                    return;
                }
                else
                {
                    UdpSocket.LogTo1(String.Format("TechnicalSending {0}:{1}", chunk.DataBlockId, chunk.ChunkId));
                }

                if (chunk.UdpPackageType != eUdpPackageType.ProtocolData)
                {
                    //We are not waiting for the answer in case of ControlType data, that's why we unlock sending
                    lock (lock_sendState)
                        ElementIsSent = false;

                    this.Send();
                }
            }
            catch (Exception ex)
            {
                                
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public void SocketAsync_Completed(object sender, SocketAsyncEventArgs e)
        {
            
        }

        /// <summary>
        /// External timer helping to handle not sent chunk etc...
        /// </summary>
        public void ResendTimerElapsed()
        {
            lock (lock_sendState)
            {
                if (DateTime.UtcNow.Subtract(this.LastSentDateTime).TotalSeconds > 3)
                {
                    ElementIsSent = false;
                }
                else
                    return;
            }

            this.Send();
        }


        public class ChunkToBeSend
        {
            public byte[] ChunkContent = null;
            public eUdpPackageType UdpPackageType = eUdpPackageType.ProtocolData;
            public int SendTryNumber = 0;
            public ulong DataBlockId = 1;
            public uint ChunkId = 1;
        }


        /// <summary>
        /// Initially start from 1,1,
        /// can return null if nothing to send
        /// </summary>
        /// <param name="arrivedDataBlockId">last arrived approvement DataBlock and its chunk</param>
        /// <param name="chunkId">last arrived approvement for DataBlock and its chunk</param>
        /// <returns></returns>
        ChunkToBeSend GetChunkToBeSent()
        {
            ChunkToBeSend chunk = new ChunkToBeSend();

            lock (lock_TechnicalChunks)
            {
                if (TechnicalChunks.Count() > 0)
                {
                    chunk.ChunkContent = TechnicalChunks.Dequeue();
                    chunk.DataBlockId = 1;
                    chunk.ChunkId = 1;
                    //!!!!!!!!!!1   MAY BE REVISIT
                    //It doesn't matter, the main rule is that this chunk should not be DataChunk
                    chunk.UdpPackageType = eUdpPackageType.ProtocolAnswer_ChunkHasArrived;
                    return chunk;
                }

            }


            _sync.EnterUpgradeableReadLock();
            try
            {
                SplittedDataBlock dataBlock = null;   

                if (!cache.TryGetValue(DataBlockIdToBeSent, out dataBlock))
                {
                    this.UdpSocket.LogTo(String.Format("UdpEndPoint.GetChunkToBeSent ret 1"));
                    return null;
                }
                else
                {
                    chunk.UdpPackageType = dataBlock.UdpPackageType;
                    chunksInDataBlock = dataBlock.Chunks.Count;
                    chunk.DataBlockId = DataBlockIdToBeSent;
                    chunk.ChunkId = ChunkIdToBeSent;

                    if (!dataBlock.Chunks.TryGetValue(ChunkIdToBeSent, out chunk.ChunkContent))
                    {                        
                        this.UdpSocket.LogTo(String.Format("UdpEndPoint.GetChunkToBeSent ret 2*****should not happen"));
                        return null;
                    }
                                                           

                    if (chunk.UdpPackageType != eUdpPackageType.ProtocolData)
                    {
                        //!!!!!!!!!! After last changes, here comes only ProtocolData (non technical) chunks 
                        //We must remove this dataBlock from the cache
                        _sync.EnterWriteLock();
                        try
                        {
                            cache.Remove(DataBlockIdToBeSent);
                            DataBlockIdToBeSent++;
                            ChunkIdToBeSent = 1;
                        }
                        finally
                        {
                            _sync.ExitWriteLock();
                        }                        
                    }

                    //Returning operational chunk
                    return chunk;
                }


            }
            finally
            {
                _sync.ExitUpgradeableReadLock();
            }


        }




        public void ChunkReceived(ulong arrivedDataBlockId, uint chunkId)
        {           
            _sync.EnterWriteLock();
            try
            {
                if (arrivedDataBlockId != DataBlockIdToBeSent || chunkId != ChunkIdToBeSent)
                {
                    //This is a diconnected from the system sequence answer, we must ignore it
                    return;
                }

                if (chunksInDataBlock == ChunkIdToBeSent)
                {
                     //Removing from cache dataBlock as sent one
                    cache.Remove(DataBlockIdToBeSent);

                    //Moving to next datablock
                    DataBlockIdToBeSent++;
                    ChunkIdToBeSent = 1;                   
                }
                else
                {
                    ChunkIdToBeSent++;
                }
            }
            finally
            {
                _sync.ExitWriteLock();
            }

            //!!!!!!!!!!!  
            //Only now we unlock ElementIsSent. It can happen that we don't receive from the receiver approvement, then Timer must take this lock away

            lock (lock_sendState)
                ElementIsSent = false;

            //Recursive call
            Send();
        }


        public void StartDataBlockFromChunkNumber(ulong arrivedDataBlockId, uint chunkId)
        {
            ChunkIdToBeSent = chunkId;
            //!!!!!!!!!!!  
            //Only now we unlock ElementIsSent. It can happen that we don't receive from the receiver approvement, then Timer must take this lock away

            lock (lock_sendState)
                ElementIsSent = false;

            //Recursive call
            Send();
        }










        ////ulong arrivedDataBlockId = 1;
        ////uint chunkId = 1;

        ///// <summary>
        ///// 
        ///// </summary>
        //public void StartDataBlockAgain()
        //{
        //    _sync.EnterWriteLock();
        //    try
        //    {
        //        chunkId = 1;
        //    }
        //    finally
        //    {
        //        _sync.ExitWriteLock();
        //    }

        //}



        

       

       

      
        
        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            _sync.EnterWriteLock();
            try
            {
                cache.Clear();
            }
            finally
            {
                _sync.ExitWriteLock();
            }

            try
            {
                if (SocketAsync != null)
                {
                    SocketAsync.Completed -= SocketAsync_Completed;
                    SocketAsync.Dispose();
                    SocketAsync = null;
                }

            }
            catch
            { }
        }
    }
}
