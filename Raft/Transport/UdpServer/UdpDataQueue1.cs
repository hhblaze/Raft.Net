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
    internal class UdpDataQueue:IDisposable
    {
        //https://ru.wikipedia.org/wiki/UDP

        ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();
        /// <summary>
        /// ChunkSize to avoid fragmentation due to UDP documentation
        /// </summary>
        const int ChunkSize = 1412; // 1432 - 20;   

        /// <summary>
        /// Every Add monotonically increases this id
        /// </summary>
        ulong DataBlockId = 0;
        /// <summary>
        /// Key: dataChunkId (data chunk is a portion od data to be send)
        /// Value: 
        ///     Key: blockId
        ///     Value: splitted by chunks
        /// </summary>      
        SortedDictionary<ulong, Tuple<UdpDataSender.eSendType, Dictionary<uint, byte[]>>> cache = new SortedDictionary<ulong, Tuple<UdpDataSender.eSendType, Dictionary<uint, byte[]>>>();

        //object lock_numbers = new object();

        //long logicalSequnceNumber = 1;
        //int inDataBlockSequenceNumber = 1;

        public IPEndPoint EndPoint = null;

        //public object lock_LastSentDateTime = new object();
        public DateTime LastSentDateTime = DateTime.UtcNow.AddHours(-1);

        ///// <summary>
        ///// Tries to send one chunk until Transport is not OK
        ///// </summary>
        //public int SendTryNumber = 0;

        /// <summary>
        /// All operations via this socket
        /// </summary>
        UdpSocketListener UdpSocket = null;

        /// <summary>
        /// 
        /// </summary>
        public SocketAsyncEventArgs ss = null;
        public bool SocketInProgress = false;

        public UdpDataQueue(IPEndPoint endPoint,UdpSocketListener udpSocket)
        {
            EndPoint = endPoint;
            UdpSocket = udpSocket;
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
         * 1432 - 14 = 1418
         
         */

        
        public void Add(UdpDataSender.eSendType sendType, byte[] data)
        {
            _sync.EnterWriteLock();
            try
            {
                DataBlockId++;

                this.UdpSocket.LogTo(String.Format("UdpDataQueue.Add {0} DB:{2} -> {1} bytes", sendType.ToString(), data.Length.ToString(),DataBlockId.ToString()));

                //Splitting dataBlock on chunk size
                Dictionary<uint, byte[]> blocks = new Dictionary<uint, byte[]>();
                int i = 0;
                uint j = 1;
                byte[] chunk = null;

                while (true)
                {

                    chunk = data.Substring(i, ChunkSize);
                    if (chunk == null || chunk.Length < 1)
                        break;

                    
                    byte[] prot = ((ushort)sendType).To_2_bytes_array_BigEndian();

                    //switch (sendType)
                    //{
                    //    case  UdpDataSender.eSendType.ProtocolData:
                    //        prot = ((ushort)UdpDataSender.eSendType.ProtocolData).To_2_bytes_array_BigEndian();
                    //        break;
                    //    case UdpDataSender.eSendType.ProtocolAnswer_ChunkHasArrived:
                    //        prot = ((ushort)UdpDataSender.eSendType.ProtocolAnswer_ChunkHasArrived).To_2_bytes_array_BigEndian();
                    //        break;
                    //    case UdpDataSender.eSendType.ProtocolAnswer_StartDataBlockAgain:
                    //        prot = ((ushort)UdpDataSender.eSendType.ProtocolAnswer_StartDataBlockAgain).To_2_bytes_array_BigEndian();
                    //        break;
                    //}

                    blocks.Add(j,
                        prot
                        .ConcatMany(
                            DataBlockId.To_8_bytes_array_BigEndian(),
                            data.Length.To_4_bytes_array_BigEndian(),
                            j.To_4_bytes_array_BigEndian(),
                            ((ushort)chunk.Length).To_2_bytes_array_BigEndian(),
                            chunk)
                            );

                    i += ChunkSize;
                    j++;
                }
                
                //Storing ready blocks in cache
                //Console.WriteLine(this.EndPoint.ToString() + " stored " + DataBlockId);
                //this.UdpSocket.LogTo(String.Format("UdpDataQueue.Add {0} DB:{2} -> {1} bytes", sendType.ToString(), data.Length.ToString(), DataBlockId.ToString()));
                cache.Add(DataBlockId, new Tuple<UdpDataSender.eSendType, Dictionary<uint, byte[]>>(sendType, blocks));
            }
            finally
            {
                _sync.ExitWriteLock();
            }

        }

        /// <summary>
        /// 
        /// </summary>
        public void StartDataBlockAgain()
        {
            _sync.EnterWriteLock();
            try
            {
                chunkId = 1;
            }
            finally
            {
                _sync.ExitWriteLock();
            }

        }

        public void ChunkReceived(ulong _arrivedDataBlockId, uint _chunkId)
        {
            _sync.EnterWriteLock();
            try
            {
                if (_arrivedDataBlockId != arrivedDataBlockId && _chunkId != chunkId)
                {
                    //This is a diconnected from the system sequence answer, we must ignore it
                    return;
                }

                Tuple<UdpDataSender.eSendType, Dictionary<uint, byte[]>> blocks = null;
                //Dictionary<uint, byte[]> blocks=null;

                if (!cache.TryGetValue(_arrivedDataBlockId, out blocks))
                {
                    //No such datablock             
                    return;
                }

                //we setup new chunks for sending
                if (_chunkId == blocks.Item2.Count())
                {
                    //The last chunk of the block was sent, moving to next block

                    //Cleaning cache
                    cache.Remove(arrivedDataBlockId);

                    arrivedDataBlockId++;
                    chunkId = 1;
                    
                    
                }
                else
                {
                    //moving to next chunk
                    chunkId++;
                }
            }
            finally
            {
                _sync.ExitWriteLock();
            }

        }

        public class ChunkToBeSend
        {
            public byte[] ChunkContent = null;
            public UdpDataSender.eSendType TypeOfBlock = UdpDataSender.eSendType.ProtocolData;

            public int SendTryNumber = 0;
        }

        ulong arrivedDataBlockId =1;
        uint chunkId = 1;

        /// <summary>
        /// Initially start from 1,1,
        /// can return null if nothing to send
        /// </summary>
        /// <param name="arrivedDataBlockId">last arrived approvement DataBlock and its chunk</param>
        /// <param name="chunkId">last arrived approvement for DataBlock and its chunk</param>
        /// <returns></returns>
        public ChunkToBeSend GetNewChunkToBeSent()
        {

            _sync.EnterUpgradeableReadLock();
            try
            {
                Tuple<UdpDataSender.eSendType, Dictionary<uint, byte[]>> blocks = null;
                //Dictionary<uint, byte[]> blocks=null;
                //byte[] chunk = null;

                ChunkToBeSend chunk = new ChunkToBeSend();

                if (!cache.TryGetValue(arrivedDataBlockId, out blocks))
                {
                    this.UdpSocket.LogTo(String.Format("UdpDataQueue.GetNewChunkToBeSent ret 1"));
                    return chunk;

                    ////!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  
                    ////Then we start from 1, 1
                    //arrivedDataBlockId = 1;
                    //chunkId = 1;

                    //if (!cache.TryGetValue(arrivedDataBlockId, out blocks))
                    //{
                    //    this.UdpSocket.LogTo(String.Format("UdpDataQueue.GetNewChunkToBeSent ret 1"));
                    //    return null;
                    //}

                    //if (!blocks.Item2.TryGetValue(chunkId, out chunk))
                    //{
                    //    this.UdpSocket.LogTo(String.Format("UdpDataQueue.GetNewChunkToBeSent ret 2"));
                    //    return null;
                    //}

                    //return chunk;
                }
                else
                {
                    chunk.TypeOfBlock = blocks.Item1;

                    if (!blocks.Item2.TryGetValue(chunkId, out chunk.ChunkContent))
                    {
                        this.UdpSocket.LogTo(String.Format("UdpDataQueue.GetNewChunkToBeSent ret 3"));
                        return null;
                    }

                    if (
                        blocks.Item1 != UdpDataSender.eSendType.ProtocolData
                        &&
                        blocks.Item2.Count() == chunkId
                        )
                    {
                        //We must clean current DataBlock Id, because it's a operational/controlling DataBlock and we don't wait for the answer on it 

                        _sync.EnterWriteLock();
                        try
                        {
                            this.UdpSocket.LogTo(String.Format("UdpDataQueue.GetNewChunkToBeSent Removing OP DataBlock {0}", arrivedDataBlockId));

                            cache.Remove(arrivedDataBlockId);

                            arrivedDataBlockId++;
                            chunkId = 1;
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

        public void ss_Completed(object sender, SocketAsyncEventArgs e)
        {
            SocketInProgress = false;
        }
        
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
                if (ss != null)
                {
                    ss.Completed -= ss_Completed;
                    ss.Dispose();
                    ss = null;
                }

            }
            catch
            { }
        }
    }
}
