using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DBreeze.Utils;

namespace Raft.Transport.UdpServer
{
    /// <summary>
    /// http://msdn.microsoft.com/en-us/library/tst0kwb1(v=vs.110).aspx
    /// </summary>
    internal class UdpDataReceiver:IDisposable
    {

        object lock_protocol = new object();
        /// <summary>
        /// 
        /// </summary>
        Dictionary<string, byte[]> epData = new Dictionary<string, byte[]>();
        Dictionary<string, DataFragment> dFragments = new Dictionary<string, DataFragment>(); 
        /// <summary>
        /// All operations via this socket
        /// </summary>
        UdpSocketListener UdpSocket = null;

        public UdpDataReceiver(UdpSocketListener udpSocket)
        {
            UdpSocket = udpSocket;
        }

        public void Dispose()
        {
            try
            {

            }
            catch
            {}
        }

        class DataFragment
        {
            /// <summary>
            /// Corresponds to logical data block (one send by a client).
            /// Accumulator for the logical packages
            /// </summary>
            public byte[] notCompletedChunk = null;
            /// <summary>
            /// Current data block id
            /// </summary>
            public ulong DataBlockId = 1;
            /// <summary>
            /// Current DataBlock chunkId
            /// </summary>
            public uint ChunkId = 1;

            /// <summary>
            /// Complete datablock is stored here.
            /// Will be cleaned up after data collection is finished
            /// </summary>
            public byte[] CompleteDataBlock = null;
        }

        public void Received(IPEndPoint endPoint, byte[] data)
        {
            try
            {
                //Unpacking

                DataFragment df = null;
                bool recursiveCall = false;

                lock (lock_protocol)
                {                    
                    string ep= endPoint.ToString();

                    if (!dFragments.TryGetValue(ep, out df))
                    {
                        df = new DataFragment();
                        dFragments.Add(ep,df);
                    }

                    //Working with data fragment

                    //Our minimal chunk protocol is
                    //2 + 8 + 4 + 4 + 2 = 20
                    //Protocol(ushort), DataBlockId (ulong), DataBlockLength (int), DataBlockChunkId (uint), ChunkLength (ushort - normally of size <= UdpDataQueue.ChunkSize what is 1412 bytes)

                    if (df.notCompletedChunk != null)
                    {
                        data = df.notCompletedChunk.Concat(data);
                    }

                    if (data.Length < 20)
                    {
                        //We go on to collect chunk
                        df.notCompletedChunk = df.notCompletedChunk.Concat(data);
                        return;
                    }                    
                    
                    //Parsing received chunk
                    ushort chunkLen = data.Substring(18, 2).To_UInt16_BigEndian();

                    if (data.Length < (20 + chunkLen))
                    {
                        //We go on to collect chunk
                        df.notCompletedChunk = df.notCompletedChunk.Concat(data);
                        return;
                    }
                    else if(data.Length > (20 + chunkLen))
                    {
                        //Probably we got here a part from the next chunk
                        df.notCompletedChunk = data.Substring(20 + chunkLen);
                        data = data.Substring(0, 20 + chunkLen);
                        recursiveCall = true;
                    }
                    else
                        df.notCompletedChunk = null;

                    ushort prot = data.Substring(0, 2).To_UInt16_BigEndian();
                    ulong dataBlockId = data.Substring(2, 8).To_UInt64_BigEndian();
                    int dataBlockLen = data.Substring(10, 4).To_Int32_BigEndian();
                    uint chunkId = data.Substring(14, 4).To_UInt32_BigEndian();
                   
                                   
                    //We have received completed chunk

                    //Chunk analyzer. Receiver can stop and start withing data transmission
                    //Becaus dataBlock is stored in memory, then in case of process termination all received chunks
                    //of not finished DataBlock will be lost. Receiver must inform Sender to start from the beginning of the DataBlock

                    if ((UdpDataSender.eSendType)prot != UdpDataSender.eSendType.ProtocolData)
                    {
                        //Technical (non business data) protocols

                        switch ((UdpDataSender.eSendType)prot)
                        {
                            case UdpDataSender.eSendType.ProtocolAnswer_ChunkHasArrived:
                                //Marking chunk as arrived
                                UdpSocket.UdpSender.ChunkReceived(endPoint, data.Substring(20, 8).To_UInt64_BigEndian(), data.Substring(28, 4).To_UInt32_BigEndian());                               
                                break;
                            case UdpDataSender.eSendType.ProtocolAnswer_StartDataBlockAgain:
                                //Starting to send DataBlock from the first chunk again to the receiver
                                UdpSocket.UdpSender.StartDataBlockFromFirstChunk(endPoint);                                                              
                                break;
                        }

                        return;
                    }
                    

                    if (df.CompleteDataBlock != null)
                    {
                        //We are inside of data collecting process
                        if (dataBlockId == df.DataBlockId && chunkId == df.ChunkId)
                        {
                            //the accepted byte is repeated ? may be because of the absense of the answer due to UDP problem
                            UdpSocket.UdpSender.Send(UdpDataSender.eSendType.ProtocolAnswer_ChunkHasArrived, data.Substring(2, 8).Concat(data.Substring(14, 4)), endPoint);
                            return;
                        }

                        if (dataBlockId != df.DataBlockId || chunkId != (df.ChunkId + 1))
                        {

                            // Error, we must start from the supplied datablock again
                            df.notCompletedChunk = null;
                            df.CompleteDataBlock = null;
                            df.DataBlockId = dataBlockId;
                            df.ChunkId = 1;

                            //  Notifying sender to start to send dataBlockId from the first chunk
                            UdpSocket.UdpSender.Send(UdpDataSender.eSendType.ProtocolAnswer_StartDataBlockAgain, data.Substring(2, 8), endPoint);
                            return;
                        }
                    }
                    else
                    {
                        //We set in any case new current DataBlock Id
                        df.DataBlockId = dataBlockId;

                        if (df.ChunkId != 1)
                        {
                            // Error. we must start from the same DataBlock, from the first chunk

                            df.notCompletedChunk = null;
                            df.CompleteDataBlock = null;
                            df.ChunkId = 1;
                           
                            //  Notifying sender to start to send dataBlockId from the first chunk
                            UdpSocket.UdpSender.Send(UdpDataSender.eSendType.ProtocolAnswer_StartDataBlockAgain, data.Substring(2, 8), endPoint);
                            return;
                        }
                    }
                    
                    df.ChunkId = chunkId;

                    //Notifying Sender that chunk has arrived
                    UdpSocket.UdpSender.Send(UdpDataSender.eSendType.ProtocolAnswer_ChunkHasArrived, data.Substring(2, 8).Concat(data.Substring(14, 4)), endPoint);

                    df.CompleteDataBlock = df.CompleteDataBlock.Concat(data.Substring(20, chunkLen));

                    if (df.CompleteDataBlock.Length == dataBlockLen)
                    {
                        //Protocol is collected
                        byte[] ready = new byte[df.CompleteDataBlock.Length];
                        df.CompleteDataBlock.CopyTo(ready, 0);

                        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   SPREAD OUT byte[] ready 

                        //this.UdpSocket.LogTo(String.Format("**************************Received.SPREAD {0} bytes", ready.Length));
                        this.UdpSocket.LogTo1(String.Format("**************************Received.SPREAD {0} bytes", ready.Length));

                        df.CompleteDataBlock = null;
                        df.notCompletedChunk = null;
                        df.ChunkId = 1;
                    }

                }
                                
                if (recursiveCall)
                    this.Received(endPoint, new byte[0]);

            }
            catch (Exception ex)
            {
                UdpSocket.Log.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.Transport.UdpServer.Received" });
            }
        }
    }
}
