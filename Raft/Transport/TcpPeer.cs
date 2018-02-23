using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport
{
    internal class TcpPeer:IDisposable
    {
        TcpClient _client;
        NetworkStream stream = null;
        cSprot1Parser _sprot1 = null;
        TcpRaftNode trn = null;        
        public TcpMsgHandshake Handshake = null;
        public NodeAddress na = null;
        string _endPointSID = "";

        public TcpPeer(TcpClient client, TcpRaftNode rn)
        {
            _client = client;
            trn = rn;
            try
            {
                stream = _client.GetStream();
                SetupSprot();
            }
            catch (Exception ex)
            {
                return;
            }

            trn.rn.TM.FireEventEach(10000, (o) => 
            {

                if (Handshake == null)
                    this.Dispose();

            }, null, true);
                        
            Task.Run(async () => await Read());
        }


        /// <summary>
        /// Combination of remote (outgoing) ip and its local listening port
        /// </summary>
        public string EndPointSID
        {
            get
            {
                if (!String.IsNullOrEmpty(_endPointSID))
                    return _endPointSID;
                if (Handshake == null)
                    return String.Empty;
                var rep = _client.Client.RemoteEndPoint.ToString();
                _endPointSID = rep.Substring(0, rep.IndexOf(':')+1) + Handshake.NodeListeningPort;
                return _endPointSID;
            }
        }

        public TcpPeer(string hostname, int port, TcpRaftNode rn)
        {
            trn = rn;
            Task.Run(async () => await Connect(hostname, port));
        }

        async Task Connect(string hostname, int port)
        {
            _client = new TcpClient();
            try
            {
                await _client.ConnectAsync(hostname, port);
                stream = _client.GetStream();
                SetupSprot();
            }
            catch (Exception ex)
            {
                return;
            }

            await Read();
        }


        void SetupSprot()
        {
            _sprot1 = new cSprot1Parser();
            _sprot1.UseBigEndian = true;
            _sprot1.DestroySelf = this.Dispose;
            _sprot1.packetParser = this.packetParser;
            //_sprot1.MessageQueue = _tcpServerClient.__IncomingDataBuffer;
            _sprot1.MaxPayLoad = 5000000;
            _sprot1.DeviceShouldSendAuthorisationBytesBeforeProceedCodec = false;
            _sprot1.ToSendToParserAuthenticationBytes = false;
        }

        internal void FillNodeAddress()
        {
            na = new NodeAddress() { NodeAddressId = Handshake.NodeListeningPort, NodeUId = Handshake.NodeUID, EndPointSID = this.EndPointSID };
        }
        
        private void packetParser(int codec, byte[] data)
        {
                 
            try
            {
                switch (codec)
                {
                    case 1: //Handshake
                            
                        Handshake = TcpMsgHandshake.BiserDecode(data);
                        if (trn.rn.NodeAddress.NodeUId != this.Handshake.NodeUID)
                        {
                            //trn.log.Log(new WarningLogEntry()
                            //{
                            //    LogType = WarningLogEntry.eLogType.DEBUG,
                            //    Description = $"{trn.port}> handshake from {this.Handshake.NodeListeningPort}"
                            //});
                        }
                        trn.spider.AddPeerToClusterEndPoints(this,true);                        
                        return;
                    case 2: //RaftMessage

                        if (this.na == null)
                            return;
                        
                        var msg = TcpMsgRaft.BiserDecode(data);
                        
                        Task.Run(() =>
                        {
                            trn.rn.IncomingSignalHandler(this.na, msg.RaftSignalType, msg.Data);
                        });
                        return;
                    case 3: //Handshake ACK

                        Handshake = TcpMsgHandshake.BiserDecode(data);
                        //trn.log.Log(new WarningLogEntry()
                        //{
                        //    LogType = WarningLogEntry.eLogType.DEBUG,
                        //    Description = $"{trn.port}> ACK from {this.Handshake.NodeListeningPort}"
                        //});
                        trn.spider.AddPeerToClusterEndPoints(this, false);                        
                        
                        return;
                    case 4: //Free Message protocol

                        var Tcpmsg = TcpMsg.BiserDecode(data);
                        if (na != null)
                        {
                            trn.log.Log(new WarningLogEntry()
                            {
                                LogType = WarningLogEntry.eLogType.DEBUG,
                                Description = $"{trn.port} ({trn.rn.NodeState})> peer {na.NodeAddressId} sent: { Tcpmsg.MsgType }"
                            });
                        }
                        return;
                    case 5: //Ping
                        
                        //if (na != null)
                        //{
                        //    trn.log.Log(new WarningLogEntry()
                        //    {
                        //        LogType = WarningLogEntry.eLogType.DEBUG,
                        //        Description = $"{trn.port} ({trn.rn.NodeState})> peer {na.NodeAddressId} sent ping"
                        //    });
                        //}
                        return;
                }
            }
            catch (Exception ex)
            {
                Dispose();
            }
           

            //MyPacketParser(codec, data);

        }

        ///// <summary>
        ///// cSprot1Parser.GetSprot1Codec(new byte[] { 00, 01 }, data);
        ///// </summary>
        ///// <param name="codec"></param>
        ///// <param name="data"></param>
        ///// <returns></returns>
        //public async Task WriteAsync(byte[] codec, byte[] data)
        //{

        //    try
        //    {
        //        //!!!!!!!!!!!!!!!   here regulate big messages, giving ability to peers to breeze, sending heartbeats first by priority (if sprot allows it)
        //        //var pd = cSprot1Parser.GetSprot1Codec(new byte[] { 00, 01 }, data);
        //        var pd = cSprot1Parser.GetSprot1Codec(codec, data);

        //        await stream.WriteAsync(pd, 0, pd.Length).ConfigureAwait(false);
        //        await stream.FlushAsync().ConfigureAwait(false);
        //    }
        //    catch (Exception ex)
        //    {
        //        Dispose();
        //    }

        //}


        object lock_writer = new object();
        bool inWrite = false;
        Queue<byte[]> writerQueue = new Queue<byte[]>();
        Queue<byte[]> highPriorityQueue = new Queue<byte[]>();

        /// <summary>
        /// !!! Due to high priority sending, don't forget to make extra protocol in case if we want to split SPROT data on several packages
        /// </summary>
        /// <param name="codec"></param>
        /// <param name="data"></param>
        /// <param name="highPriority"></param>
        public void Write(byte[] sprot, bool highPriority = false)
        {
            lock (lock_writer)
            {
                if (highPriority)
                    highPriorityQueue.Enqueue(sprot);
                else
                    writerQueue.Enqueue(sprot);

                if (inWrite)
                    return;

                inWrite = true;
            }

            Task.Run(async () => { await Writer(); });

        }
        //public void Write(byte[] codec, byte[] data, bool highPriority = false)
        //{
        //    lock(lock_writer)
        //    {
        //        if (highPriority)
        //        {
        //            highPriorityQueue.Enqueue(new Tuple<byte[], byte[]>(codec, data));
        //        }
        //        else
        //        {
        //            writerQueue.Enqueue(new Tuple<byte[], byte[]>(codec, data));
        //        }
        //        if (inWrite)
        //            return;
        //        inWrite = true;
        //    }

        //    Task.Run(async () => { await Writer(); });

        //}

        /// <summary>
        /// highPriorityQueue is served first
        /// </summary>
        /// <returns></returns>
        async Task Writer()
        {
            if (this.Disposed)
                return;
            
            byte[] sprot = null;            
            try
            {
                while (true)
                {
                    lock (lock_writer)
                    {
                        if (highPriorityQueue.Count == 0 && writerQueue.Count == 0)
                        {
                            inWrite = false;
                            return;
                        }

                        if(highPriorityQueue.Count>0)
                            sprot = highPriorityQueue.Dequeue();
                        else
                            sprot = writerQueue.Dequeue();
                    }

                    //huge sprot should be splitted, packed into new sprot codec by chunks and supplied here as a standard chunk
                    await stream.WriteAsync(sprot, 0, sprot.Length);//.ConfigureAwait(false);
                    await stream.FlushAsync();//.ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Dispose();
            }
        }
        

        async Task Read()
        {
            try
            {
                //Example of pure tcp
                byte[] rbf = new byte[10000];
                int a = 0;
                //while ((a = await stream.ReadAsync(rbf, 0, rbf.Length).ConfigureAwait(false)) > 0)
                while ((a = await stream.ReadAsync(rbf, 0, rbf.Length)) > 0)
                {
                    _sprot1.MessageQueue.Enqueue(rbf.Substring(0, a));
                    _sprot1.PacketAnalizator(false);
                    //Console.WriteLine(a);
                }

                //trn.log.Log(new WarningLogEntry()
                //{
                //    LogType = WarningLogEntry.eLogType.DEBUG,
                //    Description = $"{trn.port} ({trn.rn.NodeState})> finished Read of {((na == null) ? "unknown" : na.NodeAddressId.ToString() )}"
                //});
            }
            catch (System.Exception ex)
            {
                //Fires when remote client drops connection //Null reference              
                Dispose();
            }

        }
        
        long disposed = 0;

        public bool Disposed
        {
            get { return System.Threading.Interlocked.Read(ref disposed) == 1; }
        }

        /// <summary>
        /// all custom disposals via parametrical Dispose
        /// </summary>
        public void Dispose()
        {
            this.Dispose(false,true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="DontRemoveFromSpider"></param>
        /// <param name="calledFromDispose"></param>
        public void Dispose(bool DontRemoveFromSpider, bool calledFromDispose = false)
        {  
            if (System.Threading.Interlocked.CompareExchange(ref disposed, 1, 0) != 0)
                return;

            string endpoint = null;
            try
            {
                endpoint = this.EndPointSID;
            }
            catch (Exception ex)
            {
                
            }
          
            try
            {
                if (stream != null)
                {                    
                    stream.Dispose();
                    stream = null;
                }
            }
            catch (Exception)
            {}
            try
            {                
                if (_client != null)
                {
                    (_client as IDisposable).Dispose();
                    _client = null;
                }
            }
            catch (Exception ex)
            {

            }

            try
            {
                if (_sprot1 != null)
                {
                    _sprot1.MessageQueue.Clear();
                    _sprot1 = null;
                }
            }
            catch (Exception)
            { }

            //trn.log.Log(new WarningLogEntry()
            //{
            //    LogType = WarningLogEntry.eLogType.DEBUG,
            //    //Description = $"{trn.port}> try connect {el.Host}:{el.Port} - {el.InternalNodeSID}"
            //    Description = $"{trn.port}> disposing {(Handshake == null ? "unknown" : Handshake.NodeListeningPort.ToString())} {(DontRemoveFromSpider ? " SPIDER NO REMOVE" : "") }"
            //});

            if(!DontRemoveFromSpider && endpoint != null)
                trn.spider.RemovePeerFromClusterEndPoints(endpoint);



            //-------------  Last line
            if (!calledFromDispose)
                Dispose();
        }


    }//eoc
}//eon
