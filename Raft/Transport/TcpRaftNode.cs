using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

using DBreeze;
using DBreeze.Utils;

namespace Raft.Transport
{
    public class TcpRaftNode: IEmulatedNode, IDisposable
    {
        internal RaftNodeSettings rn_settings = new RaftNodeSettings();
        internal IWarningLog log = null;
        internal int port = 0;
        internal RaftNode rn = null;
        internal TcpSpider spider = null;
        internal List<TcpClusterEndPoint> clusterEndPoints = new List<TcpClusterEndPoint>();  //init clusterEndPoints creating 1-N connection
        
        
        public TcpRaftNode(List<TcpClusterEndPoint> clusterEndPoints, string dbreezePath, int port = 4250, Action<byte[]> OnCommit = null, IWarningLog log = null,RaftNodeSettings rn_settings = null)
        {
            this.rn_settings = rn_settings ?? new RaftNodeSettings();

            this.log = log;
            this.port = port;
            if (clusterEndPoints != null)
            {
                var bt = clusterEndPoints.SerializeBiser();
                var decoder = new Biser.Decoder(bt);
                this.clusterEndPoints = new List<TcpClusterEndPoint>();
                decoder.GetCollection(() => { return TcpClusterEndPoint.BiserDecode(extDecoder: decoder); }, this.clusterEndPoints, false);
               
                //this.clusterEndPoints.AddRange(clusterEndPoints.SerializeProtobuf().DeserializeProtobuf<List<TcpClusterEndPoint>>());
            }
            spider = new TcpSpider(this);          

            rn = new RaftNode(this.rn_settings, dbreezePath, this.spider, this.log, OnCommit);
            
#if DEBUG
            rn.Verbose = rn_settings.VerboseRaft;          //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   DEBUG PURPOSES
#endif
            rn.SetNodesQuantityInTheCluster((uint)this.clusterEndPoints.Count);             //!!!!!!!!!!!!  ENABLE 1 for debug, make it dynamic (but not less then 3 if not DEBUG)
            rn.NodeAddress.NodeAddressId = port; //for debug/emulation purposes

            rn.NodeAddress.NodeUId = Guid.NewGuid().ToByteArray().Substring(8, 8).To_Int64_BigEndian();

            
            rn.NodeStart();
            
        }

        public void EmulationStart()
        {          
        }

        public void EmulationStop()
        {
         
        }

        public void Start()
        {
            Task.Run(async () => {
                StartTcpListener();
                await spider.Handshake(); 
            });
            
        }

       

        TcpListener server = null;
        async Task StartTcpListener()
        {
            try
            {
                if(server == null)
                    server = new TcpListener(IPAddress.Any, this.port); //to telnet dev.tiesky.com 27751

                server.Start();
                
                log.Log(new WarningLogEntry() { LogType = WarningLogEntry.eLogType.DEBUG,
                    Description = $"Started TcpNode on port {server.LocalEndpoint.ToString()}"
                });

                while (true)
                {
                    var peer = await server.AcceptTcpClientAsync();//.ConfigureAwait(false);
                    spider.AddTcpClient(peer);
                }


            }
            catch (Exception ex)
            {
                if (log != null)
                    log.Log(new WarningLogEntry() { Exception = ex });
            }
        }

        public void EmulationSendToAll()
        {
            spider.SendToAllFreeMessage("test");
        }

        public void EmulationSetValue(byte[] data)
        {
            rn.AddLogEntry(data);          
        }



        long disposed = 0;
        public bool Disposed
        {
            get { return System.Threading.Interlocked.Read(ref disposed) == 1; }
        }

        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref disposed, 1, 0) != 0)
                return;

            try
            {
                if (server != null)
                {
                    server.Stop();
                    server = null;
                }
            }
            catch (Exception  ex)
            {
                
            }


            try
            {
                if (rn != null)
                {
                    rn.Dispose();
                    rn = null;
                }
            }
            catch (Exception ex)
            {

            }

            try
            {
                if (spider != null)
                {
                    spider.Dispose();
                }
            }
            catch (Exception ex)
            {

            }
      
        }
    }//eo class
}//eo namespace
