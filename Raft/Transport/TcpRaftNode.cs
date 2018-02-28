using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

using DBreeze;
using DBreeze.Utils;

namespace Raft.Transport
{
    public class TcpRaftNode: IDisposable
#if !NETSTANDARD2_0
        , IEmulatedNode
#endif
    {
        //internal RaftNodeSettings rn_settings = new RaftNodeSettings();
        internal IWarningLog log = null;
        internal int port = 0;
        internal Dictionary<string, RaftNode> raftNodes = new Dictionary<string, RaftNode>();
        internal TcpSpider spider = null;
        internal List<TcpClusterEndPoint> clusterEndPoints = new List<TcpClusterEndPoint>();  //init clusterEndPoints creating 1-N connection
        
        
        public TcpRaftNode(List<TcpClusterEndPoint> clusterEndPoints, List<RaftNodeSettings> raftNodes, string dbreezePath, Func<string, ulong, byte[], bool> OnCommit, int port = 4250,  IWarningLog log = null)
        {
            //this.rn_settings = rn_settings ?? new RaftNodeSettings();

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

            bool firstNode = true;
            foreach(var rn_settings in raftNodes)
            {
                if (firstNode)
                {
                    rn_settings.EntityName = "default";
                    firstNode = false;
                }

                if (String.IsNullOrEmpty(rn_settings.EntityName))
                    throw new Exception("Raft.Net: entities must have unique names. Change RaftNodeSettings.EntityName.");

                if (this.raftNodes.ContainsKey(rn_settings.EntityName))
                    throw new Exception("Raft.Net: entities must have unique names. Change RaftNodeSettings.EntityName.");

                var rn = new RaftNode(rn_settings ?? new RaftNodeSettings(), dbreezePath, this.spider, this.log, OnCommit);
             
#if DEBUG
                rn.Verbose = rn_settings.VerboseRaft;          //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   DEBUG PURPOSES
#endif
                rn.SetNodesQuantityInTheCluster((uint)this.clusterEndPoints.Count);             //!!!!!!!!!!!!  ENABLE 1 for debug, make it dynamic (but not less then 3 if not DEBUG)
                rn.NodeAddress.NodeAddressId = port; //for debug/emulation purposes

                rn.NodeAddress.NodeUId = Guid.NewGuid().ToByteArray().Substring(8, 8).To_Int64_BigEndian();

                this.raftNodes[rn_settings.EntityName] = rn;

                rn.NodeStart();
            }
        }

        public static TcpRaftNode GetFromConfig(int configVersion, string configuration, string dbreeze,int port, IWarningLog log, Func<string, ulong, byte[], bool> OnCommit)
        {
            try
            {
                TcpRaftNode rn = null;

                var rn_settings = new RaftNodeSettings()
                {
                    EntityName = "default",
                    VerboseRaft = false,
                    VerboseTransport = false
                };

                string[] sev;
                List<TcpClusterEndPoint> eps = new List<TcpClusterEndPoint>();

                List<RaftNodeSettings> rnSettings = new List<RaftNodeSettings>();
                string entityName = "";

                StringReader strReader = new StringReader(configuration);

                //foreach (var el in strReader.ReadLine)
                while(true)
                {
                    var el = strReader.ReadLine();
                    if (el == null)
                        break;

                    var se = el.Split(new char[] { ':' });
                    if (se.Length < 2)
                        continue;
                    switch (se[0].Trim().ToLower())
                    {
                        case "endpoint":
                            sev = se[1].Split(new char[] { ',' });
                            eps.Add(new TcpClusterEndPoint() { Host = sev[0].Trim(), Port = Convert.ToInt32(sev[1].Trim()) });
                            break;
                        //case "dbreeze":
                        //    dbreeze = String.Join(":",se.Skip(1));
                        //    break;
                        case "entity":
                            entityName = se[1].Trim();
                            if (entityName.ToLower().Equals("default"))
                                continue;
                            //flushing default entity and starting new one
                            if (String.IsNullOrEmpty(entityName))
                                throw new Exception("Raft.Net: configuration entity name must not be empty and must be unique among other entities");
                            rnSettings.Add(rn_settings);
                            rn_settings = new RaftNodeSettings { EntityName = entityName };
                            break;
                        case "verboseraft":
                            if (se[1].Trim().ToLower().Equals("true"))
                                rn_settings.VerboseRaft = true;
                            break;
                        case "verbosetransport":
                            if (se[1].Trim().ToLower().Equals("true"))
                                rn_settings.VerboseTransport = true;
                            break;
                        case "delayedpersistenceisactive":
                            if (se[1].Trim().ToLower().Equals("true"))
                                rn_settings.DelayedPersistenceIsActive = true;
                            break;
                        case "delayedpersistencems":
                            rn_settings.DelayedPersistenceMs = Convert.ToUInt32(se[1].Trim());
                            break;
                        case "inmemoryentity":
                            if (se[1].Trim().ToLower().Equals("true"))
                                rn_settings.InMemoryEntity = true;
                            break;
                        case "inmemoryentitystartsyncfromlatestentity":
                            if (se[1].Trim().ToLower().Equals("true"))
                                rn_settings.InMemoryEntityStartSyncFromLatestEntity = true;
                            break;


                    }//DelayedPersistenceMs
                }

                rnSettings.Add(rn_settings);


                rn = new TcpRaftNode(eps, rnSettings, dbreeze,
                    OnCommit,
                    port, log);

                return rn;

                //rn.Start();                
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        



        internal void PeerIsDisconnected(string endpointsid)
        {
            foreach(var rn in this.raftNodes)
                rn.Value.PeerIsDisconnected(endpointsid);
        }

        internal RaftNode GetNodeByEntityName(string entityName)
        {
            RaftNode rn = null;
            raftNodes.TryGetValue(entityName, out rn);
            return rn;
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

        public void EmulationSetValue(byte[] data, string entityName="default")
        {
            RaftNode rn = null;
            if(this.raftNodes.TryGetValue(entityName, out rn))
                rn.AddLogEntry(data);          
        }

        public AddLogEntryResult AddLogEntry(byte[] data, string entityName = "default")
        {
            RaftNode rn = null;
            if (this.raftNodes.TryGetValue(entityName, out rn))
                return rn.AddLogEntry(data);

            return new AddLogEntryResult { AddResult = AddLogEntryResult.eAddLogEntryResult.NODE_NOT_FOUND_BY_NAME };
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
                foreach (var rn in this.raftNodes)
                {
                    rn.Value.Dispose();
                }

                this.raftNodes.Clear();
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
