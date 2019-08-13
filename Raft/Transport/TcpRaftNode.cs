/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
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
        internal IWarningLog log = null;
        internal int port = 0;
        internal Dictionary<string, RaftNode> raftNodes = new Dictionary<string, RaftNode>();
        internal TcpSpider spider = null;
        //internal List<TcpClusterEndPoint> clusterEndPoints = new List<TcpClusterEndPoint>();  //init clusterEndPoints creating 1-N connection

        internal DBreezeEngine dbEngine;

        internal NodeSettings NodeSettings = null;

        //public TcpRaftNode(List<TcpClusterEndPoint> clusterEndPoints, List<RaftNodeSettings> raftNodes, string dbreezePath, Func<string, ulong, byte[], bool> OnCommit, int port = 4250,  IWarningLog log = null)
        //public TcpRaftNode(List<TcpClusterEndPoint> clusterEndPoints, List<RaftEntitySettings> raftNodes, string dbreezePath, Func<string, ulong, byte[], bool> OnCommit, int port = 4250, IWarningLog log = null)
        public TcpRaftNode(NodeSettings nodeSettings, string dbreezePath, Func<string, ulong, byte[], bool> OnCommit, int port = 4250, IWarningLog log = null)
        {
            if (nodeSettings == null)
                nodeSettings = new NodeSettings();
            this.NodeSettings = nodeSettings;

            this.log = log;
            this.port = port;

            DBreezeConfiguration conf = new DBreezeConfiguration()
            {
                DBreezeDataFolderName = dbreezePath
            };

            if (nodeSettings.RaftEntitiesSettings.Where(MyEnt => !MyEnt.InMemoryEntity).Count() == 0)
            {
                conf.Storage = DBreezeConfiguration.eStorage.MEMORY;
            }
            else
            {
                conf.Storage = DBreezeConfiguration.eStorage.DISK;
            }
            

            //conf = new DBreezeConfiguration()
            //{
            //    DBreezeDataFolderName = dbreezePath,
            //    Storage = DBreezeConfiguration.eStorage.DISK,
            //};
            conf.AlternativeTablesLocations.Add("mem_*", String.Empty);

            dbEngine = new DBreezeEngine(conf);

           
            //if (clusterEndPoints != null)
            //{
            //    var bt = clusterEndPoints.SerializeBiser();
            //    var decoder = new Biser.Decoder(bt);
            //    this.clusterEndPoints = new List<TcpClusterEndPoint>();
            //    decoder.GetCollection(() => { return TcpClusterEndPoint.BiserDecode(extDecoder: decoder); }, this.clusterEndPoints, false);

            //    //this.clusterEndPoints.AddRange(clusterEndPoints.SerializeProtobuf().DeserializeProtobuf<List<TcpClusterEndPoint>>());
            //}
            spider = new TcpSpider(this);

            //bool firstNode = true;
            if (this.NodeSettings.RaftEntitiesSettings == null)
            {
                this.NodeSettings.RaftEntitiesSettings = new List<RaftEntitySettings>();
            }

            if(this.NodeSettings.RaftEntitiesSettings.Where(r=>r.EntityName.ToLower() == "default").Count()<1)
                this.NodeSettings.RaftEntitiesSettings.Add(new RaftEntitySettings());


            foreach (var re_settings in this.NodeSettings.RaftEntitiesSettings)
            {
                //if (firstNode)
                //{
                //    re_settings.EntityName = "default";
                //    firstNode = false;
                //}

                if (String.IsNullOrEmpty(re_settings.EntityName))
                    throw new Exception("Raft.Net: entities must have unique names. Change RaftNodeSettings.EntityName.");

                if (this.raftNodes.ContainsKey(re_settings.EntityName))
                    throw new Exception("Raft.Net: entities must have unique names. Change RaftNodeSettings.EntityName.");

                var rn = new RaftNode(re_settings ?? new RaftEntitySettings(), this.dbEngine, this.spider, this.log, OnCommit);
             
#if DEBUG
                rn.Verbose = re_settings.VerboseRaft;          //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   DEBUG PURPOSES
#endif
                rn.SetNodesQuantityInTheCluster((uint)this.NodeSettings.TcpClusterEndPoints.Count);             //!!!!!!!!!!!!  ENABLE 1 for debug, make it dynamic (but not less then 3 if not DEBUG)
                rn.NodeAddress.NodeAddressId = port; //for debug/emulation purposes

                rn.NodeAddress.NodeUId = Guid.NewGuid().ToByteArray().Substring(8, 8).To_Int64_BigEndian();

                this.raftNodes[re_settings.EntityName] = rn;

                rn.NodeStart();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="jsonConfiguration">json representation of Raft.NodeSettings</param>
        /// <param name="dbreezePath"></param>
        /// <param name="port"></param>
        /// <param name="log"></param>
        /// <param name="OnCommit"></param>
        /// <returns></returns>
        public static TcpRaftNode GetFromConfig(string jsonConfiguration, string dbreezePath, int port, IWarningLog log, Func<string, ulong, byte[], bool> OnCommit)
        {
            try
            {
                TcpRaftNode rn = null;
                NodeSettings ns = NodeSettings.BiserJsonDecode(jsonConfiguration);

                //Biser.JsonEncoder encc = new Biser.JsonEncoder(new NodeSettings() { TcpClusterEndPoints = eps, RaftEntitiesSettings = reSettings });
                //var str = encc.GetJSON(Biser.JsonSettings.JsonStringStyle.Prettify);

                //NodeSettings nhz = NodeSettings.BiserJsonDecode(str);

                //encc = new Biser.JsonEncoder(nhz);
                //str = encc.GetJSON(Biser.JsonSettings.JsonStringStyle.Prettify);

                rn = new TcpRaftNode(ns, dbreezePath, OnCommit, port, log);
                return rn;
            }
            catch (Exception ex)
            {
                if (log != null)
                    log.Log(new WarningLogEntry { LogType = WarningLogEntry.eLogType.ERROR, Exception = ex, Method = "TcpRaftNode.GetFromConfig JSON" });
            }
            return null;
        }

        [Obsolete("Use GetFromConfig supplying JSON configuration instead")]
        public static TcpRaftNode GetFromConfig(int configVersion, string configuration, string dbreezePath, int port, IWarningLog log, Func<string, ulong, byte[], bool> OnCommit)
        {
            //Setip for configVersion=1
            try
            {
                TcpRaftNode rn = null;
                                

                var re_settings = new RaftEntitySettings()
                {
                    EntityName = "default",
                    VerboseRaft = false,
                    VerboseTransport = false
                };

                string[] sev;
                List<TcpClusterEndPoint> eps = new List<TcpClusterEndPoint>();

                List<RaftEntitySettings> reSettings = new List<RaftEntitySettings>();
                string entityName = "";

                StringReader strReader = new StringReader(configuration);
                                
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
                            reSettings.Add(re_settings);
                            re_settings = new RaftEntitySettings { EntityName = entityName };
                            break;
                        case "verboseraft":
                            if (se[1].Trim().ToLower().Equals("true"))
                                re_settings.VerboseRaft = true;
                            break;
                        case "verbosetransport":
                            if (se[1].Trim().ToLower().Equals("true"))
                                re_settings.VerboseTransport = true;
                            break;
                        case "delayedpersistenceisactive":
                            if (se[1].Trim().ToLower().Equals("true"))
                                re_settings.DelayedPersistenceIsActive = true;
                            break;
                        case "delayedpersistencems":
                            re_settings.DelayedPersistenceMs = Convert.ToUInt32(se[1].Trim());
                            break;
                        case "inmemoryentity":
                            if (se[1].Trim().ToLower().Equals("true"))
                                re_settings.InMemoryEntity = true;
                            break;
                        case "inmemoryentitystartsyncfromlatestentity":
                            if (se[1].Trim().ToLower().Equals("true"))
                                re_settings.InMemoryEntityStartSyncFromLatestEntity = true;
                            break;


                    }//DelayedPersistenceMs
                }

                reSettings.Add(re_settings);

               
                rn = new TcpRaftNode(new NodeSettings() { TcpClusterEndPoints = eps, RaftEntitiesSettings = reSettings }, dbreezePath,
                    OnCommit,
                    port, log);

                return rn;         
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
                    server = new TcpListener(IPAddress.Any, this.port); 

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

        public async Task<bool> AddLogEntryAsync(byte[] data, string entityName = "default", int timeoutMs = 20000)
        {
            if (System.Threading.Interlocked.Read(ref disposed) == 1)
                return false;

            RaftNode rn = null;
            if (this.raftNodes.TryGetValue(entityName, out rn))
            {
                //Generating externalId
                var msgId = AsyncResponseHandler.GetMessageId();
                var msgIdStr = msgId.ToBytesString();
                var resp = new ResponseCrate();
                resp.TimeoutsMs = timeoutMs; //enable for amre
                                             //resp.TimeoutsMs = Int32.MaxValue; //using timeout of the wait handle (not the timer), enable for mre

                //resp.Init_MRE();
                resp.Init_AMRE();

                AsyncResponseHandler.df[msgIdStr] = resp;

                var aler = rn.AddLogEntry(data,msgId);

                switch(aler.AddResult)
                {
                    case AddLogEntryResult.eAddLogEntryResult.ERROR_OCCURED:
                    case AddLogEntryResult.eAddLogEntryResult.NO_LEADER_YET:

                        resp.Dispose_MRE();
                        AsyncResponseHandler.df.TryRemove(msgIdStr, out resp);

                        return false;
                    case AddLogEntryResult.eAddLogEntryResult.LOG_ENTRY_IS_CACHED:
                    case AddLogEntryResult.eAddLogEntryResult.NODE_NOT_A_LEADER:

                        //async waiting
                        await resp.amre.WaitAsync();    //enable for amre

                        resp.Dispose_MRE();

                        if (AsyncResponseHandler.df.TryRemove(msgIdStr, out resp))
                        {
                            if (resp.IsRespOk)
                                return true;

                            //return new Tuple<bool, byte[]>(true, resp.res); //???? returning externalId or even nothing
                        }

                        break;
                    
                }
            }

            //return new AddLogEntryResult { AddResult = AddLogEntryResult.eAddLogEntryResult.NODE_NOT_FOUND_BY_NAME };
            //return new Tuple<bool, byte[]>(false, null);
            return true;
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entityName"></param>
        public void Debug_PrintOutInMemory(string entityName = "default")
        {
            RaftNode rn = null;
            if (this.raftNodes.TryGetValue(entityName, out rn))
                rn.Debug_PrintOutInMemory();
        }


    }//eo class
}//eo namespace
