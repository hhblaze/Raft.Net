using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raft.Transport
{
    /// <summary>
    /// Spider manages connections for all listed nodes of the net
    /// </summary>
    internal class TcpSpider : IRaftComSender, IDisposable
    {      
        ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();
        Dictionary<string,TcpPeer> Peers = new Dictionary<string, TcpPeer>();     //Key - NodeUID    

        internal TcpRaftNode trn = null;

        public TcpSpider(TcpRaftNode trn)
        {
            this.trn = trn;          
        }

        public void AddTcpClient(TcpClient peer)
        {   
            var p = new TcpPeer(peer, trn);           
        }

        public void RemoveAll()
        {
            var lst = Peers.ToList();

            _sync.EnterWriteLock();
            try
            {
                
                Peers.Clear();
            }
            catch (Exception ex)
            {
                //throw;
            }
            finally
            {
                _sync.ExitWriteLock();
            }

            foreach (var peer in lst)
            {
                try
                {
                    if (peer.Value != null)
                    {                        
                        peer.Value.Dispose(true);
                    }
                }
                catch
                {
                }
               
            }

        }

       

        internal void RemovePeerFromClusterEndPoints(string endpointsid)
        {
            if (String.IsNullOrEmpty(endpointsid))
                return;

            _sync.EnterWriteLock();
            try
            {
                //if (peer == null || peer.Handshake == null)
                //    return;

                //trn.log.Log(new WarningLogEntry()
                //{
                //    LogType = WarningLogEntry.eLogType.DEBUG,
                //    Description = $"{trn.port} ({trn.rn.NodeState})> removing EP: {endpointsid}; Peers: {Peers.Count}"
                //});

                Peers.Remove(endpointsid);

                //trn.log.Log(new WarningLogEntry()
                //{
                //    LogType = WarningLogEntry.eLogType.DEBUG,
                //    Description = $"{trn.port} ({trn.rn.NodeState})> removed EP: {endpointsid}; Peers: {Peers.Count}"
                //});
                
            }
            catch (Exception ex)
            {
                //throw;
            }
            finally
            {
                _sync.ExitWriteLock();
            }

            this.trn.PeerIsDisconnected(endpointsid);
        }

        public void AddPeerToClusterEndPoints(TcpPeer peer, bool handshake)
        {
            _sync.EnterWriteLock();
            try
            {
                if (peer.Handshake.NodeUID == trn.GetNodeByEntityName("default").NodeAddress.NodeUId)   //Self disconnect
                {
                 
                    trn.clusterEndPoints.Where(r => r.EndPointSID == peer.EndPointSID)
                        .FirstOrDefault().Me = true;
                                        
                    peer.Dispose(true);
                    return;
                }

                //Choosing priority connection
                if (!Peers.ContainsKey(peer.EndPointSID))
                {                 

                    if(handshake && trn.GetNodeByEntityName("default").NodeAddress.NodeUId > peer.Handshake.NodeUID)
                    {
                        //trn.log.Log(new WarningLogEntry()
                        //{
                        //    LogType = WarningLogEntry.eLogType.DEBUG,
                        //    Description = $"{trn.port}> !!!!!dropped{peer.Handshake.NodeListeningPort} {peer.Handshake.NodeListeningPort} on handshake as weak"
                        //});

                        
                        peer.Dispose(true);
                        return;
                    }

                    Peers[peer.EndPointSID] = peer;
                    peer.FillNodeAddress();

                    //trn.log.Log(new WarningLogEntry()
                    //{
                    //    LogType = WarningLogEntry.eLogType.DEBUG,
                    //    Description = $"{trn.port}> >>>>>>connected{peer.Handshake.NodeListeningPort}  {peer.Handshake.NodeListeningPort} by {(handshake ? "handshake" : "ACK")} with diff: {(trn.rn.NodeAddress.NodeUId - peer.Handshake.NodeUID)}"
                    //});

                    if (handshake)
                    {
                        //sending back handshake ack

                        //Task.Run(() => {
                        //    //must be done from parallel so our possible dispose is not in the sync_lock
                        //     peer.Write(new byte[] { 00, 03 }, (new TcpMsgHandshake()
                        //    {
                        //        NodeListeningPort = trn.port,
                        //        NodeUID = trn.rn.NodeAddress.NodeUId,                                
                        //    }).SerializeProtobuf());
                        //});
                        peer.Write(
                            cSprot1Parser.GetSprot1Codec(
                            new byte[] { 00, 03 }, (new TcpMsgHandshake()
                            {
                                NodeListeningPort = trn.port,
                                NodeUID = trn.GetNodeByEntityName("default").NodeAddress.NodeUId,
                            }).SerializeBiser())
                        );
                    }
                  
                }
                else
                {
                    //trn.log.Log(new WarningLogEntry()
                    //{
                    //    LogType = WarningLogEntry.eLogType.DEBUG,
                    //    Description = $"{trn.port}> !!!!!dropped{peer.Handshake.NodeListeningPort} {peer.Handshake.NodeListeningPort} as existing"
                    //});

                    //Sending ping on existing connection (may be it is alredy old)
                    //Task.Run(() => {
                    //    //must be done from parallel so our possible dispose is not in the sync_lock
                       
                    //});
                    Peers[peer.EndPointSID].Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 05 }, null)); //ping

                    //removing incoming connection                    
                    peer.Dispose(true);

                    return;
                }
            }
            catch (Exception ex)
            {
                //throw;
            }
            finally
            {
                _sync.ExitWriteLock();
            }

        }

        public async Task Handshake()        
        {
            await HandshakeTo(trn.clusterEndPoints);
            trn.GetNodeByEntityName("default").TM.FireEventEach(3000, RetestConnections, null, false);
        }

        async Task HandshakeTo(List<TcpClusterEndPoint> clusterEndPoints)        
        {
            foreach (var el in clusterEndPoints)
            {
                try
                {
                    TcpClient cl = new TcpClient();
                    await cl.ConnectAsync(el.Host, el.Port);
                    
                    el.Peer = new TcpPeer(cl, trn);

                    //trn.log.Log(new WarningLogEntry()
                    //{
                    //    LogType = WarningLogEntry.eLogType.DEBUG,                        
                    //    Description = $"{trn.port}> try connect {el.Host}:{el.Port}"
                    //});

                    
                    el.Peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 01 }, (new TcpMsgHandshake()                    
                    {
                        NodeListeningPort = trn.port,
                        NodeUID = trn.GetNodeByEntityName("default").NodeAddress.NodeUId, //Generated GUID on Node start                        
                    }).SerializeBiser()));
                }
                catch (Exception ex)
                {

                }
            }
        }

        public List<TcpPeer> GetPeers(bool useLock = true)
        {
            List<TcpPeer> peers = null;

            if (useLock)
            {
                _sync.EnterReadLock();
                try
                {
                    peers = Peers.Values.ToList();
                }
                catch (Exception ex)
                { }
                finally
                {
                    _sync.ExitReadLock();
                }
            }
            else
                peers = Peers.Values.ToList();
            return peers ?? new List<TcpPeer>();
        }

        void RetestConnections(object obj)
        {
            RetestConnectionsAsync();
        }

        async Task RetestConnectionsAsync()        
        {
            try
            {
                if (!trn.GetNodeByEntityName("default").IsRunning)
                    return;

                List<TcpPeer> peers = GetPeers();
                
                if (peers.Count == trn.clusterEndPoints.Count - 1)
                    return;

                var list2Lookup = new HashSet<string>(peers.Select(r => r?.EndPointSID));
                var ws = trn.clusterEndPoints.Where(r => !r.Me && (!list2Lookup.Contains(r.EndPointSID))).ToList();

                if (ws.Count > 0)
                {                    
                    //trn.log.Log(new WarningLogEntry()
                    //{
                    //    LogType = WarningLogEntry.eLogType.DEBUG,
                    //    Description = $"{trn.port} ({trn.rn.NodeState})> peers: {peers.Count}; ceps: {trn.clusterEndPoints.Count}; will send: {ws.Count}"
                    //});

                    await HandshakeTo(ws);
                }

            }
            catch (Exception ex)
            {

            }
        }

        public void SendToAll(eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName, bool highPriority = false)
        {
            try
            {
                List<TcpPeer> peers = null;
                _sync.EnterReadLock();
                try
                {
                    peers = Peers.Values.ToList();
                }
                catch (Exception ex)
                {
                    //throw;
                }
                finally
                {
                    _sync.ExitReadLock();
                }

                foreach (var peer in peers)
                {
                    
                    peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 02 },
                        (
                            new TcpMsgRaft() { EntityName = entityName, RaftSignalType = signalType, Data = data }
                        ).SerializeBiser()), highPriority);
                }
            }
            catch (Exception ex)
            {
                
            }
        }

        public void SendTo(NodeAddress nodeAddress, eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName)
        {
            try
            {
                TcpPeer peer = null;
                if (Peers.TryGetValue(nodeAddress.EndPointSID, out peer))
                {
                    peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 02 },
                       (
                           new TcpMsgRaft() { EntityName = entityName, RaftSignalType = signalType, Data = data }
                       ).SerializeBiser()));
                }
            }
            catch (Exception ex)
            {


            }



        }

        public void SendToAllFreeMessage(string msgType, string dataString="", byte[] data=null, NodeAddress senderNodeAddress = null)
        {
            try
            {
                List<TcpPeer> peers = null;
                _sync.EnterReadLock();
                try
                {
                    peers = Peers.Values.ToList();
                }
                catch (Exception ex)
                {
                    //throw;
                }
                finally
                {
                    _sync.ExitReadLock();
                }

                foreach (var peer in peers)
                {
                    peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 04 },
                        (
                            new TcpMsg() { DataString = dataString, MsgType = msgType, Data = data }
                        ).SerializeBiser()));
                }

            }
            catch (Exception ex)
            {
                
            }
           
        }

      

        

        int disposed = 0;
        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref disposed, 1, 0) != 0)
                return;

            RemoveAll();
        }
    }

}//eo namespace
