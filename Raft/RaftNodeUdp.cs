using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raft.Transport.UdpServer;
using System.Threading;
using System.Net;

namespace Raft
{
    public class RaftNodeUdp : IRaftComSender, IWarningLog,IDisposable
    {
        int UdpPort = 47777;
        UdpSocketListener udpSocket = null;   

        TimeMaster TM = null;
        RaftNode rn = null;
        ReaderWriterLockSlim sync_ListClusterEndPoints = new ReaderWriterLockSlim();
        Dictionary<string, NodeAddress> ListClusterEndPoints = new Dictionary<string, NodeAddress>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="raftNodeSettings"></param>
        /// <param name="udpPort">e.g. 47777 </param>
        /// <param name="verboseNode">Will call VerbosePrint on true</param>
        public RaftNodeUdp(RaftNodeSettings raftNodeSettings, int udpPort, bool verboseNode)
        {
            try
            {               
                TM=new TimeMaster(this);
                                
                udpSocket = new UdpSocketListener(TM,this);
                UdpPort = udpPort;

                rn = new RaftNode(raftNodeSettings, this, this);

                rn.Verbose = verboseNode;
                rn.NodeAddress.IsMe = true;

                //Can be increased on the hot system later.
                rn.SetNodesQuantityInTheCluster(raftNodeSettings.InitialQuantityOfRaftNodesInTheCluster);

                //Supplied by emulator for VerbosePrints
                rn.NodeAddress.NodeAddressId = raftNodeSettings.RaftNodeIdExternalForEmulator;

            }
            catch (Exception ex)
            {
                this.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNodeUdp.RaftNodeUdp" });
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            try
            {
                this.TM.Dispose();

                if (udpSocket != null)
                {
                    udpSocket.Stop();
                    udpSocket.Dispose();
                    udpSocket = null;
                }

                if (rn != null)
                {
                    rn.NodeStop();
                    rn.Dispose();
                    rn = null;
                }

            }
            catch
            {}
        }


        /// <summary>
        /// 
        /// </summary>
        public void StartNode()
        {
            //Starting Udp 
            try
            {
                //First starting node then listener
              //  this.rn.NodeStart();
                
                udpSocket.Start(UdpPort);

                sync_ListClusterEndPoints.EnterWriteLock();
                try
                {
                    ListClusterEndPoints.Add(udpSocket.EndPoint.ToString(), this.rn.NodeAddress);
                }
                finally
                {
                    sync_ListClusterEndPoints.ExitWriteLock();
                }
            }
            catch (Exception ex)
            {
                this.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNodeUdp.StartNode" });

                udpSocket.Stop();
                this.rn.NodeStop();
                sync_ListClusterEndPoints.EnterWriteLock();
                try
                {
                    ListClusterEndPoints.Clear();
                }
                finally
                {
                    sync_ListClusterEndPoints.ExitWriteLock();
                }
            }           
        }

        public void TEST_SendTo(byte[] data, IPEndPoint endPoint)
        {

            this.udpSocket.UdpSender.Send(eUdpPackageType.ProtocolData, data, endPoint);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        /// <param name="senderNodeAddress"></param>
        public void SendToAll(eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress)
        {           
            try
            {
                List<NodeAddress> lst = null;
                sync_ListClusterEndPoints.EnterReadLock();
                try
                {
                    lst = ListClusterEndPoints.Where(r=>!r.Value.IsMe).Select(r=>r.Value).ToList();
                }
                finally
                {
                    sync_ListClusterEndPoints.ExitReadLock();
                }
                if (lst == null || lst.Count() < 1)
                    return;
                                

                foreach (var n in lst)
                {

                    //!!!!!!!!!!!!!!!!!!!!  GO ON HERE
                   //udpSocket.SendTo(n.IpEP,signalType,data)
                    //////////if (n.NodeAddress.NodeAddressId == nodeAddress.NodeAddressId)
                    //////////{
                    //////////    //May be put it all into new Threads or so
                    //////////    ((IRaftComReceiver)n).IncomingSignalHandler(myNodeAddress, signalType, data);

                    //////////    break;
                    //////////}
                }
            }
            catch (Exception ex)
            {
                 this.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNodeUdp.SendToAll" });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="nodeAddress"></param>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        /// <param name="senderNodeAddress"></param>
        public void SendTo(NodeAddress nodeAddress, eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress)
        {
            try
            {
                //!!!!!!!!!!!!!!!!!!!!  GO ON HERE
            }
            catch (Exception ex)
            {
                this.LogError(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNodeUdp.SendTo" });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="logEntry"></param>
        public void LogError(WarningLogEntry logEntry)
        {
            //!!!!!!!!!!!!!!!!!!!!  GO ON HERE
        }



       
    }
}
