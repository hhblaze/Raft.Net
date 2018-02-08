using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//Implemented 15.08.2014

//using DBreeze;
//using DBreeze.Utils;

//http://thesecretlivesofdata.com/raft/
//https://www.youtube.com/watch?v=BYMOlWAAiOQ
//When record is commited:
/*
 https://raft.github.io/raft.pdf
Raft
never commits log entries from previous terms by counting
replicas.Only log entries from the leader’s current
term are committed by counting replicas; once an entry
from the current term has been committed in this way,
then all prior entries are committed
*/
//prior term and current term record can be considered as committed, when at least one record from CURRENT term is saved on majority of servers
//https://docs.google.com/presentation/d/e/2PACX-1vQjS7iV8k84WSVavOTbRjQ-0nihukXxh_1l44Dy0qqSWVs3uQB9C0Xh9erLdXqDTkcH3XoKiOmt3bhI/pub?start=false&loop=false&delayms=3000&slide=id.g32ff89c1fa_0_11
//If now term 4, record 3 (from term 2) can't be assumed as commited until record 4 (from term 4) gets its majority.
//All previous and next indexes (including 4, term 4) can be count as commited since now.

namespace Raft
{
    /// <summary>
    /// Main class. Initiate and Run.
    /// </summary>
    public class RaftNode : IRaftComReceiver,IDisposable, IEmulatedNode
    {
        internal enum eNodeState
        {
            Leader,
            Follower,
            Candidate
        }

        enum eTermComparationResult
        {
            CurrentTermIsSmaller,
            TermsAreEqual,
            CurrentTermIsHigher
        }

        /// <summary>
        /// Last term this node has voted
        /// </summary>
        ulong LastVotedTermId = 0;
        /// <summary>
        /// Communication interface
        /// </summary>
        IRaftComSender Sender = null;
        /// <summary>
        /// Node settings
        /// </summary>
        RaftNodeSettings nodeSettings = null;
        IWarningLog Log = null;
        object lock_Operations = new object();        
        internal TimeMaster TM = null;
        internal eNodeState NodeState = eNodeState.Follower;
        
        /// <summary>
        /// 
        /// </summary>
        ulong Election_TimerId = 0;
        /*
         * It's possible that we receive higher term from another leader 
         * (in case if this leader was disconnected for some seconds from the network, 
         * other leader can be elected and it will definitely have higher Term, so every Leader node must be ready to it)
        */
        /// <summary>
        /// We stop this timer only in case if become leader, and starting if loosing leadership.       
        /// </summary>
        ulong LeaderHeartbeat_TimerId = 0;
        /// <summary>
        /// 
        /// </summary>
        ulong Leader_TimerId = 0;
        /// <summary>
        /// 
        /// </summary>
        ulong LeaderLogResend_TimerId = 0;

        Random rnd = new Random();
        uint VotesQuantity = 0;   //After becoming a candidate
        uint NodesQuantityInTheCluster = 2; //We need this value to calculate majority while leader election

        /// <summary>
        /// Current node Term
        /// </summary>
        internal ulong NodeTerm = 0;
        /// <summary>
        /// Node StateLog
        /// </summary>
        StateLog NodeStateLog = null;
        /// <summary>
        /// Address of the current node
        /// </summary>
        public NodeAddress NodeAddress = new NodeAddress();
        /// <summary>
        /// In case if current node is not Leader. It holds leader address
        /// </summary>
        NodeAddress LeaderNodeAddress = null;
        /// <summary>
        /// Received Current leader heartbeat time
        /// </summary>
        DateTime LeaderHeartbeatArrivalTime = DateTime.MinValue;

        /// <summary>
        /// If makes Debug outputs
        /// </summary>
        public bool Verbose = false;
        /// <summary>
        /// Is node started 
        /// </summary>
        public bool IsRunning = false;
        /// <summary>
        /// Latest heartbeat from leader, can be null on start
        /// </summary>
        internal LeaderHeartbeat LeaderHeartbeat = null;
        /// <summary>
        /// 
        /// </summary>
        internal RedirectHandler redirector = null;

        public RaftNode(RaftNodeSettings settings, string dbreezePath, IRaftComSender raftSender, IWarningLog log)
        {
            if (log == null)
                throw new Exception("ILog is not supplied");
            Log = log;
            //Starting time master
            this.TM = new TimeMaster(log);
            NodeStateLog = new StateLog(dbreezePath, this);            

            Sender = raftSender;
            nodeSettings = settings;

            redirector = new RedirectHandler(this);
        }

        int disposed = 0;

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref disposed, 1, 0) != 0)
                return;

            this.NodeStop();

            if (this.TM != null)
                this.TM.Dispose();

            this.NodeStateLog.Dispose();
            this.NodeStateLog = null;
        }

        /// <summary>
        /// We need this value to calculate majority while leader election
        /// </summary>
        public void SetNodesQuantityInTheCluster(uint nodesQuantityInTheCluster)
        {
            lock (lock_Operations)
            {
                NodesQuantityInTheCluster = nodesQuantityInTheCluster;
            }
        }


        /// <summary>
        /// Starts the node
        /// </summary>
        public void NodeStart()
        {
            lock (lock_Operations)
            {
                if (IsRunning)
                {
                    VerbosePrint("Node {0} ALREADY RUNNING", NodeAddress.NodeAddressId);
                    return;
                }                

                VerbosePrint("Node {0} has started.", NodeAddress.NodeAddressId);
                //In the beginning node is Follower
                SetNodeFollower();

                IsRunning = true;
            }
                        
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        internal void VerbosePrint(string format,params object[] args)
        {      
            if(Verbose)
                Console.WriteLine( String.Format("{0}> {1}",DateTime.Now.ToString("mm:ss.ms"),String.Format(format,args)));
        }

        /// <summary>
        /// Stops the node
        /// </summary>
        public void NodeStop()
        {
            lock (lock_Operations)
            {
                RemoveElectionTimer();
                RemoveLeaderHeartbeatWaitingTimer();
                RemoveLeaderTimer();

                this.NodeState = eNodeState.Follower;
                this.NodeStateLog.LeaderSynchronizationIsActive = false;

                VerbosePrint("Node {0} state is {1}", NodeAddress.NodeAddressId,this.NodeState);

                IsRunning = false;
            }
        }


        /// <summary>
        /// If this action works, it can mean that Node can give a bid to be the candidate after specified time interval
        /// Starts Election timer only in case if it's not running yet
        /// </summary>
        /// <param name="userToken"></param>
        void LeaderHeartbeatTimeout(object userToken)     
        {
            try
            {
                lock (lock_Operations)
                {
                    if (NodeState == eNodeState.Leader) //Self is the leader
                    {
                        RemoveLeaderHeartbeatWaitingTimer();
                        return;
                    }

                    if (DateTime.Now.Subtract(this.LeaderHeartbeatArrivalTime).TotalMilliseconds < this.nodeSettings.LeaderHeartbeatMs)
                        return; //Early to elect, we receive completely heartbeat from the leader

                    VerbosePrint("Node {0} LeaderHeartbeatTimeout", NodeAddress.NodeAddressId);

                    RunElectionTimer();
                }
            }
            catch (Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNode.LeaderHeartbeatTimeout" });
            }
            
        }

        /// <summary>
        /// Returns Leader node address
        /// </summary>
        public NodeAddress LeaderNode
        {
            get {
                NodeAddress na = null;
                lock (lock_Operations)
                {
                    if (Election_TimerId == 0)
                        na = LeaderNodeAddress;                    
                }
                return na;
            }
        }

        /// <summary>
        /// Time to become a candidate
        /// </summary>
        /// <param name="userToken"></param>
        void ElectionTimeout(object userToken)
        {            
            CandidateRequest req = null;
            try
            {
                lock (lock_Operations)
                {
                    if (Election_TimerId == 0)  //Timer was switched off and we don't need to run it again
                        return;

                    Election_TimerId = 0;

                    if (this.NodeState == eNodeState.Leader)
                        return;

                  

                    VerbosePrint("Node {0} election timeout", NodeAddress.NodeAddressId);

                    this.NodeState = eNodeState.Candidate;

                    VerbosePrint("Node {0} state is {1} _ElectionTimeout", NodeAddress.NodeAddressId, this.NodeState);

                    //Voting for self
                    VotesQuantity = 1;
                    //Increasing local term number
                    NodeTerm++;
                    
                    req = new CandidateRequest()
                    {
                        TermId = this.NodeTerm,
                        LastLogId = NodeStateLog.StateLogId,
                        LastTermId = NodeStateLog.StateLogTerm
                    };


                    //send to all was here

                    //Setting up new Election Timer
                    RunElectionTimer();
                }

                this.Sender.SendToAll(eRaftSignalType.CandidateRequest, req.SerializeProtobuf(), this.NodeAddress);
            }
            catch (Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNode.ElectionTimeout" });
            }

            
        }


        void LeaderTimerElapse(object userToken)
        {
            try
            {
                //Sending signal to all (except self that it is a leader)
                LeaderHeartbeat heartBeat = null;

                lock (lock_Operations)
                {                  

                    heartBeat = new LeaderHeartbeat()
                    {
                        LeaderTerm = this.NodeTerm,
                        StateLogLatestIndex = NodeStateLog.StateLogId,
                        StateLogLatestTerm = NodeStateLog.StateLogTerm,                        
                        LastStateLogCommittedIndex = this.NodeStateLog.LastCommittedIndex,
                        LastStateLogCommittedIndexTerm = this.NodeStateLog.LastCommittedIndexTerm
                    };

                }

                //VerbosePrint($"{NodeAddress.NodeAddressId} (Leader)> leader_heartbeat");
                this.Sender.SendToAll(eRaftSignalType.LeaderHearthbeat, heartBeat.SerializeProtobuf(), this.NodeAddress, true);                

                
            }
            catch (Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNode.LeaderTimerElapse" });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="address">Address of the node who sent the signal</param>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        public void IncomingSignalHandler(NodeAddress address, eRaftSignalType signalType, byte[] data)
        {
            try
            {
                lock (lock_Operations)
                {
                    switch (signalType)
                    {
                        case eRaftSignalType.LeaderHearthbeat:
                            ParseLeaderHeartbeat(address, data);                        
                            break;
                        case eRaftSignalType.CandidateRequest:
                            ParseCandidateRequest(address, data);
                            break;
                        case eRaftSignalType.VoteOfCandidate:
                            ParseVoteOfCandidate(data);
                            break;
                        case eRaftSignalType.StateLogEntrySuggestion:
                            ParseStateLogEntrySuggestion(address, data);
                            break;
                        case eRaftSignalType.StateLogEntryRequest:
                            ParseStateLogEntryRequest(address, data);
                            break;
                        case eRaftSignalType.StateLogEntryAccepted:                                                       
                            ParseStateLogEntryAccepted(address, data);
                            break;
                        case eRaftSignalType.StateLogRedirectRequest:
                            ParseStateLogRedirectRequest(address, data);
                            break;
                        case eRaftSignalType.StateLogRedirectResponse:
                            ParseStateLogRedirectResponse(address, data);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNode.IncomingSignalHandler" });
            }

        }


        #region "TIMERS HANDLER"

        /// <summary>
        /// 
        /// </summary>
        void RunElectionTimer()
        {
            if (this.Election_TimerId == 0)
            {
                rnd.Next(System.Threading.Thread.CurrentThread.ManagedThreadId);
                int seed = rnd.Next(nodeSettings.ElectionTimeoutMinMs, nodeSettings.ElectionTimeoutMaxMs);                
                Election_TimerId = this.TM.FireEventEach((uint)seed, ElectionTimeout, null, true);

                VerbosePrint("Node {0} RunElectionTimer {1} ms", NodeAddress.NodeAddressId, seed);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        void RunLeaderHeartbeatWaitingTimer()
        {
            if (LeaderHeartbeat_TimerId == 0)
                LeaderHeartbeat_TimerId = this.TM.FireEventEach(nodeSettings.LeaderHeartbeatMs, LeaderHeartbeatTimeout, null, false);
        }

        /// <summary>
        /// 
        /// </summary>
        void RunLeaderTimer()
        {
            if (Leader_TimerId == 0)
            {
                //Raising quickly one 
                LeaderTimerElapse(null);
                Leader_TimerId = this.TM.FireEventEach(nodeSettings.LeaderHeartbeatMs / 2, LeaderTimerElapse, null, false, "LEADER");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        void RemoveLeaderTimer()
        {
            if (this.Leader_TimerId > 0)
            {                
                this.TM.RemoveEvent(this.Leader_TimerId);
                this.Leader_TimerId = 0;
            }
        }

        void RunLeaderLogResendTimer()
        {
            if (LeaderLogResend_TimerId == 0)
            {
                LeaderLogResend_TimerId = this.TM.FireEventEach(nodeSettings.LeaderLogResendIntervalMs, LeaderLogResendTimerElapse, null, true);
            }
        }

        void RemoveLeaderLogResendTimer()
        {
            if (this.LeaderLogResend_TimerId > 0)
            {
                this.TM.RemoveEvent(this.LeaderLogResend_TimerId);
                this.LeaderLogResend_TimerId = 0;
            }
        }

      

        /// <summary>
        /// 
        /// </summary>
        void RemoveElectionTimer()
        {
            if (this.Election_TimerId > 0)
            {                
                this.TM.RemoveEvent(this.Election_TimerId);
                this.Election_TimerId = 0;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        void RemoveLeaderHeartbeatWaitingTimer()
        {
            if (this.LeaderHeartbeat_TimerId > 0)
            {                
                this.TM.RemoveEvent(this.LeaderHeartbeat_TimerId);
                this.LeaderHeartbeat_TimerId = 0;
            }
        }
        #endregion
        

        /// <summary>
        /// Only for Leader.
        /// Follower requests new Log Entry Index from the Leader and Leader answers to the Follower
        /// </summary>
        /// <param name="address"></param>
        /// <param name="data"></param>
        void ParseStateLogEntryRequest(NodeAddress address, byte[] data)
        {
          
            if (this.NodeState != eNodeState.Leader)
                return;

            StateLogEntryRequest req = data.DeserializeProtobuf<StateLogEntryRequest>();

            //Getting suggestion
            var suggestion = this.NodeStateLog.GetNextStateLogEntrySuggestionFromRequested(req);

            VerbosePrint($"{NodeAddress.NodeAddressId} (Leader)> Request (I/T): {req.StateLogEntryId}/{req.StateLogEntryTerm} from {address.NodeAddressId};");

            if (suggestion != null)
            {
                this.Sender.SendTo(address, eRaftSignalType.StateLogEntrySuggestion, suggestion.SerializeProtobuf(), this.NodeAddress);
            }
            
        }



       


        uint GetMajorityQuantity()
        {
            return (uint)Math.Floor((double)NodesQuantityInTheCluster / 2) + 1;
        }


        /// <summary>
        /// Only for Follower
        ///  Is called from tryCatch and in lock
        /// </summary>
        /// <param name="address"></param>
        /// <param name="data"></param>
        void ParseStateLogEntrySuggestion(NodeAddress address, byte[] data)
        {
            if (this.NodeState != eNodeState.Follower)
                return;

            StateLogEntrySuggestion suggest = data.DeserializeProtobuf<StateLogEntrySuggestion>();

            if (this.NodeTerm > suggest.LeaderTerm)  //Sending Leader is not Leader anymore
            {
                this.NodeStateLog.LeaderSynchronizationIsActive = false;
                return;
            }

            if (this.NodeTerm < suggest.LeaderTerm)
                this.NodeTerm = suggest.LeaderTerm;


            if (suggest.StateLogEntry.Index <= NodeStateLog.LastCommittedIndex) //Preventing same entry income, can happen if restoration was sent twice (while switch of leaders)
                return;  //breakpoint don't remove

            //Checking if node can accept current suggestion
            if (suggest.StateLogEntry.PreviousStateLogId > 0)
            {
                var sle = this.NodeStateLog.GetEntryByIndexTerm(suggest.StateLogEntry.PreviousStateLogId, suggest.StateLogEntry.PreviousStateLogTerm);
                
                if (sle == null)
                {
                    //We don't have previous to this log and need new index request   
                    //VerbosePrint($"{NodeAddress.NodeAddressId}>  in sync 1 ");
                    this.SyncronizeWithLeader();
                    return;
                }                
            }          

            //We can apply new Log Entry from the Leader and answer successfully
            this.NodeStateLog.AddToLogFollower(suggest);

            StateLogEntryApplied applied = new StateLogEntryApplied()
            {
                 StateLogEntryId = suggest.StateLogEntry.Index,
                 StateLogEntryTerm = suggest.StateLogEntry.Term,
                 RedirectId = suggest.StateLogEntry.RedirectId
            };
            
            //this.NodeStateLog.LeaderSynchronizationIsActive = false;

            this.Sender.SendTo(address, eRaftSignalType.StateLogEntryAccepted, applied.SerializeProtobuf(), this.NodeAddress);          
        }

        /// <summary>
        /// 
        /// </summary>
        void SetNodeFollower()
        {
            if(this.NodeState != eNodeState.Follower)
                VerbosePrint("Node {0} state is {1} of {2}", NodeAddress.NodeAddressId, this.NodeState,this.LeaderNodeAddress.NodeAddressId);

            this.NodeState = eNodeState.Follower;
            
            this.NodeStateLog.LeaderSynchronizationIsActive = false;
            //Removing timers
            RemoveElectionTimer();
            RemoveLeaderHeartbeatWaitingTimer();
            RemoveLeaderTimer();
            RemoveLeaderLogResendTimer();
            //Starting Leaderheartbeat
            RunLeaderHeartbeatWaitingTimer();
            
        }

        /// <summary>
        /// Is called from lock_Operations and try catch
        /// </summary>
        /// <param name="address"></param>
        /// <param name="data"></param>
        void ParseLeaderHeartbeat(NodeAddress address, byte[] data)
        {
            //var LeaderHeartbeat = data.DeserializeProtobuf<LeaderHeartbeat>();
            this.LeaderHeartbeat = data.DeserializeProtobuf<LeaderHeartbeat>();

            // Setting variable of the last heartbeat
            this.LeaderHeartbeatArrivalTime = DateTime.Now;
            this.LeaderNodeAddress = address;   //Can be incorrect in case if this node is Leader, must 

            //Comparing Terms
            if (this.NodeTerm < LeaderHeartbeat.LeaderTerm)
            {
                this.NodeTerm = LeaderHeartbeat.LeaderTerm;

                switch (this.NodeState)
                {
                    case eNodeState.Leader:
                        //Stepping back from Leader to Follower
                        SetNodeFollower();
                        VerbosePrint("Node {0} state is {1} _IncomingSignalHandler", NodeAddress.NodeAddressId, this.NodeState);
                        break;
                    case eNodeState.Candidate:
                        //Stepping back
                        SetNodeFollower();
                        VerbosePrint("Node {0} state is {1} _IncomingSignalHandler", NodeAddress.NodeAddressId, this.NodeState);
                        break;
                    case eNodeState.Follower:
                        //Ignoring
                        SetNodeFollower();  //Reseting timers
                        break;
                }
            }
            else
            {
                switch (this.NodeState)
                {
                    case eNodeState.Leader:
                        //2 leaders with the same Term

                        if (this.NodeTerm > LeaderHeartbeat.LeaderTerm)
                        {
                            //Ignoring
                            //Incoming signal is not from the Leader anymore
                            return;
                        }
                        else
                        {
                            //Stepping back                         
                            SetNodeFollower();
                            VerbosePrint("Node {0} state is {1} _IncomingSignalHandler", NodeAddress.NodeAddressId, this.NodeState);
                        }                        
                        break;
                    case eNodeState.Candidate:
                        //Stepping back
                        SetNodeFollower();
                        VerbosePrint("Node {0} state is {1} _IncomingSignalHandler", NodeAddress.NodeAddressId, this.NodeState);
                        break;
                    case eNodeState.Follower:
                        SetNodeFollower();
                        break;
                }
            }


            //Here will come only Followers
            this.LeaderNodeAddress = address;
            if (!IsLeaderSynchroTimerActive && !this.NodeStateLog.SetLastCommittedIndexFromLeader(LeaderHeartbeat))
            {
                //VerbosePrint($"{NodeAddress.NodeAddressId}>  in sync 2 ");
                this.SyncronizeWithLeader();
            }
        
        }

        bool IsLeaderSynchroTimerActive
        {
            get
            {
                if (this.NodeStateLog.LeaderSynchronizationIsActive)
                {
                    if (DateTime.UtcNow.Subtract(this.NodeStateLog.LeaderSynchronizationRequestWasSent).TotalMinutes > this.NodeStateLog.LeaderSynchronizationTimeOut)
                    {
                        //Time to repeat request
                    }
                    else
                        return true; //We are already in syncrhonization mode with the Leader
                }

                return false;
            }
        }

        /// <summary>
        /// Is called from tryCatch and lock.
        /// Synchronizes starting from last committed value
        /// </summary>
        /// <param name="stateLogEntryId"></param>        
        internal void SyncronizeWithLeader(bool selfCall = false)
        {
            if (!selfCall)
            {
                if (IsLeaderSynchroTimerActive)
                    return;
                
                NodeStateLog.ClearStateLogStartingFromCommitted();
            }

            NodeStateLog.LeaderSynchronizationIsActive = true;
            NodeStateLog.LeaderSynchronizationRequestWasSent = DateTime.UtcNow;


            StateLogEntryRequest req = new StateLogEntryRequest()
            {                
                StateLogEntryId = NodeStateLog.LastCommittedIndex,
                StateLogEntryTerm = NodeStateLog.LastCommittedIndexTerm
            };

            

            this.Sender.SendTo(this.LeaderNodeAddress, eRaftSignalType.StateLogEntryRequest, req.SerializeProtobuf(), this.NodeAddress);
        }


        /// <summary>
        /// All data which comes, brings TermId, if incoming TermId is bigger then current,
        /// Node updates its current termId toincoming Id and step back to the follower (if it was not).
        /// For vote and candidate requests
        /// 
        /// Must be called from lock_Operations only
        /// </summary>
        /// <param name="incomingTermId"></param>
        eTermComparationResult CompareCurrentTermWithIncoming(ulong incomingTermId)
        {
            eTermComparationResult res = eTermComparationResult.TermsAreEqual;

            if (NodeTerm < incomingTermId)
            {
                res = eTermComparationResult.CurrentTermIsSmaller;

                // Stepping back to follower state
                this.NodeTerm = incomingTermId;

                switch (this.NodeState)
                {
                    case eNodeState.Follower:
                        //Do nothing
                        break;
                    case eNodeState.Leader:
                        //Stepping back
                        SetNodeFollower();
                        VerbosePrint("Node {0} state is {1} _CompareCurrentTermWithIncoming", NodeAddress.NodeAddressId, this.NodeState);
                        break;
                    case eNodeState.Candidate:
                        //Stepping back
                        //When node is candidate, Election_TimerId always works
                        SetNodeFollower();
                        VerbosePrint("Node {0} state is {1} _CompareCurrentTermWithIncoming", NodeAddress.NodeAddressId, this.NodeState);                        
                        break;
                }
            }            
            else
            {
                res = eTermComparationResult.CurrentTermIsHigher;
            }

            return res;
        }


        /// <summary>
        /// Is called from tryCatch and in lock
        /// </summary>
        /// <param name="data"></param>
        void ParseCandidateRequest(NodeAddress address, byte[] data)
        {
            var req = data.DeserializeProtobuf<CandidateRequest>();

            VoteOfCandidate vote = new VoteOfCandidate();
            vote.VoteType = VoteOfCandidate.eVoteType.VoteFor;

            var termState = CompareCurrentTermWithIncoming(req.TermId);

            vote.TermId = NodeTerm;

            switch (termState)
            {
                case eTermComparationResult.CurrentTermIsHigher:

                    vote.VoteType = VoteOfCandidate.eVoteType.VoteReject;

                    break;
                case eTermComparationResult.CurrentTermIsSmaller:
                    //Now this Node is Follower
                    break;
            }

            if (vote.VoteType == VoteOfCandidate.eVoteType.VoteFor)
            {
                switch (this.NodeState)
                {
                    case eNodeState.Leader:
                        vote.VoteType = VoteOfCandidate.eVoteType.VoteReject;
                        break;
                    case eNodeState.Candidate:
                        vote.VoteType = VoteOfCandidate.eVoteType.VoteReject;
                        break;
                    case eNodeState.Follower:

                        //Probably we can vote for this Node (if we didn't vote for any other one)
                        if (LastVotedTermId < req.TermId)
                        {
                            //formula of voting
                            if (
                                (NodeStateLog.StateLogTerm > req.LastTermId)
                                ||
                                (
                                    NodeStateLog.StateLogTerm == req.LastTermId
                                    &&
                                    NodeStateLog.StateLogId > req.LastLogId
                                )
                               )
                            {
                                vote.VoteType = VoteOfCandidate.eVoteType.VoteReject;
                            }
                            else
                            {
                                LastVotedTermId = req.TermId;
                                vote.VoteType = VoteOfCandidate.eVoteType.VoteFor;

                                //Restaring Election Timer
                                this.RemoveElectionTimer();
                                this.RunElectionTimer();
                            }
                           
                            
                        }
                        else
                            vote.VoteType = VoteOfCandidate.eVoteType.VoteReject;

                        break;
                }
            }

            //Sending vote signal back 
            //VerbosePrint("Node {0} voted to node {1} as {2}  _ParseCandidateRequest", NodeAddress.NodeAddressId, address.NodeAddressId, vote.VoteType);
            VerbosePrint($"Node {NodeAddress.NodeAddressId} ({this.NodeState}) {vote.VoteType} {address.NodeAddressId}  in  _ParseCandidateRequest");

            Sender.SendTo(address, eRaftSignalType.VoteOfCandidate, vote.SerializeProtobuf(), this.NodeAddress);
        }



        /// <summary>
        /// Node receives answer votes (to become Leader) from other nodes.
        /// Is called from tryCatch and in lock
        /// </summary>
        /// <param name="data"></param>
        void ParseVoteOfCandidate(byte[] data)
        {
            //Node received a node
            var vote = data.DeserializeProtobuf<VoteOfCandidate>();

            var termState = CompareCurrentTermWithIncoming(vote.TermId);

            if (this.NodeState != eNodeState.Candidate)
                return;

            switch (vote.VoteType)
            {
                case VoteOfCandidate.eVoteType.VoteFor:
                    //Calculating if node has Majority of
                   
                    VotesQuantity++;

                    if (VotesQuantity >= this.GetMajorityQuantity())
                    {
                        //Majority

                        //Node becomes a Leader
                        this.NodeState = eNodeState.Leader;
                        this.NodeStateLog.ClearLogAcceptance();

                        VerbosePrint("Node {0} state is {1} _ParseVoteOfCandidate", NodeAddress.NodeAddressId, this.NodeState);
                        VerbosePrint("Node {0} is Leader **********************************************",NodeAddress.NodeAddressId);
                        
                        //Stopping timers
                        this.RemoveElectionTimer();
                        this.RemoveLeaderHeartbeatWaitingTimer();
                                                
                        /*
                         * It's possible that we receive higher term from another leader 
                         * (in case if this leader was disconnected for some seconds from the network, 
                         * other leader can be elected and it will definitely have higher Term, so every Leader node must be ready to it)
                         */                        

                        this.RunLeaderTimer();
                    }
                    //else
                    //{
                    //    //Accumulating voices
                    //    //Do nothing
                    //}

                    break;
                case VoteOfCandidate.eVoteType.VoteReject:
                    //Do nothing
                    break;               
            }
        }


        /// <summary>
        /// called from lock try..catch
        /// </summary>
        /// <param name="address"></param>
        /// <param name="data"></param>
        void ParseStateLogRedirectRequest(NodeAddress address, byte[] data)
        {
            StateLogEntryRedirectRequest req = data.DeserializeProtobuf<StateLogEntryRedirectRequest>();
            StateLogEntryRedirectResponse resp = new StateLogEntryRedirectResponse() { RedirectId = req.RedirectId };

            if (this.NodeState != eNodeState.Leader)
            {
                resp.ResponseType = StateLogEntryRedirectResponse.eResponseType.NOT_A_LEADER;

                this.Sender.SendTo(address, eRaftSignalType.StateLogRedirectResponse, resp.SerializeProtobuf(), this.NodeAddress);
                return;
            }

            resp.ResponseType = StateLogEntryRedirectResponse.eResponseType.CACHED;
            var redirectId = this.redirector.StoreRedirect(address); //here we must store redirect data
            var addedStateLogTermIndex = this.NodeStateLog.AddStateLogEntryForDistribution(req.Data, redirectId);
            ApplyLogEntry();

            this.Sender.SendTo(address, eRaftSignalType.StateLogRedirectResponse, resp.SerializeProtobuf(), this.NodeAddress);
        }

        void ParseStateLogRedirectResponse(NodeAddress address, byte[] data)
        {
            StateLogEntryRedirectResponse resp = data.DeserializeProtobuf<StateLogEntryRedirectResponse>();

            //var redirectInfo = this.redirector.GetRedirectByIdTerm(applied.RedirectId, applied.StateLogEntryTerm);
            //if (redirectInfo != null && redirectInfo.NodeAddress != null)
            //{
            //    StateLogEntryRedirectResponse resp = new StateLogEntryRedirectResponse()
            //    {
            //        RedirectId = applied.RedirectId,
            //        ResponseType = StateLogEntryRedirectResponse.eResponseType.COMMITED
            //    };
            //    this.Sender.SendTo(redirectInfo.NodeAddress, eRaftSignalType.StateLogRedirectResponse, resp.SerializeProtobuf(), this.NodeAddress);
            //}



            //if (this.NodeState != eNodeState.Leader)
            //{
            //    resp.ResponseType = StateLogEntryRedirectResponse.eResponseType.NOT_A_LEADER;

            //    this.Sender.SendTo(address, eRaftSignalType.StateLogRedirectResponse, resp.SerializeProtobuf(), this.NodeAddress);
            //    return;
            //}

            //resp.ResponseType = StateLogEntryRedirectResponse.eResponseType.CACHED;
            //var redirectId = this.redirector.StoreRedirect(address); //here we must store redirect data
            //var addedStateLogTermIndex = this.NodeStateLog.AddStateLogEntryForDistribution(req.Data, redirectId);
            //ApplyLogEntry();

            //this.Sender.SendTo(address, eRaftSignalType.StateLogRedirectResponse, resp.SerializeProtobuf(), this.NodeAddress);
        }


        /// <summary>
        /// Leader receives accepted Log
        /// </summary>
        /// <param name="address"></param>
        /// <param name="data"></param>
        void ParseStateLogEntryAccepted(NodeAddress address, byte[] data)
        {

            if (this.NodeState != eNodeState.Leader)
                return;

            StateLogEntryApplied applied = data.DeserializeProtobuf<StateLogEntryApplied>();

            var res = this.NodeStateLog.EntryIsAccepted(GetMajorityQuantity(), applied);

            if (res == StateLog.eEntryAcceptanceResult.Committed)
            {
                this.VerbosePrint($"{this.NodeAddress.NodeAddressId}> LogEntry {applied.StateLogEntryId} is COMMITTED (answer from {address.NodeAddressId})");

                RemoveLeaderLogResendTimer();
                this.NodeStateLog.RemoveEntryFromDistribution(applied.StateLogEntryId, applied.StateLogEntryTerm);
                InLogEntrySend = false;

                //Sending back to the client information that entry is committed
                if (applied.RedirectId > 0)
                {                    
                    var redirectInfo = this.redirector.GetRedirectByIdTerm(applied.RedirectId, applied.StateLogEntryTerm);
                    if (redirectInfo != null && redirectInfo.NodeAddress != null)
                    {
                        StateLogEntryRedirectResponse resp = new StateLogEntryRedirectResponse() {
                            RedirectId = applied.RedirectId,
                            ResponseType = StateLogEntryRedirectResponse.eResponseType.COMMITED
                        };
                        this.Sender.SendTo(redirectInfo.NodeAddress, eRaftSignalType.StateLogRedirectResponse, resp.SerializeProtobuf(), this.NodeAddress);
                    }
                }

                ApplyLogEntry();
            }

        }

        void LeaderLogResendTimerElapse(object userToken)
        {
            try
            {
                lock (lock_Operations)
                {
                    if (this.LeaderLogResend_TimerId == 0)
                        return;
                    RemoveLeaderLogResendTimer();
                    InLogEntrySend = false;
                    ApplyLogEntry();
                }
                
            }
            catch (Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNode.LeaderLogResendTimerElapse" });
            }
        }

        bool InLogEntrySend = false;

        /// <summary>
        /// Tries to apply new entry, must be called from lock
        /// </summary>
        void ApplyLogEntry()
        {
            if (InLogEntrySend)
                return;
            
            var suggest = this.NodeStateLog.AddNextEntryToStateLogByLeader();
            if (suggest == null)
                return;

            VerbosePrint($"{NodeAddress.NodeAddressId} (Leader)> Sending to all (I/T): {suggest.StateLogEntry.Index}/{suggest.StateLogEntry.Term};");

            InLogEntrySend = true;                        
            RunLeaderLogResendTimer();
            this.Sender.SendToAll(eRaftSignalType.StateLogEntrySuggestion, suggest.SerializeProtobuf(), this.NodeAddress);
        }

        /// <summary>
        /// Leader and followers via redirect. (later callback info for followers is needed)
        /// </summary>
        /// <param name="data"></param>
        /// <param name="logEntryExternalId"></param>
        /// <returns></returns>
        public AddLogEntryResult AddLogEntry(byte[] data)
        {
            AddLogEntryResult res = new AddLogEntryResult();

            try
            {                
                lock (lock_Operations)
                {
                    if (this.NodeState == eNodeState.Leader)
                    {
                        res.AddedStateLogTermIndex = this.NodeStateLog.AddStateLogEntryForDistribution(data);                                                
                        ApplyLogEntry();

                        res.AddResult = AddLogEntryResult.eAddLogEntryResult.LOG_ENTRY_IS_CACHED;
                    }
                    else
                    {
                        if (this.LeaderNodeAddress == null)
                            res.AddResult = AddLogEntryResult.eAddLogEntryResult.NO_LEADER_YET;
                        else
                        {
                            res.AddResult = AddLogEntryResult.eAddLogEntryResult.NODE_NOT_A_LEADER;
                            res.LeaderAddress = this.LeaderNodeAddress;

                            //Redirecting                            
                            this.Sender.SendTo(this.LeaderNodeAddress,eRaftSignalType.StateLogRedirectRequest, 
                                (
                                new StateLogEntryRedirectRequest
                                {
                                    Data = data,
                                    RedirectId = this.redirector.StoreRedirect(null)   //!!!!!!!!!!! Later must be enhanced by the address of connected external client
                                }).SerializeProtobuf(), this.NodeAddress);
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNode.AddLogEntryLeader" });
                res.AddResult = AddLogEntryResult.eAddLogEntryResult.ERROR_OCCURED;
            }

            return res;
        }
        

        public void EmulationStop()
        {
            this.NodeStop();
        }

        public void EmulationStart()
        {
            this.NodeStart();
        }

        public void EmulationSendToAll()
        {
            //Sender.SendTo(address, eRaftSignalType.VoteOfCandidate, vote.SerializeProtobuf(), this.NodeAddress);
            Log.Log(new WarningLogEntry() { Description ="not implemented" });
        }

        public void EmulationSetValue(byte[] data)
        {
            this.AddLogEntry(data);
        }
    }//eoc

}//eo ns
