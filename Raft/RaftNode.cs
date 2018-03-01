/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Raft
{
    /// <summary>
    /// Main class. Initiate and Run.
    /// </summary>
    public class RaftNode : IRaftComReceiver,IDisposable
#if !NETSTANDARD2_0
        , IEmulatedNode
#endif
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
        internal RaftNodeSettings nodeSettings = null;
        IWarningLog Log = null;
        object lock_Operations = new object();        
        internal TimeMaster TM = null;
        internal eNodeState NodeState = eNodeState.Follower;
        
        /// <summary>
        /// 
        /// </summary>
        ulong Election_TimerId = 0;      
        /// <summary>
        /// Stopping this timer only in case if node becomes a leader, and starting when loosing leadership.       
        /// </summary>
        ulong LeaderHeartbeat_TimerId = 0;
        /// <summary>
        /// 
        /// </summary>
        ulong Delayedpersistence_TimerId = 0;
        ulong NoLeaderAddCommand_TimerId = 0;
        /// <summary>
        /// 
        /// </summary>
        ulong Leader_TimerId = 0;
        /// <summary>
        /// 
        /// </summary>
        ulong LeaderLogResend_TimerId = 0;

        Random rnd = new Random();
        //uint VotesQuantity = 0;   //After becoming a candidate
        HashSet<string> VotesQuantity = new HashSet<string>();

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
        ///// <summary>
        ///// 
        ///// </summary>
        //internal RedirectHandler redirector = null;

        /// <summary>
        /// Supplied via constructor. Will be called and supply
        /// </summary>
        Func<string, ulong, byte[], bool> OnCommit = null;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="dbreezePath"></param>
        /// <param name="raftSender"></param>
        /// <param name="log"></param>
        /// <param name="OnCommit"></param>
        public RaftNode(RaftNodeSettings settings, string dbreezePath, IRaftComSender raftSender, IWarningLog log, Func<string, ulong, byte[], bool> OnCommit)
        {

            this.Log = log ?? throw new Exception("Raft.Net: ILog is not supplied");
            this.OnCommit = OnCommit ?? throw new Exception("Raft.Net: OnCommit can'T be null");
                        
            Sender = raftSender;
            nodeSettings = settings;           

            //Starting time master
            this.TM = new TimeMaster(log);
            //Starting state logger
            NodeStateLog = new StateLog(dbreezePath, this);            
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

                if(nodeSettings.DelayedPersistenceIsActive)
                    RunDelayedPersistenceTimer();
            }

            //Tries to executed not yet applied by business logic entries
            Commited();
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
                RemoveDelayedPersistenceTimer();
                RemoveNoLeaderAddCommandTimer();


                this.NodeState = eNodeState.Follower;

                this.NodeStateLog.LeaderSynchronizationIsActive = false;
                this.NodeStateLog.FlushSleCache();
                

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
                    if (NodeState == eNodeState.Leader) //me is the leader
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

                    this.LeaderNodeAddress = null;

                    VerbosePrint("Node {0} state is {1} _ElectionTimeout", NodeAddress.NodeAddressId, this.NodeState);

                    //Voting for self
                    //VotesQuantity = 1;
                    VotesQuantity.Clear();

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

                this.Sender.SendToAll(eRaftSignalType.CandidateRequest, req.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName);
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
                this.Sender.SendToAll(eRaftSignalType.LeaderHearthbeat, heartBeat.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName, true);                

                
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
                            ParseVoteOfCandidate(address, data);
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
                        case eRaftSignalType.StateLogRedirectRequest: //Not a leader node tries to add command
                            ParseStateLogRedirectRequest(address, data);
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

        void RunDelayedPersistenceTimer()
        {
            if (Delayedpersistence_TimerId == 0)
                Delayedpersistence_TimerId = this.TM.FireEventEach(nodeSettings.DelayedPersistenceMs, (o)=> {
                    lock (lock_Operations) { this.NodeStateLog?.FlushSleCache(); } }, null, false);
        }

        void RemoveDelayedPersistenceTimer()
        {
            if (this.Delayedpersistence_TimerId > 0)
            {
                this.TM.RemoveEvent(this.Delayedpersistence_TimerId);
                this.Delayedpersistence_TimerId = 0;
            }
        }
      
        void RunNoLeaderAddCommandTimer()
        {
            if (NoLeaderAddCommand_TimerId == 0)
                NoLeaderAddCommand_TimerId = this.TM.FireEventEach(nodeSettings.NoLeaderAddCommandResendIntervalMs, (o) => {
                    this.AddLogEntry(null);
                }, null, false);
        }

        void RemoveNoLeaderAddCommandTimer()
        {
            if (this.NoLeaderAddCommand_TimerId > 0)
            {
                this.TM.RemoveEvent(this.NoLeaderAddCommand_TimerId);
                this.NoLeaderAddCommand_TimerId = 0;
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

            StateLogEntryRequest req = StateLogEntryRequest.BiserDecode(data);//.DeserializeProtobuf<StateLogEntryRequest>();

            //Getting suggestion
            var suggestion = this.NodeStateLog.GetNextStateLogEntrySuggestionFromRequested(req);

            //VerbosePrint($"{NodeAddress.NodeAddressId} (Leader)> Request (I/T): {req.StateLogEntryId}/{req.StateLogEntryTerm} from {address.NodeAddressId};");
            VerbosePrint($"{NodeAddress.NodeAddressId} (Leader)> Request (I): {req.StateLogEntryId} from {address.NodeAddressId};");

            if (suggestion != null)
            {
                this.Sender.SendTo(address, eRaftSignalType.StateLogEntrySuggestion, suggestion.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName);
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

            StateLogEntrySuggestion suggest = StateLogEntrySuggestion.BiserDecode(data); //data.DeserializeProtobuf<StateLogEntrySuggestion>();

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
                    if (nodeSettings.InMemoryEntity && nodeSettings.InMemoryEntityStartSyncFromLatestEntity && this.NodeStateLog.LastAppliedIndex == 0)
                    {
                        //helps newly starting mode with specific InMemory parameters get only latest command for the entity
                    }
                    else
                    {
                        this.SyncronizeWithLeader();
                        return;
                    }
                }                
            }          

            //We can apply new Log Entry from the Leader and answer successfully
            this.NodeStateLog.AddToLogFollower(suggest);

            StateLogEntryApplied applied = new StateLogEntryApplied()
            {
                 StateLogEntryId = suggest.StateLogEntry.Index,
                 StateLogEntryTerm = suggest.StateLogEntry.Term
                // RedirectId = suggest.StateLogEntry.RedirectId
            };
            
            //this.NodeStateLog.LeaderSynchronizationIsActive = false;

            this.Sender.SendTo(address, eRaftSignalType.StateLogEntryAccepted, applied.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName);          
        }

        /// <summary>
        /// 
        /// </summary>
        void SetNodeFollower()
        {
            if(this.NodeState != eNodeState.Follower)
                VerbosePrint("Node {0} state is {1} of {2}", NodeAddress.NodeAddressId, this.NodeState,this.LeaderNodeAddress?.NodeAddressId);

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
            this.LeaderHeartbeat = LeaderHeartbeat.BiserDecode(data); //data.DeserializeProtobuf<LeaderHeartbeat>();

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

            StateLogEntryRequest req = null;
            if (nodeSettings.InMemoryEntity && nodeSettings.InMemoryEntityStartSyncFromLatestEntity)
            {
                req = new StateLogEntryRequest()
                {
                    StateLogEntryId = this.LeaderHeartbeat.LastStateLogCommittedIndex == 0 ? 0 : this.LeaderHeartbeat.LastStateLogCommittedIndex-1                 
                };
            }
            else
            {
                req = new StateLogEntryRequest()
                {
                    StateLogEntryId = NodeStateLog.LastCommittedIndex                   
                };
            }

            

            this.Sender.SendTo(this.LeaderNodeAddress, eRaftSignalType.StateLogEntryRequest, req.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName);
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
            var req = CandidateRequest.BiserDecode(data); 
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

            Sender.SendTo(address, eRaftSignalType.VoteOfCandidate, vote.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName);
        }

        /// <summary>
        /// Removing from voting and from accepted entity
        /// </summary>
        /// <param name="endpointsid"></param>
        internal void PeerIsDisconnected(string endpointsid)
        {
            lock(lock_Operations)
            {
                if(NodeState == eNodeState.Candidate)
                {
                    VotesQuantity.Remove(endpointsid);
                }

                NodeStateLog.Clear_dStateLogEntryAcceptance_PeerDisconnected(endpointsid);
            }
        }

        /// <summary>
        /// Node receives answer votes (to become Leader) from other nodes.
        /// Is called from tryCatch and in lock
        /// </summary>
        /// <param name="data"></param>
        void ParseVoteOfCandidate(NodeAddress address, byte[] data)
        {
            //Node received a node
            var vote = VoteOfCandidate.BiserDecode(data); 

            var termState = CompareCurrentTermWithIncoming(vote.TermId);

            if (this.NodeState != eNodeState.Candidate)
                return;

            switch (vote.VoteType)
            {
                case VoteOfCandidate.eVoteType.VoteFor:
                    //Calculating if node has Majority of
                   
                    //VotesQuantity++;
                    VotesQuantity.Add(address.EndPointSID);

                    if ((VotesQuantity.Count + 1) >= this.GetMajorityQuantity())
                    {
                        //Majority

                        //Node becomes a Leader
                        this.NodeState = eNodeState.Leader;
                        this.NodeStateLog.FlushSleCache();
                        this.NodeStateLog.ClearLogAcceptance();
                        this.NodeStateLog.ClearLogEntryForDistribution();

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
            StateLogEntryRedirectRequest req = StateLogEntryRedirectRequest.BiserDecode(data);
            //StateLogEntryRedirectResponse resp = new StateLogEntryRedirectResponse(); //{ RedirectId = req.RedirectId };

            if (this.NodeState != eNodeState.Leader)  //Just return
                return;
            
            this.NodeStateLog.AddStateLogEntryForDistribution(req.Data);//, redirectId);
            ApplyLogEntry();

            //Don't answer, committed value wil be delivered via standard channel           
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

            StateLogEntryApplied applied = StateLogEntryApplied.BiserDecode(data);

            var res = this.NodeStateLog.EntryIsAccepted(address, GetMajorityQuantity(), applied);

            if (res == StateLog.eEntryAcceptanceResult.Committed)
            {
                this.VerbosePrint($"{this.NodeAddress.NodeAddressId}> LogEntry {applied.StateLogEntryId} is COMMITTED (answer from {address.NodeAddressId})");

                RemoveLeaderLogResendTimer();


                //Force heartbeat, to make followers to get faster info about commited elements
                LeaderHeartbeat heartBeat= new LeaderHeartbeat()
                {
                    LeaderTerm = this.NodeTerm,
                    StateLogLatestIndex = NodeStateLog.StateLogId,
                    StateLogLatestTerm = NodeStateLog.StateLogTerm,
                    LastStateLogCommittedIndex = this.NodeStateLog.LastCommittedIndex,
                    LastStateLogCommittedIndexTerm = this.NodeStateLog.LastCommittedIndexTerm
                };
                this.Sender.SendToAll(eRaftSignalType.LeaderHearthbeat, heartBeat.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName, true);
                //---------------------------------------

                //this.NodeStateLog.RemoveEntryFromDistribution(applied.StateLogEntryId, applied.StateLogEntryTerm);
                InLogEntrySend = false;

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
        /// Is called from lock_operations
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
            this.Sender.SendToAll(eRaftSignalType.StateLogEntrySuggestion, suggest.SerializeBiser(), this.NodeAddress, nodeSettings.EntityName);
        }

        Queue<byte[]> NoLeaderCache = new Queue<byte[]>();

        /// <summary>
        /// Leader and followers via redirect. (later callback info for followers is needed)
        /// </summary>
        /// <param name="data"></param>
        /// <param name="logEntryExternalId"></param>
        /// <returns></returns>
        public AddLogEntryResult AddLogEntry(byte[] iData)
        {
            AddLogEntryResult res = new AddLogEntryResult();

            try
            {                
                lock (lock_Operations)
                {
                    if(iData != null)
                        NoLeaderCache.Enqueue(iData);

                    if (this.NodeState == eNodeState.Leader)
                    {
                        RemoveNoLeaderAddCommandTimer();
                        
                        while (NoLeaderCache.Count > 0)
                        {
                            this.NodeStateLog.AddStateLogEntryForDistribution(NoLeaderCache.Dequeue());
                            ApplyLogEntry();
                        }

                        res.LeaderAddress = this.NodeAddress;
                        res.AddResult = AddLogEntryResult.eAddLogEntryResult.LOG_ENTRY_IS_CACHED;
                    }
                    else
                    {
                        if (this.LeaderNodeAddress == null)
                        {
                            res.AddResult = AddLogEntryResult.eAddLogEntryResult.NO_LEADER_YET;                            
                            RunNoLeaderAddCommandTimer();
                        }
                        else
                        {
                            RemoveNoLeaderAddCommandTimer();
                            res.AddResult = AddLogEntryResult.eAddLogEntryResult.NODE_NOT_A_LEADER;
                            res.LeaderAddress = this.LeaderNodeAddress;
                            
                            //Redirecting only in case if there is a leader                            
                            while (NoLeaderCache.Count > 0)
                            { 
                                this.Sender.SendTo(this.LeaderNodeAddress, eRaftSignalType.StateLogRedirectRequest,
                                (
                                    new StateLogEntryRedirectRequest
                                    {
                                        Data = NoLeaderCache.Dequeue()                                    
                                    }
                                ).SerializeBiser(), this.NodeAddress, nodeSettings.EntityName);
                            }
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


        int inCommit = 0;
                
        internal void Commited()
        {           
            if (System.Threading.Interlocked.CompareExchange(ref inCommit, 1, 0) != 0)
                return;

            Task.Run(() =>
            {                
                StateLogEntry sle = null;               
                while (true)
                {
                    lock (lock_Operations)
                    {                       
                        if (this.NodeStateLog.LastCommittedIndex == this.NodeStateLog.LastBusinessLogicCommittedIndex)
                        {
                            System.Threading.Interlocked.Exchange(ref inCommit, 0);
                            return;
                        }
                        else
                        {                            
                            sle = this.NodeStateLog.GetCommitedEntryByIndex(this.NodeStateLog.LastBusinessLogicCommittedIndex + 1);
                            if (sle == null)
                            {
                                System.Threading.Interlocked.Exchange(ref inCommit, 0);
                                return;
                            }
                        }
                    }
                    
                    try
                    {
                        if (this.OnCommit(nodeSettings.EntityName, sle.Index, sle.Data))
                        {
                            //In case if business logic commit was successful
                            lock (lock_Operations)
                            {
                                this.NodeStateLog.BusinessLogicIsApplied(sle.Index);
                            }
                        }
                        else
                        {
                            System.Threading.Thread.Sleep(500);
                            //repeating with the same id
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.RaftNode.Commited" });
                    }

                    //i++;
                }
            });            
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

        public void EmulationSetValue(byte[] data, string entityName="default")
        {
            this.AddLogEntry(data);
        }
    }//eoc

}//eo ns
