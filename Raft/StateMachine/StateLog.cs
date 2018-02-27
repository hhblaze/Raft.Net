using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DBreeze;
using DBreeze.Utils;

namespace Raft
{
    
    internal class StateLog : IDisposable
    {
        class StateLogEntryAcceptance
        {
            public StateLogEntryAcceptance()
            {
                Quantity = 0;
            }

            /// <summary>
            /// Accepted Quantity
            /// </summary>
            public uint Quantity { get; set; }
            /// <summary>
            /// StateLogEntry Index
            /// </summary>
            public ulong Index { get; set; }
            /// <summary>
            /// StateLogEntry Term
            /// </summary>
            public ulong Term { get; set; }

          
        }

        /// <summary>
        /// Main table that stores logs
        /// </summary>
        const string tblStateLogEntry = "RaftTbl_StateLogEntry";
        ///// <summary>
        ///// Leader only. Stores logs before being distributed.
        ///// </summary>
        //const string tblAppendLogEntry = "RaftTbl_AppendLogEntry";
        
        internal RaftNode rn = null;
        DBreezeEngine db = null;
        
        /// <summary>
        /// Holds last committed State Log Index.
        /// If node is leader it sets it, if follower it comes with the Leader's heartbeat
        /// </summary>
        public ulong LastCommittedIndex = 0;
        /// <summary>
        /// Holds last committed State Log Term.
        /// If node is leader it sets it, if follower it comes with the Leader's heartbeat
        /// </summary>
        public ulong LastCommittedIndexTerm = 0;
        /// <summary>
        /// For leaders
        /// Applied entry is a majority of servers can be from previous term. 
        /// It will have the same index as LastCommitted only in case if on majority of servers will be stored at least one entry from current term. Docu.
        /// </summary>
        public ulong LastAppliedIndex = 0;
        /// <summary>
        /// Monotonically grown StateLog Index.
        /// 
        /// </summary>
        public ulong StateLogId = 0;
        public ulong StateLogTerm = 0;

        public ulong PreviousStateLogId = 0;
        public ulong PreviousStateLogTerm = 0;
        
        /// <summary>
        /// Follower part. State Log synchro with Leader.
        /// Indicates that synchronization request was sent to Leader
        /// </summary>
        public bool LeaderSynchronizationIsActive = false;
        /// <summary>
        /// Follower part. State Log synchro with Leader.
        /// Registers DateTime when synchronization request was sent.
        /// Until timeout (LeaderSynchronizationTimeOut normally 1 minute)- no more requests 
        /// </summary>
        public DateTime LeaderSynchronizationRequestWasSent = DateTime.Now;
        /// <summary>
        /// Follower part. State Log synchro with Leader.
        /// </summary>
        public uint LeaderSynchronizationTimeOut = 1;        
        /// <summary>
        /// Only for the Leader.
        /// Key is StateLogEntryId, Value contains information about how many nodes accepted LogEntry
        /// </summary>
        Dictionary<ulong, StateLogEntryAcceptance> dStateLogEntryAcceptance = new Dictionary<ulong, StateLogEntryAcceptance>();              


        public StateLog(string dbreezePath, RaftNode rn)
        {
            this.rn = rn;
            
            if(String.IsNullOrEmpty(dbreezePath))
                db = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
            else
                db = new DBreezeEngine(dbreezePath);

            using (var t = db.GetTransaction())
            {
                var row = t.SelectBackwardFromTo<byte[], byte[]>(tblStateLogEntry,
                    new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue),true, 
                    new byte[] { 1 }.ToBytes(ulong.MinValue, ulong.MinValue), true)
                    .FirstOrDefault();

                StateLogEntry sle = null;
                if (row != null && row.Exists)
                {
                    sle = StateLogEntry.BiserDecode(row.Value);
                    StateLogId = sle.Index;
                    StateLogTerm = sle.Term;
                    PreviousStateLogId = sle.PreviousStateLogId;
                    PreviousStateLogTerm = sle.PreviousStateLogTerm;

                    tempPrevStateLogId = PreviousStateLogId;
                    tempPrevStateLogTerm = PreviousStateLogTerm;
                    tempStateLogId = StateLogId;
                    tempStateLogTerm = StateLogTerm;

                    rn.NodeTerm = sle.Term;
                }
                var rowTerm = t.Select<byte[], byte[]>(tblStateLogEntry, new byte[] { 2 });
                if (rowTerm.Exists)
                {
                    LastCommittedIndex = rowTerm.Value.Substring(0, 8).To_UInt64_BigEndian();
                    LastCommittedIndexTerm = rowTerm.Value.Substring(8, 8).To_UInt64_BigEndian();
                }

            }
        }

        /// <summary>
        /// Secured by RaftNode
        /// </summary>
        public void Dispose()
        {
            if(db != null)
            {
                db.Dispose();
                db = null;
            }
        }

        /// <summary>
        /// Returns null if nothing to distribute
        /// </summary>
        /// <returns></returns>
        StateLogEntrySuggestion GetNextLogEntryToBeDistributed()
        {
            if (qDistribution.Count < 1)
                return null;
            
            return new StateLogEntrySuggestion()
            {
                StateLogEntry = qDistribution.Dequeue(),
                LeaderTerm = rn.NodeTerm
            };
            

            ///*
            // * Only nodes of the current term can be distributed
            // */
            //StateLogEntrySuggestion sles = null;
            //using (var t = db.GetTransaction())
            //{
            //    var row = t.SelectForwardFromTo<byte[], byte[]>(tblAppendLogEntry, 
            //        new byte[] { 1 }.ToBytes(rn.NodeTerm, ulong.MinValue), true,
            //        new byte[] { 1 }.ToBytes(rn.NodeTerm, ulong.MaxValue), true)
            //        .FirstOrDefault();
                
            //    if(row != null && row.Exists)
            //    {
            //        sles = new StateLogEntrySuggestion()
            //        {
            //            StateLogEntry = StateLogEntry.BiserDecode(row.Value),
            //            LeaderTerm = rn.NodeTerm
            //        };                    
            //    }
            //}

            //return sles;
        }


        ulong tempPrevStateLogId = 0;
        ulong tempPrevStateLogTerm = 0;
        ulong tempStateLogId = 0;
        ulong tempStateLogTerm = 0;


        /// <summary>
        /// Leader only.Stores logs before being distributed.
        /// </summary>
        Queue<StateLogEntry> qDistribution = new Queue<StateLogEntry>();

        /// <summary>
        /// Is called from lock_operations
        /// Adds to silo table, until is moved to log table.
        /// This table can be cleared up on start
        /// returns concatenated term+index inserted identifier
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public void AddStateLogEntryForDistribution(byte[] data)//, ulong redirectId=0)
        {
            /*
             * Only nodes of the current term can be distributed
             */

            tempPrevStateLogId = tempStateLogId;
            tempPrevStateLogTerm = tempStateLogTerm;

            tempStateLogId++;
            tempStateLogTerm = rn.NodeTerm;
            

            StateLogEntry le = new StateLogEntry()
            {
                Index = tempStateLogId,
                Data = data,
                Term = tempStateLogTerm,                
                PreviousStateLogId = tempPrevStateLogId,
                PreviousStateLogTerm = tempPrevStateLogTerm
                //RedirectId = redirectId
            };

            qDistribution.Enqueue(le);

            //using (var t = db.GetTransaction())
            //{
            //    t.Insert<byte[], byte[]>(tblAppendLogEntry, new byte[] { 1 }.ToBytes(tempStateLogTerm, tempStateLogId), le.SerializeBiser());
            //    t.Commit();
            //}

            //return tempStateLogTerm.ToBytes(tempStateLogId);
        }

        public void ClearLogEntryForDistribution()
        {
            qDistribution.Clear();
        }

        /// <summary>
        /// Copyies from distribution silo table and puts in StateLog table
        /// </summary>
        /// <returns></returns>
        public StateLogEntrySuggestion AddNextEntryToStateLogByLeader()
        {
            var suggest = GetNextLogEntryToBeDistributed();
            if (suggest == null)
                return null;

            //Restoring current values
            PreviousStateLogId = suggest.StateLogEntry.PreviousStateLogId;
            PreviousStateLogTerm = suggest.StateLogEntry.PreviousStateLogTerm;
            StateLogId = suggest.StateLogEntry.Index;
            StateLogTerm = suggest.StateLogEntry.Term;
            
            using (var t = db.GetTransaction())
            {
                t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 1 }.ToBytes(suggest.StateLogEntry.Index, suggest.StateLogEntry.Term), suggest.StateLogEntry.SerializeBiser());
                t.Commit();
            }

            return suggest;

        }

        ///// <summary>
        ///// Removes from distribution silo if committed (that correct entry could be picked up on GetNextLogEntryToBeDistributed)
        ///// </summary>
        ///// <param name="stateLogId"></param>
        ///// <param name="stateLogTerm"></param>
        //public void RemoveEntryFromDistribution(ulong stateLogId, ulong stateLogTerm)
        //{
        //    using (var t = db.GetTransaction())
        //    {
        //        t.RemoveKey<byte[]>(tblAppendLogEntry, new byte[] { 1 }.ToBytes(stateLogTerm, stateLogId));
        //        t.Commit();             
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="lhb"></param>
        /// <returns>will return false if node needs synchronization from LastCommittedIndex/Term</returns>
        public bool SetLastCommittedIndexFromLeader(LeaderHeartbeat lhb)
        {
            if (this.LastCommittedIndex < lhb.LastStateLogCommittedIndex)
            {
                //Node tries to understand if it contains already this index/term, if not it will need synchronization
                //StateLogEntry committedSle = null;
                //List<byte[]> lstCommited = new List<byte[]>();
                ulong populateFrom = 0;
                using (var t = db.GetTransaction())
                {
                    t.ValuesLazyLoadingIsOn = false;
                    var row = t.Select<byte[], byte[]>(tblStateLogEntry, (new byte[] { 1 }).ToBytes(lhb.LastStateLogCommittedIndex, lhb.LastStateLogCommittedIndexTerm));
                    if(row.Exists)
                    {
                        populateFrom = this.LastCommittedIndex + 1;
                        ////Gathering all not commited entries that a bigger than latest commited index of the committed term
                        //foreach (var el in t.SelectForwardFromTo<byte[], byte[]>(tblStateLogEntry,
                        //   new byte[] { 1 }.ToBytes(this.LastCommittedIndex+1, lhb.LastStateLogCommittedIndexTerm), true,
                        //   new byte[] { 1 }.ToBytes(ulong.MaxValue, lhb.LastStateLogCommittedIndexTerm), true, true))
                        //{
                        //    lstCommited.Add(StateLogEntry.BiserDecode(row.Value).Data);
                        //}
                        
                        this.LastCommittedIndex = lhb.LastStateLogCommittedIndex;
                        this.LastCommittedIndexTerm = lhb.LastStateLogCommittedIndexTerm;

                        //rn.VerbosePrint($"{rn.NodeAddress.NodeAddressId}> AddToLogFollower (I/T): here");

                        t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 2 }, lhb.LastStateLogCommittedIndex.ToBytes(lhb.LastStateLogCommittedIndexTerm));
                        t.Commit();
                    }
                    else
                        return false;
                }

                if (populateFrom > 0)
                    this.rn.Commited(populateFrom);

                //if(lstCommited.Count > 0)
                //{
                //    //---RAISE UP COMMITTED DATA
                //    if (this.rn.OnCommit != null)
                //        Task.Run(() => { this.rn.OnCommit(lstCommited); });
                //}

            }

            return true;
        }


        /// <summary>
        /// +
        /// Only Follower makes it. Clears its current Log
        /// Clearing 
        /// </summary>
        /// <param name="logEntryId"></param>       
        public void ClearStateLogStartingFromCommitted()
        {
            if (LastCommittedIndex == 0 || LastCommittedIndexTerm == 0)
                return;
            using (var t = db.GetTransaction())
            {
                //Removing from the persisted all keys equal or bigger then suppled Log
                foreach (var el in t.SelectForwardFromTo<byte[], byte[]>(tblStateLogEntry,
                            new byte[] { 1 }.ToBytes(LastCommittedIndex, LastCommittedIndexTerm), false,                            
                            new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue), true, true))
                {
                    t.RemoveKey<byte[]>(tblStateLogEntry, el.Key);
                }

                t.Commit();
            }            
        }
        
        /// <summary>
        /// +
        /// Can be null.
        /// Must be called inside of operation lock.
        /// </summary>
        /// <param name="logEntryId"></param>
        /// <param name="LeaderTerm"></param>
        /// <returns></returns>
        public StateLogEntrySuggestion GetNextStateLogEntrySuggestionFromRequested(StateLogEntryRequest req)
        {
            StateLogEntrySuggestion le = new StateLogEntrySuggestion()
            { 
                LeaderTerm = rn.NodeTerm             
            };

            int cnt = 0;
            StateLogEntry sle = null;
            ulong prevId = 0;
            ulong prevTerm = 0;

            using (var t = db.GetTransaction())
            {
                
                if(req.StateLogEntryId == 0 && req.StateLogEntryTerm == 0)
                {
                    var trow = t.SelectForwardFromTo<byte[], byte[]>(tblStateLogEntry,
                    new byte[] { 1 }.ToBytes(ulong.MinValue, ulong.MinValue), true,
                    new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue), true).FirstOrDefault();

                    if(trow != null && trow.Exists)
                    {
                        sle = StateLogEntry.BiserDecode(trow.Value);
                        le.StateLogEntry = sle;

                        if (
                            LastCommittedIndexTerm >= le.StateLogEntry.Term
                            &&
                            LastCommittedIndex >= le.StateLogEntry.Index
                            )
                        {
                            le.IsCommitted = true;
                        }

                        cnt = 2;
                    }
                    else
                    {
                        //should not normally happen
                    }
                   
                }
                else
                {
                    foreach (var el in t.SelectForwardFromTo<byte[], byte[]>(tblStateLogEntry,
                           new byte[] { 1 }.ToBytes(req.StateLogEntryId, req.StateLogEntryTerm), true,
                           new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue), true).Take(2))
                    {
                        cnt++;
                        sle = StateLogEntry.BiserDecode(el.Value);
                        if (cnt == 1)
                        {
                            prevId = sle.Index;
                            prevTerm = sle.Term;
                        }
                        else
                        {
                            le.StateLogEntry = sle;
                        }
                    }

                    if (cnt == 2)
                    {
                        le.StateLogEntry.PreviousStateLogId = prevId;
                        le.StateLogEntry.PreviousStateLogTerm = prevTerm;
                        if (
                            LastCommittedIndexTerm >= le.StateLogEntry.Term
                            &&
                            LastCommittedIndex >= le.StateLogEntry.Index
                            )
                        {
                            le.IsCommitted = true;
                        }
                    }
                }

                
                
            }

            //if (first)
            if (cnt != 2)
                return null;
            return le;
          
        }


        /// <summary>
        /// +
        /// Get Term by EntryLogIndex. Returns First element false if not found, Second - Term (if found).
        /// Must be called inside of operation lock.
        /// </summary>
        /// <param name="logId"></param>
        /// <returns>first element true if exists</returns>
        public StateLogEntry GetEntryByIndexTerm(ulong logEntryId, ulong logEntryTerm)
        {
            try
            {
                using (var t = db.GetTransaction())
                {

                    var row = t.Select<byte[], byte[]>(tblStateLogEntry, new byte[] { 1 }.ToBytes(logEntryId, logEntryTerm));
                    if (!row.Exists)
                    {
                        //foreach (var el in t.SelectForwardStartsWith<byte[], byte[]>(tblStateLogEntry, new byte[] { 1 }))
                        //{
                        //    Console.WriteLine($"{rn.NodeAddress.NodeAddressId}> GetEntryByIndexTerm-NULL: {el.Key.ToBytesString()}");
                        //}
                        return null;
                    }

                    return StateLogEntry.BiserDecode(row.Value);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
          
            
        }

        /// <summary>
        /// +
        /// Must be called inside of operation lock.
        /// </summary>
        /// <param name="logEntryId"></param>
        /// <returns></returns>
        public StateLogEntry GetCommitedEntryByIndex(ulong logEntryId)
        {
            try
            {  
                if (this.LastCommittedIndex < logEntryId)
                    return null;
                StateLogEntry ret = null;

                using (var t = db.GetTransaction())
                {
                   
                    foreach (var el in t.SelectForwardFromTo<byte[], byte[]>(tblStateLogEntry,
                                new byte[] { 1 }.ToBytes(logEntryId, ulong.MinValue), true,
                                new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue), true, true))
                    {
                        ret = StateLogEntry.BiserDecode(el.Value);
                        break;
                    }
                    
                }

                return ret;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        /// <summary>
        /// +
        /// Must be called inside of operation lock.
        /// Follower only.
        /// </summary>
        /// <param name="logEntry"></param>
        public void AddToLogFollower(StateLogEntrySuggestion suggestion)
        {
        
            try
            {
                using (var t = db.GetTransaction())
                {
                    //Removing from the persisted all keys equal or bigger then supplied Log (also from other terms)
                    foreach (var el in t.SelectForwardFromTo<byte[], byte[]>(tblStateLogEntry,                                
                                new byte[] { 1 }.ToBytes(suggestion.StateLogEntry.Index, ulong.MinValue), true,
                                new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue), true,true))
                    {
                        t.RemoveKey<byte[]>(tblStateLogEntry, el.Key);
                    }
                   
                    t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 1 }.ToBytes(suggestion.StateLogEntry.Index, suggestion.StateLogEntry.Term), suggestion.StateLogEntry.SerializeBiser());
                    t.Commit();
                }

                rn.VerbosePrint($"{rn.NodeAddress.NodeAddressId}> AddToLogFollower (I/T): {suggestion.StateLogEntry.Index}/{suggestion.StateLogEntry.Term} -> Result:" +
                    $" { (GetEntryByIndexTerm(suggestion.StateLogEntry.Index, suggestion.StateLogEntry.Term) != null)};");

                //Setting new internal LogId
                PreviousStateLogId = StateLogId;
                PreviousStateLogTerm = StateLogTerm;
                StateLogId = suggestion.StateLogEntry.Index;
                StateLogTerm = suggestion.StateLogEntry.Term;

                tempPrevStateLogId = PreviousStateLogId;
                tempPrevStateLogTerm = PreviousStateLogTerm;
                tempStateLogId = StateLogId;
                tempStateLogTerm = StateLogTerm;

                if (suggestion.IsCommitted)
                {
                    if (
                        this.LastCommittedIndexTerm > suggestion.StateLogEntry.Term
                        ||
                        (
                            this.LastCommittedIndexTerm == suggestion.StateLogEntry.Term
                            &&
                            this.LastCommittedIndex > suggestion.StateLogEntry.Index
                         )
                    )
                    {
                        //Should be not possible
                    }
                    else
                    {
                        this.LastCommittedIndex = suggestion.StateLogEntry.Index;
                        this.LastCommittedIndexTerm = suggestion.StateLogEntry.Term;

                        this.rn.Commited(suggestion.StateLogEntry.Index);                      
                    }
                }

               
            }
            catch (Exception ex)
            {
                
            }

            if (this.LastCommittedIndex < rn.LeaderHeartbeat.LastStateLogCommittedIndex)
            {
                rn.SyncronizeWithLeader(true);
            }
            else
                LeaderSynchronizationIsActive = false;
        }

        public enum eEntryAcceptanceResult
        {
            NotAccepted,
            Committed,
            AlreadyAccepted,
            Accepted
        }
        /// <summary>
        /// +
        /// Only Leader's proc.
        /// Accepts entry return true if Committed
        /// </summary>
        /// <param name="majorityNumber"></param>
        /// <param name="LogId"></param>
        /// <param name="TermId"></param>        
        public eEntryAcceptanceResult EntryIsAccepted(uint majorityQuantity, StateLogEntryApplied applied)
        {
            //If we receive acceptance signals of already Committed entries, we just ignore them
            if (applied.StateLogEntryId <= this.LastCommittedIndex)
                return  eEntryAcceptanceResult.AlreadyAccepted;    //already accepted
            if (applied.StateLogEntryId <= this.LastAppliedIndex)
                return eEntryAcceptanceResult.AlreadyAccepted;    //already accepted
                        
            StateLogEntryAcceptance acc = null;

            if (dStateLogEntryAcceptance.TryGetValue(applied.StateLogEntryId, out acc))
            {
                if (acc.Term != applied.StateLogEntryTerm)
                    return eEntryAcceptanceResult.NotAccepted;   //Came from wrong Leader probably

                acc.Quantity += 1;              
            }
            else
            {
                acc = new StateLogEntryAcceptance()
                {
                     Quantity = 2,  //Leader + first incoming
                     Index = applied.StateLogEntryId,
                     Term = applied.StateLogEntryTerm
                };

                dStateLogEntryAcceptance[applied.StateLogEntryId] = acc;
            }
            
                     
            if (acc.Quantity >= majorityQuantity)
            {
                this.LastAppliedIndex = applied.StateLogEntryId;
                //Removing from Dictionary
                dStateLogEntryAcceptance.Remove(applied.StateLogEntryId);

                if (this.LastCommittedIndex < applied.StateLogEntryId && rn.NodeTerm == applied.StateLogEntryTerm)    //Setting LastCommittedId
                {
                    //Saving committed entry (all previous are automatically committed)
                    List<byte[]> lstCommited = new List<byte[]>();

                    using (var t = db.GetTransaction())
                    {
                        // t.SynchronizeTables(tblStateLogEntry, tblAppendLogEntry);
                        t.SynchronizeTables(tblStateLogEntry);

                        //Setting latest commited index
                        t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 2 }, applied.StateLogEntryId.ToBytes(applied.StateLogEntryTerm));
                        //Removing entry from command queue
                        //t.RemoveKey<byte[]>(tblAppendLogEntry, new byte[] { 1 }.ToBytes(applied.StateLogEntryTerm, applied.StateLogEntryId));


                        //Gathering all not commited entries that are bigger than latest committed index
                        t.ValuesLazyLoadingIsOn = false;
                        foreach (var el in t.SelectForwardFromTo<byte[], byte[]>(tblStateLogEntry,
                           new byte[] { 1 }.ToBytes(this.LastCommittedIndex + 1, applied.StateLogEntryTerm), true,
                           new byte[] { 1 }.ToBytes(ulong.MaxValue, applied.StateLogEntryTerm), true, true))
                        {
                            lstCommited.Add(StateLogEntry.BiserDecode(el.Value).Data);
                        }

                        t.Commit();
                    }

                    this.LastCommittedIndex = applied.StateLogEntryId;
                    this.LastCommittedIndexTerm = applied.StateLogEntryTerm;

                    if (lstCommited.Count > 0)
                    {
                        this.rn.Commited(applied.StateLogEntryId);
                    }

                    return eEntryAcceptanceResult.Committed;
                }
            }

            return eEntryAcceptanceResult.Accepted;

        }

        /// <summary>
        /// +
        /// When node becomes a Leader, it clears acceptance log
        /// </summary>
        public void ClearLogAcceptance()
        {
            this.dStateLogEntryAcceptance.Clear();
        }
               
       

    }
}
