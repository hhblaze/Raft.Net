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

        const string tblNewLogEntry = "RaftTbl_NewLogEntry";
        const string tblStateLogEntry = "RaftTbl_StateLogEntry";

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
        ///// <summary>
        ///// 
        ///// </summary>
        //public ulong LastCommittedTermId = 0;

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
            
            db = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
            //db = new DBreezeEngine(dbreezePath);

            using (var t = db.GetTransaction())
            {
                var row = t.SelectBackwardFromTo<byte[], byte[]>(tblStateLogEntry,
                    new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue),true, 
                    new byte[] { 1 }.ToBytes(ulong.MinValue, ulong.MinValue), true)
                    .FirstOrDefault();

                StateLogEntry sle = null;
                if (row != null && row.Exists)
                {
                    sle = row.Value.DeserializeProtobuf<StateLogEntry>();
                    StateLogId = sle.Index;
                    StateLogTerm = sle.Term;

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

        public void Dispose()
        {
            if(db != null)
            {
                db.Dispose();
                db = null;
            }
        }

        /// <summary>
        /// +
        /// Leader only. When client wants to syncronize new command it makes it via Leader Node
        /// Is done inside of operation lock
        /// </summary>
        /// <param name="nodeTerm"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public StateLogEntry AddEntryToStateLogByLeader(byte[] data, ulong stateLogEntryExternalId)
        {
            PreviousStateLogId = StateLogId;
            PreviousStateLogTerm = StateLogTerm;
            StateLogId++;
            StateLogTerm = rn.NodeTerm;

            StateLogEntry le = new StateLogEntry()
            {
                Index = StateLogId,
                Data = data,
                Term = rn.NodeTerm,
                ExternalId = stateLogEntryExternalId,
                PreviousStateLogId = PreviousStateLogId, 
                PreviousStateLogTerm = PreviousStateLogTerm
            };

            byte[] fdf = le.SerializeProtobuf();

            using(var t = db.GetTransaction())
            {
                t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 1 }.ToBytes(le.Index, le.Term), le.SerializeProtobuf());
                t.Commit();
            }
            
            return le;
        }
        
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
                using (var t = db.GetTransaction())
                {
                    var row = t.Select<byte[], byte[]>(tblStateLogEntry, (new byte[] { 1 }).ToBytes(lhb.LastStateLogCommittedIndex, lhb.LastStateLogCommittedIndexTerm));
                    if(row.Exists)
                    {
                        this.LastCommittedIndex = lhb.LastStateLogCommittedIndex;
                        this.LastCommittedIndexTerm = lhb.LastStateLogCommittedIndexTerm;

                        t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 2 }, lhb.LastStateLogCommittedIndex.ToBytes(lhb.LastStateLogCommittedIndexTerm));
                        t.Commit();
                    }
                    else
                        return false;
                }
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
                        sle = trow.Value.DeserializeProtobuf<StateLogEntry>();
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
                        sle = el.Value.DeserializeProtobuf<StateLogEntry>();
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

                    return row.Value.DeserializeProtobuf<StateLogEntry>();
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
                                //new byte[] { 1 }.ToBytes(suggestion.StateLogEntry.Index, suggestion.StateLogEntry.Term), true,
                                new byte[] { 1 }.ToBytes(suggestion.StateLogEntry.Index, ulong.MinValue), true,
                                new byte[] { 1 }.ToBytes(ulong.MaxValue, ulong.MaxValue), true,true))
                    {
                        t.RemoveKey<byte[]>(tblStateLogEntry, el.Key);
                    }
                   
                    t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 1 }.ToBytes(suggestion.StateLogEntry.Index, suggestion.StateLogEntry.Term), suggestion.StateLogEntry.SerializeProtobuf());
                    t.Commit();
                }

                rn.VerbosePrint($"{rn.NodeAddress.NodeAddressId}> AddToLogFollower (I/T): {suggestion.StateLogEntry.Index}/{suggestion.StateLogEntry.Term} -> Result:" +
                    $" { (GetEntryByIndexTerm(suggestion.StateLogEntry.Index, suggestion.StateLogEntry.Term) != null)};");

                //Setting new internal LogId
                PreviousStateLogId = StateLogId;
                PreviousStateLogTerm = StateLogTerm;
                StateLogId = suggestion.StateLogEntry.Index;
                StateLogTerm = suggestion.StateLogEntry.Term;

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
            if (applied.AppliedLogEntryIndex <= this.LastCommittedIndex)
                return  eEntryAcceptanceResult.AlreadyAccepted;    //already accepted
            if (applied.AppliedLogEntryIndex <= this.LastAppliedIndex)
                return eEntryAcceptanceResult.AlreadyAccepted;    //already accepted
                        
            StateLogEntryAcceptance acc = null;

            if (dStateLogEntryAcceptance.TryGetValue(applied.AppliedLogEntryIndex, out acc))
            {
                if (acc.Term != applied.AppliedLogEntryTerm)
                    return eEntryAcceptanceResult.NotAccepted;   //Came from wrong Leader probably

                acc.Quantity += 1;              
            }
            else
            {
                acc = new StateLogEntryAcceptance()
                {
                     Quantity = 2,  //Leader + first incoming
                     Index = applied.AppliedLogEntryIndex,
                     Term = applied.AppliedLogEntryTerm
                };

                dStateLogEntryAcceptance[applied.AppliedLogEntryIndex] = acc;
            }
            
                     
            if (acc.Quantity >= majorityQuantity)
            {
                this.LastAppliedIndex = applied.AppliedLogEntryIndex;
                //Removing from Dictionary
                dStateLogEntryAcceptance.Remove(applied.AppliedLogEntryIndex);

                if (this.LastCommittedIndex < applied.AppliedLogEntryIndex && rn.NodeTerm == applied.AppliedLogEntryTerm)    //Setting LastCommittedId
                {
                    this.LastCommittedIndex = applied.AppliedLogEntryIndex;
                    this.LastCommittedIndexTerm = applied.AppliedLogEntryTerm;

                    //Saving committed entry (all previous are automatically committed)
                    
                    using (var t = db.GetTransaction())
                    {
                        t.Insert<byte[], byte[]>(tblStateLogEntry, new byte[] { 2 }, this.LastCommittedIndex.ToBytes(this.LastCommittedIndexTerm));
                        t.Commit();
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
