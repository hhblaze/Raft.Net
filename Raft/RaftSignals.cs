﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    public enum eRaftSignalType
    {
        /// <summary>
        /// Heartbeat which comes to the node from the Leader, followed by LeaderId and payload.
        /// </summary>
        LeaderHearthbeat,
        /// <summary>
        /// Heartbeat which comes from the node who want to become a candidate (previous known LeaderId+1)
        /// </summary>
        CandidateRequest,
        /// <summary>
        /// Answer on Vote of the candidate request
        /// </summary>
        VoteOfCandidate,           

        /// <summary>
        /// Follower requests new State Log Entry from the Leader
        /// </summary>
        StateLogEntryRequest,
        /// <summary>
        /// Leader sends to the Follower requested LogEntry (and waits acceptance)
        /// </summary>
        StateLogEntrySuggestion,
        /// <summary>
        /// Sent by Follower to Leader, to acknowledge logEntry acceptance
        /// </summary>
        StateLogEntryAccepted
        

        //THEN WAITING FOR COMMIT SIGNAL





        ///// <summary>
        ///// Nodes-followers ask Leader to send them a LogEntry 
        ///// (
        /////     it happens either on node start after getting LeaderHeartbeat !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!, containing current committed Log TermId and Log Index
        /////     or
        /////     after receiving from the Leader StateLogSuggestion (new Log Entry)
        ///// )
        ///// </summary>
        //StateLogRequest
        ///// <summary>
        ///// Leader sends to all information that it has new command for the StateLog.
        ///// </summary>
        //StateLogSuggestion,
    }

}