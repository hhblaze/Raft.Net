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
    public enum eRaftSignalType
    {
        /// <summary>
        /// Heartbeat which comes to the node from the Leader, followed by LeaderId and payload.
        /// </summary>
        LeaderHearthbeat,
        /// <summary>
        /// Heartbeat which comes from the node who wants to become a candidate (previous known LeaderId+1)
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
        StateLogEntryAccepted,
        /// <summary>
        /// Follower redirects a StateLogEntry from the connected client and expects StateLogRedirectResponse 
        /// </summary>
        StateLogRedirectRequest
       
    }

}
