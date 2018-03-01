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
    public class AddLogEntryResult
    {
        public enum eAddLogEntryResult
        {
            /// <summary>
            /// Cached to be accepted by majority, but can be rejected in some cases... in any case, client connected to the node waits, until majority confirms save of the LogEntry
            /// </summary>
            LOG_ENTRY_IS_CACHED,
            /// <summary>
            /// If current node is not a leader, then LeaderAddress must be filled
            /// </summary>
            NODE_NOT_A_LEADER,
            /// <summary>
            /// If unexpected error occured
            /// </summary>
            ERROR_OCCURED,
            /// <summary>
            /// If cluster hasn't a leader yet, but command is cached to be sent and timer is on
            /// </summary>
            NO_LEADER_YET,
            NODE_NOT_FOUND_BY_NAME
        }

        public AddLogEntryResult()
        {
            AddResult = eAddLogEntryResult.NO_LEADER_YET;
        }


        public eAddLogEntryResult AddResult { get; set; }

        /// <summary>
        /// If AddResult is NODE_NOT_A_LEADER
        /// </summary>
        public NodeAddress LeaderAddress { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public byte[] AddedStateLogTermIndex { get; set; }

    }
}
