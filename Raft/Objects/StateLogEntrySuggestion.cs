using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    [ProtoBuf.ProtoContract]
    internal class StateLogEntrySuggestion
    {
        public StateLogEntrySuggestion()
        {

        }

        /// <summary>
        /// Current leader TermId, must be always included
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public ulong LeaderTerm { get; set; }

        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public StateLogEntry StateLogEntry { get; set; }

        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public bool IsCommitted { get; set; } = false;
    }
}
