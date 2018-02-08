using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    [ProtoBuf.ProtoContract]
    internal class LeaderHeartbeat
    {
        public LeaderHeartbeat()
        {
            LeaderTerm = 0;
        }

        /// <summary>
        /// Leader's current Term
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public ulong LeaderTerm { get; set; }

        /// <summary>
        /// Latest inserted into Leader State Log Term
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public ulong StateLogLatestTerm { get; set; }

        /// <summary>
        /// Latest inserted into Leader State Log ID
        /// </summary>
        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public ulong StateLogLatestIndex { get; set; }

        /// <summary>
        /// Leader includes LastCommitted Index, Followers must apply if it's bigger then theirs
        /// </summary>
        [ProtoBuf.ProtoMember(4, IsRequired = true)]
        public ulong LastStateLogCommittedIndex { get; set; }

        /// <summary>
        /// Leader includes LastCommitted Term, Followers must apply if it's bigger then theirs
        /// </summary>
        [ProtoBuf.ProtoMember(5, IsRequired = true)]
        public ulong LastStateLogCommittedIndexTerm { get; set; }

    }
}
