using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Raft
{
    [ProtoBuf.ProtoContract]
    internal class VoteOfCandidate
    {
        public enum eVoteType
        {
            VoteFor,           
            VoteReject
        }
        public VoteOfCandidate()
        {
            TermId = 0;
            VoteType = eVoteType.VoteFor;
        }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public ulong TermId { get; set; }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public eVoteType VoteType { get; set; }
    }
}
