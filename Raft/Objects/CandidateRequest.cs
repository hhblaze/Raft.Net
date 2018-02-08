using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    [ProtoBuf.ProtoContract]
    internal class CandidateRequest
    {
        public CandidateRequest()
        {
            TermId = 0;
            LastLogId = 0;
            LastTermId = 0;
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
        public ulong LastLogId { get; set; }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public ulong LastTermId { get; set; }
    }
}
