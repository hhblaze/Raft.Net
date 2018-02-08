using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
     [ProtoBuf.ProtoContract]
    internal class StateLogEntryApplied
    {
        public StateLogEntryApplied()
        {
        }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public ulong StateLogEntryId { get; set; }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public ulong StateLogEntryTerm { get; set; }

        /// <summary>
        /// Requested client redirect id
        /// </summary>
        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public ulong RedirectId { get; set; }
    }
}
