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
        public ulong AppliedLogEntryIndex { get; set; }

        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public ulong AppliedLogEntryTerm { get; set; }
    }
}
