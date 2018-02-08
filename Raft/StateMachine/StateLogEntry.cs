using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    [ProtoBuf.ProtoContract]
    public class StateLogEntry
    {
        public StateLogEntry()
        {
            Term = 0;
            Index = 0;
            IsCommitted = false;
        }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public ulong Term { get; set; }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public ulong Index { get; set; }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public byte[] Data { get; set; }

        /// <summary>
        /// Id supplied by the client, to understand in async response that this data was applied
        /// </summary>
        [ProtoBuf.ProtoMember(4, IsRequired = true)]
        public ulong ExternalId { get; set; }

        /// <summary>
        /// If value is committed by Leader
        /// </summary>
        [ProtoBuf.ProtoMember(5, IsRequired = true)]
        public bool IsCommitted { get; set; }

        /// <summary>
        /// Out of protobuf
        /// </summary>
        [ProtoBuf.ProtoMember(6, IsRequired = true)]
        public ulong PreviousStateLogId = 0;
        /// <summary>
        /// Out of protobuf
        /// </summary>
        [ProtoBuf.ProtoMember(7, IsRequired = true)]
        public ulong PreviousStateLogTerm = 0;

    }
}
