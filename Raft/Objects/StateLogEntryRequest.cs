using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    /// <summary>
    /// Comes from the Follower to Leader in time of state log synchronization
    /// </summary>
    [ProtoBuf.ProtoContract]
    internal class StateLogEntryRequest
    {
        public StateLogEntryRequest()
        {

        }

        /// <summary>
        /// Id of the State log, which Leader must send to the Follower
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public ulong StateLogEntryId { get; set; }
        /// <summary>
        /// Id of the State log, which Leader must send to the Follower
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public ulong StateLogEntryTerm { get; set; }
    }
}
