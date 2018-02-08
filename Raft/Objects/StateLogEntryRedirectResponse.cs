using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    [ProtoBuf.ProtoContract]
    public class StateLogEntryRedirectResponse
    {
        public enum eResponseType
        {
            NOT_A_LEADER,
            CACHED,
            COMMITED,
            ERROR
        }

        public StateLogEntryRedirectResponse()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public eResponseType ResponseType { get; set; } = eResponseType.CACHED;

        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public ulong RedirectId { get; set; }

    }
}
