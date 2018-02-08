using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{    
    [ProtoBuf.ProtoContract]
    public class StateLogEntryRedirectRequest
    {
        public StateLogEntryRedirectRequest()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public byte[] Data { get; set; }

        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public ulong RedirectId { get; set; }

    }
}
