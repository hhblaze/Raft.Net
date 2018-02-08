using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport
{
    [ProtoBuf.ProtoContract]
    public class TcpMsgRaft
    {
        public TcpMsgRaft()
        {

        }

        /// <summary>
        /// from Raft.eRaftSignalType
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public int RaftSignalType { get; set; }
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public byte[] Data { get; set; }
    }
}
