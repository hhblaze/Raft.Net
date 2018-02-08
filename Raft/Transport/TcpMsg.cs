using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport
{
    [ProtoBuf.ProtoContract]
    public class TcpMsg
    {
        public TcpMsg()
        {
            MsgType = "Default";
        }

        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public string MsgType { get; set; }

        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public byte[] Data { get; set; }

        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public string DataString { get; set; }
    }
}
