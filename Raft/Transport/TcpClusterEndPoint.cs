using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport
{
    [ProtoBuf.ProtoContract]
    public class TcpClusterEndPoint
    {
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public string Host { get; set; } = "127.0.0.1";

        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public int Port { get; set; } = 4320;
                
        internal bool Me { get; set; } = false;

        public string EndPointSID { get { return Host + ":" + Port; } }

       
        internal TcpPeer Peer { get; set; } = null;
    }
}
