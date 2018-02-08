using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    /// <summary>
    /// Node destination address
    /// </summary>
    public class NodeAddress
    {

        public NodeAddress()
        {            
            NodeAddressId = -1;
        }

        /// <summary>
        /// 
        /// </summary>
        public long NodeAddressId { get; set; }

        /// <summary>
        /// GUID, substring 4,8 converted to int64
        /// Helps to resolve priority conflicts
        /// </summary>
        public long NodeUId { get; set; }

        public string EndPointSID { get; set; }

        ///// <summary>
        ///// Node which represents self instance of RaftNode
        ///// </summary>
        //public bool IsMe = false;

        ///// <summary>
        ///// IpEndPoint of the node (used in RaftNodeUdp)
        ///// </summary>
        //public IPEndPoint IpEP = null;

    }
}
