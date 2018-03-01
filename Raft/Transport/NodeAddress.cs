/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
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
        

    }
}
