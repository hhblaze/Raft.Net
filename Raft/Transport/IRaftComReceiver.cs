using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    /// <summary>
    /// Receiver of incoming messages concerning RAFT protocol
    /// </summary>
    public interface IRaftComReceiver
    {
        
        /// <param name="address">Address of the node-sender</param>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        void IncomingSignalHandler(NodeAddress address, eRaftSignalType signalType, byte[] data);
    }
}
