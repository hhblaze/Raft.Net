using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    /// <summary>
    /// Sender of messages, concerning Raft protocol
    /// </summary>
    public interface IRaftComSender
    {
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  WHEN SENDING TO ALL CHECK, THAT IT DOESN'T SEND TO ITSELF

        /// <summary>
        /// 
        /// </summary>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        void SendToAll(eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, bool highPriority = false);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="nodeAddress">Address of the node, signal recipient</param>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        void SendTo(NodeAddress nodeAddress,eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress);
    }
}
