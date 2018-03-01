/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        /// <param name="senderNodeAddress"></param>
        /// <param name="entityName"></param>
        /// <param name="highPriority"></param>
        void SendToAll(eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName, bool highPriority = false);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="nodeAddress"></param>
        /// <param name="signalType"></param>
        /// <param name="data"></param>
        /// <param name="senderNodeAddress"></param>
        /// <param name="entityName"></param>
        void SendTo(NodeAddress nodeAddress,eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName);
    }
}
