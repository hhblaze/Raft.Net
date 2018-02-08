using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    public class RaftNodeSettings
    {
        public RaftNodeSettings()
        {

        }

        /// <summary>
        /// Leader heartbeat interval in ms.
        /// !!!!!!!!!!!!! Note that via Leader heartbeat are 
        /// </summary>
        public uint LeaderHeartbeatMs = 1000 * 5;
        //public uint LeaderHeartbeatMs = 1000 * 5;

        /// <summary>
        /// in ms.
        /// </summary>
        public int ElectionTimeoutMinMs = 200;
        //public int ElectionTimeoutMinMs = 500;

        /// <summary>
        /// in ms.
        /// </summary>
        public int ElectionTimeoutMaxMs = 600;
        //public int ElectionTimeoutMaxMs = 2000;

        /// <summary>
        /// Exceptionally for emulators
        /// </summary>
        public int RaftNodeIdExternalForEmulator = 0;

        /// <summary>
        /// Initial value for calculating majority
        /// </summary>
        public uint InitialQuantityOfRaftNodesInTheCluster = 3;

        /// <summary>
        /// Log Raft debug info
        /// </summary>
        public bool VerboseRaft = false;

        /// <summary>
        /// Log Transport debug info
        /// </summary>
        public bool VerboseTransport = false;
    }
}
