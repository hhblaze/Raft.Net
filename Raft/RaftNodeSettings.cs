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
        /// First entity must be always default (it will be automatically changed on default)
        /// the others must be unique
        /// </summary>
        public string EntityName = "default";        

        /// <summary>
        /// Leader heartbeat interval in ms.
        /// </summary>
        public uint LeaderHeartbeatMs = 1000 * 5;
        //public uint LeaderHeartbeatMs = 1000 * 5;

        public uint DelayedPersistenceMs = 1000 * 10;

        public uint NoLeaderAddCommandResendIntervalMs = 1000 * 1;

        /// <summary>
        /// Speeds up processing on HDD's will flush changes on disk onces per DelayedPersistenceMs for the non InMemoryEntity
        /// </summary>
        public bool DelayedPersistenceIsActive = false;

        /// <summary>
        /// Is completely handled in-memory
        /// </summary>
        public bool InMemoryEntity = false;
        /// <summary>
        /// Only in case if it is an InMemoryEntity it can ask to get sync (after long sleep) starting from latest leader index/term
        /// </summary>
        public bool InMemoryEntityStartSyncFromLatestEntity = false;

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
        /// If leader doesn't receive COMMIT on log-entry within interval it tries to resend it.
        /// </summary>
        public uint LeaderLogResendIntervalMs = 1000 * 3;

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
