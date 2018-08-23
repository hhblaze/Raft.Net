/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBreeze.Utils;

namespace Raft
{
    public class RaftEntitySettings : Biser.IJsonEncoder
    {
        public RaftEntitySettings()
        {

        }

        /// <summary>
        /// First entity must always to have name "default" (or will be automatically changed to "default")
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
        /// Speeds up processing events on HDD by flush changes on disk onces per DelayedPersistenceMs for the non InMemoryEntity
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

        public void BiserJsonEncode(Biser.JsonEncoder encoder)
        {
            encoder.Add("EntityName", this.EntityName);
                //encoder.Add("LeaderHeartbeatMs", this.LeaderHeartbeatMs);
            encoder.Add("DelayedPersistenceMs", this.DelayedPersistenceMs);
                //encoder.Add("NoLeaderAddCommandResendIntervalMs", this.NoLeaderAddCommandResendIntervalMs);
            encoder.Add("DelayedPersistenceIsActive", this.DelayedPersistenceIsActive);
            encoder.Add("InMemoryEntity", this.InMemoryEntity);
            encoder.Add("InMemoryEntityStartSyncFromLatestEntity", this.InMemoryEntityStartSyncFromLatestEntity);
                //encoder.Add("ElectionTimeoutMinMs", this.ElectionTimeoutMinMs);
                //encoder.Add("ElectionTimeoutMaxMs", this.ElectionTimeoutMaxMs);
                //encoder.Add("LeaderLogResendIntervalMs", this.LeaderLogResendIntervalMs);
                //encoder.Add("RaftNodeIdExternalForEmulator", this.RaftNodeIdExternalForEmulator);
                //encoder.Add("InitialQuantityOfRaftNodesInTheCluster", this.InitialQuantityOfRaftNodesInTheCluster);
            encoder.Add("VerboseRaft", this.VerboseRaft);
            encoder.Add("VerboseTransport", this.VerboseTransport);
        }

        public static RaftEntitySettings BiserJsonDecode(string enc = null, Biser.JsonDecoder extDecoder = null, Biser.JsonSettings settings = null) //!!!!!!!!!!!!!! change return type
        {
            Biser.JsonDecoder decoder = null;

            if (extDecoder == null)
            {
                if (enc == null || String.IsNullOrEmpty(enc))
                    return null;
                decoder = new Biser.JsonDecoder(enc, settings);
                if (decoder.CheckNull())
                    return null;
            }
            else
            {
                //JSONSettings of the existing decoder will be used
                decoder = extDecoder;
            }

            RaftEntitySettings m = new RaftEntitySettings();  //!!!!!!!!!!!!!! change return type
            foreach (var props in decoder.GetDictionary<string>())
            {
                switch (props)
                {
                    case "EntityName":
                        m.EntityName = decoder.GetString();
                        break;
                    //case "LeaderHeartbeatMs":
                    //    m.LeaderHeartbeatMs = decoder.GetUInt();
                    //    break;
                    case "DelayedPersistenceMs":
                        m.DelayedPersistenceMs = decoder.GetUInt();
                        break;
                    //case "NoLeaderAddCommandResendIntervalMs":
                    //    m.NoLeaderAddCommandResendIntervalMs = decoder.GetUInt();
                    //    break;
                    case "DelayedPersistenceIsActive":
                        m.DelayedPersistenceIsActive = decoder.GetBool();
                        break;
                    case "InMemoryEntity":
                        m.InMemoryEntity = decoder.GetBool();
                        break;
                    case "InMemoryEntityStartSyncFromLatestEntity":
                        m.InMemoryEntityStartSyncFromLatestEntity = decoder.GetBool();
                        break;                    
                    //case "ElectionTimeoutMinMs":
                    //    m.ElectionTimeoutMinMs = decoder.GetInt();
                    //    break;
                    //case "ElectionTimeoutMaxMs":
                    //    m.ElectionTimeoutMaxMs = decoder.GetInt();
                    //    break;
                    //case "LeaderLogResendIntervalMs":
                    //    m.LeaderLogResendIntervalMs = decoder.GetUInt();
                    //    break;
                    //case "RaftNodeIdExternalForEmulator":
                    //    m.RaftNodeIdExternalForEmulator = decoder.GetInt();
                    //    break;
                    //case "InitialQuantityOfRaftNodesInTheCluster":
                    //    m.InitialQuantityOfRaftNodesInTheCluster = decoder.GetUInt();
                    //    break;                    
                    case "VerboseRaft":
                        m.VerboseRaft = decoder.GetBool();
                        break;
                    case "VerboseTransport":
                        m.VerboseTransport = decoder.GetBool();
                        break;
                    default:
                        decoder.SkipValue();//MUST BE HERE
                        break;
                }
            }
            return m;
        }//eof

    }//eoc
}//eon
