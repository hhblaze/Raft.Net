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
    internal class LeaderHeartbeat:Biser.IEncoder
    {
        public LeaderHeartbeat()
        {
            LeaderTerm = 0;
        }

        /// <summary>
        /// Leader's current Term
        /// </summary>        
        public ulong LeaderTerm { get; set; }

        /// <summary>
        /// Latest inserted into Leader State Log Term
        /// </summary>        
        public ulong StateLogLatestTerm { get; set; }

        /// <summary>
        /// Latest inserted into Leader State Log ID
        /// </summary>        
        public ulong StateLogLatestIndex { get; set; }

        /// <summary>
        /// Leader includes LastCommitted Index, Followers must apply if it's bigger than theirs
        /// </summary>        
        public ulong LastStateLogCommittedIndex { get; set; }

        /// <summary>
        /// Leader includes LastCommitted Term, Followers must apply if it's bigger than theirs
        /// </summary>        
        public ulong LastStateLogCommittedIndexTerm { get; set; }


        #region "Biser"
        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(LeaderTerm)
            .Add(StateLogLatestTerm)
            .Add(StateLogLatestIndex)
            .Add(LastStateLogCommittedIndex)
            .Add(LastStateLogCommittedIndexTerm)
            ;
            return enc;
        }

        public static LeaderHeartbeat BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
        {
            Biser.Decoder decoder = null;
            if (extDecoder == null)
            {
                if (enc == null || enc.Length == 0)
                    return null;
                decoder = new Biser.Decoder(enc);
                if (decoder.CheckNull())
                    return null;
            }
            else
            {
                decoder = new Biser.Decoder(extDecoder);
                if (decoder.IsNull)
                    return null;
            }

            LeaderHeartbeat m = new LeaderHeartbeat();  //!!!!!!!!!!!!!! change return type

            m.LeaderTerm = decoder.GetULong();
            m.StateLogLatestTerm = decoder.GetULong();
            m.StateLogLatestIndex = decoder.GetULong();
            m.LastStateLogCommittedIndex = decoder.GetULong();
            m.LastStateLogCommittedIndexTerm = decoder.GetULong();
            

            return m;
        }
        #endregion

    }
}
