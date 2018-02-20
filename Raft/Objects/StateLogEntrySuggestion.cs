using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DBreeze.Utils;

namespace Raft
{    
    internal class StateLogEntrySuggestion : Biser.IEncoder
    {
        public StateLogEntrySuggestion()
        {

        }

        /// <summary>
        /// Current leader TermId, must be always included
        /// </summary> 
        public ulong LeaderTerm { get; set; }
                
        public StateLogEntry StateLogEntry { get; set; }
                
        public bool IsCommitted { get; set; } = false;

        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(LeaderTerm)
            .Add(StateLogEntry)
            .Add(IsCommitted)
            ;
            return enc;
        }

        public static StateLogEntrySuggestion BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            StateLogEntrySuggestion m = new StateLogEntrySuggestion();  //!!!!!!!!!!!!!! change return type

            m.LeaderTerm = decoder.GetULong();
            m.StateLogEntry = StateLogEntry.BiserDecode(extDecoder: decoder);
            m.IsCommitted = decoder.GetBool();

            return m;
        }
    }
}
