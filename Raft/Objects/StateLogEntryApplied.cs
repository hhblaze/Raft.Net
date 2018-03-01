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
     
    internal class StateLogEntryApplied : Biser.IEncoder
    {
        public StateLogEntryApplied()
        {
        }

        /// <summary>
        /// 
        /// </summary>        
        public ulong StateLogEntryId { get; set; }

        /// <summary>
        /// 
        /// </summary>        
        public ulong StateLogEntryTerm { get; set; }
        

        #region "Biser"
        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(StateLogEntryId)
            .Add(StateLogEntryTerm)
            
            ;
            return enc;
        }

        public static StateLogEntryApplied BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            StateLogEntryApplied m = new StateLogEntryApplied();  //!!!!!!!!!!!!!! change return type

            m.StateLogEntryId = decoder.GetULong();
            m.StateLogEntryTerm = decoder.GetULong();
           

            return m;
        }
        #endregion
    }
}
