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
    
    internal class CandidateRequest: Biser.IEncoder
    {
        public CandidateRequest()
        {
            TermId = 0;
            LastLogId = 0;
            LastTermId = 0;
        }

        /// <summary>
        /// 
        /// </summary>    
        public ulong TermId { get; set; }

        /// <summary>
        /// 
        /// </summary>        
        public ulong LastLogId { get; set; }

        /// <summary>
        /// 
        /// </summary>        
        public ulong LastTermId { get; set; }

        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(TermId)
            .Add(LastLogId)
            .Add(LastTermId)
            ;
            return enc;
        }

        public static CandidateRequest BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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
                if (extDecoder.CheckNull())
                    return null;
                else
                    decoder = extDecoder;
            }

            CandidateRequest m = new CandidateRequest();  //!!!!!!!!!!!!!! change return type

            m.TermId = decoder.GetULong();
            m.LastLogId = decoder.GetULong();
            m.LastTermId = decoder.GetULong();           

            return m;
        }
    }
}
