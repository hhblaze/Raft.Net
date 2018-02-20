using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DBreeze.Utils;

namespace Raft
{   
    internal class VoteOfCandidate :Biser.IEncoder
    {
        public enum eVoteType
        {
            VoteFor,           
            VoteReject
        }
        public VoteOfCandidate()
        {
            TermId = 0;
            VoteType = eVoteType.VoteFor;
        }

        /// <summary>
        /// 
        /// </summary>       
        public ulong TermId { get; set; }

        /// <summary>
        /// 
        /// </summary>        
        public eVoteType VoteType { get; set; }


        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(TermId)
            .Add((int)VoteType)            
            ;
            return enc;
        }

        public static VoteOfCandidate BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            VoteOfCandidate m = new VoteOfCandidate();  //!!!!!!!!!!!!!! change return type

            m.TermId = decoder.GetULong();
            m.VoteType = (eVoteType)decoder.GetInt();
         
            return m;
        }
    }
}
