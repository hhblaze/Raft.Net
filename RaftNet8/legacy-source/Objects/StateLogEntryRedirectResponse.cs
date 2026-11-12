using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DBreeze.Utils;

namespace Raft
{    
    public class StateLogEntryRedirectResponse: Biser.IEncoder
    {
        public enum eResponseType
        {
            NOT_A_LEADER,
            CACHED,
            COMMITED,
            ERROR
        }

        public StateLogEntryRedirectResponse()
        {

        }

        /// <summary>
        /// 
        /// </summary>        
        public eResponseType ResponseType { get; set; } = eResponseType.CACHED;
        
        //public ulong RedirectId { get; set; }

        #region "Biser"
        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add((int)ResponseType)
            //.Add(RedirectId)
            ;
            return enc;
        }

        public static StateLogEntryRedirectResponse BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            StateLogEntryRedirectResponse m = new StateLogEntryRedirectResponse();  //!!!!!!!!!!!!!! change return type

            m.ResponseType = (eResponseType)decoder.GetInt();
            //m.RedirectId = decoder.GetULong();

            return m;
        }
        #endregion

    }
}
