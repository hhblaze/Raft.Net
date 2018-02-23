using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DBreeze.Utils;

namespace Raft
{   
    public class StateLogEntryRedirectRequest : Biser.IEncoder
    {
        public StateLogEntryRedirectRequest()
        {

        }

        /// <summary>
        /// 
        /// </summary>   
        public byte[] Data { get; set; }
                
        //public ulong RedirectId { get; set; }

        #region "Biser"
        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(Data)
            //.Add(RedirectId)
            ;
            return enc;
        }

        public static StateLogEntryRedirectRequest BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            StateLogEntryRedirectRequest m = new StateLogEntryRedirectRequest();  //!!!!!!!!!!!!!! change return type

            m.Data = decoder.GetByteArray();
            //m.RedirectId = decoder.GetULong();
            

            return m;
        }
        #endregion
    }
}
