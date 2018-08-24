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
    public class StateLogEntry: Biser.IEncoder
    {
        public StateLogEntry()
        {
            Term = 0;
            Index = 0;
            IsCommitted = false;
        }

        /// <summary>
        /// 
        /// </summary>        
        public ulong Term { get; set; }

        /// <summary>
        /// 
        /// </summary>        
        public ulong Index { get; set; }

        /// <summary>
        /// 
        /// </summary>        
        public byte[] Data { get; set; }

        /// <summary>
        /// If value is committed by Leader
        /// </summary>        
        public bool IsCommitted { get; set; }

        /// <summary>
        /// Out of protobuf
        /// </summary>        
        public ulong PreviousStateLogId = 0;
        /// <summary>
        /// Out of protobuf
        /// </summary>        
        public ulong PreviousStateLogTerm = 0;

        //public ulong RedirectId = 0;


        #region "Biser"
        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(Term)
            .Add(Index)
            .Add(Data)
            .Add(IsCommitted)
            .Add(PreviousStateLogId)
            .Add(PreviousStateLogTerm)          
            ;
            return enc;
        }

        public static StateLogEntry BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            StateLogEntry m = new StateLogEntry();  //!!!!!!!!!!!!!! change return type

            m.Term = decoder.GetULong();
            m.Index = decoder.GetULong();
            m.Data = decoder.GetByteArray();
            m.IsCommitted = decoder.GetBool();
            m.PreviousStateLogId = decoder.GetULong();
            m.PreviousStateLogTerm = decoder.GetULong();         

            return m;
        }

        #endregion
    }
}
