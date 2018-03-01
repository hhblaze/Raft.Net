/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DBreeze.Utils;

namespace Raft.Transport
{    
    public class TcpMsgRaft : Biser.IEncoder
    {
        public TcpMsgRaft()
        {

        }

        /// <summary>
        /// from Raft.eRaftSignalType
        /// </summary>      
        public eRaftSignalType RaftSignalType { get; set; }
                
        public byte[] Data { get; set; }

        public string EntityName { get; set; } = "default";

    public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add((int)RaftSignalType)
            .Add(Data)
            .Add(EntityName)
            ;
            return enc;
        }

        public static TcpMsgRaft BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            TcpMsgRaft m = new TcpMsgRaft();  //!!!!!!!!!!!!!! change return type

            m.RaftSignalType = (eRaftSignalType)decoder.GetInt();
            m.Data = decoder.GetByteArray();
            m.EntityName = decoder.GetString();

            return m;
        }
    }

}
