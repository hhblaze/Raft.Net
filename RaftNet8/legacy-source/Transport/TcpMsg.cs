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

namespace Raft.Transport
{
    
    public class TcpMsg : Biser.IEncoder
    {
        public TcpMsg()
        {
            MsgType = "Default";
        }
                
        public string MsgType { get; set; }
                
        public byte[] Data { get; set; }
                
        public string DataString { get; set; }

        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(MsgType)
            .Add(Data)
            .Add(DataString)
            ;
            return enc;
        }

        public static TcpMsg BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            TcpMsg m = new TcpMsg();  //!!!!!!!!!!!!!! change return type

            m.MsgType = decoder.GetString();
            m.Data = decoder.GetByteArray();
            m.DataString = decoder.GetString();

            return m;
        }
    }
}
