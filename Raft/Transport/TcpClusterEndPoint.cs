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
    public class TcpClusterEndPoint : Biser.IEncoder
    { 
        public string Host { get; set; } = "127.0.0.1";
             
        public int Port { get; set; } = 4320;
                
        internal bool Me { get; set; } = false;

        public string EndPointSID { get { return Host + ":" + Port; } }

       
        internal TcpPeer Peer { get; set; } = null;


        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(Host)
            .Add(Port)
            ;
            return enc;
        }

        public static TcpClusterEndPoint BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
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

            TcpClusterEndPoint m = new TcpClusterEndPoint();  //!!!!!!!!!!!!!! change return type

            m.Host = decoder.GetString();
            m.Port = decoder.GetInt();

            return m;
        }
    }
}
