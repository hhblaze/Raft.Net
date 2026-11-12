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
    public class TcpClusterEndPoint : Biser.IEncoder, Biser.IJsonEncoder
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
                if (extDecoder.CheckNull())
                    return null;
                else
                    decoder = extDecoder;
            }

            TcpClusterEndPoint m = new TcpClusterEndPoint();  //!!!!!!!!!!!!!! change return type

            m.Host = decoder.GetString();
            m.Port = decoder.GetInt();

            return m;
        }

        public void BiserJsonEncode(Biser.JsonEncoder encoder)
        {
            encoder.Add("Host", this.Host);
            encoder.Add("Port",this.Port);
        }

        public static TcpClusterEndPoint BiserJsonDecode(string enc = null, Biser.JsonDecoder extDecoder = null, Biser.JsonSettings settings = null) //!!!!!!!!!!!!!! change return type
        {
            Biser.JsonDecoder decoder = null;

            if (extDecoder == null)
            {
                if (enc == null || String.IsNullOrEmpty(enc))
                    return null;
                decoder = new Biser.JsonDecoder(enc, settings);
                if (decoder.CheckNull())
                    return null;
            }
            else
            {
                //JSONSettings of the existing decoder will be used
                decoder = extDecoder;
            }

            TcpClusterEndPoint m = new TcpClusterEndPoint();  //!!!!!!!!!!!!!! change return type
            foreach (var props in decoder.GetDictionary<string>())
            {
                switch (props)
                {
                    case "Host":
                        m.Host = decoder.GetString();
                        break;
                    case "Port":
                        m.Port = decoder.GetInt();
                        break;
                    default:
                        decoder.SkipValue();//MUST BE HERE
                        break;
                }
            }
            return m;
        }//eof
    }
}
