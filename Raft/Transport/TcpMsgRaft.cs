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
        public int RaftSignalType { get; set; }
                
        public byte[] Data { get; set; }

        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(RaftSignalType)
            .Add(Data)
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

            m.RaftSignalType = decoder.GetInt();
            m.Data = decoder.GetByteArray();

            return m;
        }
    }

    public class TestSer
    {        
        public static void Test()
        {
            //this class (StateLogEntryRedirectRequest) implements Biser.IDecoder and is decribed in the bottom
            TcpMsgHandshake voc = new TcpMsgHandshake()
            {
                NodeListeningPort = 44,
                 NodeUID =long.MaxValue
            };

            //Biser.Encoder enc = new Biser.Encoder().Add(voc);
            var btEnc = new Biser.Encoder().Add(voc).Encode();
            var des = TcpMsgHandshake.BiserDecode(btEnc);

        }
    }


}
