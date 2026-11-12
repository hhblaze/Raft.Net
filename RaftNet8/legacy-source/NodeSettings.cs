using Raft.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBreeze.Utils;

namespace Raft
{
    public class NodeSettings:Biser.IJsonEncoder
    {
        /// <summary>
        /// List of Raft Entities that will use one TCP Transport to get the same state for all entites
        /// </summary>
        public List<RaftEntitySettings> RaftEntitiesSettings { get; set; } = new List<RaftEntitySettings>();
        /// <summary>
        /// Quantity of Cluster EndPoints is used to get majority of servers to Commit Entity
        /// </summary>
        public List<TcpClusterEndPoint> TcpClusterEndPoints { get; set; } = new List<TcpClusterEndPoint>();

        public void BiserJsonEncode(Biser.JsonEncoder encoder)
        {
            encoder.Add("RaftEntitiesSettings", RaftEntitiesSettings, (r) => { encoder.Add(r); });
            encoder.Add("TcpClusterEndPoints", TcpClusterEndPoints, (r) => { encoder.Add(r); });
        }

        public static NodeSettings BiserJsonDecode(string enc = null, Biser.JsonDecoder extDecoder = null, Biser.JsonSettings settings = null) //!!!!!!!!!!!!!! change return type
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

            NodeSettings m = new NodeSettings();  //!!!!!!!!!!!!!! change return type
            foreach (var props in decoder.GetDictionary<string>())
            {
                switch (props)
                {
                    case "RaftEntitiesSettings":
                        m.RaftEntitiesSettings = decoder.CheckNull() ? null : new List<RaftEntitySettings>();
                        if (m.RaftEntitiesSettings != null)
                        {
                            foreach (var el in decoder.GetList())
                                m.RaftEntitiesSettings.Add(RaftEntitySettings.BiserJsonDecode(null, decoder));
                        }
                        break;
                    case "TcpClusterEndPoints":
                        m.TcpClusterEndPoints = decoder.CheckNull() ? null : new List<TcpClusterEndPoint>();
                        if (m.TcpClusterEndPoints != null)
                        {
                            foreach (var el in decoder.GetList())
                                m.TcpClusterEndPoints.Add(TcpClusterEndPoint.BiserJsonDecode(null, decoder));
                        }
                        break;                   
                    default:
                        decoder.SkipValue();//MUST BE HERE
                        break;
                }
            }
            return m;
        }//eof
    }//eoc
}//eon
