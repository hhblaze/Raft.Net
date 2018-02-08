using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport.UdpServer
{
    public class UdpTester
    {
        //int _port = 40000;
        UdpSocketListener l = null;

        public UdpTester(TimeMaster tm, IWarningLog log)
        {           
            l = new UdpSocketListener(tm, log);         
        }

        public void Start(int port)
        {
            l.Start(port);
        }

        public void Stop(int port)
        {
            l.Stop();
        }
    }
}
