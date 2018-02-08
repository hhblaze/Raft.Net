using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport
{
    internal interface ITcpClient
    {
        void Dispose();
    }
}
