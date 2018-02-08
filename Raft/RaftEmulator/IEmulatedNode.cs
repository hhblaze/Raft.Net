using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    /// <summary>
    /// Sender of messages, concerning Raft protocol
    /// </summary>
    public interface IEmulatedNode
    {       
        void EmulationStop();

        void EmulationStart();

        //Emulation node sends to all greeting
        void EmulationSendToAll();

        void EmulationSetValue(byte[] data);
    }
}
