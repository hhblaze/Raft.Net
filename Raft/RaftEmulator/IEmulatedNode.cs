/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
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

        void EmulationSetValue(byte[] data, string entityName = "default");
    }
}
