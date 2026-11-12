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

namespace Raft
{
    public static class BiserExtension
    {
        public static byte[] SerializeBiser(this Biser.IEncoder obj)
        {
            return new Biser.Encoder().Add(obj).Encode();            
        }

        public static byte[] SerializeBiser(this IEnumerable<Biser.IEncoder> objs)
        {
            var en = new Biser.Encoder();
            en.Add(objs, r => { en.Add(r); });            
            return en.Encode();

            
        }
    }
}
