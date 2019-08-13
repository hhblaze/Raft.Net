/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBreeze.Utils;

namespace Raft
{
    internal static class AsyncResponseHandler
    {
        static object Sync = new object();
        static ulong MessageIdCnt = 0;

        public static ConcurrentDictionary<string, ResponseCrate> df = new ConcurrentDictionary<string, ResponseCrate>();

        public static byte[] GetMessageId()
        {
            lock (Sync)
            {
                MessageIdCnt++;
                return DateTime.UtcNow.To_8_bytes_array().Concat(MessageIdCnt.To_8_bytes_array_BigEndian());
            }
        }
    }
}
