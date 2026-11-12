/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Raft
{
    internal class ResponseCrate
    {
        /// <summary>
        /// Not SLIM version must be used (it works faster for longer delay which RPCs are)
        /// </summary>
        public ManualResetEvent mre = null;
        public byte[] res = null;
        public Action<Tuple<bool, byte[]>> callBack = null;
        public bool IsRespOk = false;

        public AsyncManualResetEvent amre = null;

        public DateTime created = DateTime.UtcNow;
        public int TimeoutsMs = 30000;

        public void Init_MRE()
        {
            mre = new ManualResetEvent(false);
        }

        /// <summary>
        /// Works faster with timer than WaitOneAsync
        /// </summary>
        public void Init_AMRE()
        {
            amre = new AsyncManualResetEvent();
        }

        public void Set_MRE()
        {

            if (mre != null)
            {
                mre.Set();
            }
            else if (amre != null)
            {
                amre.Set();
            }

        }

        public bool WaitOne_MRE(int timeouts)
        {
            //if (Interlocked.Read(ref IsDisposed) == 1 || mre == null)  //No sense
            //    return false;
            return mre.WaitOne(timeouts);
        }

        /// <summary>
        /// Works slower than amre (AsyncManualResetEvent) with the timer
        /// </summary>
        /// <param name="timeouts"></param>
        /// <returns></returns>
        async public Task WaitOneAsync(int timeouts)
        {

            //await resp.amre.WaitAsync();
            await WaitHandleAsyncFactory.FromWaitHandle(mre, TimeSpan.FromMilliseconds(timeouts));
            //await mre.AsTask(TimeSpan.FromMilliseconds(timeouts));
        }

        //async public Task<bool> WaitOneAsync()
        //{
        //    //if (Interlocked.Read(ref IsDisposed) == 1 || amre == null)
        //    //    return false;

        //    return await amre.WaitAsync();

        //}


        long IsDisposed = 0;
        public void Dispose_MRE()
        {
            if (System.Threading.Interlocked.CompareExchange(ref IsDisposed, 1, 0) != 0)
                return;

            if (mre != null)
            {
                mre.Set();
                mre.Dispose();
                mre = null;
            }
            else if (amre != null)
            {
                amre.Set();
                amre = null;
            }
        }

    }
}
