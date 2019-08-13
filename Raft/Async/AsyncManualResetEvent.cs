/*
 In this class all credits to https://github.com/StephenCleary/AsyncEx 
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    internal class AsyncManualResetEvent
    {
        // can be used WaitHandleAsyncFactory.cs (From WaitHandle)

        private volatile TaskCompletionSource<bool> _tcs = new TaskCompletionSource<bool>();
        private readonly object _mutex;

        public AsyncManualResetEvent()
        {
            _mutex = new object();
        }

        public Task<bool> WaitAsync()
        {
            lock (_mutex)
            {
                return _tcs.Task;
            }
        }


        public void Set()
        {
            lock (_mutex)
            {
                _tcs.TrySetResult(true);
            }
        }

        public void Reset()
        {
            lock (_mutex)
            {
                if (_tcs.Task.IsCompleted)
                    _tcs = new TaskCompletionSource<bool>();
            }
        }

    }
}
