/*
 In this class all credits to https://github.com/StephenCleary/AsyncEx 
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raft
{
    /// <summary>
    /// Provides interop utilities for <see cref="WaitHandle"/> types.
    /// </summary>
    internal static class WaitHandleAsyncFactory
    {
        /// <summary>
        /// Wraps a <see cref="WaitHandle"/> with a <see cref="Task"/>. When the <see cref="WaitHandle"/> is signalled, the returned <see cref="Task"/> is completed. If the handle is already signalled, this method acts synchronously.
        /// </summary>
        /// <param name="handle">The <see cref="WaitHandle"/> to observe.</param>
        public static Task FromWaitHandle(WaitHandle handle)
        {
            return FromWaitHandle(handle, Timeout.InfiniteTimeSpan, CancellationToken.None);
        }

        /// <summary>
        /// Wraps a <see cref="WaitHandle"/> with a <see cref="Task{Boolean}"/>. If the <see cref="WaitHandle"/> is signalled, the returned task is completed with a <c>true</c> result. If the observation times out, the returned task is completed with a <c>false</c> result. If the handle is already signalled or the timeout is zero, this method acts synchronously.
        /// </summary>
        /// <param name="handle">The <see cref="WaitHandle"/> to observe.</param>
        /// <param name="timeout">The timeout after which the <see cref="WaitHandle"/> is no longer observed.</param>
        public static Task<bool> FromWaitHandle(WaitHandle handle, TimeSpan timeout)
        {
            return FromWaitHandle(handle, timeout, CancellationToken.None);
        }

        /// <summary>
        /// Wraps a <see cref="WaitHandle"/> with a <see cref="Task{Boolean}"/>. If the <see cref="WaitHandle"/> is signalled, the returned task is (successfully) completed. If the observation is cancelled, the returned task is cancelled. If the handle is already signalled or the cancellation token is already cancelled, this method acts synchronously.
        /// </summary>
        /// <param name="handle">The <see cref="WaitHandle"/> to observe.</param>
        /// <param name="token">The cancellation token that cancels observing the <see cref="WaitHandle"/>.</param>
        public static Task FromWaitHandle(WaitHandle handle, CancellationToken token)
        {
            return FromWaitHandle(handle, Timeout.InfiniteTimeSpan, token);
        }

        public static Task<bool> FromWaitHandle(WaitHandle handle, TimeSpan timeout, CancellationToken token)
        {
            // Handle synchronous cases.
            var alreadySignalled = handle.WaitOne(0);
            if (alreadySignalled)
                return TaskConstants.BooleanTrue;
            if (timeout == TimeSpan.Zero)
                return TaskConstants.BooleanFalse;
            if (token.IsCancellationRequested)
                return TaskConstants<bool>.Canceled;

            // Register all asynchronous cases.
            return DoFromWaitHandle(handle, timeout, token);
        }

        private static async Task<bool> DoFromWaitHandle(WaitHandle handle, TimeSpan timeout, CancellationToken token)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (new ThreadPoolRegistration(handle, timeout, tcs))
            using (token.Register(state => ((TaskCompletionSource<bool>)state).TrySetCanceled(), tcs, useSynchronizationContext: false))
                return await tcs.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Provides completed task constants.
        /// </summary>
        public static class TaskConstants
        {
            private static readonly Task<bool> booleanTrue = Task.FromResult(true);
            private static readonly Task<int> intNegativeOne = Task.FromResult(-1);

            /// <summary>
            /// A task that has been completed with the value <c>true</c>.
            /// </summary>
            public static Task<bool> BooleanTrue
            {
                get
                {
                    return booleanTrue;
                }
            }

            public static Task Completed
            {
                get
                {
                    return booleanTrue;
                }
            }

            /// <summary>
            /// A task that has been completed with the value <c>false</c>.
            /// </summary>
            public static Task<bool> BooleanFalse
            {
                get
                {
                    return TaskConstants<bool>.Default;
                }
            }

            /// <summary>
            /// A task that has been completed with the value <c>0</c>.
            /// </summary>
            public static Task<int> Int32Zero
            {
                get
                {
                    return TaskConstants<int>.Default;
                }
            }

            /// <summary>
            /// A task that has been completed with the value <c>-1</c>.
            /// </summary>
            public static Task<int> Int32NegativeOne
            {
                get
                {
                    return intNegativeOne;
                }
            }

            /// <summary>
            /// A task that has been canceled.
            /// </summary>
            public static Task Canceled
            {
                get
                {
                    return TaskConstants<object>.Canceled;
                }
            }
        }

        /// <summary>
        /// Provides completed task constants.
        /// </summary>
        /// <typeparam name="T">The type of the task result.</typeparam>
        public static class TaskConstants<T>
        {
            private static readonly Task<T> defaultValue = Task.FromResult(default(T));
            private static Task<T> canceled = null; //Task.FromCanceled<T>(new CancellationToken(true));

            /// <summary>
            /// A task that has been completed with the default value of <typeparamref name="T"/>.
            /// </summary>
            public static Task<T> Default
            {
                get
                {
                    return defaultValue;
                }
            }

            public static Task<T> Canceled
            {
                get
                {

                    if (canceled == null)
                    {
                        var tcs = new TaskCompletionSource<T>();
                        tcs.SetCanceled();
                        canceled = tcs.Task;
                    }

                    return canceled;
                }

            }


        }


        private sealed class ThreadPoolRegistration : IDisposable
        {
            private readonly RegisteredWaitHandle _registeredWaitHandle;

            public ThreadPoolRegistration(WaitHandle handle, TimeSpan timeout, TaskCompletionSource<bool> tcs)
            {
                _registeredWaitHandle = ThreadPool.RegisterWaitForSingleObject(handle,
                    (state, timedOut) => ((TaskCompletionSource<bool>)state).TrySetResult(!timedOut), tcs,
                    timeout, executeOnlyOnce: true);
            }

            void IDisposable.Dispose() => _registeredWaitHandle.Unregister(null);
        }

    }
}
