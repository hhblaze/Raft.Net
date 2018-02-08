using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;

namespace Raft
{
    public class TimeMaster:IDisposable
    {
        System.Timers.Timer tmr = new System.Timers.Timer();
        ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();
        ulong eventId = 0;
        IWarningLog Log = null;
        Dictionary<ulong, EventToStore> Events = new Dictionary<ulong, EventToStore>();
        const double DefaultTimerInterval = 1000 * 60 * 60; //1 hour                
        /// <summary>
        /// Sets minimal elapsed interval in ms. Default is 30 ms.
        /// </summary>
        public double MinimalIntervalInMs = 30;
        bool disposed = false;

        public TimeMaster(IWarningLog log)
        {
            if (log == null)
                throw new Exception("ILog is not supplied");

            Log = log;

            tmr.Elapsed += tmr_Elapsed;
            tmr.Interval = DefaultTimerInterval;   //Default interval
            tmr.Start();
        }

        void tmr_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            tmr.Stop();
            tmr.Enabled = false;

            if (disposed)
                return;

            try
            {
                List<ulong> ids2remove = new List<ulong>();

                _sync.EnterWriteLock();
                try
                {
                    var now = DateTime.UtcNow;
                    double msPassed = 0;

                    foreach (var el in Events)
                    {
                        msPassed = (now - el.Value.ElapseAt).TotalMilliseconds;

                        if (msPassed > 0)                        
                        {
                            //if (el.Value.Name == "LEADER")
                            //{
                            //    //Console.WriteLine("Elapsed: " + now.ToString("HH:mm:ss.ms"));
                            //    Console.WriteLine("Must elapse: " + el.Value.ElapseAt);
                            //}

                            if (el.Value.RepeatOnce)
                            {
                                ids2remove.Add(el.Key);
                            }
                            else
                            {
                                el.Value.ElapseAt = now.AddMilliseconds(el.Value.Milliseconds);
                                //if (el.Value.Name == "LEADER")
                                //{
                                //    //Console.WriteLine("Elapsed: " + now.ToString("HH:mm:ss.ms"));
                                //    Console.WriteLine("Next elapse: " + el.Value.ElapseAt);
                                //}
                            }

                            Task.Run(() =>
                            {
                                el.Value.Action(el.Value.UserToken);
                            });
                        }
                    }

                    if (ids2remove.Count() > 0)
                        ids2remove.ForEach(r => Events.Remove(r));

                    this.RecalculateTimer();

                }
                finally
                {
                    _sync.ExitWriteLock();
                }

            }
            catch (Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.TimeMaster.tmr_Elapsed" });
            }

            
            tmr.Enabled = true;
            tmr.Start();
        }

        class EventToStore
        {
            public DateTime ElapseAt = DateTime.UtcNow;
            public object UserToken=null;
            public Action<object> Action { get; set; }
            public bool RepeatOnce { get; set; }
            public uint Milliseconds { get; set; }
            public string Name { get; set; } = "";
        }


        /// <summary>
        /// Will call "action" and supplies to it "userToken" in "milliseconds"
        /// </summary>
        /// <param name="milliseconds"></param>
        /// <param name="fireAnObject"></param>
        /// <param name="repeatOnce"></param>
        /// <returns>returns ID of the created event; If 0 - then mistake</returns>
        public ulong FireEventEach(uint milliseconds, Action<object> action, object userToken, bool repeatOnce, string eventName="")
        {
            if (milliseconds < MinimalIntervalInMs)
                throw new Exception("Minimal interval is " + this.MinimalIntervalInMs + " ms.");

            _sync.EnterWriteLock();
            try
            {
                var elapseAt = DateTime.UtcNow.AddMilliseconds(milliseconds);
                eventId++;

                Events.Add(eventId, new EventToStore()
                {
                    ElapseAt = elapseAt,
                    Action = action,
                    RepeatOnce = repeatOnce,
                    UserToken = userToken,
                    Milliseconds = milliseconds,
                    Name = eventName
                });

                RecalculateTimer();
            }
            catch (System.Exception ex)
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.TimeMaster.FireEventAfter" });
            }
            finally
            {
                _sync.ExitWriteLock();
            }

            return eventId;
        }

        /// <summary>
        /// Must be called ONLY from _sync
        /// </summary>
        void RecalculateTimer()
        {
            try
            {
                if (Events.Count() < 1)
                    tmr.Interval = DefaultTimerInterval;
                else
                {
                    var dt = Events.Min(r => r.Value.ElapseAt);
                    double interval = (dt - DateTime.UtcNow).TotalMilliseconds;

                    if (interval < MinimalIntervalInMs)
                        interval = MinimalIntervalInMs;

                    tmr.Interval = interval;

                    //Console.WriteLine("Interval: " + interval);

                }
            }
            catch (Exception ex) 
            {
                Log.Log(new WarningLogEntry() { Exception = ex, Method = "Raft.TimeMaster.RecalculateTimer" });
            }
          
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="eventId"></param>
        public void RemoveEvent(ulong eventId)
        {

            _sync.EnterWriteLock();
            try
            {
                Events.Remove(eventId);
                RecalculateTimer();
            }
            finally
            {
                _sync.ExitWriteLock();
            }

        }

        public void RemoveAllEvents()
        {
            _sync.EnterWriteLock();
            try
            {
                Events.Clear();
                RecalculateTimer();
            }
            finally
            {
                _sync.ExitWriteLock();
            }
        }
        
        public void Dispose()
        {
            disposed = true;
            tmr.Stop();
        }
    }
}
