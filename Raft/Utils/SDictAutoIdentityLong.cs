/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Utils
{
    internal class SDictAutoIdentityLong<TValue> : SDictionary<long, TValue>
    {

        long _safeLongIncreaser = 0;

        DateTime lastCheckedRemove = DateTime.Now;

        /// <summary>
        /// Remove helper. Leaves only last inserted values in quantity of topX. 
        /// Second param helps to balance check interval in seconds.
        /// </summary>
        /// <param name="topX"></param>
        /// <param name="secondsCheckInterval"></param>
        public void DeleteKeysLessThenTop(int topX, long secondsCheckInterval)
        {

            if (lastCheckedRemove.AddSeconds(secondsCheckInterval) < DateTime.Now)
            {
                _lock.EnterWriteLock();
                try
                {
                    foreach (var k in _dict.Keys.Where(r => r < (_safeLongIncreaser - topX)).ToList())
                    {
                        _dict.Remove(k);
                    }
                }
                catch
                {
                }
                finally
                {
                    _lock.ExitWriteLock();
                }

                lastCheckedRemove = DateTime.Now;
            }


        }

        public override bool AddSafe(KeyValuePair<long, TValue> pair)
        {
            bool r = false;
            long id = -1;

            _lock.EnterWriteLock();
            try
            {
                _safeLongIncreaser++;
                id = _safeLongIncreaser;

                _dict.Add(id, pair.Value);
                r = true;
            }
            catch
            {
                r = false;
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            return r;
        }

        /// <summary>
        /// Best Method to use here. Like Other Add Methods automatically creates key identity and returns it
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public long Add(TValue value)
        {
            long id = -1;
            _lock.EnterWriteLock();
            try
            {
                _safeLongIncreaser++;
                id = _safeLongIncreaser;

                _dict.Add(id, value);
            }
            catch
            {
                id = -1;
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            return id;
        }
    }

}
