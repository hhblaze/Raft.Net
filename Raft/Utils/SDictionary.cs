/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raft.Utils
{
    /// <summary>
    /// Syncronized Dictionary with ReaderWriterLockSlim, if smth. is not implemented or other desires implement later
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class SDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        internal Dictionary<TKey, TValue> _dict = new Dictionary<TKey, TValue>();
        internal ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();


        /// <summary>
        /// Safely Returns List by Predicate
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public List<KeyValuePair<TKey, TValue>> GetSafe(Func<KeyValuePair<TKey, TValue>, bool> predicate)
        {
            _lock.EnterReadLock();
            try
            {
                return _dict.Where(predicate).ToList();
            }
            catch
            {
                return new List<KeyValuePair<TKey, TValue>>();
            }
            finally
            {
                _lock.ExitReadLock();
            }


        }



        /// <summary>
        /// All add method are safe here, but this returns also a value
        /// Enhanced functionality also for update
        /// </summary>
        /// <param name="pair"></param>
        /// <returns></returns>
        public virtual bool AddSafe(KeyValuePair<TKey, TValue> pair)
        {
            bool r = false;


            if (pair.Key == null)
                return r;

            _lock.EnterWriteLock();
            try
            {
                if (!_dict.ContainsKey(pair.Key))
                {
                    _dict.Add(pair.Key, pair.Value);
                }
                else
                {
                    //here we supply new instance of a value
                    _dict[pair.Key] = pair.Value;
                }

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

        public bool RemoveThenAdd(Func<TKey, bool> toRemoveKeysPredicate, Dictionary<TKey, TValue> toAdd)
        {
            bool r = false;

            _lock.EnterWriteLock();
            try
            {
                var keys = _dict.Keys.Where(k => toRemoveKeysPredicate(k)).ToList();
                foreach (var key in keys)
                {
                    _dict.Remove(key);
                }

                foreach (var kvp in toAdd)
                {
                    if (_dict.ContainsKey(kvp.Key))
                        _dict[kvp.Key] = kvp.Value;
                    else
                        _dict.Add(kvp.Key, kvp.Value);
                }

                r = true;
            }
            catch (Exception ex)
            {
                r = false;
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            return r;
        }


        public bool Remove(TKey key)
        {
            bool r = false;

            if (key == null)
                return r;

            _lock.EnterWriteLock();
            try
            {
                if (_dict.ContainsKey(key))
                {
                    _dict.Remove(key);
                    r = true;
                }

            }
            catch (Exception ex)
            {
                r = false;
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            return r;
        }

        public TValue this[TKey key]
        {
            get
            {
                if (key == null)
                    return default(TValue);

                _lock.EnterReadLock();
                try
                {
                    if (_dict.ContainsKey(key))
                        return _dict[key];
                }
                catch
                {
                }
                finally
                {
                    _lock.ExitReadLock();
                }

                return default(TValue);
            }
            set
            {
                _lock.EnterWriteLock();
                try
                {
                    if (_dict.ContainsKey(key))
                        _dict[key] = value;
                    else
                        _dict.Add(key, value);

                }
                catch (Exception ex)
                {
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }
        }


        /// <summary>
        /// Adds or updates value
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Add(TKey key, TValue value)
        {
            this.AddSafe(new KeyValuePair<TKey, TValue>(key, value));
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            this.AddSafe(item);
        }

        public bool ContainsKey(TKey key)
        {
            _lock.EnterReadLock();
            try
            {
                return _dict.ContainsKey(key);
            }
            catch
            {
                return false;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                _lock.EnterReadLock();
                try
                {
                    return _dict.Keys;
                }
                finally
                {
                    _lock.ExitReadLock();
                }
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            _lock.EnterReadLock();
            try
            {
                return _dict.TryGetValue(key, out value);
            }
            catch
            {
                value = default(TValue);
                return false;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public ICollection<TValue> Values
        {
            get
            {
                _lock.EnterReadLock();
                try
                {
                    return _dict.Values;
                }
                finally
                {
                    _lock.ExitReadLock();
                }
            }
        }



        public void Clear()
        {
            _lock.EnterWriteLock();
            try
            {
                _dict.Clear();
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            _lock.EnterReadLock();
            try
            {
                return _dict.Contains(item);
            }
            catch
            {
                return false;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            _lock.EnterReadLock();
            try
            {
                _dict.ToArray().CopyTo(array, arrayIndex);
            }
            catch
            {
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public int Count
        {
            get
            {
                _lock.EnterReadLock();
                try
                {
                    return _dict.Count;
                }
                finally
                {
                    _lock.ExitReadLock();
                }
            }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return this.Remove(item.Key);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            _lock.EnterReadLock();
            try
            {
                return _dict.GetEnumerator();
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            _lock.EnterReadLock();
            try
            {
                return _dict.GetEnumerator();
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }
}
