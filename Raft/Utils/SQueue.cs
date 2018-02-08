using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Utils
{
    /// <summary>
    /// Syncronized Queue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class SQueue<T>//:Queue<T>
    {
       
        #region "Original SQueue"


        Queue<T> myQueue = new Queue<T>();
        object lock_deq = new object();


        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        public void Enqueue(T item)
        {
            lock (lock_deq)
            {
                myQueue.Enqueue(item);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            lock (lock_deq)
            {
                myQueue.Clear();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int Count
        {
            get
            {
                lock (lock_deq)
                {
                    return this.myQueue.Count;
                }
            }
        }

        /// <summary>
        /// Now Dequeue can be called when Queue is Empty - default (T) will be returned (so, NULL)
        /// </summary>
        /// <returns></returns>
        public T Dequeue()
        {
            T res = default(T);
            lock (lock_deq)
            {
                if (myQueue.Count > 0)
                    res = myQueue.Dequeue();  //can call itself many times if this.Dequeue(); and derived from Queue<T>
            }

            return res;
        }
        #endregion

    }
}
