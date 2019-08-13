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
    /// Access to each function via lock(instance.Sync)
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    internal class IndexTermDict<TValue>
    {
        /// <summary>
        /// Each function call must be inside the lock(instance.Sync) construction
        /// </summary>
        public object Sync = new object();
        SortedDictionary<ulong, SortedDictionary<ulong, TValue>> d = new SortedDictionary<ulong, SortedDictionary<ulong, TValue>>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <param name="term"></param>
        /// <param name="val"></param>
        public void Add(ulong index, ulong term, TValue val)
        {
            SortedDictionary<ulong, TValue> d1;
            if (d.TryGetValue(index, out d1))
                d1[term] = val;
            else
            {
                d1 = new SortedDictionary<ulong, TValue>();
                d1[term] = val;
                d[index] = d1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <param name="term"></param>
        public void Remove(ulong index, ulong term)
        {
            if (d.TryGetValue(index, out var d1))
            {
                d1.Remove(term);

                if(d1.Count == 0)
                    d.Remove(index);
            }
        }

        /// <summary>
        /// Collect argument from SelectForwardFromTo.ToList() or SelectBackwardFromTo.ToList(), Item3 is not used inside
        /// </summary>
        /// <param name="remove"></param>
        public void Remove(List<Tuple<ulong, ulong, TValue>> remove)
        {
            ulong prevIndex = ulong.MaxValue;
            SortedDictionary<ulong, TValue> d1 = null;

            foreach (var el in remove.OrderBy(r=>r.Item1))
            {
                if(prevIndex != el.Item1)
                {
                    prevIndex = el.Item1;
                    if (!d.TryGetValue(el.Item1, out d1))
                        continue;

                }

                d1.Remove(el.Item2);

                if (d1.Count == 0)
                    d.Remove(el.Item1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <param name="term"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        public bool Select(ulong index, ulong term, out TValue val)
        {
            if (d.TryGetValue(index, out var d1))
                if (d1.TryGetValue(term, out val))
                    return true;

            val = default(TValue);
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startIndex"></param>
        /// <param name="startTerm"></param>
        /// <param name="includeStartTerm"></param>
        /// <param name="endIndex"></param>
        /// <param name="endTerm"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<ulong,ulong,TValue>> SelectForwardFromTo(ulong startIndex, ulong startTerm, bool includeStartTerm, ulong endIndex, ulong endTerm)
        {
            foreach (var el1 in d.Where(r => r.Key >= startIndex && r.Key <= endIndex))
            {                
                foreach (var el in el1.Value)
                {
                    if (el1.Key == startIndex)
                    {
                        if (el.Key < startTerm)
                            continue;
                        if (el.Key == startTerm && !includeStartTerm)
                            continue;
                    }

                    if (el1.Key == endIndex)
                    {
                        if (el.Key > endTerm)
                            break;
                    }

                    yield return new Tuple<ulong, ulong, TValue>(el1.Key,el.Key,el.Value);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public ulong GetOneIndexDownFrom(ulong index)
        {
            foreach (var el1 in d.Where(r => r.Key < index).OrderByDescending(r => r.Key))
            {
                return el1.Key;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startIndex"></param>
        /// <param name="startTerm"></param>
        /// <param name="includeEndTerm"></param>
        /// <param name="endIndex"></param>
        /// <param name="endTerm"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<ulong, ulong, TValue>> SelectBackwardFromTo(ulong startIndex, ulong startTerm, bool includeEndTerm, ulong endIndex, ulong endTerm)
        {
            foreach (var el1 in d.Where(r => r.Key >= endIndex  && r.Key <= startIndex).OrderByDescending(r=>r.Key))
            {
                foreach (var el in el1.Value.OrderByDescending(r => r.Key))
                {
                    if (el1.Key == startIndex)
                    {
                        if (el.Key > startTerm)
                            continue;
                        if (el.Key == startTerm && !includeEndTerm)
                            continue;
                    }

                    if (el1.Key == endIndex)
                    {
                        if (el.Key < endTerm)
                            break;
                    }

                    yield return new Tuple<ulong, ulong, TValue>(el1.Key, el.Key, el.Value);
                }
            }
        }


        #region "Tests"

        /// <summary>
        /// 
        /// </summary>
        internal static void test1()
        {
            //IndexTermDict<string>.test1();
            //Console.ReadLine();
            //return;


            IndexTermDict<string> inMem = new IndexTermDict<string>();
            ulong index = 1;
            ulong term = 1;

            inMem.Add(index, term, index.ToString() + "_" + term.ToString());

            index = 2;
            term = 1;
            inMem.Add(index, term, index.ToString() + "_" + term.ToString());

            index = 2;
            term = 2;
            inMem.Add(index, term, index.ToString() + "_" + term.ToString());

            index = 2;
            term = 3;
            inMem.Add(index, term, index.ToString() + "_" + term.ToString());


            index = 3;
            term = 2;
            inMem.Add(index, term, index.ToString() + "_" + term.ToString());
            index = 3;
            term = 1;
            inMem.Add(index, term, index.ToString() + "_" + term.ToString());

            index = 3;
            term = 3;
            inMem.Add(index, term, index.ToString() + "_" + term.ToString());


            Console.WriteLine("---------------Tests---------------------");

            if (inMem.Select(13, 1, out var billy))
                Console.WriteLine(billy);
            else
                Console.WriteLine("billy not found");


            Console.WriteLine("------------------------------------");

            foreach (var el in inMem.SelectForwardFromTo(0, 0, true, 1000, 1000))
            {
                Console.WriteLine($"v: {el.Item3}");
            }

            Console.WriteLine("------------------------------------");

            foreach (var el in inMem.SelectForwardFromTo(2, 2, false, 3, 2))
            {
                Console.WriteLine($"v: {el.Item3}");
            }

            //Console.WriteLine("------------------------------------");

            //foreach (var el in inMem.SelectBackwardFromTo(3, 2, false, 2, 2))
            //{
            //    Console.WriteLine($"v: {el.Item3}");
            //}


            Console.WriteLine("-----------After deletion-------------------------");

            inMem.Remove(inMem.SelectForwardFromTo(2, 2, false, 3, 2).ToList());

            foreach (var el in inMem.SelectForwardFromTo(0, 0, true, 1000, 1000))
            {
                Console.WriteLine($"v: {el.Item3}");
            }


            Console.ReadLine();
            return;
        }
        #endregion

    }
}
