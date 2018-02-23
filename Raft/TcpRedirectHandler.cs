using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raft
{
    internal class RedirectHandler
    {
        RaftNode rn = null;
        ulong redirectId = 0;

        //ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();
        //!!! Called from RaftNode lock_Operations
        Dictionary<ulong, RedirectInfo> _d = new Dictionary<ulong, RedirectInfo>();

        public class RedirectInfo
        {
            public ulong Term { get; set; } = 0;
            public NodeAddress NodeAddress { get; set; }
        }

        public RedirectHandler(RaftNode rn)
        {
            this.rn = rn;
        }

        /// <summary>
        /// Is called from RaftNode lock_Operations
        /// </summary>
        public ulong StoreRedirect(NodeAddress na)
        {
            redirectId++;
            _d[redirectId] = new RedirectInfo()
            {
                Term = rn.NodeTerm,
                NodeAddress = na
            };
            //...store here callbacks in dictionary also term that shoud be checked before returning redirect id
            //make auto clean by timeout etc...
            return redirectId;
        }

        /// <summary>
        /// can return null, if id is not found or term is wrong
        /// </summary>
        /// <param name="redirectId"></param>
        /// <param name="term"></param>
        /// <returns></returns>
        public RedirectInfo GetRedirectByIdTerm(ulong redirectId, ulong term)
        {
            RedirectInfo ri = null;
            if (!_d.TryGetValue(redirectId, out ri))
                return null;
            return ri.Term == term ? ri : null;
        }

    }
}
