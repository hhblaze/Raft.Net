# Raft.Net
Implementation of RAFT consensus algorithm on .NET and .NET Standard with TCP Peers.

----
Implementation in progress.

Though it already works: 
 - Checkout Revision 3
 - make console app as a start up one in VS studio
 - Run
 - after leader is elected (in console) type several times "set 1"
   It will create several new log entries in all 5 test nodes
 - stop one of the nodes:  "stop 4250" or "stop 4251"  (there are 5 nodes from 4250-4254)
 - Create new entries: type several times "set 1". 
 - Start node(s): "start 4250"
 - "set 1" to add new entites, observere how they are replicated and commited on all servers
 - "test 4250" shows peer connections from client 4250.
 
 
 hhblaze@gmail.com
 
