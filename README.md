# Raft.Net
Implementation of RAFT consensus algorithm on .NET and .NETStandard .NETCore with TCP Peers.

----
Implementation is in progress (polishing the code).

Though it already works: 
 - Checkout Revision 6
 - make console app as a start up one in VS studio
 - Run
 - after leader is elected (in console) type several times "set 1"
   It will create several new log entries in all test nodes.
 - stop one or two nodes:  "stop 4250" / "stop 4251". There are 5 testing nodes from 4250-4254.
 - Create new entries: type several times "set 1", to make log difference among rinning and stopped nodes. 
 - Start node(s): "start 4250". Observe how values are replicated among started nodes.
 - "set 1" to add new entry(ies), observe how they are in tact commited among all nodes.
 - "test 4250" - shows peer connections from client 4250.
 
 
 hhblaze@gmail.com
 
