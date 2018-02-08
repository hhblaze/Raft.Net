# Raft.Net
Implementation of RAFT consensus algorithm among TCP peers 
for dotnet / .NET / .NETStandard /.NETCore

----
Implementation is in progress (polishing the code).

Though it already works: 
 - Checkout Revision 6
 - make console app as a start up one in VS studio
 - Depends on [DBreeze](https://github.com/hhblaze/DBreeze) and protobuf-net (from NuGet).
 - Run
 - after leader is elected (in console) type several times "set 1"
   It will create several new log entries in all test nodes.
 - stop one or two nodes:  "stop 4250" / "stop 4251". There are 5 testing nodes from 4250-4254 (these are also tcp listening ports).
 - Create new entries: type several times "set 1", to make log difference among running and stopped nodes. 
 - Start node(s): "start 4250". Observe how values are replicated among started nodes.
 - "set 1" to add new entry(ies), observe how they are in tact commited among all nodes.
 - "test 4250" - shows peer connections from client 4250.
 
 Current test stores log data in DBreeze memory mode, it is possible to change it on real disk mode 
 inside Raft.Net/Raft/StateMachine/StateLog.cs, switching
 
  db = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
  //db = new DBreezeEngine(dbreezePath);
  
  and setting dbreezePath for each node inside Raft/RaftEmulator/Emulator.cs
  like rn = new TcpRaftNode(eps, @"D:\Temp\RaftDBreeze\node" + (4250 + i), 4250 + i, this, rn_settings);  
 
 hhblaze@gmail.com
 
