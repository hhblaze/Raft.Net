using DBreeze;
using DBreeze.Utils;
using Raft;
using Raft.Transport;
//using Raft.Transport.UdpServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace _NodeTest
{
    class Program
    {
        static TimeMaster tm = null;
        static IWarningLog log = null;

        public class Logger : IWarningLog
        {
            public void Log(WarningLogEntry logEntry)
            {
                Console.WriteLine(logEntry.ToString());
            }
        }

        static Raft.RaftEmulator.Emulator dd = null;
        static byte val = 0;

        static void Main(string[] args)
        {
            //IndexTermDict<string>.test1();
            //Console.ReadLine();
            //return;

            log = new Logger();
            //tm = new TimeMaster(log);

            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "1":
                        Scenario1(args);
                        return;
                }
            }


            //UdpTester t = new UdpTester(tm, log);
            //t.Start(40000);

            dd = new Raft.RaftEmulator.Emulator();
            //dd.StartEmulateNodes(5);
            dd.StartEmulateTcpNodes(5);



            Console.WriteLine("--------");
            while (true)
            {
                string cmd = Console.ReadLine();
                switch (cmd)
                {
                    case "quit":
                        return;
                    default:
                        try
                        {
                            string[] spl = cmd.Split(' ');
                            if (spl.Count() > 1)
                            {
                                switch (spl[0])
                                {
                                    case "start": //start 2
                                        dd.Start(Convert.ToInt32(spl[1]));
                                        break;
                                    case "stop": //stop 1
                                        dd.Stop(Convert.ToInt32(spl[1]));
                                        break;
                                    case "test": //sendtestall 4254 - means node 4254 must send to all TcpMsg "Test"
                                        dd.SendTestAll(Convert.ToInt32(spl[1]));
                                        break;
                                    case "set": //set 1 - will create an entity
                                        val++;
                                        dd.SetValue(new byte[] { (byte)val });
                                        break;

                                    case "set10": //set10 1 - will create an entity
                                        for(int qi = 0;qi<10;qi++)
                                            dd.SetValue(new byte[] { 12 });
                                        break;
                                }
                            }
                            //else if(spl.Count() == 1)
                            //{
                            //    switch (spl[0])
                            //    {                                   
                            //        case "testall": //sendtestall 4254 - means node 4254 must send to all TcpMsg "Test"
                            //            Console.WriteLine("test 4250");
                            //            dd.SendTestAll(4250);
                                        
                            //            Console.WriteLine("test 4251");
                            //            dd.SendTestAll(4251);
                                       
                            //            Console.WriteLine("test 4252");
                            //            dd.SendTestAll(4252);
                                        
                            //            Console.WriteLine("test 4253");
                            //            dd.SendTestAll(4253);
                                       
                            //            Console.WriteLine("test 4254");
                            //            dd.SendTestAll(4254);
                            //            break;
                            //    }
                            //}
                            else
                                Console.WriteLine("Unknown command");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Error: " + ex.ToString());
                        }
                        
                        break;
                }
            }
        }

        static void Scenario1(string[] args)
        {
            Console.WriteLine("Scenario 1 is running");
            if(args.Length<1)
            {
                Console.WriteLine("RaftCluster TCP listening port is not specified");
                return;
            }

            if (args.Length < 2 || !(new System.IO.FileInfo(args[2])).Exists)
            {
                Console.WriteLine("Path to configuration file is not supplied or file is not found");
                return;
            }

            string dbreezePath = "";
            if(args.Length >= 3)
                dbreezePath = args[3];
            
            Console.WriteLine($"Listening port: {args[1]}; Path to config: {args[2]}; Path to DBreeze folder: {args[3]}");
            var configLines = System.IO.File.ReadAllLines(args[2]);
            


           TcpRaftNode rn = null;

            //rn = TcpRaftNode.GetFromConfig(1, System.IO.File.ReadAllText(args[2]),
            //    dbreezePath, Convert.ToInt32(args[1]), log,
            //    (entName, index, data) => { Console.WriteLine($"wow committed {entName}/{index}"); return true; });

            rn = TcpRaftNode.GetFromConfig(System.IO.File.ReadAllText(args[2]),
               dbreezePath, Convert.ToInt32(args[1]), log,
               (entName, index, data) => { Console.WriteLine($"wow committed {entName}/{index}"); return true; });



            rn.Start();

            AddLogEntryResult addRes = null;
            while(true)
            {
                var cmd = Console.ReadLine();
                switch(cmd)
                {
                    case "set1":
                        addRes = rn.AddLogEntry(new byte[] { 23 });
                        Console.WriteLine($"Adding: {addRes.AddResult.ToString()}");
                        break;
                    case "s2":
                        addRes = rn.AddLogEntry(new byte[] { 23 }, entityName: "inMemory2");
                        Console.WriteLine($"Adding: {addRes.AddResult.ToString()}");
                        break;
                    case "p2":
                        rn.Debug_PrintOutInMemory(entityName: "inMemory2");
                        //Console.WriteLine($"Adding: {addRes.AddResult.ToString()}");
                        break;
                    case "set1a":
                        addRes = rn.AddLogEntry(new byte[] { 27 },entityName: "inMemory1");
                        Console.WriteLine($"Adding: {addRes.AddResult.ToString()}");
                        break;
                    case "set10":
                        for (int k = 0; k < 10; k++)
                        {
                            addRes = rn.AddLogEntry(new byte[] { 23 });
                            Console.WriteLine($"Adding: {addRes.AddResult.ToString()}");
                        }
                        break;
                    case "set10a":
                        for (int k = 0; k < 10; k++)
                        {
                            addRes = rn.AddLogEntry(new byte[] { 23 }, entityName: "inMemory1");
                            Console.WriteLine($"Adding: {addRes.AddResult.ToString()}");
                        }
                        break;
                }
            }
            
        }
    }
}
