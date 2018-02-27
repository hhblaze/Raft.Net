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
            log = new Logger();
            tm = new TimeMaster(log);

            //if (args.Length > 0)
            //{
            //    switch (args[0])
            //    {
            //        case "1":
            //            Scenario1(args);
            //            return;
            //    }
            //}


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

            Console.WriteLine($"Listening port: {args[1]}; Path to config: {args[2]}");
            var configLines = System.IO.File.ReadAllLines(args[2]);
            

           TcpRaftNode rn = null;

            var rn_settings = new RaftNodeSettings()
            {
                VerboseRaft = false,                
                VerboseTransport = false
            };

            string[] sev;
            List<TcpClusterEndPoint> eps = new List<TcpClusterEndPoint>();
            string dbreeze = "";

            foreach (var el in configLines)
            {
                var se = el.Split(new char[] { ':' });
                if (se.Length < 2)
                    continue;
                switch(se[0].Trim().ToLower())
                {
                    case "endpoint":
                        sev = se[1].Split(new char[] { ',' });
                        eps.Add(new TcpClusterEndPoint() { Host = sev[0].Trim(), Port = Convert.ToInt32(sev[1].Trim()) });
                        break;
                    case "verboseraft":
                        if (se[1].Trim().ToLower().Equals("true"))
                            rn_settings.VerboseRaft = true;
                        break;
                    case "verbosetransport":
                        if (se[1].Trim().ToLower().Equals("true"))
                            rn_settings.VerboseRaft = true;
                        break;
                    case "dbreeze":
                        dbreeze = se[1].Trim();                        
                        break;
                }
            }

            rn = new TcpRaftNode(eps, new List<RaftNodeSettings> { rn_settings }, dbreeze, Convert.ToInt32(args[1]),
                           (data) => {
                               Console.WriteLine($"wow committed");
                           }, log );

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
                    case "set10":
                        for (int k = 0; k < 10; k++)
                        {
                            addRes = rn.AddLogEntry(new byte[] { 23 });
                            Console.WriteLine($"Adding: {addRes.AddResult.ToString()}");
                        }
                        break;
                }
            }
            
        }
    }
}
