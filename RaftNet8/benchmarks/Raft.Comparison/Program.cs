using Raft.Benchmarks;

try { return args.Contains("--verify-adapter") ? await AdapterChecks.RunAsync() : await ThreeWayRunner.RunAsync(args); }
catch (Exception error) { Console.Error.WriteLine(error); return 1; }
