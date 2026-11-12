using Raft.Demo;

var scenario = args.Contains("--scenario");
if (args.Contains("--help"))
{
    Console.WriteLine("Raft.Demo [--scenario] [--data directory] [--base-port 5000]\nDefault: interactive three-node TCP cluster with persistent DBreeze state.");
    return 0;
}
string? Option(string name)
{
    var index = Array.IndexOf(args, name);
    if (index < 0) return null;
    if (index + 1 >= args.Length) throw new ArgumentException($"{name} needs a value.");
    return args[index + 1];
}
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
try
{
    var path = Option("--data") ?? (scenario
        ? Path.Combine(".data", "scenarios", DateTime.UtcNow.ToString("yyyyMMdd-HHmmss") + "-" + Guid.NewGuid().ToString("N")[..8])
        : Path.Combine(".data", "human-test"));
    var basePort = int.Parse(Option("--base-port") ?? "0", System.Globalization.CultureInfo.InvariantCulture);
    DemoConsole.Write($"Raft .NET 8 human test\nData: {Path.GetFullPath(path)}\nThree real TCP endpoints; each node has its own DBreeze engine and durable application checkpoint.");
    await using var cluster = new DemoCluster(path, basePort);
    await cluster.StartAsync();
    if (scenario)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
        deadline.CancelAfter(TimeSpan.FromSeconds(90));
        await cluster.RunScenarioAsync(deadline.Token);
        return 0;
    }
    const string help = "Commands: status | set <key> <value> | get <key> | stop <n1|n2|n3> | start <node> | isolate <node> | heal | load <count> | help | exit";
    DemoConsole.Write(help);
    await cluster.LeaderAsync(stop.Token);
    cluster.ShowStatus();
    while (!stop.IsCancellationRequested)
    {
        Console.Write("raft> ");
        var line = await Console.In.ReadLineAsync(stop.Token);
        if (line is null) break;
        var command = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (command.Length == 0) continue;
        if (command[0] is "exit" or "quit") break;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
        deadline.CancelAfter(TimeSpan.FromSeconds(command[0] == "load" ? 300 : 20));
        try
        {
            switch (command[0])
            {
                case "status": cluster.ShowStatus(); break;
                case "help": DemoConsole.Write(help); break;
                case "set" when command.Length == 3:
                    var result = await cluster.SetAsync(command[1], command[2], deadline.Token);
                    DemoConsole.Write($"COMMITTED index={result.Index} term={result.Term}");
                    break;
                case "get" when command.Length == 2: DemoConsole.Write(await cluster.GetAsync(command[1], deadline.Token) ?? "(missing)"); break;
                case "stop" when command.Length == 2: await cluster.StopMemberAsync(command[1]); break;
                case "start" when command.Length == 2: await cluster.StartMemberAsync(command[1]); break;
                case "isolate" when command.Length == 2: cluster.Isolate(command[1]); break;
                case "heal": cluster.Heal(); break;
                case "load" when command.Length == 2:
                    await cluster.LoadAsync(int.Parse(command[1], System.Globalization.CultureInfo.InvariantCulture), deadline.Token);
                    break;
                default: DemoConsole.Write(help); break;
            }
        }
        catch (Exception error) { DemoConsole.Write($"{error.GetType().Name}: {error.Message}"); }
    }
    return 0;
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { return 0; }
catch (Exception error) { Console.Error.WriteLine($"DEMO FAILED: {error}"); return 1; }
