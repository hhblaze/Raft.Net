using System.Text;
using System.Text.Json;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tutorial;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        // Use the same encoding for a terminal and redirected UTF-8 stdin/stdout.
        Console.InputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        Console.OutputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        TutorialSettings settings;
        try { settings = TutorialSettings.Parse(args); }
        catch (Exception error) when (error is ArgumentException or FormatException or OverflowException)
        {
            Console.Error.WriteLine(error.Message);
            return 2;
        }
        using var stop = new CancellationTokenSource();
        ConsoleCancelEventHandler cancel = (_, eventArgs) => { eventArgs.Cancel = true; stop.Cancel(); };
        Console.CancelKeyPress += cancel;
        try { await RunAsync(settings, stop.Token); return 0; }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { return 0; }
        catch (Exception error) { Console.Error.WriteLine(error); return 1; }
        finally { Console.CancelKeyPress -= cancel; }
    }

    private static async Task RunAsync(TutorialSettings settings, CancellationToken token)
    {
        var options = settings.Options;
        using IBookState app = settings.InMemory
            ? new MemoryBookState()
            : new DiskBookState(Path.Combine(settings.Root, "application"));
        // book:start startup
        var storage = settings.InMemory
            ? DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV3)
            : new DBreezeLogStorage(Path.Combine(settings.Root, "log"), options,
                DBreezeLogLayout.UnifiedTableV3);
        // The guard disposes the log even if CreateAsync fails.
        // After successful creation, the node also owns it; disposal is idempotent.
        await using var log = new RaftStorageAdapter(storage);
        await using var tcp = new TcpRaftTransport(options, settings.Endpoints);
        RaftNode? node = null;
        try
        {
            node = await RaftNode.CreateAsync(options, log, tcp, app, cancellationToken: token);
            await node.StartAsync();
            tcp.Start(node.HandleAsync);
            // book:end startup
            Console.WriteLine($"STARTED node={options.NodeId} group={options.GroupId} " +
                $"mode={(settings.InMemory ? "memory" : "disk")} endpoint={tcp.LocalEndPoint}");
            Console.WriteLine(settings.InMemory
                ? "LAB ONLY: log, votes, values and request IDs disappear on exit."
                : $"DATA {settings.Root}");
            Console.WriteLine("Commands: status | put <request-id> <key> <value> | get <key> | " +
                "local <key> | snapshot | quit");
            await CommandLoopAsync(node, app, options.MaxCommandBytes, token);
        }
        finally
        {
            // book:start shutdown
            await tcp.DisposeAsync(); // Stop inbound RPCs first.
            if (node is not null) await node.DisposeAsync();
            // Then log guard and caller-owned app are disposed by their scopes.
            // book:end shutdown
        }
    }

    private static async Task CommandLoopAsync(RaftNode node, IBookState app, int maxBytes,
        CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            // Observe fail-stop even while the console is idle.
            var input = Console.In.ReadLineAsync(token).AsTask();
            if (await Task.WhenAny(input, node.Completion) == node.Completion)
            {
                await node.Completion; // Preserve the original storage/application exception.
                return;
            }
            var line = await input;
            if (line is null || line.Trim() == "quit") return;
            var parts = line.Split(' ', 4, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0) continue;

            // Parse/validate before calling the consensus API.
            byte[]? bytes = null;
            SetValue? command = null;
            if (parts[0] == "put")
            {
                try
                {
                    if (parts.Length != 4) throw new ArgumentException("put <request-id> <key> <value>");
                    command = new(1, parts[1], parts[2], parts[3]);
                    bytes = BookData.Encode(command, maxBytes);
                }
                catch (ArgumentException error) { Console.WriteLine($"INPUT {error.Message}"); continue; }
            }

            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
            deadline.CancelAfter(TimeSpan.FromSeconds(15));
            try
            {
                switch (parts[0])
                {
                    case "status": PrintStatus(node); break;
                    case "put":
                        // book:start propose
                        var written = await node.ProposeAsync(bytes!, deadline.Token);
                        var original = app.FindRequest(command!.RequestId)
                            ?? throw new InvalidDataException("Applied request result is missing.");
                        if (original.Key != command.Key || original.Value != command.Value)
                            Console.WriteLine("CONFLICT request ID belongs to a different command.");
                        else
                            Console.WriteLine($"OK index={written.Index} term={written.Term} " +
                                $"originalIndex={original.Index}");
                        // book:end propose
                        break;
                    case "get" when parts.Length == 2:
                        // book:start read
                        var fence = await node.ReadIndexAsync(deadline.Token);
                        var value = app.Get(parts[1]);
                        Console.WriteLine($"VALUE {JsonSerializer.Serialize(value)} fence={fence.Index}");
                        // book:end read
                        break;
                    case "local" when parts.Length == 2:
                        Console.WriteLine($"LOCAL {JsonSerializer.Serialize(app.Get(parts[1]))} (may be stale)");
                        break;
                    case "snapshot" when app is ISnapshotStateMachine:
                        var image = await node.CreateSnapshotAsync(deadline.Token);
                        Console.WriteLine($"SNAPSHOT index={image.LastIncludedIndex} id={image.StorageId}");
                        break;
                    case "snapshot": Console.WriteLine("NO SNAPSHOTS in this memory example."); break;
                    default: Console.WriteLine("INPUT unknown command or argument count."); break;
                }
            }
            catch (NotLeaderException error) { Console.WriteLine($"NOT_LEADER leader={error.LeaderId ?? "unknown"}"); }
            catch (UncommittedLogLimitException error) { Console.WriteLine($"BACKPRESSURE {error.Message}"); }
            catch (LeadershipTransferInProgressException error) { Console.WriteLine($"TRANSFER {error.Target}"); }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested && !token.IsCancellationRequested)
            {
                Console.WriteLine("TIMEOUT outcome may be committed; retry with the SAME request ID and payload.");
            }
            // Other execution failures propagate and terminate the process.
        }
    }

    private static void PrintStatus(RaftNode node)
    {
        var status = node.Status;
        Console.WriteLine($"STATUS node={status.NodeId} role={status.Role} term={status.Term} " +
            $"leader={status.LeaderId ?? "unknown"} commit={status.CommitIndex} applied={status.LastApplied} " +
            $"tail={status.LastLogIndex} snapshot={status.Snapshot?.LastIncludedIndex ?? 0} " +
            $"fault={status.Fault?.GetType().Name ?? "none"}");
    }
}
