using BenchmarkDotNet.Running;
using Raft.Benchmarks;

if (args.Contains("--latency"))
{
    try { return await LatencyRunner.RunAsync(args); }
    catch (Exception error) { Console.Error.WriteLine(error); return 1; }
}
var summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
// BDN can print build/execution failures and return normally. Make automation fail on
// missing results instead of accepting a process exit code as proof that cases ran.
if (summaries.Any(summary => summary.HasCriticalValidationErrors || summary.Reports.Any(report => !report.Success))) return 1;
return args.Contains("--filter") && !args.Contains("--list") && !summaries.Any() ? 1 : 0;
