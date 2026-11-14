"""Prepare isolated, instrumented copies AFTER capacity timing. Never edits live sources.

The ordinary benchmark workload is unchanged; diagnostic harnesses hold the process
for three seconds after publishing the completed result so EventPipe can collect rundown.
"""
from pathlib import Path
import argparse
import shutil
import subprocess
import zipfile

ROOT=Path(__file__).resolve().parents[1]


def extract(archive, target, prefix=''):
    with zipfile.ZipFile(archive) as zip:
        for name in zip.namelist():
            if prefix and not name.startswith(prefix): continue
            rel=Path(name[len(prefix):])
            if rel.is_absolute() or '..' in rel.parts: raise ValueError('Unsafe archive')
            if not name.endswith('/'):
                file=target/rel;file.parent.mkdir(parents=True,exist_ok=True);file.write_bytes(zip.read(name))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--after-source',type=Path)
    args=parser.parse_args()
    out=args.out.resolve()
    if not out.is_relative_to(ROOT/'.artifacts'): raise ValueError('Diagnostic workspaces belong under .artifacts')
    out.mkdir(parents=True,exist_ok=False)
    recovery=ROOT/'.artifacts/performance-recovery'
    for variant in ('before','after','e06'):
        source=out/(variant+'-source');source.mkdir()
        if variant=='e06':
            # Keep the original API-specific harness, and original production sources.
            extract(ROOT/'.artifacts/hot-membership/baseline-source-e06c4f9.zip',source,'RaftNet8/')
            shutil.copy2(ROOT/'benchmarks/Raft.Comparison/MeasurementTrace.cs',source/'benchmarks/Raft.Comparison/MeasurementTrace.cs')
            file=source/'benchmarks/Raft.Comparison/ThreeWayRunner.cs';text=file.read_text(encoding='utf-8-sig')
            text=text.replace('var results = new List<Result>();','var results = new List<Result>();\n        var trace = MeasurementTrace.Log;')
            text=text.replace('var allocated = GC.GetTotalAllocatedBytes(precise: true);','var runtimeBefore = profile is null ? null : RuntimeSample.Capture();\n                        var allocated = GC.GetTotalAllocatedBytes(precise: true);')
            text=text.replace('var timer = Stopwatch.StartNew();','var timer = Stopwatch.StartNew();\n                        if (trace.IsEnabled()) trace.Start(configuration.ToString(), round + 1, Stopwatch.GetTimestamp());')
            text=text.replace('timer.Stop();','timer.Stop();\n                        if (trace.IsEnabled()) trace.Stop(Stopwatch.GetTimestamp());')
            text=text.replace('profile?.End();','profile?.End();\n                        var runtimeAfter = profile is null ? null : RuntimeSample.Capture();')
            text=text.replace('diagnostics = profile.Snapshot();','diagnostics = new { Counters = profile.Snapshot(), RuntimeBefore = runtimeBefore, RuntimeAfter = runtimeAfter };')
            file.write_text(text,encoding='utf-8')
        else:
            extract(ROOT/'.artifacts/winner/r19/current-source.zip',source)
            if variant=='after': extract(args.after_source or recovery/'after-source.zip',source)
            else:
                for tree in ('benchmarks/Raft.Comparison','benchmarks/Raft.Benchmarks'):
                    for file in (recovery/'after-cwd'/tree).rglob('*.cs'):
                        dest=source/file.relative_to(recovery/'after-cwd');dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(file,dest)
                shutil.copy2(ROOT/'src/Raft/Runtime/ProposalQueueTrace.cs',source/'src/Raft/Runtime/ProposalQueueTrace.cs')
                file=source/'src/Raft/RaftNode.cs';text=file.read_text(encoding='utf-8-sig')
                text=text.replace('''                            }
                            break;
                        case ReadWork''','''                            }
                            else ProposalQueueTrace.Record(_options.NodeId, "EmptyDispatch", 0);
                            break;
                        case ReadWork''')
                text=text.replace('''while (await _proposalChanges.Reader.WaitToReadAsync(_lifetime.Token).ConfigureAwait(false))
            {''','''while (await _proposalChanges.Reader.WaitToReadAsync(_lifetime.Token).ConfigureAwait(false))
            {
                ProposalQueueTrace.Record(_options.NodeId, "CollectorWake");''')
                text=text.replace('''if (Interlocked.CompareExchange(ref _proposalQueued, 1, 0) == 0)
                            await _work.Writer.WriteAsync(ProposalMessage, _lifetime.Token).ConfigureAwait(false);''','''if (Interlocked.CompareExchange(ref _proposalQueued, 1, 0) == 0)
                        {
                            ProposalQueueTrace.Record(_options.NodeId, "ReadySignal");
                            await _work.Writer.WriteAsync(ProposalMessage, _lifetime.Token).ConfigureAwait(false);
                        }''')
                text=text.replace('''_proposalChanges.Writer.TryWrite(0);
            if (proposals.Count == 0)''','''_proposalChanges.Writer.TryWrite(0);
            ProposalQueueTrace.Record(_options.NodeId, "Group", proposals.Count);
            if (proposals.Count == 0)''')
                if text.count('ProposalQueueTrace.Record')!=4: raise AssertionError('Before instrumentation anchors changed')
                file.write_text(text,encoding='utf-8')
            file=source/'src/Raft/RaftNode.cs';text=file.read_text(encoding='utf-8-sig')
            begin=text.index('    private async Task CollectProposalsAsync()')
            end=text.index('    private async ValueTask ProcessRequestsAsync',begin)
            collector=text[begin:end]
            collector=collector.replace('using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);',
                              'ProposalQueueTrace.Record(_options.NodeId, "DeadlineWait");\n                    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);')
            collector=collector.replace('if (!await _proposalChanges.Reader.WaitToReadAsync(deadline.Token).ConfigureAwait(false)) return;',
                              'if (!await _proposalChanges.Reader.WaitToReadAsync(deadline.Token).ConfigureAwait(false)) return;\n                        ProposalQueueTrace.Record(_options.NodeId, "DeadlineChangeWake");')
            collector=collector.replace('catch (OperationCanceledException) when (!_lifetime.IsCancellationRequested) { }',
                                        'catch (OperationCanceledException) when (!_lifetime.IsCancellationRequested) { ProposalQueueTrace.Record(_options.NodeId, "DeadlineTimeout"); }')
            text=text[:begin]+collector+text[end:]
            file.write_text(text,encoding='utf-8')
        shutil.copy2(ROOT/'benchmarks/Raft.Comparison/MeasurementTrace.cs',source/'benchmarks/Raft.Comparison/MeasurementTrace.cs')
        if variant!='e06': shutil.copy2(ROOT/'src/Raft/Runtime/ProposalQueueTrace.cs',source/'src/Raft/Runtime/ProposalQueueTrace.cs')
        file=source/'benchmarks/Raft.Comparison/ThreeWayRunner.cs';text=file.read_text(encoding='utf-8-sig')
        text=text.replace('        return 0;\n    }','        await Task.Delay(3000); // diagnostic-only rundown hold, outside all timed fences\n        return 0;\n    }',1)
        file.write_text(text,encoding='utf-8')
        project=source/'benchmarks/Raft.Comparison/Raft.Comparison.csproj'
        subprocess.run(['dotnet','restore',str(project),'--locked-mode'],cwd=source,check=True)
        subprocess.run(['dotnet','build',str(project),'-c','Release','--no-restore','-nodeReuse:false',
                        '-p:DefineConstants=RAFT_QUEUE_DIAGNOSTICS','-o',str(out/(variant+'-runtime'))],cwd=source,check=True)
        # Metadata stays source-only, avoiding unrelated project discovery by later BDN runs.
        for file in source.rglob('*.csproj'): file.unlink()
    print(out,flush=True)


if __name__=='__main__':main()
