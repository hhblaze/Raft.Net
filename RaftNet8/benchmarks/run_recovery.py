"""Fresh-process, alternating controls for the e06 performance recovery.

Frozen runtimes and source-only working directories are supplied in a JSON file.
Never builds during timing, overwrites raw results, or executes competitors.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def source_hash(directory):
    digest = hashlib.sha256()
    for path in sorted((p for p in directory.rglob('*.cs') if not {'bin','obj'}.intersection(p.relative_to(directory).parts)), key=str):
        digest.update((path.relative_to(directory).as_posix()+'\n').encode())
        digest.update(path.read_bytes())
    return digest.hexdigest().upper()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--variants', default='e06,before,after')
    parser.add_argument('--cases', default='3-64-Pipeline,5-64-Pipeline')
    parser.add_argument('--series', default='short,long')
    parser.add_argument('--rounds', type=int, default=5)
    parser.add_argument('--start-round', type=int, default=1)
    parser.add_argument('--diagnose', action='store_true')
    parser.add_argument('--hot-add', action='store_true')
    parser.add_argument('--warmup', type=int)
    parser.add_argument('--operations', type=int)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding='utf-8-sig'))
    variants = args.variants.split(',')
    if any(v not in config['variants'] for v in variants):
        raise ValueError('Unknown variant')
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    manifest = dict(complete=False, config=config, config_sha256=sha(args.config), runs=[],
                    diagnose=args.diagnose, hot_add=args.hot_add,
                    fresh_process_per_measurement=True, concurrency=64)
    for v in variants:
        item = config['variants'][v]
        item['binary_sha256'] = {p.name: sha(p) for p in Path(item['dll']).parent.glob('*.dll')}
        item['source_sha256'] = source_hash(Path(item['cwd'])/'src/Raft')
        item['harness_sha256'] = source_hash(Path(item['cwd'])/'benchmarks/Raft.Comparison')
    business = set()
    for v in variants:
        text = (Path(config['variants'][v]['cwd'])/'benchmarks/Raft.Benchmarks/ComparisonCluster.cs').read_text(encoding='utf-8-sig')
        common = text.replace('\r\n','\n').split('internal sealed class ComparisonMachine',1)[1].split('internal sealed class ComparisonCluster',1)[0]
        business.add(hashlib.sha256(common.encode()).hexdigest().upper())
    if len(business) != 1: raise AssertionError('Business/checkpoint transaction changed')
    manifest['business_sha256'] = business.pop()
    def save():
        (out/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n', encoding='utf-8')
    save()
    env = dict(os.environ, RAFT_BENCHMARK_ROOT=config['data_root'])
    for round_number in range(args.start_round, args.start_round + args.rounds):
        for series in args.series.split(','):
            operations, warmup = (512,128) if series == 'short' else (4096,2048)
            operations = args.operations or operations
            warmup = args.warmup or warmup
            for index, case in enumerate(args.cases.split(',')):
                nodes, payload, mode = case.split('-')
                order = list(variants)
                # Reverse every other pair; rotate triples to balance startup/time drift.
                if len(order) == 2:
                    if (round_number + index) % 2 == 0: order.reverse()
                else:
                    rotation = (round_number + index - 1) % len(order)
                    order = order[rotation:] + order[:rotation]
                    if round_number % 2 == 0: order.reverse()
                for variant in order:
                    item = config['variants'][variant]
                    stem = f'{series}-r{round_number}-{case}-{variant}'
                    raw = out/(stem+'.json')
                    command = ['dotnet', item['dll'], '--case', f'Current-{nodes}n-{payload}B-{mode}',
                               '--operations', str(operations), '--warmup', str(warmup),
                               '--repeats', '1', '--concurrency', '64', '--out', str(raw)]
                    command += ['--hot-add' if args.hot_add else '--hot-membership'] if item['wire'] == 3 else ['--unified']
                    if args.diagnose: command.append('--diagnose')
                    with (out/(stem+'.log')).open('w', encoding='utf-8') as log:
                        subprocess.run(command, cwd=item['cwd'], env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
                    report = json.loads(raw.read_text(encoding='utf-8-sig'))
                    if not report['Complete'] or report['Failure'] or len(report['Results']) != 1:
                        raise AssertionError(f'Incomplete run: {stem}')
                    if report['BinarySha256']['Raft.Net8.dll'] != item['binary_sha256']['Raft.Net8.dll']:
                        raise AssertionError('Runtime changed during measurement')
                    if report['CurrentSourceSha256'] != item['source_sha256'] or report['HarnessSourceSha256'] != item['harness_sha256']:
                        raise AssertionError('Frozen source metadata disagrees with measured runner')
                    row = report['Results'][0]
                    manifest['runs'].append(dict(variant=variant, series=series, case=case,
                                                 round=round_number, file=raw.name, sha256=sha(raw)))
                    save()
                    print(f'{stem}: {row["ProposalsPerSecond"]:.0f}/s p99={row["P99Milliseconds"]:.2f}ms alloc={row["AllocatedBytesPerProposal"]}', flush=True)
    for v in variants:
        item = config['variants'][v]
        if any(sha(Path(item['dll']).parent/name) != value for name,value in item['binary_sha256'].items()):
            raise AssertionError('Frozen runtime changed')
    manifest['complete'] = True
    save()
    print(f'COMPLETE: {len(manifest["runs"])} measurements', flush=True)


if __name__ == '__main__':
    main()
