"""Measure only current wire3/v4; retain R18's historical reference provenance."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

ROOT=Path(__file__).resolve().parents[1]

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()

def source_hash(folder):
    value=hashlib.sha256()
    for file in sorted((p for p in folder.rglob('*.cs') if not {'bin','obj'}.intersection(p.relative_to(folder).parts)),key=str):
        value.update((file.relative_to(folder).as_posix()+'\n').encode())
        value.update(file.read_bytes())
    return value.hexdigest().upper()

def business(text):
    return text.replace('\r\n','\n').split('internal sealed class ComparisonMachine',1)[1].split('internal sealed class ComparisonCluster',1)[0]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,default=ROOT/'.artifacts/winner/r19')
    parser.add_argument('--references',type=Path,default=ROOT/'benchmarks/results/winner-r18-summary-2026-11-13.json')
    parser.add_argument('--reference-source',type=Path,default=ROOT/'.artifacts/winner/final-sources-r18.zip')
    args=parser.parse_args()
    out=args.out.resolve();out.mkdir(parents=True,exist_ok=True)
    if any((out/name).exists() for name in ('manifest.json','short.json','long.json','runtime','current-source.zip')):
        raise FileExistsError('Use a new output directory; existing R19 evidence is preserved.')
    references=args.references.resolve()
    reference_hash=sha(references)
    reference=json.loads(references.read_text(encoding='utf-8-sig'))
    with zipfile.ZipFile(args.reference_source.resolve()) as archive:
        name,=[n for n in archive.namelist() if n.endswith('benchmarks/Raft.Benchmarks/ComparisonCluster.cs')]
        old_business=business(archive.read(name).decode('utf-8-sig'))
    current_business=business((ROOT/'benchmarks/Raft.Benchmarks/ComparisonCluster.cs').read_text(encoding='utf-8-sig'))
    if old_business!=current_business:raise AssertionError('Common business transaction/checkpoint differs from R18.')
    shutil.copyfile(references,out/'r18-reference-summary.json')
    source=ROOT/'benchmarks/Raft.Comparison/bin/Release/net8.0'
    shutil.copytree(source,out/'runtime')
    with zipfile.ZipFile(out/'current-source.zip','x',compression=zipfile.ZIP_DEFLATED) as archive:
        for directory in ('src','benchmarks','tools','HumanTest'):
            for file in sorted((ROOT/directory).rglob('*')):
                if file.is_file() and not {'bin','obj'}.intersection(file.relative_to(ROOT).parts) and file.suffix in ('.cs','.csproj','.sln','.json'):
                    archive.write(file,file.relative_to(ROOT).as_posix())
        for file in sorted(ROOT.iterdir()):
            if file.is_file() and file.suffix in ('.props','.targets','.json','.config','.sln'):
                archive.write(file,file.name)
    info=subprocess.run(['dotnet','--info'],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,check=True)
    (out/'dotnet-info.txt').write_bytes(info.stdout)
    data=reference['LongMetadata']['DataRoot']
    manifest=dict(Complete=False,ReferenceReport=str(references),ReferenceSummarySha256=reference_hash,
        ReferenceSourceSha256=sha(args.reference_source.resolve()),SourceArchiveSha256=sha(out/'current-source.zip'),
        Commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        CurrentSourceSha256=source_hash(ROOT/'src/Raft'),HarnessSourceSha256=source_hash(ROOT/'benchmarks/Raft.Comparison'),
        SourceFingerprintOrder="Ordinal full Windows path, matching the C# runner",
        CommonBusinessSha256=hashlib.sha256(current_business.encode()).hexdigest().upper(),
        BinarySha256={p.name:sha(p) for p in (out/'runtime').glob('*.dll')},DataRoot=data,
        MeasuredImplementations=['Current'],ReusedImplementations=['Baseline','DotNext'],Runs=[])
    def save():
        (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    save()
    env=dict(os.environ,RAFT_BENCHMARK_ROOT=data)
    for series,operations,warmup in [('long',4096,2048),('short',512,128)]:
        command=['dotnet',str(out/'runtime/Raft.Comparison.dll'),'--case','Current-','--hot-membership',
                 '--warmup',str(warmup),'--operations',str(operations),'--repeats','5','--concurrency','64','--out',str(out/(series+'.json'))]
        if series=='long':command.append('--batched-only')
        print(f'START {series}: current only, wire3/v4, five rounds',flush=True)
        with (out/(series+'.log')).open('w',encoding='utf-8') as log:
            subprocess.run(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        result=json.loads((out/(series+'.json')).read_text(encoding='utf-8-sig'))
        if not result['Complete'] or result['Failure'] is not None or any(r['Case']['Implementation']!='Current' for r in result['Results']):
            raise AssertionError(f'Unexpected or incomplete measurements: {series}')
        manifest['Runs'].append(dict(Series=series,Measurements=len(result['Results']),RawSha256=sha(out/(series+'.json'))))
        save()
        print(f'COMPLETE {series}: {len(result["Results"])} new measurements',flush=True)
    if source_hash(ROOT/'src/Raft')!=manifest['CurrentSourceSha256'] or source_hash(ROOT/'benchmarks/Raft.Comparison')!=manifest['HarnessSourceSha256']:
        raise AssertionError('Measured C# source changed during timing.')
    if sha(references)!=reference_hash:raise AssertionError('R18 reference report changed.')
    manifest['Complete']=True;save()
    print('COMPLETE R19: 180 new measurements, zero baseline/DotNext runs',flush=True)

if __name__=='__main__':main()
