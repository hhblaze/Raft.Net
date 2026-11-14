"""R18-style report: fresh current wire3/v4 measurements and explicitly reused R18 references."""
from __future__ import annotations
import argparse
from collections import defaultdict
import csv
import hashlib
import html
import json
import math
from pathlib import Path
import re
import statistics
import zipfile

import render_dotnext as shared
import render_winner as previous
from render_html import ASSETS, CHART_SHA256, number, payload

SCHEMA="raft8/v4: ToIndex prefixes 1=meta,2=log,3=config,4=service index,5=results; RF4M manifest; checked entries"
KIND="CurrentWire3V4WithHistoricalR18References"
REPEATS=5

def sha(data):
    return hashlib.sha256(data).hexdigest().upper()

def archived_source_hash(archive,prefix,windows_path_order=False):
    names=[n for n in archive.namelist() if n.startswith(prefix+"/") and n.endswith(".cs")]
    if windows_path_order:
        from pathlib import PureWindowsPath
        names=sorted(names,key=PureWindowsPath)
    else:names=sorted(names,key=lambda n:n.replace("/","\\"))
    result=hashlib.sha256()
    for name in names:
        result.update((name[len(prefix)+1:]+"\n").encode())
        result.update(archive.read(name))
    return result.hexdigest().upper()

def current_summary(path, long):
    raw=json.loads(path.read_text(encoding="utf-8-sig"))
    modes=["Batched","Pipeline"] if long else ["Batched","Pipeline","Unbatched","UnbatchedPipeline"]
    expected={(n,p,"Current",m) for n in (1,3,5) for p in (64,16384) for m in modes}
    operations,warmup=(4096,2048) if long else (512,128)
    if (not raw["Complete"] or raw["Failure"] is not None or raw["Repeats"]!=REPEATS
        or raw["ExpectedMeasurements"]!=len(expected)*REPEATS or len(raw["Results"])!=len(expected)*REPEATS
        or raw["RequestedOperations"]!=operations or raw["WarmupOperationsPerRound"]!=warmup or raw["Concurrency"]!=64
        or raw["CurrentLayout"]!="UnifiedTableV4 (wire3)" or raw["CurrentLayoutSchema"]!=SCHEMA
        or not raw["HotMembership"] or raw["HotAddDuringWorkload"] or raw["CurrentProposalBatchBytes"] is not None
        or not raw["Runtime"].startswith(".NET 8.")):
        raise ValueError(f"Incomplete/unexpected current-only workload: {path}")
    groups=defaultdict(list)
    for r in raw["Results"]:
        c=r["Case"];key=(c["Nodes"],c["PayloadBytes"],c["Implementation"],c["Mode"])
        if key not in expected or r["Operations"]!=operations or r["Concurrency"]!=64 or r["Diagnostics"] is not None:
            raise ValueError(f"Unexpected/instrumented case: {key}")
        samples=sorted(r["LatencyMilliseconds"])
        if len(samples)!=operations or any(not math.isfinite(s) or s<=0 for s in samples):
            raise ValueError("Invalid request latencies")
        checks=[(r["ProposalsPerSecond"],operations/r["Seconds"]),
                (r["P50Milliseconds"],shared.quantile(samples,.5)),(r["P99Milliseconds"],shared.quantile(samples,.99)),
                (r["MaxMilliseconds"],samples[-1])]
        if any(not math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-10) for a,b in checks):
            raise ValueError("Round summary differs from raw samples")
        groups[key].append(r)
    if set(groups)!=expected:raise ValueError("Missing current cases")
    rows=[]
    for n,p,v,m in sorted(expected,key=lambda k:(k[0],k[1],shared.VARIANTS.index((k[2],k[3])))):
        rounds=sorted(groups[n,p,v,m],key=lambda r:r["Round"])
        if [r["Round"] for r in rounds]!=list(range(1,6)):raise ValueError("Missing/duplicate rounds")
        samples=sorted(s for r in rounds for s in r["LatencyMilliseconds"]);rates=[r["ProposalsPerSecond"] for r in rounds]
        rows.append(dict(Nodes=n,PayloadBytes=p,Implementation=v,Mode=m,Rounds=5,Requests=len(samples),
            Ops=statistics.median(rates),MinOps=min(rates),MaxOps=max(rates),
            P50Ms=shared.quantile(samples,.5),P99Ms=shared.quantile(samples,.99),
            AllocatedKiB=statistics.median(r["AllocatedBytesPerProposal"] for r in rounds)/1024,
            RoundOps=rates,RoundP99Ms=[r["P99Milliseconds"] for r in rounds]))
    meta={k:v for k,v in raw.items() if k!="Results"}
    meta.update(RawFile=path.name,RawSha256=sha(path.read_bytes()))
    return rows,meta

def audit(summary):
    rows=[]
    for series in ("Short","Long"):
        for current in summary[series]:
            if current["Implementation"]!="Current" or current["Mode"]!="Pipeline":continue
            row={k:current[k] for k in ("Nodes","PayloadBytes","Ops")};row["Series"]=series
            for implementation,prefix in (("Baseline","Base"),("DotNext","Dot")):
                reference=shared.reference(current,summary[series],implementation)
                row[prefix+"Ratio"]=current["Ops"]/reference["Ops"]
                row[prefix+"AllocationRatio"]=current["AllocatedKiB"]/reference["AllocatedKiB"]
                row[prefix+"P99Ratio"]=current["P99Ms"]/reference["P99Ms"]
            rows.append(row)
    wins=sum(r["BaseRatio"]>1 and r["DotRatio"]>1 for r in rows)
    return dict(Mode="Pipeline",Comparisons=len(rows),MedianWinsOverBoth=wins,WinnerByMedianThroughput=wins==12,
        MultiNodeComparisons=sum(r["Nodes"]>1 for r in rows),
        MultiNodeMedianWinsOverBoth=sum(r["Nodes"]>1 and r["BaseRatio"]>1 and r["DotRatio"]>1 for r in rows),
        AllocationLossesToBase=sum(r["BaseAllocationRatio"]>1 for r in rows),
        AllocationLossesToDotNext=sum(r["DotAllocationRatio"]>1 for r in rows),
        MultiNodeGeometricRatioToBase=statistics.geometric_mean(r["BaseRatio"] for r in rows if r["Nodes"]>1),
        MultiNodeGeometricRatioToDotNext=statistics.geometric_mean(r["DotRatio"] for r in rows if r["Nodes"]>1),
        HistoricalReferences=True,Rows=rows)

def r18_changes(summary):
    reference=json.loads(summary["References"]["SnapshotUtf8"])
    rows=[]
    for series in ("Short","Long"):
        for now in summary[series]:
            if now["Implementation"]!="Current" or now["Mode"]!="Pipeline":continue
            old=next(r for r in reference[series] if (r["Nodes"],r["PayloadBytes"],r["Implementation"],r["Mode"])==
                     (now["Nodes"],now["PayloadBytes"],"Current","Pipeline"))
            rows.append(dict(Series=series,Nodes=now["Nodes"],PayloadBytes=now["PayloadBytes"],R18Ops=old["Ops"],R19Ops=now["Ops"],
                OpsChange=(now["Ops"]/old["Ops"]-1)*100,P99Change=(now["P99Ms"]/old["P99Ms"]-1)*100,
                AllocationChange=(now["AllocatedKiB"]/old["AllocatedKiB"]-1)*100))
    return rows

def build_summary(artifacts):
    manifest=json.loads((artifacts/"manifest.json").read_text(encoding="utf-8"))
    if not manifest["Complete"] or manifest["MeasuredImplementations"]!=["Current"] or manifest["ReusedImplementations"]!=["Baseline","DotNext"]:
        raise ValueError("Incomplete experiment or unexpected measured implementations")
    reference_bytes=(artifacts/"r18-reference-summary.json").read_bytes()
    if sha(reference_bytes)!=manifest["ReferenceSummarySha256"]:raise ValueError("Reference snapshot changed")
    reference=json.loads(reference_bytes.decode("utf-8-sig"));previous.validate(reference)
    result=dict(ReportVersion=2,ReportKind=KIND,Manifest=manifest,
        References=dict(Report="winner-r18-2026-11-13.html",SummarySha256=manifest["ReferenceSummarySha256"],SnapshotUtf8=reference_bytes.decode("utf-8-sig")))
    if sha((artifacts/"current-source.zip").read_bytes())!=manifest["SourceArchiveSha256"]:raise ValueError("Source archive changed")
    with zipfile.ZipFile(artifacts/"current-source.zip") as archive:
        result["SourceHashes"]={key:archived_source_hash(archive,prefix) for key,prefix in
            (("CurrentSourceSha256","src/Raft"),("HarnessSourceSha256","benchmarks/Raft.Comparison"))}
        preflight={key:archived_source_hash(archive,prefix,True) for key,prefix in
            (("CurrentSourceSha256","src/Raft"),("HarnessSourceSha256","benchmarks/Raft.Comparison"))}
    if all(manifest[k]==v for k,v in result["SourceHashes"].items()):
        result["PreflightFingerprintOrder"]="Ordinal (same as C# runner)"
    elif all(manifest[k]==v for k,v in preflight.items()):
        result["PreflightFingerprintOrder"]="WindowsPath case-insensitive preflight ordering; canonical archive hashes in SourceHashes match C#"
    else:raise ValueError("Preflight source fingerprint differs from the immutable archive")
    for series in ("Short","Long"):
        fresh,meta=current_summary(artifacts/(series.lower()+".json"),series=="Long")
        if meta["RawSha256"]!=next(r["RawSha256"] for r in manifest["Runs"] if r["Series"]==series.lower()):
            raise ValueError("Raw file differs from frozen manifest")
        reused=[r for r in reference[series] if r["Implementation"]!="Current"]
        result[series]=sorted(fresh+reused,key=lambda r:(r["Nodes"],r["PayloadBytes"],shared.VARIANTS.index((r["Implementation"],r["Mode"]))))
        result[series+"Metadata"]=meta
    result["Date"]=min(result[s+"Metadata"]["StartedAtUtc"] for s in ("Short","Long"))[:10]
    result["WinnerAudit"]=audit(result);result["R18CurrentComparison"]=r18_changes(result)
    return result

def validate(summary):
    if summary["ReportVersion"]!=2 or summary["ReportKind"]!=KIND:raise ValueError("Wrong report type")
    manifest=summary["Manifest"]
    if not manifest["Complete"] or manifest["MeasuredImplementations"]!=["Current"] or manifest["ReusedImplementations"]!=["Baseline","DotNext"]:
        raise ValueError("Incomplete or unexpected execution manifest")
    if {(r["Series"],r["Measurements"]) for r in manifest["Runs"]}!={("short",120),("long",60)} or len(manifest["Runs"])!=2:
        raise ValueError("Unexpected executed measurement counts")
    if sha(summary["References"]["SnapshotUtf8"].encode())!=summary["References"]["SummarySha256"]:
        raise ValueError("Historical summary hash differs")
    if summary["References"]["SummarySha256"]!=summary["Manifest"]["ReferenceSummarySha256"]:
        raise ValueError("Historical snapshot differs from the manifest")
    reference=json.loads(summary["References"]["SnapshotUtf8"]);previous.validate(reference)
    shared.validate_summary(summary,5)
    for series,measurements,ops,warmup in (("Short",120,512,128),("Long",60,4096,2048)):
        meta=summary[series+"Metadata"]
        if meta["RawSha256"]!=next(r["RawSha256"] for r in manifest["Runs"] if r["Series"]==series.lower()):
            raise ValueError("Saved raw metadata differs from execution manifest")
        if (meta["ExpectedMeasurements"]!=measurements or meta["RequestedOperations"]!=ops or meta["WarmupOperationsPerRound"]!=warmup
            or meta["CurrentLayout"]!="UnifiedTableV4 (wire3)" or meta["CurrentLayoutSchema"]!=SCHEMA
            or meta["Repeats"]!=5 or not meta["HotMembership"] or meta["HotAddDuringWorkload"]):
            raise ValueError("Current metadata differs from current-only workload")
        if [r for r in summary[series] if r["Implementation"]!="Current"]!=[r for r in reference[series] if r["Implementation"]!="Current"]:
            raise ValueError("Historical reference rows were changed")
        for row in summary[series]:
            if len(row["RoundP99Ms"])!=5 or any(not math.isfinite(v) or v<=0 for v in row["RoundP99Ms"]):
                raise ValueError("Invalid round p99")
        for key in ("CurrentSourceSha256","HarnessSourceSha256"):
            if meta[key]!=summary["SourceHashes"][key]:raise ValueError(f"Source changed: {key}")
        if meta["BinarySha256"]!={k:summary["Manifest"]["BinarySha256"][k] for k in meta["BinarySha256"]}:
            raise ValueError("Binary differs from frozen runtime")
    for key in ("BinarySha256","CurrentSourceSha256","HarnessSourceSha256","PackageLockSha256","Runtime","OS",
                "Architecture","ProcessorCount","DataRoot","ServerGC","CurrentLayout","CurrentLayoutSchema"):
        if summary["ShortMetadata"][key]!=summary["LongMetadata"][key]:raise ValueError(f"Series metadata differs: {key}")
    for series in ("Short","Long"):
        meta=summary[series+"Metadata"]
        expected={"CurrentDefaultBatchedGroupBytes":1051264,"CurrentReplicationBatchBytes":1051264,
                  "CurrentApplicationBatchBytes":262144,"CurrentBatchedProposalDelayMilliseconds":2,
                  "CurrentSingleVoterCollectionDelayMilliseconds":1,"CurrentBulkProposalDelayMilliseconds":8,
                  "CurrentCommitNotificationPolicy":previous.COMMIT_R18,"CurrentProposalFlushPolicy":previous.POLICY_R17}
        if any(meta.get(k)!=v for k,v in expected.items()):raise ValueError("Unexpected current collection/byte/commit policies")
    if summary["WinnerAudit"]!=audit(summary) or summary["R18CurrentComparison"]!=r18_changes(summary):
        raise ValueError("Recomputed comparison differs")

def percent(value, lower=False):
    good=value<0 if lower else value>=0
    return f'<span class="{"gain" if good else "loss"}">{"+" if value>=0 else "−"}{number(abs(value),1)} %</span>'

def audit_html(summary):
    a=summary["WinnerAudit"]
    changes_from_old=summary["R18CurrentComparison"]
    loss_count=sum(r["OpsChange"]<0 for r in changes_from_old)
    largest_loss=min(r["OpsChange"] for r in changes_from_old)
    largest_p99=max(r["P99Change"] for r in changes_from_old)
    body=[]
    for row in a["Rows"]:
        body.append(f'<tr><th scope="row">{"Короткая" if row["Series"]=="Short" else "Длинная"}</th><td>{row["Nodes"]}</td><td>{payload(row["PayloadBytes"])}</td><td>{number(row["Ops"])}</td><td>{percent((row["BaseRatio"]-1)*100)}</td><td>{percent((row["DotRatio"]-1)*100)}</td></tr>')
    changes=[]
    for row in summary["R18CurrentComparison"]:
        changes.append(f'<tr><td>{"Короткая" if row["Series"]=="Short" else "Длинная"}</td><td>{row["Nodes"]}</td><td>{payload(row["PayloadBytes"])}</td><td>{number(row["R18Ops"])}</td><td>{number(row["R19Ops"])}</td><td>{percent(row["OpsChange"])}</td><td>{percent(row["P99Change"],True)}</td><td>{percent(row["AllocationChange"],True)}</td></tr>')
    return f'''<section class="panel" id="winner-audit" aria-labelledby="winner-title">
<h2 id="winner-title">R19: сопоставление с сохранёнными результатами R18</h2>
<p class="native-note" id="r18-regressions"><strong>Относительно нашего Raft в R18:</strong> медианная скорость R19 ниже в {loss_count}/12 точек, наибольшее изменение — <strong>{percent(largest_loss)}</strong>; наибольший рост p99 — <strong>{percent(largest_p99,True)}</strong>. Это прямое сопоставление сохранённых значений разных запусков, без нового контрольного прогона R18; причинный вывод о коде из этих чисел не следует. Все 12 точек открыты ниже.</p>
<p>Фиксированный режим <strong>«пакеты + pipeline4»</strong>: медиана выше обеих исторических опор в <strong>{a["MedianWinsOverBoth"]}/12</strong> точек; для 3/5 узлов — <strong>{a["MultiNodeMedianWinsOverBoth"]}/8</strong>. Режим не выбирался отдельно под строку.</p>
<p class="native-note"><strong>База и DotNext здесь не измерялись заново.</strong> Их пять раундов перенесены из R18 без изменения чисел. Это сравнение разных запусков, не синхронный парный эксперимент. Номера новых и старых раундов не образуют пары; число «парных выигрышей» не рассчитывается. Различия могут включать изменения состояния стенда.</p>
<details><summary>Все 12 точек относительно исторических опор</summary><div class="details-body table-scroll"><table id="winner-table">
<caption>Медианы пяти раундов; опоры — пакетные режимы R18.</caption><thead><tr><th>Серия</th><th>Узлы</th><th>Команда</th><th>R19, /с</th><th>Δ к базе R18</th><th>Δ к DotNext R18</th></tr></thead><tbody>{''.join(body)}</tbody></table></div></details>
<details open><summary>Что изменилось относительно нашего Raft в R18</summary><div class="details-body table-scroll"><table id="r18-current-table">
<caption>Одинаковый Pipeline и параметры нагрузки; R18 — wire2/v3, R19 — wire3/v4. Разные даты. Это дополнительное сопоставление, вне 360 наблюдений основной трёхсторонней таблицы. Для p99/выделений меньше лучше.</caption><thead><tr><th>Серия</th><th>Узлы</th><th>Команда</th><th>R18, /с</th><th>R19, /с</th><th>Δ скорость</th><th>Δ p99</th><th>Δ выделения</th></tr></thead><tbody>{''.join(changes)}</tbody></table></div></details>
<p class="native-note">Геометрическое отношение скорости на 3/5 узлах к базе R18: {percent((a["MultiNodeGeometricRatioToBase"]-1)*100)}, к DotNext R18: {percent((a["MultiNodeGeometricRatioToDotNext"]-1)*100)}. Выделения больше базы в {a["AllocationLossesToBase"]}/12 точек и DotNext в {a["AllocationLossesToDotNext"]}/12. Все задержки, диапазоны и проигрыши сохранены.</p></section>'''

def adapt_template(document):
    def replace(old,new):
        nonlocal document
        if old not in document:raise ValueError(f"Missing R18 template text: {old[:70]}")
        document=document.replace(old,new)
    def row(label,text):
        nonlocal document
        pattern=r'(<tr><th scope="row">'+re.escape(label)+r'</th><td>).*?(</td></tr>)'
        document,n=re.subn(pattern,lambda m:m[1]+text+m[2],document,flags=re.S)
        if n!=1:raise ValueError(f"Method row {label}: {n}")
    replace("Raft.Net после оптимизации — повторное сравнение на .NET 8","Raft.Net R19 — текущая wire3/v4 и исторические опоры R18")
    replace("После оптимизации · .NET 8 · TCP + диск · 5 раундов","R19 · .NET 8 · TCP + диск · 5 раундов")
    replace("Raft.Net: повторное сравнение","Raft.Net R19: текущая реализация")
    replace("текущая Raft.Net с журналом v3 / ToIndex","текущая Raft.Net wire3 / raft8-v4")
    replace('<h2>Пять раундов для всех</h2><p>База, текущая версия и DotNext измерены заново на одном томе. Порядок меняется между раундами; медленные повторы и все отдельные задержки сохранены.</p>',
            '<h2>Новые данные только для Raft.Net</h2><p>180 новых замеров, 307 200 задержек: 24 короткие и 12 длинных конфигураций × 5 раундов. Остальные 180 замеров/307 200 задержек — исторические база и DotNext из R18. Медленные повторы сохранены.</p>')
    replace("Результат относится к явному v3 и фиксированному рекомендованному режиму «пакеты + pipeline4». Старые конструкторы сохраняют v2; для существующих данных нужен offline-переход.",
            "R19 использует wire3/raft8-v4 и фиксированный режим «пакеты + pipeline4». Режим предназначен для свежих баз; конвертера v2/v3→v4 нет. Исторические опоры R18 относятся к прежним датам.")
    row("Платформа","Все участники измерялись на .NET 8 / Windows x64, по одному кластеру за раз. В R19 заново исполнялась только текущая Raft.Net; среда и даты исторических опор сохранены отдельно в metadata.")
    row("Короткие / длинные серии","Коротко: прогрев 128, затем 512 × 5. Долго: прогрев 2048, затем 4096 × 5; пакетные режимы. Новый кластер на каждый раунд. R19 циклически меняет порядок только режимов Current; база/DotNext целиком взяты из R18.")
    row("Дисковый журнал","База R18 — DBreeze v1. R19 — raft8-v4: одна основная физическая таблица, префиксы 1=meta, 2=log, 3=config, 4=service index, 5=results, RF4M и CRC. Служебное состояние hot membership включено, состав в этих нагрузках фиксирован. Общие бизнес-таблицы не изменены, один engine на узел. DotNext R18 — штатный persistent WAL AutoFlush/IntegrityCheck и DBreeze для бизнеса.")
    replace("V3, одна физическая таблица с ToIndex-префиксами, CRC записей и единый проверяемый manifest(state,frontier); atomic checkpoint(index,term), явная offline-миграция.",
            "V4, ToIndex-префиксы, CRC/RF4M, atomic checkpoint(index,term), служебное состояние и результаты операций. Для новых баз; автоматической миграции в v4 нет.")
    replace("Voters/learners, одиночные изменения состава, потоковые восстанавливаемые snapshots.",
            "Журналируемые hot Add/Remove, operationId, learner→voter, автоматическая передача при удалении лидера, потоковые snapshots. В R19 измеряется фиксированный состав, без Add/Remove.")
    replace("Wire2, multiplexing и управляемый pipeline; mTLS доступен.","Wire3, multiplexing, pipeline и доверенный обмен справочником; mTLS доступен, здесь не измеряется.")
    replace("Locked restore, Release build без предупреждений и@@@TEST_COUNT@@@ теста прошли перед измерениями, включая v2/v3, реальные process-kill, снимки и3/5-процессное членство. После оптимизаций проверены демонстрация и BenchmarkDotNet smoke. Бинарники для этой матрицы зафиксированы; исходники базы8b991bd и DotNext не менялись. Хеши сохранены.",
            "Для R19 выполнены restore и Release build без предупреждений; runtime и исходники зафиксированы. Реализация ранее прошла 402/402 теста, HumanTest и 29 BDN smoke; это предшествующая функциональная проверка hot membership, а не новый прогон тестов внутри R19. Все новые замеры проверяют эффекты каждой реплики и повторное открытие DBreeze. Хеши и сохранённая история R18 приведены отдельно.")
    replace("Пробные128-командные smoke-прогоны и прежние измерения не входят в эти таблицы. Актуальная6.x DotNext здесь не исполнялась; её скорость из5.x не выводится.",
            "Smoke-прогоны не входят в таблицы. База/DotNext перенесены из R18 по запросу пользователя, без новых исполнений и без удаления старых раундов. Версии/особенности DotNext относятся к проверке R18, не к новой проверке актуальности пакетов.")
    replace("python -B benchmarks/render_winner.py","python -B benchmarks/render_r19.py --artifacts .artifacts/winner/r19")
    replace("Период выполнения (ISO8601, UTC):","Период новых измерений Current (ISO8601, UTC):")
    replace('<p class="note">Период новых измерений Current (ISO8601, UTC): @@@PERIOD@@@</p>',
            '<p class="note">Период новых измерений Current (ISO8601, UTC): @@@PERIOD@@@</p><p class="note">Период сохранённых опор R18: @@@REFERENCE_PERIOD@@@. @@@ENVIRONMENT_NOTE@@@</p>')
    replace("Трёхстороннее сравнение · отдельные повторные измерения на .NET8","R19 · Current измерен заново · база и DotNext — исторические результаты R18")
    replace("Текущая v3","Текущая v4 · R19")
    replace("Базовая',Current:","Базовая · R18',Current:")
    replace("DotNext:'DotNext 5.26.2'","DotNext:'DotNext 5.26.2 · R18'")
    replace("label:'База ·","label:'База R18 ·")
    replace("label:'DotNext + адаптер ·","label:'DotNext R18 + адаптер ·")
    replace("label:'Текущая ·","label:'Текущая R19 ·")
    replace("JSON.stringify({ShortMetadata:DATA.ShortMetadata,LongMetadata:DATA.LongMetadata},null,2)",
            "JSON.stringify({R19:{ShortMetadata:DATA.ShortMetadata,LongMetadata:DATA.LongMetadata,Manifest:DATA.Manifest,CanonicalArchiveSourceHashes:DATA.SourceHashes,PreflightFingerprintOrder:DATA.PreflightFingerprintOrder},HistoricalR18:{SummarySha256:DATA.References.SummarySha256,ShortMetadata:JSON.parse(DATA.References.SnapshotUtf8).ShortMetadata,LongMetadata:JSON.parse(DATA.References.SnapshotUtf8).LongMetadata}},null,2)")
    replace("Предыдущий отчёт до оптимизаций","Исторический отчёт R18")
    replace('href="dotnext-comparison-2026-11-13.html"','href="winner-r18-2026-11-13.html"')
    replace("../../.artifacts/winner/dotnet-info.txt","@@@RAW_BASE@@@/dotnet-info.txt")
    replace("../../.artifacts/winner/tests/@@@TEST_RUN@@@-full.trx","../../.artifacts/hot-membership/tests/final-validated-full.trx")
    replace("Эти игнорируемые artifacts могут отсутствовать в другой копии.",
            'Эти игнорируемые artifacts могут отсутствовать в другой копии. Raw-файлы R19 содержат только Current. Исторические raw R18: <a href="../../.artifacts/winner/r18/short.json">short</a>, <a href="../../.artifacts/winner/r18/long.json">long</a>. Полный snapshot сводки R18 встроен в JSON R19.')
    # Preserve the old report; only R19's links use the new date/tag.
    for part in ("summary","short","long"):
        replace(f"winner-{part}-2026-11-13.",f"@@@PREFIX@@@-{part}-@@@DATE@@@.")
    return document

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts",type=Path,default=Path(".artifacts/winner/r19"))
    parser.add_argument("--output",type=Path,default=Path("benchmarks/results"))
    parser.add_argument("--summary",type=Path,help="Recreate using only the verified saved summary.")
    args=parser.parse_args()
    summary=json.loads(args.summary.read_text(encoding="utf-8-sig")) if args.summary else build_summary(args.artifacts)
    validate(summary)
    prefix="winner-r19";date=summary["Date"]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}",date):raise ValueError("Invalid report date")
    args.output.mkdir(parents=True,exist_ok=True)
    summary_path=args.output/f"{prefix}-summary-{date}.json"
    summary_path.write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8",newline="\n")
    for series in ("Short","Long"):
        rows=summary[series]
        with (args.output/f"{prefix}-{series.lower()}-{date}.csv").open("w",encoding="utf-8",newline="") as file:
            fields=[key for key in rows[0] if not key.startswith("Round") or key=="Rounds"]
            writer=csv.DictWriter(file,fieldnames=fields,extrasaction="ignore");writer.writeheader();writer.writerows(rows)
    library=(ASSETS/"chart.umd.js").read_bytes()
    if hashlib.sha256(library).hexdigest()!=CHART_SHA256:raise ValueError("Pinned Chart.js differs")
    css=re.search(r"<style>(.*?)</style>",(ASSETS/"report.html").read_text(encoding="utf-8"),re.S)[1]
    shared.NAMES.update(Current="Текущая v4 · R19",Baseline="Базовая · R18",DotNext="DotNext 5.26.2 · R18")
    short,long=summary["Short"],summary["Long"]
    historical=json.loads(summary["References"]["SnapshotUtf8"])
    environment_keys=("Runtime","OS","Architecture","ProcessorCount","ServerGC","DataRoot")
    differences=[key for key in environment_keys if summary["LongMetadata"][key]!=historical["LongMetadata"][key]]
    environment_note=("Различаются зарегистрированные параметры среды: "+", ".join(differences)+"."
                      if differences else "Зарегистрированные runtime/OS/CPU count/GC/том совпадают; состояние кэшей и фоновой нагрузки между датами не контролировалось.")
    def period(data):
        first=min(data[s+"Metadata"]["StartedAtUtc"] for s in ("Short","Long"))
        last=max(data[s+"Metadata"]["UpdatedAtUtc"] for s in ("Short","Long"))
        return html.escape(first+" — "+last)
    import os
    raw_base=Path(os.path.relpath(args.artifacts.resolve(),args.output.resolve())).as_posix()
    values=dict(
        CSS=css,WINNER_AUDIT=audit_html(summary),PREFIX=prefix,DATE=date,RAW_BASE=html.escape(raw_base),TEST_COUNT="402",
        SHORT_OVERVIEW=shared.overview(short),LONG_OVERVIEW=shared.overview(long),HEAD=shared.headers(),
        SHORT_ROWS=shared.rows_html(short),LONG_ROWS=shared.rows_html(long),FINDINGS=shared.findings(short,long),
        SAMPLES=number(sum(r["Requests"] for r in short+long)),SOURCE_HASH=hashlib.sha256(summary_path.read_bytes()).hexdigest(),
        PERIOD=period(summary),
        REFERENCE_PERIOD=period(historical),
        ENVIRONMENT_NOTE=html.escape(environment_note),
        DATA=json.dumps(summary,ensure_ascii=False,separators=(",",":")).replace("<","\\u003c"),
        CHART_JS=re.sub(r"^//# sourceMappingURL=.*$","",library.decode(),flags=re.M),
        LICENSE=html.escape((ASSETS/"Chart.js.LICENSE.md").read_text(encoding="utf-8")),
        BATCH_BOUNDS_NOTE="Применение всех стеков: до 128 записей / 256 КиБ, 21 Б метаданных на запись; для 16 КиБ — до 15 команд. База R18 — wire 256 КиБ; Current R19 — wire 1 МиБ + 2688 Б при том же count128. Бизнес-транзакция не менялась.",
        GROUP_SPLIT_NOTE="затем делит его на сетевые порции до 1 МиБ + 2688 Б и прикладные до 256 КиБ",
        FRAME_NOTE=" Wire3/v4 выбран явно. Максимальный кадр по умолчанию 1052288 Б; около границы 1 МиБ пул может выделить блок 2 МиБ. В одиночных режимах count1 и задержки0. Пределы кадров и очередей ограничены.",
        COMMIT_NOTE=" Обычный commit index передаётся с очередными данными/heartbeat. ReadIndex, конфигурация и transfer используют немедленные управляющие пути. Клиент по-прежнему ждёт durable-кворум и apply лидера; follower apply может отстать на логический heartbeat плюс I/O/сеть/очередь.",
        FOLLOWER_NOTE=" Follower объединяет только уже стоящие в очереди свежие однотермовые последовательные хвостовые пакеты; отдельные коррелированные ACK выходят после ограниченной общей durable-записи.",
        CACHE_NOTE=" Опубликованный ограниченный кэш доступен во время I/O; публикация атомарна под отдельным коротким lock.",
        COLLECTION_NOTE="Current собирает предложения вне владельца консенсуса: до 2 мс для малых групп, до 8 мс для байтовых групп с другим принятым work-in-flight, один voter — до 1 мс",
    )
    document=adapt_template((ASSETS/"winner.html").read_text(encoding="utf-8"))
    for token in set(re.findall(r"@@@(\w+)@@@",document)):
        if token not in values:raise ValueError(f"Unresolved token {token}")
        document=document.replace(f"@@@{token}@@@",values[token])
    if re.search(r"@@@\w+@@@",document):raise ValueError("Unresolved template")
    target=args.output/f"{prefix}-{date}.html"
    target.write_text(document,encoding="utf-8",newline="\n")
    print(json.dumps(dict(report=str(target),freshMeasurements=180,reusedMeasurements=180,
        freshSamples=307200,reusedSamples=307200,medianWins=summary["WinnerAudit"]["MedianWinsOverBoth"],
        comparisons=12,currentChangesFromR18=summary["R18CurrentComparison"]),ensure_ascii=False))

if __name__=="__main__":main()
