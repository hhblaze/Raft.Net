"""Validate the complete five-round prefixed-v3 comparison and render a new offline report.

From RaftNet8/: python -B benchmarks/render_winner.py
The earlier DotNext report and all exploratory measurements remain separate.
"""
import argparse
import csv
import hashlib
import html
import json
import math
import re
import statistics
from pathlib import Path

import render_dotnext as shared
from render_html import ASSETS, CHART_SHA256, number, payload

DATE = "2026-11-13"
REPEATS = 5
SCHEMA = "raft8/v3: ToIndex prefixes 1=meta,2=log,3=config; RF3M packed state/frontier; checked v2 entries"
POLICY_R4 = "drain queued group; flush full replication batch without waiting; bounded 2ms for small groups"
POLICY_R5 = "drain queued group; flush full replication batch without waiting; bounded 2ms for multi-voter,1ms for single-voter groups"
CACHE_R6 = "published entries readable during unrelated flush; atomic short-gate publication"
POLICY_R11 = "off-owner work-conserving deadline; bounded 2ms count-limited,16ms byte-limited with committed backlog,1ms single-voter collection"
COLLECTOR_R11 = "bounded change signal; owner drains admitted commands without waiting"
FOLLOWER_R12 = "already-queued plain same-term tail extensions; original per-frame replies after one bounded durable write"
POLICY_R13 = "off-owner queue-head deadline; bounded 2ms count-limited,16ms byte-limited selected with committed backlog,1ms single-voter collection"
WINDOW_R13 = "selected once per queue head; prior apply completion does not shorten an open deadline"
POLICY_R16 = "off-owner in-flight feedback deadline; bounded 2ms count-limited,25ms byte-limited with other admitted work,1ms single-voter collection"
POLICY_R17 = "off-owner in-flight feedback deadline; bounded 2ms count-limited,8ms byte-limited with other admitted work,1ms single-voter collection"
COMMIT_R18 = "ordinary idle commit notifications piggyback next data/heartbeat; read/config/transfer immediate"


def audit(summary):
    rows = []
    for name in ("Short", "Long"):
        source = summary[name]
        for current in source:
            if current["Implementation"] != "Current" or current["Mode"] != "Pipeline":
                continue
            row = {k: current[k] for k in ("Nodes", "PayloadBytes", "Ops")}
            row["Series"] = name
            for implementation, prefix in (("Baseline", "Base"), ("DotNext", "Dot")):
                reference = shared.reference(current, source, implementation)
                row[prefix + "Ratio"] = current["Ops"] / reference["Ops"]
                row[prefix + "RoundWins"] = sum(a > b for a, b in zip(current["RoundOps"], reference["RoundOps"]))
                row[prefix + "RoundRatios"] = [a / b for a, b in zip(current["RoundOps"], reference["RoundOps"])]
                row[prefix + "AllocationRatio"] = current["AllocatedKiB"] / reference["AllocatedKiB"]
                row[prefix + "P99Ratio"] = current["P99Ms"] / reference["P99Ms"]
            rows.append(row)
    wins = sum(r["BaseRatio"] > 1 and r["DotRatio"] > 1 for r in rows)
    return {"Mode": "Pipeline", "Comparisons": len(rows), "MedianWinsOverBoth": wins,
            "WinnerByMedianThroughput": wins == len(rows) == 12,
            "MultiNodeComparisons": sum(r["Nodes"] > 1 for r in rows),
            "MultiNodeMedianWinsOverBoth": sum(r["Nodes"] > 1 and r["BaseRatio"] > 1 and r["DotRatio"] > 1 for r in rows),
            "AllocationLossesToBase": sum(r["BaseAllocationRatio"] > 1 for r in rows),
            "AllocationLossesToDotNext": sum(r["DotAllocationRatio"] > 1 for r in rows),
            "MultiNodeGeometricRatioToBase": statistics.geometric_mean(r["BaseRatio"] for r in rows if r["Nodes"] > 1),
            "MultiNodeGeometricRatioToDotNext": statistics.geometric_mean(r["DotRatio"] for r in rows if r["Nodes"] > 1),
            "Rows": rows}


def validate(summary):
    shared.validate_summary(summary, REPEATS)
    if summary.get("ReportVersion") != 1 or summary.get("WinnerAudit") != audit(summary):
        raise ValueError("Saved winner audit differs from the measured rows")
    first, second = summary["ShortMetadata"], summary["LongMetadata"]
    if first.get("CurrentCacheReadPolicy") != second.get("CurrentCacheReadPolicy"):
        raise ValueError("Workloads used different cache publication policies")
    for key in ("CurrentProposalCollector", "CurrentBulkProposalDelayMilliseconds", "CurrentFollowerAppendGrouping", "CurrentBulkWindowPolicy",
                "CurrentReplicationBatchBytes", "CurrentApplicationBatchBytes", "CurrentCommitNotificationPolicy"):
        if first.get(key) != second.get(key):
            raise ValueError(f"Workloads used different collection policies: {key}")
    for key in ("BinarySha256", "PackageLockSha256", "CurrentSourceSha256", "HarnessSourceSha256",
                "CommonApplicationSourceSha256", "Runtime", "OS", "Architecture", "ProcessorCount",
                "DataRoot", "ServerGC", "CurrentLayout", "CurrentLayoutSchema", "CurrentBatchedProposalDelayMilliseconds", "CurrentProposalFlushPolicy"):
        if first[key] != second[key]:
            raise ValueError(f"Workloads used different source/binary/environment: {key}")
    for name, operations, warmup, cases in (("Short", 512, 128, 48), ("Long", 4096, 2048, 24)):
        meta = summary[name + "Metadata"]
        if (meta["Repeats"] != REPEATS or meta["RequestedOperations"] != operations
                or meta["WarmupOperationsPerRound"] != warmup or meta["ExpectedMeasurements"] != cases * REPEATS
                or meta["CurrentLayout"] != "UnifiedTableV3 (explicit)" or meta["CurrentLayoutSchema"] != SCHEMA
                or meta["CurrentProposalBatchBytes"] is not None
                or meta["CurrentDefaultBatchedGroupBytes"] != 1_051_264
                or meta.get("CurrentCacheReadPolicy") not in (None, CACHE_R6)
                or meta.get("CurrentFollowerAppendGrouping") not in (None, FOLLOWER_R12)
                or meta.get("CurrentBulkWindowPolicy") not in (None, WINDOW_R13)
                or meta.get("CurrentReplicationBatchBytes") not in (None, 1_048_576, 1_051_264)
                or meta.get("CurrentApplicationBatchBytes") not in (None, 262_144)
                or meta.get("CurrentTcpSendBufferPolicy") is not None
                or meta.get("CurrentCommitNotificationPolicy") not in (None, COMMIT_R18)
                or meta["CurrentBatchedProposalDelayMilliseconds"] != 2
                or (meta["CurrentProposalFlushPolicy"], meta.get("CurrentSingleVoterCollectionDelayMilliseconds", 2)) not in ((POLICY_R4, 2), (POLICY_R5, 1), (POLICY_R11, 1), (POLICY_R13, 1), (POLICY_R16, 1), (POLICY_R17, 1))
                or meta["Baseline"] != "8b991bd" or meta["DotNextPackage"] != "5.26.2"
                or not meta["Runtime"].startswith(".NET 8.")):
            raise ValueError(f"Unexpected final workload/layout: {name}")
        if meta["CurrentProposalFlushPolicy"] in (POLICY_R11, POLICY_R13, POLICY_R16, POLICY_R17) and (meta.get("CurrentProposalCollector") != COLLECTOR_R11
                or meta.get("CurrentBulkProposalDelayMilliseconds") != (8 if meta["CurrentProposalFlushPolicy"] == POLICY_R17 else 25 if meta["CurrentProposalFlushPolicy"] == POLICY_R16 else 16)
                or meta.get("CurrentCacheReadPolicy") != CACHE_R6):
            raise ValueError("Unexpected adaptive collection/cache configuration")
        if ((meta["CurrentProposalFlushPolicy"] == POLICY_R13) != (meta.get("CurrentBulkWindowPolicy") == WINDOW_R13)
                or meta.get("CurrentBulkWindowPolicy") == WINDOW_R13 and meta.get("CurrentFollowerAppendGrouping") != FOLLOWER_R12):
            raise ValueError("Unexpected queue-head collection configuration")
        if ((meta.get("CurrentReplicationBatchBytes") is None) != (meta.get("CurrentApplicationBatchBytes") is None)
                or meta.get("CurrentReplicationBatchBytes") is not None and
                    (meta.get("CurrentFollowerAppendGrouping") != FOLLOWER_R12 or meta.get("CurrentBulkWindowPolicy") is not None)):
            raise ValueError("Unexpected independent replication/application budgets")
        if ((meta["CurrentProposalFlushPolicy"] == POLICY_R17) != (meta.get("CurrentReplicationBatchBytes") == 1_051_264)):
            raise ValueError("Unexpected frame headroom/collection policy")
        if meta.get("CurrentCommitNotificationPolicy") == COMMIT_R18 and meta["CurrentProposalFlushPolicy"] != POLICY_R17:
            raise ValueError("Unexpected commit-notification configuration")
        for row in summary[name]:
            if len(row["RoundP99Ms"]) != REPEATS or any(not math.isfinite(v) or v <= 0 for v in row["RoundP99Ms"]):
                raise ValueError("Invalid per-round latency data")


def audit_html(data):
    def percent(ratio):
        delta = (ratio - 1) * 100
        return f'<span class="{"gain" if delta >= 0 else "loss"}">{"+" if delta >= 0 else "−"}{number(abs(delta), 1)} %</span>'
    body = []
    for row in data["Rows"]:
        body.append('<tr><th scope="row">' + ("Короткая" if row["Series"] == "Short" else "Длинная") + '</th>'
                    f'<td class="numeric">{row["Nodes"]}</td><td>{payload(row["PayloadBytes"])}</td>'
                    f'<td class="numeric strong">{number(row["Ops"])}</td>'
                    f'<td class="numeric">{percent(row["BaseRatio"])}</td>'
                    f'<td class="numeric">{percent(row["DotRatio"])}</td>'
                    f'<td class="numeric">{row["BaseRoundWins"]}/5</td><td class="numeric">{row["DotRoundWins"]}/5</td></tr>')
    title = ("Текущая v3 лидирует по медианной скорости" if data["WinnerByMedianThroughput"]
             else "В сравнении остаются проигрыши текущей v3")
    return f'''<section class="panel" id="winner-audit" aria-labelledby="winner-title">
<h2 id="winner-title">{title}</h2>
<p>Фиксированный режим <strong>«пакеты + pipeline4»</strong>: выше обеих реализаций в
<strong>{data["MedianWinsOverBoth"]}/12</strong> конфигураций по медианам пяти раундов;
для 3 и 5 узлов — <strong>{data["MultiNodeMedianWinsOverBoth"]}/8</strong>.
Размеры команд и короткая/длинная нагрузка имеют равный вес в этой проверке. Режим не выбирался отдельно под каждую строку.</p>
<p>Геометрическое среднее отношения скорости на 3/5 узлах: к базе {percent(data["MultiNodeGeometricRatioToBase"])},
к DotNext {percent(data["MultiNodeGeometricRatioToDotNext"])}. Это результаты данного стенда и нагрузки.
Разброс дисковых повторов сохранён; междураундовый диапазон и число выигрышей по раундам показаны явно.</p>
<details><summary>Проверка всех 12 конфигураций и выигрышей по раундам</summary><div class="details-body table-scroll">
<table id="winner-table"><caption>Медиана текущей v3; Δ относительно пакетного режима каждой опорной реализации.
Последние столбцы считают пары с одинаковым номером раунда; кластеры выполнялись последовательно.</caption>
<thead><tr><th scope="col">Серия</th><th scope="col">Узлы</th><th scope="col">Команда</th>
<th scope="col">Текущая, /с</th><th scope="col">Δ к базе</th><th scope="col">Δ к DotNext</th>
<th scope="col">Раунды выше базы</th><th scope="col">Раунды выше DotNext</th></tr></thead>
<tbody>{''.join(body)}</tbody></table></div></details>
<p class="native-note"><strong>Удельные выделения памяти:</strong> текущая версия выделяет больше базы в
{data["AllocationLossesToBase"]}/12 конфигураций и больше DotNext в {data["AllocationLossesToDotNext"]}/12.
Точные КиБ/команду и задержки находятся в таблицах и графиках. Process-wide allocation включает все совместно работающие реплики;
это не размер удерживаемого кэша или RSS. Полные диапазоны, отдельные раунды и хеши сохранены в JSON.</p>
</section>'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=Path(".artifacts/winner"))
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results"))
    parser.add_argument("--summary", type=Path, help="Regenerate from the verified saved summary without raw files.")
    parser.add_argument("--tag", help="Keep distinct iteration filenames, for example r11.")
    args = parser.parse_args()
    if args.tag and not re.fullmatch(r"[a-z][a-z0-9-]{0,20}", args.tag):
        parser.error("Invalid report tag")
    if args.summary:
        summary = json.loads(args.summary.read_text(encoding="utf-8-sig"))
    else:
        summary = {"ReportVersion": 1}
        for name in ("Short", "Long"):
            path = args.artifacts / (name.lower() + ".json")
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
            if any(r["Case"]["Implementation"] != "DotNext" and r["Diagnostics"] is not None for r in raw["Results"]):
                raise ValueError("Instrumented exploratory runs are not final capacity data")
            summary[name], summary[name + "Metadata"] = shared.summarize(path, name == "Long", REPEATS)
        summary["WinnerAudit"] = audit(summary)
    validate(summary)
    modern = summary["LongMetadata"].get("CurrentProposalCollector") == COLLECTOR_R11
    follower_groups = summary["LongMetadata"].get("CurrentFollowerAppendGrouping") == FOLLOWER_R12
    selected_window = summary["LongMetadata"].get("CurrentBulkWindowPolicy") == WINDOW_R13
    separate_replication = summary["LongMetadata"].get("CurrentReplicationBatchBytes") in (1_048_576, 1_051_264)
    frame_headroom = summary["LongMetadata"].get("CurrentReplicationBatchBytes") == 1_051_264
    inflight_feedback = summary["LongMetadata"]["CurrentProposalFlushPolicy"] in (POLICY_R16, POLICY_R17)
    coalesced_commit = summary["LongMetadata"].get("CurrentCommitNotificationPolicy") == COMMIT_R18
    if modern and not args.tag:
        parser.error("Use a distinct --tag to preserve previous complete reports")
    short, long = summary["Short"], summary["Long"]
    args.output.mkdir(parents=True, exist_ok=True)
    prefix = "winner" + ("-" + args.tag if args.tag else "")
    summary_path = args.output / f"{prefix}-summary-{DATE}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    for name, rows in (("short", short), ("long", long)):
        with (args.output / f"{prefix}-{name}-{DATE}.csv").open("w", encoding="utf-8", newline="") as file:
            fields = [key for key in rows[0] if not key.startswith("Round") or key == "Rounds"]
            writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    css = re.search(r"<style>(.*?)</style>", (ASSETS / "report.html").read_text(encoding="utf-8"), re.DOTALL).group(1)
    library = (ASSETS / "chart.umd.js").read_bytes()
    if hashlib.sha256(library).hexdigest() != CHART_SHA256:
        raise ValueError("Pinned Chart.js asset differs")
    shared.NAMES["Current"] = "Текущая v3"
    replacements = {
        "CSS": css, "WINNER_AUDIT": audit_html(summary["WinnerAudit"]),
        "RAW_BASE": html.escape("../../" + args.artifacts.as_posix()),
        "TEST_RUN": (args.tag if modern else "r6" if summary["LongMetadata"].get("CurrentCacheReadPolicy") == CACHE_R6 else
                     "r5" if summary["LongMetadata"].get("CurrentSingleVoterCollectionDelayMilliseconds") == 1 else "r4"),
        "TEST_COUNT": "342" if coalesced_commit else "330" if frame_headroom else "329" if inflight_feedback else "327" if separate_replication else "320" if selected_window else "318" if follower_groups else "304" if modern else "299" if summary["LongMetadata"].get("CurrentCacheReadPolicy") == CACHE_R6 else "293",
        "BATCH_BOUNDS_NOTE": ("Прикладное применение всех участников: до128 записей и256 КиБ с21 Б учётных накладных расходов; для16 КиБ — до15 записей. Сетевой бюджет базы —256 КиБ, текущей версии —1 МиБ+2688 Б запаса под метаданные (вмещает64 команды по16 КиБ при том же пределе128 записей). Сетевой и прикладной пределы разделены; общая бизнес-транзакция не менялась."
                              if frame_headroom else "Прикладное применение всех участников: до128 записей и256 КиБ с21 Б учётных накладных расходов; для16 КиБ — до15 записей. Сетевой бюджет базы —256 КиБ, текущей версии —1 МиБ (до63 команд по16 КиБ при том же пределе128 записей). Сетевой и прикладной пределы разделены; общая бизнес-транзакция не менялась."
                              if separate_replication else "Сеть Raft.Net и бизнес-применение всех участников: до128 записей и256 КиБ с21 Б учётных накладных расходов; для16 КиБ — до15 записей."),
        "GROUP_SPLIT_NOTE": ("затем делит его на сетевые порции до1 МиБ+2688 Б и прежние прикладные до256 КиБ"
                             if frame_headroom else "затем делит его на сетевые порции до1 МиБ и прежние прикладные до256 КиБ"
                             if separate_replication else "затем делит его на прежние сетевые/прикладные порции"),
        "FRAME_NOTE": (" Большие wire2-пакеты требуют совместимых пределов на всех узлах; при обновлении старых v2-узлов явно оставляют256 КиБ, затем согласованно меняют конфигурацию. Байтовый формат не менялся. Максимальный кадр по умолчанию1052288 Б; около границы1 МиБ пул может выделить блок2 МиБ. Это дополнительный ограниченный расход памяти. Показаны автоматические бюджеты пакетных режимов(count128); в режимах по одной записи count1, автоматический wire/local бюджет1048597 Б и задержки0. Прикладной предел256 КиБ сохраняется."
                       if frame_headroom else ""),
        "COMMIT_NOTE": (" Обычный новый commit index передаётся с очередными данными или плановым heartbeat вместо отдельного немедленного пустого RPC. Чтения, конфигурация и transfer обслуживаются без этой намеренной задержки. Журнал/term/vote/commit сохраняются до зависимых действий; клиент ждёт кворум и apply лидера. Followers могут узнать новый commit на один логический heartbeat позже (дополнительно влияют I/O/сеть/очередь); CoalesceCommitNotifications=false возвращает прежнюю попытку немедленной рассылки."
                        if coalesced_commit else ""),
        "FOLLOWER_NOTE": (" Follower объединяет уже стоящие подряд в очереди обычные пакеты одного срока, продолжающие текущий хвост, в одну локальную durable-запись. Сбор не задерживается; отдельные ответы с исходными ID и индексами выдаются после записи. Число записей и локальные байты ограничены, исходный сетевой предел сохраняется; голоса, ReadIndex, конфигурация и snapshot служат границами."
                          if follower_groups else ""),
        "CACHE_NOTE": (" Опубликованный кэш доступен во время I/O; отдельный короткий lock защищает согласованную публикацию."
                       if summary["LongMetadata"].get("CurrentCacheReadPolicy") == CACHE_R6 else ""),
        "COLLECTION_NOTE": ("Current собирает команды вне цикла консенсуса: до2 мс для малых групп, до" + ("8" if frame_headroom else "25") + " мс для групп с байтовым ограничением, пока другие уже принятые команды ожидают кворум или apply. Когда остаётся только очередь, используется короткое окно; один голосующий — до1 мс"
                            if inflight_feedback else "Current собирает команды вне цикла консенсуса: до2 мс для малых групп, до16 мс для групп с байтовым ограничением, если при открытии окна есть committed-запас применения; завершение старой волны apply не сокращает открытый срок. Новая голова очереди без такого запаса использует короткое окно; один голосующий — до1 мс"
                            if selected_window else "Current собирает команды вне цикла консенсуса: до2 мс для малых групп, до16 мс для групп с байтовым ограничением при наличии committed-запаса применения; при пустом конвейере — короткое окно; один голосующий — до1 мс"
                            if modern else "Current собирает до2 мс при нескольких голосующих и до1 мс при единственном голосующем, завершая сбор раньше при заполнении сетевого пакета"
                            if summary["LongMetadata"].get("CurrentSingleVoterCollectionDelayMilliseconds") == 1 else
                            "Current собирает до2 мс, завершая сбор раньше при заполнении сетевого пакета"),
        "SHORT_OVERVIEW": shared.overview(short), "LONG_OVERVIEW": shared.overview(long),
        "HEAD": shared.headers(), "SHORT_ROWS": shared.rows_html(short), "LONG_ROWS": shared.rows_html(long),
        "FINDINGS": shared.findings(short, long),
        "DATA": json.dumps(summary, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c"),
        "CHART_JS": re.sub(r"^//# sourceMappingURL=.*$", "", library.decode("utf-8"), flags=re.MULTILINE),
        "LICENSE": html.escape((ASSETS / "Chart.js.LICENSE.md").read_text(encoding="utf-8")),
        "SAMPLES": number(sum(r["Requests"] for r in short + long)),
        "SOURCE_HASH": hashlib.sha256(summary_path.read_bytes()).hexdigest(),
        "PERIOD": html.escape(min(summary[k]["StartedAtUtc"] for k in ("ShortMetadata", "LongMetadata")) + " — " +
                              max(summary[k]["UpdatedAtUtc"] for k in ("ShortMetadata", "LongMetadata"))),
    }
    if modern and "8ms full-wire" in summary["LongMetadata"].get("Batch", ""):
        replacements["COLLECTION_NOTE"] += (
            "; в описательном поле Batch исходного JSON осталась старая подпись8 мс: "
            "фактические16 мс записаны в числовом CurrentBulkProposalDelayMilliseconds "
            "и подтверждаются зафиксированным исходным кодом")
    document = (ASSETS / "winner.html").read_text(encoding="utf-8")
    for key, value in replacements.items():
        token = f"@@@{key}@@@"
        if token not in document:
            raise ValueError(f"Missing template token {token}")
        document = document.replace(token, value)
    if re.search(r"@@@\w+@@@", document):
        raise ValueError("Unresolved template tokens")
    if prefix != "winner":
        for name in ("summary", "short", "long"):
            document = document.replace(f"winner-{name}-{DATE}.", f"{prefix}-{name}-{DATE}.")
    target = args.output / f"{prefix}-{DATE}.html"
    target.write_text(document, encoding="utf-8")
    result = summary["WinnerAudit"]
    print(f'Verified {len(short)} short + {len(long)} long cases, 5 rounds, '
          f'{sum(r["Requests"] for r in short+long):,} samples; median wins {result["MedianWinsOverBoth"]}/12; {target}.')


if __name__ == "__main__":
    main()
