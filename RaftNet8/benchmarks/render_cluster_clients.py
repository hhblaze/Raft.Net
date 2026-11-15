"""Render saved external-client measurements as an offline HTML report (standard library only)."""
from __future__ import annotations
import argparse
import csv
import hashlib
import html
import json
from pathlib import Path
import statistics


def aggregate(report):
    groups = {}
    for row in report["Results"]:
        groups.setdefault((row["Voters"], row["PayloadBytes"]), []).append(row)
    output = []
    for (voters, payload), rows in sorted(groups.items()):
        samples = sorted(value for row in rows for value in row["LatencyMilliseconds"])
        assert len(rows) == report["Rounds"]
        assert len(samples) == report["Requests"] * report["Rounds"]
        output.append({"Voters": voters, "PayloadBytes": payload, "Rounds": len(rows), "Requests": len(samples),
            "CommandsPerSecond": statistics.median(row["CommandsPerSecond"] for row in rows),
            "P50Milliseconds": samples[(len(samples) * 50 + 99) // 100 - 1],
            "P99Milliseconds": samples[(len(samples) * 99 + 99) // 100 - 1],
            "AllocatedKiBPerCommand": statistics.median(row["AllocatedBytesPerCommand"] for row in rows) / 1024,
            "MinimumCommandsPerSecond": min(row["CommandsPerSecond"] for row in rows),
            "MaximumCommandsPerSecond": max(row["CommandsPerSecond"] for row in rows)})
    return output


def esc(value):
    return html.escape(str(value), quote=True)


def number(value):
    return f"{value:,.2f}".replace(",", " ")


def chart(rows, metric, title, unit):
    maximum = max(row[metric] for row in rows)
    bars = []
    for index, row in enumerate(rows):
        y = 35 + index * 58
        value = row[metric]
        label = f'{row["Voters"]} уз. · {"64 B" if row["PayloadBytes"] == 64 else "16 KiB"}'
        color = "#087f79" if row["Voters"] == 3 else "#2f5bcc"
        bars.append(f'<text x="5" y="{y + 18}" font-size="14">{esc(label)}</text><rect x="138" y="{y}" width="{value / maximum * 350:.2f}" height="29" rx="4" fill="{color}"/><text x="{148 + value / maximum * 350:.2f}" y="{y + 19}" font-size="14">{number(value)}</text>')
    return f'<section><h2>{esc(title)}</h2><svg viewBox="0 0 610 285" role="img" aria-label="{esc(title)}">{"".join(bars)}<text x="138" y="282" font-size="12">{esc(unit)}</text></svg></section>'


def table(rows):
    body = "".join(f'<tr><td>{r["Voters"]}</td><td>{"64 B" if r["PayloadBytes"] == 64 else "16 KiB"}</td><td>{number(r["CommandsPerSecond"])}</td><td>{number(r["P50Milliseconds"])}</td><td>{number(r["P99Milliseconds"])}</td><td>{number(r["AllocatedKiBPerCommand"])}</td><td>{number(r["MinimumCommandsPerSecond"])}–{number(r["MaximumCommandsPerSecond"])}</td></tr>' for r in rows)
    return '<div class="scroll"><table><thead><tr><th>Узлы</th><th>Тело</th><th>Команд/с ↑</th><th>p50, ms ↓</th><th>p99, ms ↓</th><th>KiB/ком. ↓</th><th>Мин.–макс., команд/с</th></tr></thead><tbody>' + body + '</tbody></table></div>'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("initial", type=Path); parser.add_argument("final", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--additional", type=Path, action="append", default=[])
    args = parser.parse_args()
    initial = json.loads(args.initial.read_text(encoding="utf-8-sig")); final = json.loads(args.final.read_text(encoding="utf-8-sig"))
    assert initial["Completed"] and final["Completed"]
    assert initial["Rounds"] == final["Rounds"] and initial["Requests"] == final["Requests"] and initial["Concurrency"] == final["Concurrency"]
    summary = aggregate(final); first = aggregate(initial)
    source = Path(__file__).resolve().parents[1] / "src/Raft"
    digest = hashlib.sha256()
    files = sorted((p for p in source.rglob("*.cs") if not {"bin", "obj"}.intersection(p.parts)), key=lambda p: p.relative_to(source).as_posix())
    for path in files:
        digest.update((path.relative_to(source).as_posix() + "\n").encode())
        digest.update(path.read_text(encoding="utf-8-sig").encode())
    fingerprint = digest.hexdigest().upper()
    assert len(files) == 36 and fingerprint == "8851CD78E3A5042138F12E367805DDBB50CA51F28119347DB9EF0EACC29E58E5"
    additional = []
    for path in args.additional:
        data = json.loads(path.read_text(encoding="utf-8-sig")); assert data["Completed"]
        additional.append({"Path": str(path), "Sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(), "Measurements": data, "Summary": aggregate(data)})
    report = {"SchemaVersion": 1, "SourceCommit": "a8c2bbb3d5eb53c3e46990d0272e8b2ce071a842 + uncommitted client/server implementation",
        "CoreSourceMatchesAcceptedBaseline": True, "CoreSourceFingerprint": fingerprint,
        "InitialInput": {"Path": str(args.initial), "Sha256": hashlib.sha256(args.initial.read_bytes()).hexdigest().upper()},
        "FinalInput": {"Path": str(args.final), "Sha256": hashlib.sha256(args.final.read_bytes()).hexdigest().upper()},
        "Initial": initial, "Final": final, "Summary": summary, "InitialSummary": first, "AdditionalMeasurements": additional}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.with_suffix(".json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with args.out.with_suffix(".csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["Version", *summary[0].keys()]); writer.writeheader()
        for version, rows in (("Initial", first), ("Final", summary)):
            writer.writerows({"Version": version, **row} for row in rows)
        for index, item in enumerate(additional, 1):
            writer.writerows({"Version": f"Intermediate-{index}", **row} for row in item["Summary"])
    total_samples = sum(len(row["LatencyMilliseconds"]) for run in [initial, final, *[item["Measurements"] for item in additional]] for row in run["Results"])
    intermediate = "".join(f'<details><summary>Промежуточный прогон {index + 1}: все повторы сохранены</summary><p><code>{esc(item["Path"])}</code>; SHA-256 исходного JSON <code>{item["Sha256"]}</code>. Эти результаты доступны в полном JSON отчёта.</p>{table(item["Summary"])}</details>' for index, item in enumerate(additional))
    document = f'''<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Raft .NET8 — внешний клиент, 15 ноября 2026</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#f3f5fa;color:#183047;font:17px/1.6 system-ui,sans-serif}}main{{max-width:1450px;margin:auto;padding:28px}}section,header,details{{background:white;border:1px solid #dbe3ee;border-radius:12px;padding:25px;margin:0 0 20px}}h1{{font-size:32px}}h2{{font-size:22px}}p{{max-width:1150px}}.note{{background:#fff4dd;border-left:5px solid #bd7b1d;padding:15px}}.grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:20px}}svg{{width:100%;height:auto}}.scroll{{overflow:auto}}table{{border-collapse:collapse;width:100%;white-space:nowrap}}th,td{{padding:12px;text-align:right;border-bottom:1px solid #dfe5f0}}th{{background:#eef3f9}}tr:nth-child(even){{background:#f7f9fd}}code{{overflow-wrap:anywhere}}a{{color:#087f79}}summary{{cursor:pointer;font-weight:600}}@media(max-width:850px){{main{{padding:12px}}.grid{{grid-template-columns:1fr}}}}@media print{{body{{background:white}}section,header,details{{break-inside:avoid;border-radius:0}}details{{display:block}}.grid{{display:block}}}}
</style><main><header><h1>Внешний клиент Raft .NET8</h1><p>Свежие замеры HTTP-сервиса с полной историей сообщений Standard. Пять раундов на каждую из четырёх конфигураций, 3/5 узлов и тела64B/16KiB.</p><p class="note">Это другая нагрузка, чем прежний benchmark embedded-ядра: здесь есть HTTP, requestId, полный ledger и отдельная прикладная БД. Числа нельзя объявлять регрессией или ускорением относительно R18/R19. Все36 исходных файлов ядра совпадают с принятой базой d44147e.</p></header>
<section><h2>Итоговый вариант</h2><p>После128 прогревочных команд —512 измеренных запросов, concurrency64, отдельно новый дисковый кластер на каждый раунд. Команд/с и KiB/ком. — медианы пяти раундов. p50/p99 — nearest-rank по всем2560 индивидуальным задержкам конфигурации.</p>{table(summary)}<p>Выделения измерены для всего процесса: клиент, HTTP и все реплики. Это allocated bytes, а не RSS/живой размер памяти.</p></section>
<div class="grid">{chart(summary, "CommandsPerSecond", "Пропускная способность", "команд/с, больше лучше")}{chart(summary, "P99Milliseconds", "Хвост задержки p99", "миллисекунды, меньше лучше")}</div>
<section><h2>Воспроизводимость и границы</h2><p>{esc(final["Workload"])}. {esc(final["Runtime"])}; {esc(final["OS"])}. Данные: <code>{esc(final["DataRoot"])}</code>. Все реплики после измерений прошли проверку счётчика и applied fence. Batching/pipeline и durability не отключались; измерения не включают Add/Remove или снимки.</p><p>Ядро: <code>{esc(final["CoreSha256"])}</code><br>Клиент: <code>{esc(final["ClientSha256"])}</code><br>Сервер: <code>{esc(final["ServerSha256"])}</code><br>Нормализованные исходники ядра: <code>{fingerprint}</code>.</p><p>Большие сообщения16MB, восстановление снимком, отмена Add, смена лидера и файл3GiB проверяются отдельно функциональными сценариями. Этот benchmark не доказывает ёмкость production-кластера, устойчивость оборудования к потере питания или тест80GB.</p><p><a href="{esc(args.out.with_suffix('.json').name)}">Полные данные и все задержки JSON</a> · <a href="{esc(args.out.with_suffix('.csv').name)}">Сводка CSV</a> · <a href="../../docs/CLUSTER-CLIENTS.md">Контракт клиента</a></p></section>
<details><summary>Первоначальный вариант сервиса — отдельные последовательные пять раундов</summary><p>Сохранён для прозрачности. Итоговый путь убирает предварительную дисковую проверку новых requestId в пользу ограниченного кэша результатов и сохраняет несколько TCP flights при больших лимитах сообщения. Сравнение последовательных серий не изолирует влияние нагрузки компьютера; устойчивый прирост throughput этими данными не установлен.</p>{table(first)}<p>Исходные данные: <code>{esc(initial["DataRoot"])}</code>; SHA-256 сервера <code>{esc(initial["ServerSha256"])}</code>. Все {total_samples} измеренных задержек всех сохранённых серий доступны в JSON.</p></details>{intermediate}
</main></html>'''
    args.out.write_text(document, encoding="utf-8", newline="\n")
    print(json.dumps({"Report": str(args.out), "FinalMeasuredRequests": sum(row["Requests"] for row in summary), "Summary": summary}, ensure_ascii=False))


if __name__ == "__main__":
    main()
