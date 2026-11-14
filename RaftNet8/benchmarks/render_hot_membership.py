"""Render verified paired hot-membership measurements as a standalone offline HTML report."""
from __future__ import annotations
import argparse
from collections import defaultdict
import csv
import html
import json
import math
from pathlib import Path
from statistics import median

def esc(value):
    return html.escape(str(value), quote=True)

def number(value, digits=0):
    return f"{value:,.{digits}f}".replace(",", " ").replace(".", ",")

def percentile(values, probability):
    return sorted(values)[math.ceil(len(values) * probability) - 1]

def delta(value, lower_is_better=False):
    good = value < 0 if lower_is_better else value > 0
    return f'<span class="{"good" if good else "bad"}">{value:+.1f}%</span>'

def chart(title, rows, labels, metric, unit):
    width, height = 760, 340
    maximum = max(row[variant][metric] for row in rows for variant, _, _ in labels) * 1.2
    parts = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}"><title>{esc(title)}</title>']
    for tick in range(5):
        x = 150 + 480 * tick / 4
        parts += [f'<line x1="{x}" y1="38" x2="{x}" y2="300" stroke="#dce4ed"/>',
                  f'<text x="{x}" y="325" text-anchor="middle">{number(maximum*tick/4, 0 if maximum>100 else 1)}</text>']
    for i, row in enumerate(rows):
        y = 55 + i * 61
        label = f'{row["nodes"]} {"узла" if row["nodes"] == 3 else "узлов"} · {"64 B" if row["payload"] == 64 else "16 КиБ"}'
        parts.append(f'<text x="140" y="{y+15}" text-anchor="end">{esc(label)}</text>')
        for offset, (variant, color, _) in enumerate(labels):
            value = row[variant][metric]
            bar_y = y + offset * 21
            bar = value / maximum * 480
            parts += [f'<rect x="150" y="{bar_y}" width="{bar:.2f}" height="15" rx="3" fill="{color}"/>',
                      f'<text x="{156+bar:.2f}" y="{bar_y+12}">{number(value, 1 if maximum<100 else 0)}</text>']
    parts.append("</svg>")
    legend = " · ".join(f'<span style="color:{color}">■ {esc(label)}</span>' for _, color, label in labels)
    return f'<figure><h3>{esc(title)}</h3>{"".join(parts)}<figcaption>{legend} · {unit}</figcaption></figure>'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("raw", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--followup", type=Path, help="Optional disclosure of an untimed post-measurement change")
    args = parser.parse_args()
    manifest = json.loads((args.raw / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["complete"], "Incomplete experiment must not be published"
    groups = defaultdict(list)
    round_rows, metadata = [], {}
    for run in manifest["runs"]:
        data = json.loads((args.raw / run["file"]).read_text(encoding="utf-8-sig"))
        assert data["Complete"] and data["Failure"] is None and len(data["Results"]) == 1
        result = data["Results"][0]
        case = result["Case"]
        assert case["Implementation"] == "Current" and case["Mode"] == "Pipeline"
        assert result["Operations"] == manifest["operations"] == len(result["LatencyMilliseconds"])
        assert result["Concurrency"] == manifest["concurrency"] == 64
        variant = run["variant"]
        groups[(case["Nodes"], case["PayloadBytes"], variant)].append(result)
        assert all(value >= 0 for value in result["LatencyMilliseconds"])
        if variant == "head":
            assert data["BinarySha256"]["Raft.Net8.dll"] == "268C8F2A5DD121D2F65C44A60507413A974A57B22C7D5FD4BA0AE11C7AB856C9"
        if variant == "add":
            assert result["Diagnostics"]["Membership"]["Stage"] == "Completed"
        run_metadata = {key: data[key] for key in ("Runtime", "OS", "Architecture", "ProcessorCount", "ServerGC", "BinarySha256",
            "CurrentSourceSha256", "HarnessSourceSha256", "CurrentLayout", "DataRoot")}
        if variant in metadata:
            assert metadata[variant] == run_metadata, "Runtime/source/parameters changed within the measured series"
        metadata[variant] = run_metadata
        round_rows.append(dict(variant=variant, round=run["round"], nodes=case["Nodes"], payload=case["PayloadBytes"],
            throughput=result["ProposalsPerSecond"], p99=result["P99Milliseconds"],
            kib=result["AllocatedBytesPerProposal"]/1024, diagnostics=result["Diagnostics"]))
    summary = []
    for nodes in (3,5):
        for payload in (64,16384):
            row = dict(nodes=nodes, payload=payload)
            for variant in ("head", "wire3", "add"):
                values = groups[(nodes,payload,variant)]
                assert len(values) == (manifest["add_rounds"] if variant == "add" else manifest["rounds"])
                latencies = [value for result in values for value in result["LatencyMilliseconds"]]
                row[variant] = dict(throughput=median(v["ProposalsPerSecond"] for v in values),
                    minimum=min(v["ProposalsPerSecond"] for v in values), maximum=max(v["ProposalsPerSecond"] for v in values),
                    p50=percentile(latencies,.5), p99=percentile(latencies,.99),
                    kib=median(v["AllocatedBytesPerProposal"] for v in values)/1024, samples=len(latencies))
                if variant=="add":
                    row[variant].update(add_ms=median(v["Diagnostics"]["AddMilliseconds"] for v in values),
                        completed_during_commands=sum(v["Diagnostics"]["AddFinishedDuringCommands"] for v in values),
                        command_ms=median(v["Diagnostics"]["CommandMilliseconds"] for v in values))
            row["throughput_change"] = (row["wire3"]["throughput"]/row["head"]["throughput"]-1)*100
            row["p99_change"] = (row["wire3"]["p99"]/row["head"]["p99"]-1)*100
            row["allocation_change"] = (row["wire3"]["kib"]/row["head"]["kib"]-1)*100
            paired = [(new["ProposalsPerSecond"]/old["ProposalsPerSecond"]-1)*100 for old,new in
                      zip(groups[(nodes,payload,"head")],groups[(nodes,payload,"wire3")])]
            row["paired_throughput_changes"] = paired
            row["paired_wins"] = sum(value>0 for value in paired)
            summary.append(row)
    output = args.out.resolve()
    output.parent.mkdir(parents=True,exist_ok=True)
    report = dict(manifest=manifest, metadata=metadata, summary=summary, rounds=round_rows,
                  aggregation="Throughput/allocations: round medians; p50/p99: pooled nearest rank; min/max: round throughput.")
    if args.followup:
        report["validation_followup"] = json.loads(args.followup.read_text(encoding="utf-8"))
    output.with_suffix(".summary.json").write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    with output.with_suffix(".csv").open("w",newline="",encoding="utf-8-sig") as file:
        writer=csv.writer(file)
        writer.writerow(["nodes","payload_bytes","variant","median_commands_s","p50_ms","pooled_p99_ms","KiB_command","min_commands_s","max_commands_s"])
        for row in summary:
            for variant in ("head","wire3","add"):
                v=row[variant]
                writer.writerow([row["nodes"],row["payload"],variant,v["throughput"],v["p50"],v["p99"],v["kib"],v["minimum"],v["maximum"]])
    body=[]
    for row in summary:
        for variant, label in (("head","HEAD e06c4f9 · wire2/v3"),("wire3","Hot membership · wire3/v4")):
            v=row[variant]
            change="—" if variant=="head" else delta(row["throughput_change"])
            body.append(f'<tr data-nodes="{row["nodes"]}" data-payload="{row["payload"]}"><td>{row["nodes"]}</td><td>{"64 B" if row["payload"]==64 else "16 КиБ"}</td><td>{esc(label)}</td><td><b>{number(v["throughput"])}</b></td><td>{change}</td><td>{number(v["p50"],2)}</td><td>{number(v["p99"],2)}</td><td>{number(v["kib"],2)}</td><td>{number(v["minimum"])}–{number(v["maximum"])}</td></tr>')
    changes_body=[]
    for row in summary:
        changes_body.append(f'<tr><td>{row["nodes"]}</td><td>{"64 B" if row["payload"]==64 else "16 КиБ"}</td><td>{delta(row["throughput_change"])}</td><td>{delta(row["p99_change"],True)}</td><td>{delta(row["allocation_change"],True)}</td><td>{row["paired_wins"]}/{manifest["rounds"]}</td></tr>')
    add_body=[]
    for row in summary:
        v=row["add"]
        change=(v["throughput"]/row["wire3"]["throughput"]-1)*100
        add_body.append(f'<tr data-nodes="{row["nodes"]}" data-payload="{row["payload"]}"><td>{row["nodes"]}→{row["nodes"]+1}</td><td>{"64 B" if row["payload"]==64 else "16 КиБ"}</td><td>{number(v["throughput"])}</td><td>{change:+.1f}%</td><td>{number(v["p99"],2)}</td><td>{number(v["kib"],2)}</td><td>{number(v["add_ms"],1)}</td><td>{v["completed_during_commands"]}/{manifest["add_rounds"]}</td></tr>')
    raw_rows=[]
    for v in round_rows:
        raw_rows.append(f'<tr><td>{esc(v["variant"])}</td><td>{v["round"]}</td><td>{v["nodes"]}</td><td>{v["payload"]}</td><td>{number(v["throughput"])}</td><td>{number(v["p99"],2)}</td><td>{number(v["kib"],2)}</td></tr>')
    labels=[("head","#64748b","HEAD e06c4f9"),("wire3","#0f766e","wire3/v4")]
    figures=chart("Пропускная способность · выше лучше",summary,labels,"throughput","команд/с")
    figures+=chart("p99 запросов · ниже лучше",summary,labels,"p99","мс, объединённые измерения")
    figures+=chart("Выделения памяти · ниже лучше",summary,labels,"kib","КиБ/команду, весь процесс")
    figures+=chart("Во время Add и без Add",summary,[("wire3","#0f766e","wire3 без Add"),("add","#2563eb","полный Add под нагрузкой")],"throughput","команд/с; Add изменяет число voters")
    smallest=min(row["throughput_change"] for row in summary)
    largest=max(row["throughput_change"] for row in summary)
    samples=sum(v["samples"] for row in summary for v in (row["head"],row["wire3"],row["add"]))
    embedded=json.dumps(report,ensure_ascii=False).replace("<","\\u003c")
    document="""<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Raft.Net8 — hot membership и производительность</title><style>
:root{color-scheme:light;--ink:#17324d;--line:#dce4ed;--muted:#506780}*{box-sizing:border-box}body{margin:0;overflow-wrap:anywhere;background:#f3f6fb;color:var(--ink);font:16px/1.55 system-ui,sans-serif}main{max-width:1440px;margin:auto;padding:32px 22px 60px}h1{font-size:34px;line-height:1.2;margin:8px 0 18px}h2{font-size:24px;margin-top:0}h3{font-size:18px}p{max-width:1100px}.eyebrow{color:#0f766e;font-weight:700;letter-spacing:.06em}.cards{display:flex;gap:16px;flex-wrap:wrap}.card,section,figure{background:#fff;border:1px solid var(--line);border-radius:14px}.card{padding:16px 22px;flex:1;min-width:200px}.card strong{display:block;font-size:25px}section{padding:25px;margin-top:24px}.charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:20px;margin-top:24px}figure{margin:0;padding:20px;min-width:0;overflow:auto}svg{width:100%;height:auto}svg text{font:13px system-ui,sans-serif;fill:#17324d}figcaption{font-size:13px;color:var(--muted)}.wrap{overflow:auto}table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;white-space:nowrap;font-size:14px}th,td{padding:12px 14px;text-align:right;border-bottom:1px solid var(--line)}th{background:#eef3f8;text-align:right}th:first-child,td:first-child,th:nth-child(3),td:nth-child(3){text-align:left}tbody tr:nth-child(even){background:#f7fafc}.notice{border-left:4px solid #d97706;padding:12px 18px;background:#fff8eb}.good{color:#087b59;font-weight:700}.bad{color:#c32936;font-weight:700}.muted{color:var(--muted)}code{background:#edf2f7;padding:2px 4px;border-radius:4px}details{margin-top:20px}summary{cursor:pointer;font-weight:700}select{font:inherit;padding:7px;border:1px solid var(--line);border-radius:6px;margin:0 14px 14px 6px}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px}a{color:#075ea8}@media(max-width:650px){svg{min-width:640px}}@media(max-width:850px){.charts{grid-template-columns:1fr}h1{font-size:27px}main{padding:20px 12px}section{padding:18px}}@media print{body{background:#fff}main{max-width:none;padding:0}section,figure{break-inside:avoid}.charts{grid-template-columns:1fr 1fr;gap:8px}figure{padding:8px}table{font-size:9px}th,td{padding:5px}select{display:none}details{break-before:page}h1{font-size:25px}}
</style><main><div class="eyebrow">.NET 8 · DBREEZE · DURABLE TCP</div><h1>Hot Add/Remove: сравнение с сохранённым HEAD</h1>"""
    document+=f'<p>Исходная версия — <b>e06c4f9</b>, wire2/raft8-v3. Новая — wire3/raft8-v4 с журналируемым составом. Изменение медианы пропускной способности на четырёх точках: <b>{smallest:+.1f}%…{largest:+.1f}%</b>. Пять раундов; это измерение данной машины и нагрузки, без статистической гарантии для других систем.</p>'
    worst_p99=max(row["p99_change"] for row in summary)
    if worst_p99>0:
        document+=f'<p class="notice"><b>Рост хвостовых задержек:</b> p99 ухудшился до {worst_p99:+.1f}% относительно HEAD. Скорость почти сохранилась, но общего улучшения по всем метрикам нет. Все проигрыши и отдельные раунды приведены ниже.</p>'
    document+=f'<div class="cards"><div class="card"><strong>{len(round_rows)}</strong>полных замеров</div><div class="card"><strong>{number(samples)}</strong>индивидуальных задержек</div><div class="card"><strong>3 / 5 узлов</strong>64 B и 16 КиБ · concurrency64</div><div class="card"><strong>5 + 3 раунда</strong>обычная работа + Add под нагрузкой</div></div>'
    document+='<section><h2>Изменения к HEAD по каждой нагрузке</h2><p>Зелёный — улучшение, красный — ухудшение. Для p99 и памяти меньше лучше. Последний столбец показывает, в скольких парных раундах wire3 быстрее HEAD; это не тест статистической значимости.</p><div class="wrap"><table><thead><tr><th>Узлы</th><th>Команда</th><th>Скорость ↑</th><th>p99 ↓</th><th>Выделения ↓</th><th>Быстрее в парах</th></tr></thead><tbody>'+''.join(changes_body)+'</tbody></table></div></section>'
    document+='<div class="charts">'+figures+'</div><section><h2>Обычная работа без изменения состава</h2><p>Пропускная способность и выделения — медианы пяти раундов. p50/p99 — nearest rank по всем40960 запросам конфигурации. Диапазон — минимум/максимум скорости отдельных раундов. Все запросы ждут долговечный кворум и применение лидером.</p><label>Узлы<select id="nodes"><option value="">Все</option><option>3</option><option>5</option></select></label><label>Команда<select id="payload"><option value="">Все</option><option value="64">64 B</option><option value="16384">16 КиБ</option></select></label><div class="wrap"><table><thead><tr><th>Узлы</th><th>Команда</th><th>Версия</th><th>Команд/с ↑</th><th>∆ к HEAD</th><th>p50, мс ↓</th><th>p99, мс ↓</th><th>КиБ/ком. ↓</th><th>Мин.–макс.</th></tr></thead><tbody>'+''.join(body)+'</tbody></table></div></section>'
    document+='<section><h2>Полный Add во время записи данных</h2><p>Новая TCP-реплика с отдельным DBreeze engine подготовлена до таймера в том же benchmark-процессе. На старте8192 запросов вызывается полный Add: регистрация → learner → догоняние → voter → результат. Замер включает ожидание полного Add, если он длится дольше команд; индивидуальные задержки относятся к бизнес-запросам. После таймера проверяются все реплики, закрываются базы и повторно открываются для проверки эффектов/checkpoint.</p><p class="notice">Число voters меняется3→4 или5→6. Это цена всего сценария с дополнительной репликацией и служебной работой; её нельзя приписать только накладным расходам API или сравнивать как неизменную топологию.</p><div class="wrap"><table><thead><tr><th>Узлы</th><th>Команда</th><th>Команд/с</th><th>∆ к wire3 без Add</th><th>p99, мс</th><th>КиБ/ком.</th><th>Add, мс</th><th>Успел до конца записи</th></tr></thead><tbody>'+''.join(add_body)+'</tbody></table></div></section>'
    document+='<section><h2>Методика и границы сравнения</h2><ul><li>Один кластер за раз; все его реплики в одном процессе с отдельными DBreeze engines. Loopback TCP, дисковый DBreeze1.142.2026.1016-beta на H:. .NET8.0.34, Workstation GC. Порядок HEAD/wire3 меняется между парами/раундами.</li><li>8192 измеряемые команды,2048 прогрев,64 параллельных клиента; pipeline включён. Application256КиБ/128 записей; автоматические proposal/wire бюджеты1051264B; одинаковые heartbeat/election/RPC.</li><li>Один и тот же бизнес-обработчик: payload ring256, счётчик, атомарный CRC(index,term) checkpoint. Оркестратор проверяет неизменность его исходника. Команды не заменены счётчиком в памяти.</li><li>Выделения — весь процесс, включая runtime/TCP/DBreeze и служебные операции; managed bytes не равны RSS. Дисковые файлы, ОС-кэш и память хоста сюда не входят.</li><li>Snapshot/compaction в нагрузке выключены. Измерение не охватывает разные машины, WAN, TLS overhead или открытый поток запросов. Dry smoke не используется как результат скорости.</li><li>Предыдущая полная серия performance-final показала до −9,4% на малой команде и сохранена как дооптимизационная. Затем сокращены копирование редко меняющегося состояния и накладные расходы TCP. Частичная performance-r1 тоже сохранена. Этот отчёт включает только новую полную серию, указанную ниже; медленные раунды из неё не удалены. Отдельные непарные диагностические прогоны не входят в результаты.</li></ul>'
    document+='<details><summary>Все отдельные раунды</summary><div class="wrap"><table><thead><tr><th>Режим</th><th>Раунд</th><th>Узлы</th><th>Байт</th><th>Команд/с</th><th>p99 раунда, мс</th><th>КиБ/ком.</th></tr></thead><tbody>'+''.join(raw_rows)+'</tbody></table></div></details>'
    document+='<details><summary>Параметры и SHA-256 измеренных сборок</summary><pre>'+esc(json.dumps(dict(metadata=metadata,business_source_sha256=manifest["business_source_sha256"]),ensure_ascii=False,indent=2))+'</pre></details>'
    if "validation_followup" in report:
        document+='<p class="notice">'+esc(report["validation_followup"]["explanation"])+'</p><details><summary>После замеров: только стартовая валидация</summary><pre>'+esc(json.dumps(report["validation_followup"],ensure_ascii=False,indent=2))+'</pre></details>'
    document+=f'<p class="muted">Исходные JSON/логи/manifest: {esc(args.raw.resolve())}. Рядом с HTML сохранены CSV и summary JSON; полный отчёт встроен в этот файл. Перегенерация: python benchmarks/render_hot_membership.py &lt;raw-folder&gt; --out &lt;report.html&gt;.</p></section>'
    document+='<script id="report-data" type="application/json">'+embedded+'</script><script>function filter(){var n=document.getElementById("nodes").value,p=document.getElementById("payload").value;document.querySelectorAll("tr[data-nodes]").forEach(function(r){r.hidden=(n&&r.dataset.nodes!==n)||(p&&r.dataset.payload!==p)})}document.getElementById("nodes").addEventListener("change",filter);document.getElementById("payload").addEventListener("change",filter);</script></main></html>'
    output.write_text(document,encoding="utf-8")
    print(json.dumps(dict(report=str(output),measurements=len(round_rows),samples=samples,
        throughput_changes=[round(r["throughput_change"],2) for r in summary],
        p99_changes=[round(r["p99_change"],2) for r in summary],
        allocation_changes=[round(r["allocation_change"],2) for r in summary]),ensure_ascii=False))

if __name__=="__main__":
    main()
