"""Build an offline, auditable HTML report from measure_dashboard.ps1 cohorts (standard library)."""
import argparse
import hashlib
import html
import json
import math
import random
import statistics
from pathlib import Path


def median(values):
    return statistics.median(values)


def interval(values):
    rng = random.Random(20261115)
    draws = sorted(median(rng.choices(values, k=len(values))) for _ in range(10000))
    return [draws[249], draws[9749]]


def percentile(values, fraction):
    values = sorted(values)
    return values[math.ceil(len(values) * fraction) - 1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('cohorts', nargs='+', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    notes = parser.add_mutually_exclusive_group()
    notes.add_argument('--delivery-notes', action='store_true', help='Include selected dashboard changes and preserved acceptance limitations.')
    notes.add_argument('--sdk8-tail-control', action='store_true', help='Label the separate 3-node/64-byte SDK8 control without pooling it with the full matrix.')
    notes.add_argument('--final-sdk8', action='store_true', help='Label the full matrix of frozen, finally validated SDK8 baseline/off/on binaries.')
    args = parser.parse_args()
    esc = lambda value: html.escape(str(value))
    samples = []
    sources = []
    for cohort in args.cohorts:
        manifest = json.loads((cohort / 'runs.json').read_text(encoding='utf-8-sig'))
        if isinstance(manifest, dict):
            manifest = [manifest]
        for run in manifest:
            if run['ExitCode'] != 0:
                continue
            files = list(Path(run['Folder']).glob('client-bench-*/results.json'))
            if len(files) != 1:
                raise ValueError(f'Expected exactly one result: {run["Folder"]}')
            file = files[0]
            report = json.loads(file.read_text(encoding='utf-8-sig'))
            if not report['Completed']:
                raise ValueError(f'Incomplete result: {file}')
            source = {'path': str(file), 'sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
                      'core': report['CoreSha256'], 'client': report['ClientSha256'], 'server': report['ServerSha256'],
                      'runtime': report['Runtime'], 'os': report['OS'], 'variant': run['Variant'], 'started': run['Started'], 'ended': run['Ended']}
            dashboard = report.get('DashboardAssembly')
            source['dashboardSha256AfterCohort'] = hashlib.sha256(Path(dashboard).read_bytes()).hexdigest() if dashboard else None
            source['harnessSha256'] = run['AssemblySha256']
            sources.append(source)
            for row in report['Results']:
                observations = next((item for item in report.get('Observations', [])
                                     if item['Voters'] == row['Voters'] and item['PayloadBytes'] == row['PayloadBytes']), {})
                samples.append({**row, 'variant': run['Variant'], 'pair': str(cohort) + '/' + str(run['Round']), 'source': str(file),
                                'observationRequests': observations.get('Requests'), 'observationReadIndexCalls': observations.get('ReadIndexCalls')})
    groups = {}
    for row in samples:
        groups.setdefault((row['Voters'], row['PayloadBytes'], row['variant']), []).append(row)
    labels = {'baseline': '3fafaa2', 'off': 'Текущий · dashboard off', 'before': 'До объединения обзора · dashboard on', 'on': 'Текущий · dashboard on',
              'tokens': 'Токены · telemetry off', 'tls': 'HTTPS + peer mTLS + токены'}
    colors = {'baseline': '#77859a', 'off': '#28aa8f', 'before': '#dc9b66', 'on': '#b5dc61', 'tokens': '#6e9cef', 'tls': '#cc89dd'}
    cells = sorted({key[:2] for key in groups})
    variants = [name for name in labels if any(key[2] == name for key in groups)]
    if args.sdk8_tail_control and (cells != [(3, 64)] or set(variants) != {'baseline', 'off'}):
        raise ValueError('SDK8 tail control must contain only baseline/off, 3 voters, 64 bytes.')
    if args.final_sdk8 and (cells != [(3, 64), (3, 16384), (5, 64), (5, 16384)] or set(variants) != {'baseline', 'off', 'on'}):
        raise ValueError('Final SDK8 matrix must contain baseline/off/on at all four workload points.')
    summaries, comparisons = [], []
    for cell in cells:
        for variant in variants:
            rows = groups.get((*cell, variant), [])
            if not rows:
                continue
            summaries.append({'voters': cell[0], 'payload': cell[1], 'variant': variant, 'rounds': len(rows),
                'throughput': median([row['CommandsPerSecond'] for row in rows]),
                'allocation': median([row['AllocatedBytesPerCommand'] for row in rows]),
                'p50': median([row['P50Milliseconds'] for row in rows]), 'p99': median([row['P99Milliseconds'] for row in rows]),
                'pooledP99': percentile([value for row in rows for value in row['LatencyMilliseconds']], .99),
                'minimum': min(row['CommandsPerSecond'] for row in rows), 'maximum': max(row['CommandsPerSecond'] for row in rows)})
        for candidate, control in [('off', 'baseline'), ('on', 'off'), ('on', 'baseline'), ('on', 'before'), ('tokens', 'off'), ('tls', 'tokens')]:
            a = {row['pair']: row for row in groups.get((*cell, candidate), [])}
            b = {row['pair']: row for row in groups.get((*cell, control), [])}
            pairs = sorted(a.keys() & b.keys())
            if not pairs:
                continue
            measures = {}
            uncertain = False
            passed = True
            for field, name, limit, upper in [('CommandsPerSecond', 'throughput', .95, False), ('AllocatedBytesPerCommand', 'allocation', 1.05, True), ('P99Milliseconds', 'p99', 1.10, True)]:
                ratios = [a[pair][field] / b[pair][field] for pair in pairs]
                ci = interval(ratios)
                value = median(ratios)
                measures[name] = {'ratio': value, 'ci95': ci, 'pairs': ratios}
                uncertain |= ci[0] <= limit <= ci[1]
                passed &= value <= limit if upper else value >= limit
            comparisons.append({'voters': cell[0], 'payload': cell[1], 'candidate': candidate, 'control': control,
                                'rounds': len(pairs), 'passed': passed, 'uncertain': uncertain, 'metrics': measures})
    audit = {'cohorts': [str(path) for path in args.cohorts], 'summaries': summaries, 'comparisons': comparisons, 'sources': sources,
             'workload': {'requestsPerCell': sorted({row['Requests'] for row in samples}), 'concurrency': sorted({row['Concurrency'] for row in samples}),
                          'warmup': 128, 'observations': len(samples), 'latencies': sum(len(row['LatencyMilliseconds']) for row in samples)},
             'rounds': [{key: value for key, value in row.items() if key != 'LatencyMilliseconds'} for row in samples]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix('.json').write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding='utf-8')
    def fmt(value, decimals=1):
        return f'{value:,.{decimals}f}'.replace(',', ' ')
    rows = ''
    for row in summaries:
        rows += f'<tr><td>{row["voters"]}</td><td>{row["payload"]:,} B</td><td><i style="background:{colors[row["variant"]]}"></i>{labels[row["variant"]]}</td><td>{row["rounds"]}</td><td><b>{fmt(row["throughput"],0)}</b></td><td>{fmt(row["p50"],2)}</td><td>{fmt(row["p99"],2)}</td><td>{fmt(row["pooledP99"],2)}</td><td>{fmt(row["allocation"]/1024,2)}</td><td>{fmt(row["minimum"],0)}–{fmt(row["maximum"],0)}</td></tr>'
    comparison_rows = ''
    for row in comparisons:
        values = ''.join(f'<td>{(row["metrics"][key]["ratio"]-1)*100:+.1f}%<small>95% CI {(row["metrics"][key]["ci95"][0]-1)*100:+.1f}…{(row["metrics"][key]["ci95"][1]-1)*100:+.1f}%</small></td>' for key in ['throughput', 'allocation', 'p99'])
        gate = ('PASS' if row['passed'] else 'FAIL') if row['candidate'] in ('off', 'on') else 'cost'
        comparison_rows += f'<tr><td>{row["voters"]} / {row["payload"]} B</td><td>{labels[row["candidate"]]} / {labels[row["control"]]}</td><td>{row["rounds"]}</td>{values}<td class="{gate.lower()}">{gate}{" · CI crosses gate" if row["uncertain"] else ""}</td></tr>'
    charts = ''
    for cell in cells:
        values = [row for row in summaries if (row['voters'], row['payload']) == cell]
        maximum = max(row['throughput'] for row in values) * 1.05
        drawing = ''
        for index, row in enumerate(values):
            y = 30 + index * 44
            drawing += f'<text x="0" y="{y+16}">{esc(labels[row["variant"]])}</text><rect x="280" y="{y}" width="{row["throughput"]/maximum*410:.1f}" height="23" fill="{colors[row["variant"]]}" rx="4"/><text x="{290+row["throughput"]/maximum*410:.1f}" y="{y+16}">{fmt(row["throughput"],0)}</text>'
        charts += f'<article><h3>{cell[0]} ноды · {cell[1]} B</h3><div class="chart-scroll" tabindex="0" aria-label="График: горизонтальная прокрутка на узком экране"><svg role="img" aria-label="commands per second" viewBox="0 0 800 {45+len(values)*44}">{drawing}</svg></div></article>'
    details = ''.join(f'<tr><td>{esc(row["variant"])}</td><td>{esc(row["runtime"])}<small>{esc(row["os"])}</small></td><td class="hash">core {row["core"]}<br>client {row["client"]}<br>server {row["server"]}<br>harness {row["harnessSha256"]}<br>dashboard post-cohort {row["dashboardSha256AfterCohort"] or "—"}<br>raw {row["sha256"]}</td><td class="path">{esc(row["path"])}</td></tr>' for row in sources)
    repeats = ''.join(f'<tr><td>{esc(row["pair"].rsplit("/", 1)[-1])}</td><td>{row["Voters"]} / {row["PayloadBytes"]} B</td><td>{esc(labels[row["variant"]])}</td><td>{fmt(row["CommandsPerSecond"], 1)}</td><td>{fmt(row["P99Milliseconds"], 2)}</td><td>{fmt(row["AllocatedBytesPerCommand"] / 1024, 2)}</td><td>{row["observationRequests"] if row["observationRequests"] is not None else "—"} / {row["observationReadIndexCalls"] if row["observationReadIndexCalls"] is not None else "—"}</td></tr>' for row in samples)
    main_checks = [row for row in comparisons if (row['candidate'], row['control']) in [('off', 'baseline'), ('on', 'off'), ('on', 'baseline')]]
    gate_passes = sum(row['passed'] for row in main_checks)
    uncertain_checks = sum(row['uncertain'] for row in main_checks)
    experiment_note = '<p>Вариант «до объединения обзора» — замороженная сборка со всеми функциями dashboard, но с отдельными запросами метаданных. Новый обзор объединяет семь последовательных ReadIndex-барьеров в один и использует полученную диагностику лидера в том же опросе. Таймеры, дисковая фиксация, права и частота измерений сохранены. Эффект оценивается свежими парами, а не сравнением с лучшим старым повтором.</p>' if 'before' in variants else ''
    delivery_notes = '''<section><h2>Что входит в выбранный вариант</h2>
    <p>Обзор метаданных объединён в один согласованный запрос. Справочник management-адресов читается из ограниченного неизменяемого RAM-представления после commit/restore. Одновременные диагностические чтения собираются2мс перед свежим ReadIndex; запрос после начала барьера ждёт следующий. Готовый барьер не используется как lease, права проверяются после ожидания. Лимит SDK dashboard8 согласован с8 диагностическими запросами: пять нод опрашиваются одной волной.</p>
    <p>В отдельных пяти A/B-парах75 наблюдений вызывали30 публичных ReadIndex при лимите4 и17 при лимите8. Сокращение обращений подтверждено счётчиками; само по себе оно не доказывает улучшение каждой задержки. Конкурентные публикации, их durability/CRC, таймеры и исходники Core не менялись. У измеренных baseline/current Core совпадает IL всех 4491 методов, имеющих тело, несмотря на разные SHA метаданных сборки. Runtime обеих сторон.NET8.0.34. Измеренные net8.0 сборки созданы SDK10.0.115; итоговая проверка проекта использует закреплённый SDK8.0.428. Их бинарные хеши различаются; проверенные после измерений изменения касаются отображения конфигурации и выбора последних точек истории dashboard.</p>
    <h3>Один численный контроль остаётся FAIL</h3><p>Все4 конфигурации dashboard on/baseline и все4 on/off укладываются в контрольные медианы. В off/baseline3ноды64Б p99 выше на16,37%: это FAIL, при throughput−1,00% и allocations+1,15%. Он не объявлен допустимой регрессией. В ряде сравнений CI пересекает границу; редкие крупные провалы встречались и без dashboard. Все они сохранены. Общее число PASS ниже относится к12 отношениям, а не к12 разным нагрузкам.</p>
    <p><a href="dashboard-tail-sdk8-2026-11-15.html">Отдельные10 свежих пар для этой точки, обе стороны SDK8.0.428</a>: throughput−0,62%, allocations+0,91%, p99−4,42%, PASS по медианам. Верхняя граница95% CI p99 всё ещё+11,63%. Старый FAIL не воспроизвёлся, но изменение причины кодом или компилятором не доказано. Этот повтор не объединён с полной матрицей. После заморозки уточнены лимит истории dashboard, показ HTTP-ошибок и их учёт при отказе429; эти изменения отмечены отдельно в итоговой проверке.</p>
    <p>Попытка заполнять кэш результатов непосредственно после application commit прошла34 целевых теста, но ухудшила throughput на15–30% в пяти свежих парах3ноды64Бoff. Она откатана; в выбранной версии этого изменения нет. <a href="dashboard-overhead-before-result-cache-2026-11-15.html">Сохранённый десятипарный результат до отклонённого эксперимента</a>.</p>
    <h3>Другие серии не объединяются с этой сборкой</h3><p><a href="dashboard-overhead-before-overview-2026-11-15.html">Первоначальные10 пар и отдельные5 пар стоимости токенов/TLS</a>;
    <a href="dashboard-overhead-after-overview-2026-11-15.html">После объединения обзора, до кэша маршрутов/группировки/лимита8</a>;
    <a href="dashboard-overhead-combined-five-2026-11-15.html">Первые5 пар выбранной сборки</a>. Стоимость токенов включает native management; TLS измерен поверх того же token-профиля, без BFF/telemetry. Эти измерения были сделаны до изменения пути наблюдений, который в тех двух режимах не вызывается. Отрицательная разница TLS на локальном fsync-насыщенном тесте не означает бесплатного шифрования в production.</p></section>''' if args.delivery_notes else ''
    tail = next((item for item in comparisons if item['candidate'] == 'off' and item['control'] == 'baseline'), None)
    title = 'Отдельный контроль p99: SDK 8 · 3 ноды · 64 Б' if args.sdk8_tail_control else 'Цена dashboard, токенов и TLS'
    introduction = (f'{tail["rounds"]} свежих A/B-пар: 3fafaa2 и cluster-v2 без dashboard. Обе стороны собраны SDK8.0.428 и работают на.NET8.0.34. '
                    'Изолирована только ранее провалившаяся точка. Полная матрица, токены и TLS в эту серию не входят.' if args.sdk8_tail_control else
                    'Свежий3fafaa2 → текущая cluster-v2 без dashboard → текущая версия с реальным отдельным backend-сборщиком. Токены и HTTPS/peer mTLS измерены отдельно. Старые e06/R18/DotNext повторно не запускались.')
    control_notes = f'''<section><h2>Что проверяет этот повтор</h2><p>{len(sources)} отдельных процессов, каждый с новой БД: только3 ноды/64Б,128 запросов прогрева,{audit['workload']['requestsPerCell'][0]} измеряемых запросов, concurrency64. Порядок A/B меняется через пару. В старый benchmark добавлен только фильтр этой конфигурации; алгоритм, библиотечные исходники, бизнес-транзакция и нагрузка базы не менялись. SDK8 соответствует global.json проекта. Исходные скрипт, контрольные сборки и результаты сохранены в каталоге когорты.</p>
    <p>В предыдущей полной матрице off/baseline p99 был+16,37%, FAIL. Здесь медиана парных отношений: throughput{(tail['metrics']['throughput']['ratio']-1)*100:+.2f}%, allocations{(tail['metrics']['allocation']['ratio']-1)*100:+.2f}%, p99{(tail['metrics']['p99']['ratio']-1)*100:+.2f}%, {'PASS' if tail['passed'] else 'FAIL'}.95% CI p99 составляет{(tail['metrics']['p99']['ci95'][0]-1)*100:+.2f}…{(tail['metrics']['p99']['ci95'][1]-1)*100:+.2f}%. Уверенный вывод об отсутствии любой потери из этого не следует. Большие провалы встречались у обеих сторон, в том числе у базы; они сохранены.</p>
    <p>Производственный путь успешной публикации ради повтора не менялся. Этот результат не доказывает, что прежнюю причину исправил код или SDK. Серии не объединяются, прежний FAIL не заменён: <a href="dashboard-overhead-3fafaa2-2026-11-15.html">полная матрица и её ограничения</a>. Последующие уточнения лимита истории dashboard, отображения HTTP-ошибок и счётчика отказов429 не входят в эти замороженные сборки.</p></section>''' if args.sdk8_tail_control else ''
    if args.final_sdk8:
        title = 'Финальный контроль dashboard · .NET8 / SDK8'
        introduction = ('Полная матрица 3/5 нод × 64 Б/16 КиБ: свежий 3fafaa2, итоговая cluster-v2 без dashboard и та же сборка с реальным отдельным сборщиком. '
                        'Все варианты собраны SDK8.0.428 и работают на.NET8.0.34. Это новая серия, предыдущие кандидаты не объединены с ней.')
        control_notes = '''<section><h2>Границы итоговой сборки</h2><p>Заморожены именно итоговые native и опубликованные UI/backend сборки после 473 пройденных тестов, HTTP/HTTPS/TOTP браузерных проверок. В них включены 400 000 строк истории по умолчанию, отображение HTTP-ошибок и учёт отказа 429. После заморозки менялись только документация и генератор отчёта. Каждый вариант/раунд запускается новым процессом и пишет в новые каталоги на том же диске.</p>
        <p>Путь успешной публикации не изменялся ради повторного замера. Сохранены batching/pipeline, прежние таймеры, durability/CRC, схема wire3/raft8-v4, токены и hot membership. Исходники Core совпадают с базой; в SDK8 baseline/current совпадает IL всех 4491 методов, имеющих тело (4537 определений всего). SHA различаются из-за метаданных сборок. Публичный ReadIndex в служебных наблюдениях считается отдельно от сетевых RPC.</p>
        <h3>Предыдущие результаты сохранены</h3><p><a href="dashboard-overhead-3fafaa2-2026-11-15.html">Предыдущая полная SDK10→net8 серия: 11/12, включая off/baseline, 3 ноды, 64 Б, p99 +16,37% FAIL</a>;
        <a href="dashboard-tail-sdk8-2026-11-15.html">отдельный SDK8 повтор только этой точки</a>;
        <a href="dashboard-overhead-before-overview-2026-11-15.html">первоначальные серии и отдельная стоимость токенов/TLS</a>. Ни один FAIL или выброс не удалён. Отличия компилятора и состояния хоста не позволяют приписывать изменение результата только коду или только SDK. PASS по медианам ограничен этими нагрузками; доверительные интервалы и каждый повтор приведены ниже.</p></section>'''
    html_report = f'''<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{title}</title><style>
    :root{{color-scheme:dark light}}body{{margin:0;background:#0e1714;color:#e1e9df;font:16px system-ui,sans-serif}}main{{max-width:1600px;margin:auto;padding:40px 24px}}h1{{font-size:38px;letter-spacing:-1px}}h2{{margin-top:40px}}p{{color:#b5c4ba;line-height:1.7}}section,article{{background:#17231d;border:1px solid #354139;border-radius:14px;padding:24px;margin:20px 0}}.grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}article{{margin:0;min-width:0}}.chart-scroll{{overflow:auto}}svg{{width:100%;min-width:640px}}svg text{{fill:#dce5db;font:13px system-ui}}table{{border-collapse:collapse;width:100%;white-space:nowrap}}th,td{{padding:14px;text-align:right;border-bottom:1px solid #344239}}th:nth-child(-n+3),td:nth-child(-n+3){{text-align:left}}th{{color:#b5dc61}}.scroll{{overflow:auto}}small{{display:block;color:#9eafa3;font-size:12px;margin-top:6px}}i{{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:8px}}.pass{{color:#b5dc61}}.fail{{color:#ff8a86}}.hash{{font:11px monospace;text-align:left;white-space:normal;word-break:break-all;min-width:240px}}.path{{font:11px monospace;white-space:normal;word-break:break-all;min-width:240px}}a{{color:#b5dc61}}details{{margin:25px 0}}summary{{cursor:pointer}}@media(max-width:800px){{.grid{{grid-template-columns:1fr}}main{{padding:20px 12px}}h1{{font-size:28px}}}}@media print{{body{{background:white;color:black}}section,article{{background:white;color:black}}svg{{min-width:0}}svg text{{fill:black}}p,small{{color:#333}}.scroll{{overflow:visible}}table{{font-size:10px;white-space:normal}}details{{display:block}}}}
    </style><main><p>RAFT / NET8 · DASHBOARD · 2026-11-15</p><h1>{title}</h1>
    <p>{introduction}</p>{control_notes}
    <section><h2>Проверка допусков: {gate_passes} / {len(main_checks)}</h2><p>{audit['workload']['observations']} измерений конфигураций; {audit['workload']['latencies']:,} индивидуальных задержек. В {uncertain_checks} сравнениях доверительный интервал пересекает границу допуска: PASS по медиане не доказывает отсутствие потери во всех условиях. Все повторы сохранены ниже и в JSON.</p>{experiment_note}</section>
    <section><h2>Границы эксперимента</h2><p>Дисковые DBreeze1.142, .NET8, loopback; внешний HTTP SDK, полная Standard история,3/5 нод и клиент в одном процессе. Каждый вариант/раунд — отдельный процесс и новые БД. Порядок вариантов меняется через раунд. Прогрев128; concurrency64; одинаковое число запросов на ячейку. Dashboard on означает работающий сборщик и защищённый профиль подключения; браузерный рендеринг и SSE потребители в этот benchmark не включены. Управляемые выделения относятся к процессу client+реплики; отдельный dashboard имеет собственную память и в этот счётчик не входит.</p><p>Throughput, p50/p99 и allocations — медианы отдельных раундов. Pooled p99 также приведён по всем исходным latency samples; смешивать его с медианой p99 нельзя. CI — детерминированный парный bootstrap медианы отношений,10000 выборок; он не гарантирует отсутствие фоновой нагрузки. Gate: throughput≥95%, allocations≤105%, p99≤110%. Стоимость токенов/TLS показана без требования уложиться в бесплатный мониторинг. Короткая ячейка может не охватить минутную запись истории и повторный30-секундный обход файлов. Это локальный тест, не прогноз удалённого production.</p></section>
    {delivery_notes}<div class="grid">{charts}</div><h2>Все варианты и распределённые топологии</h2><section class="scroll"><table><thead><tr><th>Ноды</th><th>Payload</th><th>Вариант</th><th>Раунды</th><th>команд/с ↑</th><th>p50 ms ↓</th><th>median p99 ms ↓</th><th>pooled p99 ms ↓</th><th>KiB/command ↓</th><th>Min–max throughput</th></tr></thead><tbody>{rows}</tbody></table></section>
    <h2>Парные изменения, без выбора лучшего повтора</h2><section class="scroll"><table><thead><tr><th>Ячейка</th><th>Отношение</th><th>Пары</th><th>Throughput Δ ↑</th><th>Alloc Δ ↓</th><th>p99 Δ ↓</th><th>Критерий</th></tr></thead><tbody>{comparison_rows}</tbody></table></section>
    <details><summary>Каждый исходный повтор, включая выбросы</summary><p>Последний столбец — management-наблюдения / вызовы публичного ReadIndex из этих наблюдений за измеряемый участок. Это не число сетевых RPC; фоновые обращения обычного data SDK в него не включены. Старые сборки не записывали эти счётчики.</p><div class="scroll"><table><thead><tr><th>Раунд</th><th>Ноды / payload</th><th>Вариант</th><th>команд/с</th><th>p99 ms</th><th>KiB/command</th><th>Наблюдения / ReadIndex</th></tr></thead><tbody>{repeats}</tbody></table></div></details>
    <details><summary>Исходные результаты, SHA256 и бинарные сборки</summary><div class="scroll"><table><thead><tr><th>Вариант</th><th>Runtime</th><th>SHA256</th><th>Файл</th></tr></thead><tbody>{details}</tbody></table></div></details><p>Сопутствующий JSON содержит каждый повтор, отношения и перечень raw файлов. Не удаляйте неудачные/диагностические когорты и не объединяйте их с новой сборкой без явной маркировки. <a href="../../docs/CLUSTER-DASHBOARD.md">Эксплуатационные ограничения</a>.</p></main></html>'''
    args.output.write_text(html_report, encoding='utf-8')
    print(json.dumps({'samples': len(samples), 'latencies': sum(len(row['LatencyMilliseconds']) for row in samples), 'comparisons': comparisons}, ensure_ascii=False))


if __name__ == '__main__':
    main()
