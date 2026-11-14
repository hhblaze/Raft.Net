"""Offline e06 / before / after report. Reject incomplete or modified raw evidence."""
from __future__ import annotations
import argparse
import csv
import hashlib
import html
import json
import math
from pathlib import Path
import statistics

METRICS = ('ProposalsPerSecond','P99Milliseconds','AllocatedBytesPerProposal')


def appendix(validation, cells):
    if not validation: return ''
    def table(headers, rows):
        return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+html.escape(str(h))+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in r)+'</tr>' for r in rows)+'</tbody></table></div>'
    def delta(value): return f'{value:+.1f}%'
    pipeline=[r for r in cells if r['Mode']=='Pipeline'];large=[r for r in cells if r['Payload']==16384]
    failed=[r for r in cells if not r['Pass']]
    verdict='Открытых точек: '+str(len(failed))+'. Полный критерий ещё не выполнен.' if failed else 'Все точки выполняют три рабочих допуска в этом локальном опыте.'
    rate_pass=sum(r['Ratios']['Throughput']>=.95 for r in cells);allocation_pass=sum(r['Ratios']['Allocation']<=1.05 for r in cells);tail_pass=sum(r['Ratios']['P99']<=1.1 for r in cells)
    out=['<section><h2>Где критерий выполнен</h2><p>По отдельным метрикам: скорость <strong>'+str(rate_pass)+'/16</strong>, выделения <strong>'+str(allocation_pass)+'/16</strong>, p99 <strong>'+str(tail_pass)+'/16</strong>. Pipeline: <strong>'+str(sum(r['Pass'] for r in pipeline))+'/'+str(len(pipeline))+'</strong> точек. Команды 16 КиБ: <strong>'+str(sum(r['Pass'] for r in large))+'/'+str(len(large))+'</strong>. '+verdict+'</p>']
    out.append(table(['Открытая точка','Скорость к e06','p99 к e06','Выделения к e06'],[(r['Series']+' / '+r['Case'],delta((r['Ratios']['Throughput']-1)*100),delta((r['Ratios']['P99']-1)*100),delta((r['Ratios']['Allocation']-1)*100)) for r in cells if not r['Pass']]))
    out.append('</section><section><h2>Почему одной медианы недостаточно</h2><p>Дополнительный контроль запускал <strong>одну и ту же e06 DLL</strong> в двух ролях A/B, пять пар на точку. Исходники, библиотека и параметры совпадают. Даже здесь p99 и выделения иногда выходят за рабочие допуски. Эти числа не включены в основную матрицу и не отменяют её FAIL.</p>')
    out.append(table(['Одинаковая DLL: точка','A, команд/с','B, команд/с','B/A скорость','B/A p99','B/A выделения'],[(r['Case'],round(r['Variants']['control-a']['Throughput']),round(r['Variants']['control-b']['Throughput']),delta(r['DeltaPercent']['Throughput']),delta(r['DeltaPercent']['P99']),delta(r['DeltaPercent']['Allocation'])) for r in validation['SameBinaryControl']]))
    out.append('<p>Есть два наблюдаемых режима: почти полные пакеты и дроблёные волны. В отдельном профиле 5 узлов / 64 Б / Batched e06 записывал 62,1 команды за append, текущий wire2/v3 — 42,1, wire3/v4 — 40,9. В другом свежем контроле тот же e06 получил 2127 команд/с, текущий v3 — 2725; следующий контроль дал 2992 и2961. Это показывает изменчивость пакетирования, но не устанавливает единственную причину всей исторической разницы R18/R19. Остаточная потеря в основной матрице остаётся открытой.</p></section>')
    out.append('<section><h2>Проверенные и отклонённые изменения</h2><p>Сохранены основные оптимизации e06: batching, pipeline, coalescing commit-уведомлений, packed manifest, единое физическое хранилище и чтение опубликованного кэша во время flush. Исправление сигнала дало32 → 0 пустых dispatch в профиле 16 384 команд. Прежний сборщик при этом выполнил около 4800 ожиданий с CTS и 4300 таймаутных отмен. Итоговый сборщик использует один таймер; его callback только будит канал и не может принудительно сбросить пакет.</p><p>Изолированные варианты на 5 узлах / 64 Б / Batched, wire2/v3, пять раундов; это отдельный опыт, не строки основной матрицы:</p>')
    out.append(table(['Вариант','Команд/с','Медиана p99 раунда, мс','Б/команду'],[(r['Variant'],round(r['Throughput']),f'{r["MedianRoundP99"]:.2f}',round(r['Allocation'])) for r in validation['GeneralVariants']]))
    out.append('<p>Плоские поля состояния не дали преимущества. Точная прежняя цепочка TCP-completion оказалась медленнее; оба варианта отклонены. Менять форматы, отключать membership, ослаблять durability/CRC или повышать задержки сбора ради результата не потребовалось.</p>')
    if validation.get('AdditionalProbes'):
        out.append('<p>Дополнительные изолированные A/B-опыты против версии с таймером (2007AE4C…), отдельно от e06 и основной матрицы. Wakeup сокращает сигналы коллектора; admission создаёт linked CTS только при ожидании свободного места. Все повторы сохранены; p99 объединяет запросы всех раундов.</p>')
        out.append(table(['Опыт / серия / точка','Пар','Побед throughput','Скорость, Δ','p99, Δ','Выделения, Δ'],[(r['Experiment']+' / '+r['Series']+' / '+r['Case'],r['Rounds'],str(r['ThroughputWins'])+'/'+str(r['Rounds']),delta(r['DeltaPercent']['Throughput']),delta(r['DeltaPercent']['P99']),delta(r['DeltaPercent']['Allocation'])) for r in validation['AdditionalProbes']]))
        out.append('<p>Wakeup отклонён: устойчивого выигрыша нет. Быстрый admission показал выигрыш длинных серий после десяти пар, но сохраняет штраф p99 на пяти узлах. Его итог оценивается новой основной матрицей, а не переносом этих процентов на e06.</p>')
    out.append('</section>')
    out.append('<section><h2>Прогрев, отдельный диагностический опыт</h2><p>На 4096 запросах после стандартного прогрева 2048 JIT всё ещё компилировал сотни методов. После 16 384 команд прогрева работа компилятора в измеряемом участке существенно меньше. Более длинный прогрев также увеличивает сохранённый лог: все изменения скорости нельзя приписать одному JIT. Настройки JIT/ThreadPool не изменялись, исходные 128/2048 прогревов основной матрицы сохранены.</p>')
    out.append(table(['Вариант','Прогрев','Методов JIT в участке','JIT, мс','Worker threads до/после'],[(r['Variant'],r['Warmup'],r['CompiledMethods'],f'{r["CompilationMilliseconds"]:.2f}',f'{r["WorkersBefore"]}/{r["WorkersAfter"]}') for r in validation['Warmup']]))
    if validation.get('FinalProfile'):
        out.append('<p>Отдельная трасса финального исходного кода: 5 узлов / 16 КиБ / Pipeline, 2048 прогрев + 4096 команд. Начало и конец заданы QPC-событиями. Это диагностический запуск с наблюдателями, он не включён в основную матрицу и не отменяет её плохой p99.</p>')
        out.append(table(['Профиль','Участок, мс','CPU процесса, мс','Membership-запросов'],[(r['Variant'],f'{r["WallMilliseconds"]:.1f}',f'{r["ProcessCpuMilliseconds"]:.1f}',r['MembershipQueries']) for r in validation['FinalProfile']]))
        out.append('<p>CPU учитывает несколько потоков и может превышать wall time. Сэмплы стеков включают заблокированные потоки и не являются CPU-временем. Суммы ожиданий store перекрываются. В этом профиле длительный медленный период основной матрицы не воспроизвёлся; единственная причина остаточного хвоста не установлена.</p>')
    out.append('</section><section><h2>Регрессия и операции под нагрузкой</h2><p><strong>410/410 тестов</strong>, ноль пропусков; стабильные operationId проверены повторными нагрузочными случаями. Четыре производителя продолжают писать при Add и автоматическом удалении текущего лидера в 3/5-узловых кластерах. Существующие объекты узлов сохраняются; после передачи/удаления записи продолжаются и активные дисковые state machine совпадают. HumanTest и 29 BDN Dry случаев прошли; Dry — функциональная проверка.</p><p>Три парных раунда дополнительных режимов, отдельные от основной матрицы:</p>')
    regression=[]
    for r in validation['RegressionControls']:
        a=r['Variants']['e06'];b=r['Variants']['after'];regression.append((r['Case'],delta((b['Throughput']/a['Throughput']-1)*100),delta((b['P99']/a['P99']-1)*100),delta((b['Allocation']/a['Allocation']-1)*100)))
    out.append(table(['Контроль','Скорость к e06','p99 к e06','Выделения к e06'],regression))
    out.append('<p>Полный Add во время нагрузки 4096 команд после 2048 команд прогрева, по одному smoke-прогону. Новый узел меняет топологию 3→4 или 5→6; эти скорости не смешаны с фиксированным составом основной матрицы.</p>')
    out.append(table(['Нагрузка','Статус Add','Завершился до конца команд','Add, мс'],[(r['Case'],r['Stage'],r['FinishedDuringCommands'],f'{r["AddMilliseconds"]:.1f}') for r in validation['AddUnderLoad']]))
    out.append('</section>')
    return ''.join(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('inputs', nargs='+', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--validation', type=Path)
    args = parser.parse_args()
    groups, runs, manifests = {}, [], []
    identities = {}
    for directory in args.inputs:
        manifest = json.loads((directory/'manifest.json').read_text(encoding='utf-8-sig'))
        if not manifest['complete'] or manifest['diagnose'] or manifest['hot_add']:
            raise ValueError('Main tables require completed, uninstrumented steady-state runs')
        manifests.append(dict(path=str(directory.resolve()), manifest=manifest))
        for run in manifest['runs']:
            variant = run['variant']
            if variant not in ('e06','before','after'): raise ValueError('Unexpected main-table variant')
            raw = directory/run['file']
            if hashlib.sha256(raw.read_bytes()).hexdigest().upper() != run['sha256']:
                raise ValueError('Raw evidence changed')
            report = json.loads(raw.read_text(encoding='utf-8-sig'))
            if not report['Complete'] or report['Failure'] or len(report['Results']) != 1: raise ValueError('Incomplete measurement')
            row = report['Results'][0]
            key = (run['series'],run['case'])
            bucket = groups.setdefault(key,{v:[] for v in ('e06','before','after')})[variant]
            if any(r['Round']==run['round'] for r in bucket): raise ValueError('Duplicate paired round')
            item = dict(row, Variant=variant, Series=run['series'], CaseName=run['case'], Round=run['round'], File=str(raw.resolve()))
            bucket.append(item)
            runs.append({k:v for k,v in item.items() if k not in ('LatencyMilliseconds','Diagnostics')})
            identity = {k:report[k] for k in ('BinarySha256','CurrentSourceSha256','HarnessSourceSha256','Runtime','OS','Architecture','ProcessorCount','ServerGC','DataRoot','CurrentLayout')}
            if variant in identities and identities[variant]!=identity: raise ValueError('Variant identity changed')
            identities[variant]=identity
    expected={(series,f'{nodes}-{size}-{mode}') for series in ('short','long')
              for nodes in (3,5) for size in (64,16384) for mode in ('Batched','Pipeline')}
    if set(groups)!=expected:
        raise ValueError('The main recovery report requires the complete 16-cell distributed matrix')
    rows = []
    for (series,case), variants in sorted(groups.items()):
        rounds = [{r['Round'] for r in variants[v]} for v in variants]
        if not rounds[0] or any(r!=rounds[0] for r in rounds) or len(rounds[0])<5: raise ValueError('Need at least five complete paired rounds per cell')
        summary = {}
        for v, values in variants.items():
            pooled = sorted(latency for r in values for latency in r['LatencyMilliseconds'])
            summary[v] = dict(Throughput=statistics.median(r['ProposalsPerSecond'] for r in values),
                              P99=pooled[math.ceil(len(pooled)*.99)-1],
                              MedianRoundP99=statistics.median(r['P99Milliseconds'] for r in values),
                              Allocation=statistics.median(r['AllocatedBytesPerProposal'] for r in values),
                              MinThroughput=min(r['ProposalsPerSecond'] for r in values),
                              MaxThroughput=max(r['ProposalsPerSecond'] for r in values), Samples=len(pooled))
        ratios = {k:summary['after'][k]/summary['e06'][k] for k in ('Throughput','P99','Allocation')}
        passed = ratios['Throughput']>=.95 and ratios['P99']<=1.10 and ratios['Allocation']<=1.05
        nodes,payload,mode=case.split('-')
        rows.append(dict(Series=series, Case=case, Nodes=int(nodes), Payload=int(payload), Mode=mode,
                         Rounds=len(rounds[0]), Variants=summary, Ratios=ratios, Pass=passed))
    validation = json.loads(args.validation.read_text(encoding='utf-8-sig')) if args.validation else None
    result = dict(Complete=True, PerformanceAccepted=bool(rows) and all(r['Pass'] for r in rows), Cells=rows, Measurements=len(runs), Samples=sum(r['Operations'] for r in runs),
                  PassedCells=sum(r['Pass'] for r in rows), TotalCells=len(rows),
                  Thresholds=dict(Throughput=.95, P99=1.10, Allocation=1.05),
                  Identities=identities, Runs=runs, Manifests=manifests, Validation=validation)
    output=args.out.resolve();output.parent.mkdir(parents=True,exist_ok=True)
    output.with_suffix('.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    with output.with_suffix('.csv').open('w',newline='',encoding='utf-8-sig') as file:
        writer=csv.writer(file);writer.writerow(['series','case','variant','rounds','commands/s','pooled_p99_ms','median_round_p99_ms','bytes/command','min_commands/s','max_commands/s'])
        for row in rows:
            for v,s in row['Variants'].items(): writer.writerow([row['Series'],row['Case'],v,row['Rounds'],s['Throughput'],s['P99'],s['MedianRoundP99'],s['Allocation'],s['MinThroughput'],s['MaxThroughput']])
    data=json.dumps(result,ensure_ascii=False,separators=(',',':')).replace('</','<\\/')
    template=HTML.replace('@@DATA@@',data)
    template=template.replace('@@PASS@@',str(result['PassedCells'])).replace('@@TOTAL@@',str(len(rows)))
    template=template.replace('@@MEASUREMENTS@@',str(len(runs))).replace('@@SAMPLES@@',f'{result["Samples"]:,}'.replace(',',' '))
    counts=sorted({r['Rounds'] for r in rows})
    template=template.replace('@@ROUND_RANGE@@',str(counts[0]) if len(counts)==1 else f'{counts[0]}–{counts[-1]}')
    verdict = ('Все точки находятся в трёх рабочих допусках.' if result['PassedCells']==len(rows) else
               f'Полный критерий восстановления пока не выполнен: {result["PassedCells"]} из {len(rows)} точек проходят все три допуска.')
    template=template.replace('@@VERDICT@@',html.escape(verdict))
    template=template.replace('@@APPENDIX@@',appendix(validation,rows))
    # Static fallback contains every cell, available without JavaScript and when printing.
    cells=[]
    for row in rows:
        v=row['Variants'];label=f'{row["Series"]} · {row["Nodes"]} узла · {row["Payload"]} Б · {row["Mode"]}'
        cells.append('<tr><th>'+html.escape(label)+'</th>'+''.join(f'<td>{v[x]["Throughput"]:.0f}</td><td>{v[x]["P99"]:.2f}</td><td>{v[x]["Allocation"]/1024:.2f}</td>' for x in ('e06','before','after'))+f'<td>{"PASS" if row["Pass"] else "FAIL"}</td></tr>')
    template=template.replace('@@STATIC@@',''.join(cells))
    template=template.replace('@@IDENTITY@@',html.escape(json.dumps(identities,ensure_ascii=False,indent=2)))
    template=template.replace('@@VALIDATION@@',html.escape(json.dumps(validation,ensure_ascii=False,indent=2)))
    round_rows=[]
    for r in runs:
        round_rows.append(f'<tr><td>{html.escape(r["Series"]+" / "+r["CaseName"])}</td><td>{html.escape(r["Variant"])}</td><td>{r["Round"]}</td><td>{r["ProposalsPerSecond"]:.0f}</td><td>{r["P99Milliseconds"]:.2f}</td><td>{r["AllocatedBytesPerProposal"]}</td></tr>')
    template=template.replace('@@ROUNDS@@','<table><thead><tr><th>Серия / точка</th><th>Версия</th><th>Раунд</th><th>Команд/с</th><th>p99, мс</th><th>Б/команду</th></tr></thead><tbody>'+''.join(round_rows)+'</tbody></table>')
    output.write_text(template,encoding='utf-8')
    print(f'{output}: {result["PassedCells"]}/{len(rows)} cells within all three tolerances, {len(runs)} measurements')


HTML=r'''<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Raft .NET 8 — восстановление производительности e06</title>
<style>
:root{--ink:#16314a;--muted:#526b85;--line:#d8e2ee;--bg:#f2f5fa;--old:#718399;--before:#c27b25;--after:#087e82;--red:#b41d36}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.6 system-ui,Segoe UI,sans-serif}main{max-width:1600px;margin:auto;padding:32px}h1{font-size:clamp(26px,3vw,42px);line-height:1.2}h2{font-size:24px;margin:0 0 16px}p{max-width:115ch}section{padding:28px;margin:24px 0;border:1px solid var(--line);background:white;border-radius:16px}code{font-size:.87em;overflow-wrap:anywhere}.muted{color:var(--muted)}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.card{padding:18px;background:white;border:1px solid var(--line);border-radius:14px}.card b{display:block;font-size:30px}.warning{border-left:5px solid var(--before);background:#fff8ec;padding:16px 20px}.good{color:#007667}.bad{color:var(--red)}.filters{display:flex;gap:18px;flex-wrap:wrap;align-items:end}.filters label{display:grid;gap:5px}select{font:inherit;color:var(--ink);padding:8px;border:1px solid #bacbdc;border-radius:6px;background:white}.scroll{overflow:auto;margin:16px 0}.chart{min-width:680px}svg{width:100%;display:block}table{border-collapse:collapse;min-width:1050px;width:100%;font-size:14px}th,td{padding:12px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}th:first-child,td:first-child{text-align:left}thead{background:#eef3f9}tbody tr:nth-child(even){background:#f8faff}summary{font-weight:650;cursor:pointer}details{margin:18px 0}.legend{display:flex;gap:20px;flex-wrap:wrap}.dot{width:12px;height:12px;display:inline-block;border-radius:3px;margin-right:6px}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.5 ui-monospace,monospace;background:#f4f6fa;padding:18px}.js-only{display:none}body.js .js-only{display:block}.explain li{margin:10px 0}a{color:#0759a1}@media(max-width:720px){main{padding:12px}section{padding:18px}.cards{grid-template-columns:1fr 1fr}.card b{font-size:25px}}@media print{main{padding:0}.filters{display:none}section{break-inside:avoid;padding:12px}details{display:block}details>*{display:block}.scroll{overflow:visible}table{min-width:0;font-size:8px}th,td{padding:5px}svg{max-height:580px}}
</style><main>
<p class="muted">14 ноября 2026 · .NET 8 · DBreeze 1.142.2026.1016-beta · реальные TCP и диск H:</p>
<h1>e06 / до исправления / после исправления</h1><p class="warning"><strong>@@VERDICT@@</strong></p>
<p>База: <code>e06c4f95edd6836c9a8782ecad2542bef298fa32</code>. «До» — замороженная библиотека R19; «после» — рабочие изменения поверх b6ba58a, без нового коммита.</p><p>Свежие измерения нашей библиотеки. Hot Add/Remove сохранён. Исторические R18/R19 не изменены; 8b991bd и DotNext повторно не запускались.</p>
<div class="cards"><div class="card"><b>@@PASS@@ / @@TOTAL@@</b>точек в трёх допусках</div><div class="card"><b>@@MEASUREMENTS@@</b>отдельных процессов</div><div class="card"><b>@@SAMPLES@@</b>измерений запросов</div><div class="card"><b>@@ROUND_RANGE@@</b>парных раундов на точку</div></div>
<section><h2>Как читать результат</h2><p class="warning"><strong>Зелёный throughput сам по себе не означает готовность.</strong> Для каждой точки требуется одновременно ≥95% скорости e06, ≤105% выделений и ≤110% p99. Все нарушения выделены красным. Допуски относятся к свежему e06 на этом хосте, а не к абсолютным значениям вчерашних отчётов.</p>
<ul class="explain"><li><strong>Подтверждённая лишняя работа:</strong> после каждой записи адаптер создавал полный RecoveredLogState, а в v4 ещё MembershipView, чтобы узнать один счётчик байтов. Теперь читает кэшированный долговечный фронтир под тем же lock; тест проверяет нулевые выделения повторного чтения.</li><li><strong>Очередь предложений:</strong> резервирование сигнала сохраняется до разбора всего пакета; проверка дедлайна и постановка сигнала используют одно состояние очереди. Повторный сигнал по устаревшей волне больше не должен преждевременно сбрасывать следующую. Задержки пакетирования не увеличены.</li><li><strong>Отклонённый эксперимент:</strong> дополнительный async/CTS-слой старой схемы завершения TCP не дал преимущества. Обычный TCP-путь оставлен прямым.</li><li><strong>Граница вывода:</strong> основные оптимизации e06 сохранились; единственной причины всей исторической разницы R18/R19 не установлено. Свежий e06 тоже заметно колеблется. Обнаруженная гонка очереди существовала и до Hot Add/Remove; это исправление общего runtime, а не доказательство, что вся потеря была вызвана membership.</li></ul>
<p>При свободном месте команда теперь принимается без linked CTS: он создаётся только для ожидания занятого admission-лимита. Отмена и остановка узла проверяются до приёма и под замком перед передачей команды очереди; отмена ожидания результата не откатывает принятую команду.</p><p>Обычная запись не сканирует историю membership и не переписывает неизменившийся справочник. CRC, подтверждение после durability/application, wire3/v4 и публичные API сохранены. Профилирование и дополнительный JIT-прогрев не смешиваются с таблицами ёмкости.</p></section>
<section class="js-only"><h2>Сравнение по точкам</h2><div class="filters"><label>Серия<select id="series"><option value="all">Обе</option><option value="short">Короткая: 128 + 512</option><option value="long">Длинная: 2048 + 4096</option></select></label><label>Узлы<select id="nodes"><option value="all">3 и 5</option><option>3</option><option>5</option></select></label><label>Режим<select id="mode"><option value="all">Batched и Pipeline</option><option>Batched</option><option>Pipeline</option></select></label></div><p class="legend"><span><i class="dot" style="background:var(--old)"></i>e06 · wire2/v3</span><span><i class="dot" style="background:var(--before)"></i>До · wire3/v4</span><span><i class="dot" style="background:var(--after)"></i>После · wire3/v4</span></p>
<h3>Пропускная способность, команд/с ↑</h3><div class="scroll"><div id="rate" class="chart"></div></div>
<h3>p99 всех запросов точки, мс ↓</h3><div class="scroll"><div id="tail" class="chart"></div></div>
<h3>Выделения managed memory, КиБ/команду ↓</h3><div class="scroll"><div id="memory" class="chart"></div></div></section>
<section><h2>Полная матрица и допуски</h2><p class="muted">Скорость и выделения — медианы раундов. p99 — nearest rank по объединённым задержкам всех раундов данной версии и точки. Короткие и длинные серии не объединяются. Число раундов одинаково для трёх вариантов внутри каждой точки.</p><div class="scroll" id="matrix"><table><thead><tr><th>Точка</th><th>e06, ком/с</th><th>e06 p99</th><th>e06 КиБ</th><th>До, ком/с</th><th>До p99</th><th>До КиБ</th><th>После, ком/с</th><th>После p99</th><th>После КиБ</th><th>Допуски</th></tr></thead><tbody>@@STATIC@@</tbody></table></div></section>
@@APPENDIX@@<section><h2>Методика и практические ограничения</h2><p>3/5 узлов × 64 Б/16 КиБ × Batched/Pipeline × две серии =16 точек. Закрытая нагрузка, concurrency64; короткая серия:128 прогрев +512 запросов, длинная:2048+4096. Каждый вариант каждого раунда — новый процесс, порядок чередуется. Один процесс содержит несколько независимых узлов/DBreeze engines, соединённых loopback TCP. Проверки процессов, аварий и Add/Remove выполняются отдельно.</p><p>Без изменения нагрузки: tick25мс, heartbeat1, election[5000,6000)мс, RPC2с, сбор2мс/8мс bulk, пределы байтов, бизнес-транзакция и диск. Подтверждение следует после кворума и локального применения; после прогона проверены durable counters/checkpoints всех реплик с закрытием и повторным открытием. Компакция выключена. Это локальный сравнительный опыт, не оценка производительности разных машин или открытой нагрузки.</p><p>Результаты включают JIT, GC и планировщик измеряемого процесса. Случайный выбор лидера, диск и работа других приложений создают разброс. Дополнительные диагностические прогоны с длительным прогревом не заменяют исходные короткие/длинные серии. Времена storage/RPC могут перекрываться; суммировать их как CPU-время нельзя.</p><details><summary>Все отдельные раунды и диапазоны</summary><div class="scroll" id="rounds">@@ROUNDS@@</div></details><details><summary>Идентичность исходников, библиотек и среды</summary><pre id="identity">@@IDENTITY@@</pre></details><details><summary>Проверки корректности и дополнительные эксперименты</summary><pre id="validation">@@VALIDATION@@</pre></details><p class="muted">CSV и JSON с тем же именем содержат агрегаты, все раунды и manifest с SHA-256 сырых файлов. Сами индивидуальные задержки и логи находятся по путям File в JSON, под .artifacts/performance-recovery/. Исходные R18/R19 сохранены.</p></section>
<script id="data" type="application/json">@@DATA@@</script><script>
'use strict';document.body.classList.add('js');const data=JSON.parse(document.getElementById('data').textContent);const fmt=(v,d=0)=>new Intl.NumberFormat('ru-RU',{maximumFractionDigits:d,minimumFractionDigits:d}).format(v);const pct=v=>(v>=1?'+':'')+fmt((v-1)*100,1)+'%';const series=document.getElementById('series'),nodes=document.getElementById('nodes'),mode=document.getElementById('mode');const colors=['#718399','#c27b25','#087e82'];const names=['e06','before','after'];const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function selected(){return data.Cells.filter(r=>(series.value==='all'||r.Series===series.value)&&(nodes.value==='all'||String(r.Nodes)===nodes.value)&&(mode.value==='all'||r.Mode===mode.value));}
function label(r){return(r.Series==='short'?'К':'Д')+' · '+r.Nodes+' уз. · '+(r.Payload===64?'64 Б':'16 КиБ')+' · '+r.Mode;}
function draw(id,metric,divisor){const rows=selected(),w=1300,left=240,right=95,h=rows.length*74+35;const max=Math.max(1,...rows.flatMap(r=>names.map(v=>r.Variants[v][metric]/divisor)))*1.05;let s=`<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(metric)}">`;for(let k=0;k<=4;k++){const x=left+(w-left-right)*k/4;s+=`<path d="M${x},20 V${h-10}" stroke="#e2e9f2"/><text x="${x}" y="14" text-anchor="middle" fill="#526b85" font-size="12">${fmt(max*k/4,metric==='Throughput'?0:1)}</text>`;}rows.forEach((r,i)=>{const y=30+i*74;s+=`<text x="0" y="${y+25}" font-size="14" fill="#16314a">${esc(label(r))}</text>`;names.forEach((v,j)=>{const val=r.Variants[v][metric]/divisor,width=val/max*(w-left-right),yy=y+j*19;s+=`<rect x="${left}" y="${yy}" width="${width}" height="15" rx="2" fill="${colors[j]}"><title>${esc(label(r)+' '+v+': '+fmt(val,2))}</title></rect><text x="${left+width+6}" y="${yy+12}" font-size="12" fill="#16314a">${fmt(val,metric==='Throughput'?0:2)}</text>`;});});document.getElementById(id).innerHTML=s+'</svg>';}
function render(){draw('rate','Throughput',1);draw('tail','P99',1);draw('memory','Allocation',1024);const body=data.Cells.map(r=>{const v=r.Variants,t=r.Ratios;return`<tr><th>${esc(label(r))}<br><small>${r.Rounds} раундов</small></th>${names.map(n=>`<td title="${fmt(v[n].MinThroughput)}–${fmt(v[n].MaxThroughput)}">${fmt(v[n].Throughput)}</td>`).join('')}<td class="${t.Throughput>=.95?'good':'bad'}">${pct(t.Throughput)}</td><td class="${v.after.Throughput>=v.before.Throughput?'good':'bad'}">${pct(v.after.Throughput/v.before.Throughput)}</td><td>${fmt(v.e06.P99,2)} / ${fmt(v.before.P99,2)} / ${fmt(v.after.P99,2)}<br><span class="${t.P99<=1.1?'good':'bad'}">${pct(t.P99)}</span></td><td>${fmt(v.e06.Allocation/1024,2)} / ${fmt(v.before.Allocation/1024,2)} / ${fmt(v.after.Allocation/1024,2)}<br><span class="${t.Allocation<=1.05?'good':'bad'}">${pct(t.Allocation)}</span></td><td class="${r.Pass?'good':'bad'}"><strong>${r.Pass?'PASS':'FAIL'}</strong></td></tr>`;}).join('');document.getElementById('matrix').innerHTML='<table><thead><tr><th>Точка</th><th>e06, ком/с</th><th>До, ком/с</th><th>После, ком/с</th><th>После/e06, Δ</th><th>После/до, Δ</th><th>p99 e06/до/после, мс ↓</th><th>КиБ e06/до/после ↓</th><th>Три допуска</th></tr></thead><tbody>'+body+'</tbody></table>';}
for(const id of ['series','nodes','mode'])document.getElementById(id).addEventListener('change',render);render();
document.getElementById('rounds').innerHTML='<table><thead><tr><th>Серия / точка</th><th>Версия</th><th>Раунд</th><th>Команд/с</th><th>p99, мс</th><th>Б/команду</th></tr></thead><tbody>'+data.Runs.map(r=>`<tr><td>${esc(r.Series+' / '+r.CaseName)}</td><td>${esc(r.Variant)}</td><td>${r.Round}</td><td>${fmt(r.ProposalsPerSecond)}</td><td>${fmt(r.P99Milliseconds,2)}</td><td>${fmt(r.AllocatedBytesPerProposal)}</td></tr>`).join('')+'</tbody></table>';
document.getElementById('identity').textContent=JSON.stringify(data.Identities,null,2);document.getElementById('validation').textContent=JSON.stringify(data.Validation,null,2);
</script></main></html>'''


if __name__ == '__main__': main()
