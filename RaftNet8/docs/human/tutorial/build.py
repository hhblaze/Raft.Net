"""Build the two offline books with Python 3 standard library only.

Run from any directory: python -X utf8 -B <path-to-this-file>
Code excerpts are taken from the adjacent compiled TutorialNode project.
"""
from __future__ import annotations

import hashlib
import html
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAFT_ROOT = HERE.parents[2]
REPO_ROOT = RAFT_ROOT.parent
EXAMPLES = HERE / "examples" / "TutorialNode"


def esc(value):
    return html.escape(str(value), quote=True)


def search_text(value):
    return esc(" ".join(re.sub(r"<[^>]+>", " ", value).split()).lower())


def p(text):
    return f"<p>{text}</p>"


def h(text):
    return f"<h3>{text}</h3>"


def ul(*items):
    return "<ul>" + "".join(f"<li>{item}</li>" for item in items) + "</ul>"


def ol(*items):
    return "<ol>" + "".join(f"<li>{item}</li>" for item in items) + "</ol>"


def callout(title, body, kind="note"):
    return f'<aside class="callout {esc(kind)}"><strong>{title}</strong>{p(body)}</aside>'


def table(headers, rows):
    return '<div class="table-wrap" tabindex="0"><table><thead><tr>' + "".join(
        f"<th scope=\"col\">{cell}</th>" for cell in headers
    ) + "</tr></thead><tbody>" + "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows
    ) + "</tbody></table></div>"


def highlight(source, language):
    if language != "csharp":
        return esc(source)
    keywords = set("using namespace public private internal protected sealed static readonly record class interface enum new var return void string long int bool byte null true false if else try catch finally throw await async Task ValueTask CancellationToken CancellationTokenSource typeof nameof is not with get init required foreach while break continue switch case default when out in params this base for const checked".split())
    pattern = re.compile(r'//[^\n]*|/\*[\s\S]*?\*/|\$?@?"(?:\\.|""|[^"\\])*"|\b\d[\d_]*\b|\b[A-Za-z_]\w*\b')
    output, start = [], 0
    for match in pattern.finditer(source):
        output.append(esc(source[start:match.start()]))
        token = match.group()
        kind = "comment" if token.startswith(("//", "/*")) else "str" if '"' in token else "num" if token[0].isdigit() else "kw" if token in keywords else None
        output.append(f'<span class="{kind}">{esc(token)}</span>' if kind else esc(token))
        start = match.end()
    output.append(esc(source[start:]))
    return "".join(output)


def code(source, language="csharp", title=None, region=None):
    source = source.strip("\n")
    label = title or {"csharp": "C#", "shell": "Terminal", "text": "Text", "xml": "XML"}.get(language, language)
    attribute = f' data-region="{esc(region)}"' if region else ""
    return f'<figure class="code-block"{attribute}><figcaption><span>{esc(label)}</span><button class="copy" type="button" data-copy>Copy</button></figcaption><pre tabindex="0"><code class="language-{esc(language)}">{highlight(source, language)}</code></pre></figure>'


def excerpt(filename, name, title=None):
    text = (EXAMPLES / filename).read_text(encoding="utf-8")
    match = re.search(r"^[ \t]*// book:start " + re.escape(name) + r"\r?\n([\s\S]*?)^[ \t]*// book:end " + re.escape(name) + r"\s*$", text, re.M)
    if not match:
        raise ValueError(f"Missing region {filename}:{name}")
    import textwrap
    source = textwrap.dedent(match[1]).rstrip()
    return code(source, title=title or f"{filename} · {name}", region=f"{filename}:{name}")


def source_link(relative, label):
    return f'<a href="../../../{esc(relative)}">{esc(label)}</a>'


def path_tree(ru=False):
    tree = """<data root>/n1/
  log/                            DBreeze consensus database
    ...                           scheme, journals, numbered table files
  application/
    owner.lock
    current                       checked active-generation pointer
    generations/
      gen-<id>/database/          business rows + applied checkpoint
  snapshots/
    owner.lock
    completed/                    immutable images + checked descriptors
    incoming/                     resumable .part data + durable progress
    building/                     unpublished local image staging"""
    if ru:
        for source, target in {
            "DBreeze consensus database": "База журнала консенсуса DBreeze",
            "scheme, journals, numbered table files": "схема, журналы, нумерованные файлы таблиц",
            "checked active-generation pointer": "проверяемый указатель активного поколения",
            "business rows + applied checkpoint": "бизнес-строки и checkpoint применения",
            "immutable images + checked descriptors": "неизменяемые образы и проверяемые дескрипторы",
            "resumable .part data + durable progress": "возобновляемые .part и постоянный прогресс",
            "unpublished local image staging": "временные ещё не опубликованные образы"
        }.items():
            tree = tree.replace(source, target)
    return code(tree, "text")


def flow(ru):
    labels = ["Приложение", "Лидер", "Кворум дисковых копий", "Применение лидером", "Успех клиенту"] if ru else ["Application", "Leader", "Durable voter quorum", "Leader application", "Client success"]
    desc = "Данные проходят от клиента к лидеру, сохраняются кворумом, применяются лидером и только затем подтверждаются." if ru else "A command goes to the leader, reaches durable quorum, is applied by the leader, and then completes."
    boxes = ""
    for i, label in enumerate(labels):
        x = 12 + i * 188
        boxes += f'<rect x="{x}" y="18" width="172" height="64" rx="12"/><text x="{x+86}" y="55" text-anchor="middle">{label}</text>'
        if i != 4:
            boxes += f'<path d="M {x+173} 50 h 13" marker-end="url(#arrow)"/>'
    return f'<figure class="flow"><svg viewBox="0 0 960 102" role="img" aria-label="{esc(desc)}"><defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z"/></marker></defs>{boxes}</svg><figcaption>{desc}</figcaption></figure>'


def quorum_lab(ru):
    title = "Попробуйте потерю узлов" if ru else "Explore lost voters"
    hint = "Только арифметика фиксированного состава: доступность также требует связи, избираемого лидера и исправного хранилища." if ru else "Fixed-membership arithmetic only: progress also requires connectivity, an eligible leader and healthy storage."
    size = "Число голосующих" if ru else "Voter count"
    available = "Доступно" if ru else "Available"
    return f'''<div class="lab" data-quorum data-lang="{'ru' if ru else 'en'}"><h3>{title}</h3>
    <label>{size} <select data-voters><option>1</option><option selected>3</option><option>5</option><option>7</option></select></label>
    <div class="voter-buttons" data-voter-buttons></div><output aria-live="polite" data-quorum-result></output><p class="muted">{hint}</p><noscript>{available}: 3/3; quorum: 2. 3 voters tolerate 1 loss; 5 voters tolerate 2.</noscript></div>'''


def memory_lab(ru):
    labels = ["Предел заявок", "Команда, КиБ", "Узлы", "Грубая оценка управляемых полезных данных", "Это отдельные верхние оценки, а не итоговый RSS. Данные DBreeze, прикладные записи и ID запросов, очереди ожидающих вызывающих, снимки, объекты CLR и сокеты добавляют память."] if ru else ["Proposal limit", "Command KiB", "Nodes", "Rough accounted payload bounds", "These are separate upper bounds, not total RSS. DBreeze, business rows and request IDs, waiting callers, snapshots, CLR objects and sockets add memory."]
    return f'''<div class="lab" data-memory data-lang="{'ru' if ru else 'en'}"><h3>{labels[3]}</h3>
    <div class="memory-inputs"><label>{labels[0]}<input data-pending type="number" min="1" max="1000000" value="1024"></label><label>{labels[1]}<input data-command type="number" min="1" max="16384" value="64"></label><label>{labels[2]}<select data-nodes><option>1</option><option selected>3</option><option>5</option><option>7</option></select></label></div><output data-memory-result aria-live="polite"></output><p class="muted">{labels[4]}</p><noscript>1024 × 64 KiB = 64 MiB payload. Cache: 8 MiB. Peer windows for 3 voters: 8 MiB.</noscript></div>'''


CSS = r'''
:root{--bg:#f3f5f7;--paper:#fff;--ink:#182b3c;--muted:#52677a;--line:#dbe3e9;--accent:#087d78;--soft:#e9f6f3;--nav:#152e3b;--code:#152833;--code-ink:#e8f0f5;--warn:#fff3db;--shadow:0 12px 38px #18394e0a;color-scheme:light}
*{box-sizing:border-box}html{scroll-behavior:smooth;scroll-padding-top:90px}body{margin:0;background:var(--bg);color:var(--ink);font:17px/1.72 'Segoe UI',system-ui,sans-serif}a{color:var(--accent);text-underline-offset:3px}a:hover{text-decoration-thickness:2px}a:focus-visible,button:focus-visible,input:focus-visible,select:focus-visible,summary:focus-visible,[tabindex]:focus-visible{outline:3px solid #d08c14;outline-offset:4px}button,input,select{font:inherit}button{cursor:pointer}.skip{position:fixed;left:10px;top:-100px;background:white;padding:12px;z-index:20}.skip:focus{top:10px}.topbar{position:sticky;top:0;z-index:5;background:var(--nav);color:white;box-shadow:0 2px 14px #0002}.topbar-inner{max-width:1580px;padding:12px 30px;margin:auto;display:flex;align-items:center;justify-content:space-between;gap:16px}.brand{font-size:16px;color:#fff;text-decoration:none;font-weight:650;letter-spacing:.02em}.brand span{font-weight:400;color:#b5cdd4}.top-actions{display:flex;gap:10px;align-items:center;font-size:14px;flex-wrap:wrap}.top-actions a,.top-actions button{color:white;background:transparent;border:1px solid #ffffff42;border-radius:7px;padding:4px 10px;text-decoration:none}.progress{height:3px;background:#ffffff1a}.progress>div{height:3px;background:#72d6bb;width:0}.layout{display:grid;grid-template-columns:272px minmax(0,1fr);max-width:1510px;margin:0 auto;gap:32px;padding:36px 30px 70px}.sidebar{position:sticky;top:92px;align-self:start;max-height:calc(100vh - 110px);overflow:auto;padding:0 16px 18px 0;font-size:14px;scrollbar-width:thin}.sidebar label{font-weight:650;display:block;margin-bottom:12px}.sidebar input{width:100%;padding:10px 12px;border:1px solid var(--line);border-radius:8px;background:var(--paper);color:var(--ink);font-size:14px;margin:6px 0}.sidebar ol{list-style:none;padding:0;margin:0}.sidebar li{margin:2px 0}.sidebar a{display:grid;grid-template-columns:25px 1fr;gap:6px;color:var(--muted);padding:6px 8px;text-decoration:none;border-radius:6px;line-height:1.5}.sidebar a:hover,.sidebar a[aria-current=true]{color:var(--accent);background:var(--soft)}.sidebar a span:first-child{font-size:12px;padding-top:2px;opacity:.7;font-variant-numeric:tabular-nums}.toc-caption{text-transform:uppercase;letter-spacing:.1em;font-size:11px;color:var(--muted);margin:18px 8px 7px}.book{min-width:0;max-width:1130px;overflow-wrap:anywhere}.cover{padding:44px 48px;background:var(--paper);border:1px solid var(--line);border-top:5px solid var(--accent);border-radius:14px;box-shadow:var(--shadow);margin-bottom:28px}.eyebrow{text-transform:uppercase;letter-spacing:.13em;font-size:12px;font-weight:700;color:var(--accent)}h1,h2,h3,h4{line-height:1.25;letter-spacing:-.02em;text-wrap:balance}h1{font-size:clamp(32px,3.2vw,48px);margin:12px 0 22px;max-width:800px}h2{font-size:30px;margin:8px 0 25px}h3{font-size:21px;margin:32px 0 14px}h4{font-size:18px;margin:25px 0 10px}p{margin:0 0 18px}.lead{font:21px/1.65 Georgia,'Times New Roman',serif;max-width:820px}.tags{display:flex;flex-wrap:wrap;gap:8px;margin:26px 0 0}.tags span{font:12px/1.4 'Segoe UI',sans-serif;background:var(--soft);padding:6px 10px;border-radius:20px;color:var(--accent);font-weight:600}.cover .reading{font-size:13px;color:var(--muted);margin-top:22px}.chapter{padding:36px 48px 32px;background:var(--paper);border:1px solid var(--line);border-radius:12px;margin-bottom:26px;box-shadow:var(--shadow);scroll-margin-top:90px}.chapter-number{color:var(--accent);font-size:12px;font-weight:700;letter-spacing:.12em;text-transform:uppercase}.chapter>p,.chapter>ul,.chapter>ol{max-width:900px}.chapter>p{font-family:Georgia,'Times New Roman',serif;font-size:18px;line-height:1.8}li{padding-left:3px;margin-bottom:8px}ul,ol{padding-left:26px}code{font: .88em/1.5 Consolas,'Cascadia Code',monospace;overflow-wrap:anywhere}p code,li code,td code{background:var(--soft);padding:2px 5px;border-radius:4px}.code-block{margin:22px 0 26px;border:1px solid #334a56;border-radius:9px;overflow:hidden;background:var(--code);color:var(--code-ink);max-width:100%}.code-block figcaption{background:#1f3946;color:#cbdde4;display:flex;justify-content:space-between;align-items:center;padding:8px 14px;font:12px/1.5 'Segoe UI',sans-serif;gap:10px}.copy{border:1px solid #ffffff3b;border-radius:5px;background:transparent;color:white;padding:3px 10px;font-size:12px;flex-shrink:0}pre{margin:0;padding:19px 20px;overflow:auto;tab-size:4;font:13px/1.72 Consolas,'Cascadia Code',monospace;max-height:720px}pre code{font:inherit;white-space:pre;overflow-wrap:normal}.kw{color:#83c6ff}.str{color:#f3cc98}.num{color:#c5a8fa}.comment{color:#9bb4ab}.callout{padding:18px 22px;border-left:4px solid var(--accent);background:var(--soft);border-radius:0 9px 9px 0;margin:25px 0}.callout strong{font-size:16px}.callout p{margin:7px 0 0;font-size:15px;line-height:1.75}.callout.warn{background:var(--warn);border-left-color:#c68b20}.table-wrap{overflow:auto;margin:24px 0;max-width:100%;border:1px solid var(--line);border-radius:8px}table{border-collapse:collapse;width:100%;font-size:14px;line-height:1.65}th,td{text-align:left;vertical-align:top;padding:12px 15px;border-bottom:1px solid var(--line)}thead{background:var(--soft)}th{font-size:12px;color:var(--muted);font-weight:700}tr:last-child td{border-bottom:0}tbody tr:nth-child(2n){background:#8cacc009}td:first-child{font-weight:550;overflow-wrap:normal}.muted{color:var(--muted);font-size:13px}details{border:1px solid var(--line);border-radius:8px;padding:13px 18px;margin:20px 0}summary{cursor:pointer;font-weight:600;color:var(--accent)}details .code-block{margin-bottom:5px}.flow{margin:22px 0}.flow svg{display:block;width:100%;height:auto}.flow rect{fill:var(--soft);stroke:var(--accent);stroke-width:1}.flow text{font:12px 'Segoe UI',sans-serif;fill:var(--ink)}.flow path{stroke:var(--accent);fill:var(--accent)}.flow figcaption{font-size:13px;color:var(--muted);margin:8px 0}.lab{padding:24px;background:var(--soft);border:1px solid var(--line);border-radius:10px;margin:24px 0}.lab h3{margin:0 0 18px}.lab select,.lab input{background:var(--paper);color:var(--ink);border:1px solid var(--line);border-radius:6px;padding:5px 9px;max-width:140px}.lab output{display:block;font-weight:650;font-size:17px;margin:17px 0}.lab p{margin-bottom:0}.voter-buttons{display:flex;gap:8px;flex-wrap:wrap;margin-top:18px}.voter-buttons button{background:var(--accent);color:white;border:2px solid transparent;border-radius:7px;font-size:13px;padding:10px 14px}.voter-buttons button[aria-pressed=false]{background:var(--paper);color:var(--muted);border-color:var(--line);text-decoration:line-through}.memory-inputs{display:flex;gap:24px;flex-wrap:wrap}.memory-inputs label{font-size:13px;display:grid;gap:5px}.footer{padding:15px 8px 30px;color:var(--muted);font-size:13px}.chapter-bottom{display:flex;justify-content:space-between;margin-top:30px;padding-top:15px;border-top:1px solid var(--line);font-size:13px;gap:20px}.index{max-width:1000px;margin:70px auto;padding:0 24px}.index .cover{padding:50px}.editions{display:grid;grid-template-columns:1fr 1fr;gap:22px;margin-top:30px}.edition{padding:28px;border:1px solid var(--line);border-radius:10px;text-decoration:none;background:var(--soft)}.edition strong{display:block;font-size:25px;margin-bottom:12px}.edition p{font-size:15px;color:var(--muted);margin-bottom:0}[data-theme=dark]{--bg:#101d26;--paper:#172a35;--ink:#e1edf0;--muted:#b2c5cf;--line:#334b59;--accent:#7ad8c4;--soft:#203c45;--warn:#443c29;--nav:#0b1922;color-scheme:dark}[hidden]{display:none!important}
.toc-fold{border:0;padding:0;margin:0}.toc-fold>summary{display:none}.toc-fold label{margin-top:0}
@media(max-width:850px){.toc-fold>summary{display:list-item;font-size:15px}.toc-fold[open]>summary{margin-bottom:16px}}
@media(max-width:1120px){.layout{grid-template-columns:220px minmax(0,1fr);gap:22px;padding:28px 22px}.chapter,.cover{padding:30px}.sidebar{font-size:13px}}
@media(max-width:850px){.layout{display:block;padding:22px 18px}.sidebar{position:static;max-height:none;padding:14px 18px;background:var(--paper);border:1px solid var(--line);border-radius:9px;margin-bottom:24px}.sidebar ol{columns:2;column-gap:20px}.sidebar li{break-inside:avoid}.sidebar label{margin:0}.sidebar input{max-width:420px}.topbar-inner{padding:12px 18px}.brand span{display:none}.chapter,.cover{padding:28px}.chapter>p{font-size:18px}.flow svg{min-width:700px}.flow{overflow:auto}.editions{grid-template-columns:1fr}}
@media(max-width:500px){body{font-size:16px}.topbar-inner{gap:8px;padding:10px 12px}.top-actions{gap:5px;font-size:12px}.top-actions a,.top-actions button{padding:4px 7px}.brand{font-size:14px}.layout{padding:18px 12px}.cover,.chapter{padding:24px 19px}.sidebar ol{columns:1}h1{font-size:33px}h2{font-size:26px}h3{font-size:20px}.lead{font-size:20px}.callout{padding:15px 17px}pre{font-size:12px;padding:15px}.code-block figcaption{padding:8px 10px;font-size:11px}th,td{padding:10px;min-width:125px}.lab{padding:18px}.index{margin-top:30px}.index .cover{padding:25px}.memory-inputs{gap:14px}}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}
@media print{@page{size:A4;margin:16mm 14mm}html{scroll-behavior:auto}body{background:white;color:#111;font-size:10pt;line-height:1.55}.topbar,.sidebar,.skip,.copy,.chapter-bottom,.no-print{display:none!important}.layout{display:block;padding:0;max-width:none}.book{max-width:none}.cover,.chapter{border:0;box-shadow:none;padding:0;margin:0 0 20pt;border-radius:0}.cover{break-after:page;padding-top:60pt}.chapter{break-before:page}h1{font-size:30pt}h2{font-size:21pt}h3{font-size:14pt}h2,h3,h4{break-after:avoid}.chapter>p{font-size:10.5pt;line-height:1.6}a{color:#075c59}pre{max-height:none;white-space:pre-wrap;overflow:visible;font-size:8pt;line-height:1.5;padding:12pt}pre code{white-space:pre-wrap;word-break:break-word}.code-block{background:#f3f6f7;color:#172833;border-color:#ccd6db;break-inside:auto}.code-block figcaption{background:#e6edf0;color:#243d4c}.kw{color:#174a9f}.str{color:#7e3c11}.num{color:#734b9b}.comment{color:#46674f}.table-wrap{overflow:visible}table{font-size:8pt}thead{display:table-header-group}tr{break-inside:avoid}th,td{min-width:0;padding:7pt}.flow{overflow:visible}.flow svg{min-width:0}.flow text{fill:#172833}.flow rect{fill:#edf6f3;stroke:#087d78}.lab{background:#f3f8f6;border:1px solid #ceded8;break-inside:avoid}.lab select,.lab input{color:#111;background:white}.lab button{color:#111}.callout{background:#edf6f3;break-inside:avoid}.callout.warn{background:#fff4df}.footer{font-size:8pt}details{border:0;padding:0}.lead{font-size:14pt}.memory-inputs{gap:20pt}}
'''

JS = r'''
(()=>{
const ru=document.documentElement.lang==='ru';
const words=ru?{copy:'Копировать',copied:'Скопировано',failed:'Выделите код вручную',alive:'доступен',down:'отключён'}:{copy:'Copy',copied:'Copied',failed:'Select code manually',alive:'online',down:'offline'};
document.querySelectorAll('[data-copy]').forEach(button=>{button.textContent=words.copy;button.addEventListener('click',async()=>{const value=button.closest('figure').querySelector('code').textContent;try{if(navigator.clipboard&&window.isSecureContext){await navigator.clipboard.writeText(value)}else{const area=document.createElement('textarea');area.value=value;area.style.position='fixed';area.style.opacity='0';document.body.append(area);area.select();if(!document.execCommand('copy'))throw Error('copy');area.remove()}button.textContent=words.copied}catch{button.textContent=words.failed}setTimeout(()=>button.textContent=words.copy,1800)})});
document.querySelector('[data-print]')?.addEventListener('click',()=>window.print());
document.querySelector('[data-theme-toggle]')?.addEventListener('click',()=>{document.documentElement.dataset.theme=document.documentElement.dataset.theme==='dark'?'light':'dark'});
const other=document.querySelector('[data-other-language]');const updateOther=()=>{if(other)other.href=other.dataset.base+location.hash};updateOther();window.addEventListener('hashchange',updateOther);
const search=document.querySelector('[data-toc-search]');search?.addEventListener('input',()=>{const term=search.value.toLocaleLowerCase().trim();document.querySelectorAll('[data-toc-item]').forEach(item=>{item.hidden=term!==''&&!item.dataset.search.includes(term)})});
const links=[...document.querySelectorAll('.sidebar a[href^="#"]')];if('IntersectionObserver'in window){const observer=new IntersectionObserver(entries=>{for(const entry of entries)if(entry.isIntersecting)links.forEach(link=>link.setAttribute('aria-current',String(link.hash==='#'+entry.target.id)))},{rootMargin:'-10% 0px -72% 0px'});document.querySelectorAll('.chapter').forEach(chapter=>observer.observe(chapter))}
const fold=document.querySelector('.toc-fold');if(fold){const narrow=matchMedia('(max-width:850px)');fold.open=!narrow.matches;narrow.addEventListener('change',event=>fold.open=!event.matches)}
const progress=document.querySelector('[data-progress]');const advance=()=>{const length=document.documentElement.scrollHeight-innerHeight;if(progress)progress.style.width=(length>0?Math.max(0,Math.min(100,scrollY/length*100)):0)+'%'};addEventListener('scroll',advance,{passive:true});advance();
document.querySelectorAll('[data-quorum]').forEach(lab=>{const select=lab.querySelector('[data-voters]'),buttons=lab.querySelector('[data-voter-buttons]'),output=lab.querySelector('output');const update=()=>{const n=Number(select.value),count=buttons.querySelectorAll('[aria-pressed=true]').length,q=Math.floor(n/2)+1;output.textContent=ru?`${count}/${n} доступны · кворум ${q} · ${count>=q?'кворум достижим':'запись и свежее чтение невозможны'}`:`${count}/${n} online · quorum ${q} · ${count>=q?'quorum can be reached':'writes and fresh reads unavailable'}`};const reset=()=>{buttons.replaceChildren();for(let i=1;i<=Number(select.value);i++){const b=document.createElement('button');b.type='button';b.textContent='n'+i+' · '+words.alive;b.setAttribute('aria-pressed','true');b.addEventListener('click',()=>{const online=b.getAttribute('aria-pressed')!=='true';b.setAttribute('aria-pressed',String(online));b.textContent='n'+i+' · '+(online?words.alive:words.down);update()});buttons.append(b)}update()};select.addEventListener('change',reset);reset()});
document.querySelectorAll('[data-memory]').forEach(lab=>{const inputs=[...lab.querySelectorAll('input,select')],out=lab.querySelector('output');const update=()=>{const pending=Number(lab.querySelector('[data-pending]').value),kb=Number(lab.querySelector('[data-command]').value),nodes=Number(lab.querySelector('[data-nodes]').value);if(!Number.isFinite(pending)||!Number.isFinite(kb)||pending<1||pending>1000000||kb<1||kb>16384){out.textContent=ru?'Введите допустимые положительные значения.':'Enter valid positive values.';return}const payload=(pending*kb/1024).toLocaleString(ru?'ru':'en',{maximumFractionDigits:2}),window=4*(nodes-1);out.textContent=ru?`Копии команд: до ${payload} МиБ полезных данных · кэш: до 8 МиБ · окна лидера: до ${window} МиБ`:`Command copies: up to ${payload} MiB payload · cache: up to 8 MiB · leader peer windows: up to ${window} MiB`};inputs.forEach(input=>input.addEventListener('input',update));update()});
let openBeforePrint=[];addEventListener('beforeprint',()=>{openBeforePrint=[...document.querySelectorAll('details:not([open])')];openBeforePrint.forEach(d=>d.open=true)});addEventListener('afterprint',()=>openBeforePrint.forEach(d=>d.open=false));
})();
'''


def build_book(language, chapters):
    ru = language == "ru"
    title = "Raft .NET 8 — практическая книга" if ru else "Raft .NET 8 — a practical book"
    subtitle = "От первого кластера до восстановления данных" if ru else "From your first cluster to data recovery"
    lead = "Как встроить Raft в приложение на C#, запустить три узла, безопасно записывать и читать данные, управлять составом кластера и понимать, что происходит с памятью и диском." if ru else "Embed Raft in a C# application, start three nodes, write and read safely, manage cluster membership, and understand exactly what happens in memory and on disk."
    other = "en" if ru else "ru"
    other_label = "English" if ru else "Русский"
    toc_label = "Оглавление" if ru else "Contents"
    search_label = "Найти главу" if ru else "Find a chapter"
    chapter_label = "Глава" if ru else "Chapter"
    toc = "".join(f'<li data-toc-item data-search="{search_text(name+" "+body)}"><a href="#{ident}"><span>{i:02}</span><span>{name}</span></a></li>' for i, (ident, name, body) in enumerate(chapters, 1))
    contents = "".join(f'<section class="chapter" id="{ident}"><div class="chapter-number">{chapter_label} {i:02}</div><h2>{name}</h2>{body}<div class="chapter-bottom"><a href="#contents">{toc_label} ↑</a>' + (f'<a href="#{chapters[i][0]}">{chapters[i][1]} →</a>' if i < len(chapters) else '<a href="#cover">↑</a>') + '</div></section>' for i, (ident, name, body) in enumerate(chapters, 1))
    words = len(re.sub(r"<[^>]+>", " ", " ".join(body for _, _, body in chapters)).split())
    mins = max(1, round(words / 180))
    reading = f"{len(chapters)} глав · около {mins} минут с примерами · 14 ноября 2026" if ru else f"{len(chapters)} chapters · about {mins} minutes including examples · 14 November 2026"
    footer = "Самодостаточная HTML-книга. Для чтения не нужны сервер, интернет или JavaScript. Дополнительные исходники лежат рядом в examples/TutorialNode/." if ru else "A self-contained HTML book. Reading requires no server, internet connection or JavaScript. Companion sources are next to the book in examples/TutorialNode/."
    document = f'''<!doctype html>
<html lang="{language}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><meta name="description" content="{esc(lead)}"><title>{title}</title><style>{CSS}</style></head><body>
<a class="skip" href="#main">{'К книге' if ru else 'Skip to book'}</a>
<header class="topbar"><div class="topbar-inner"><a class="brand" href="index.html">Raft .NET 8 <span> / {'Руководство' if ru else 'Field guide'}</span></a><nav class="top-actions" aria-label="{'Инструменты книги' if ru else 'Book tools'}"><a href="book-{other}.html" data-other-language data-base="book-{other}.html">{other_label}</a><button type="button" data-theme-toggle>{'Тема' if ru else 'Theme'}</button><button type="button" data-print>{'Печать / PDF' if ru else 'Print / PDF'}</button></nav></div><div class="progress"><div data-progress></div></div></header>
<div class="layout"><nav class="sidebar" id="contents" aria-label="{toc_label}"><details class="toc-fold" open><summary>{toc_label} · {len(chapters)}</summary><label>{search_label}<input type="search" data-toc-search placeholder="{'Например, ReadIndex' if ru else 'For example, ReadIndex'}" autocomplete="off"></label><div class="toc-caption">{toc_label}</div><ol>{toc}</ol></details></nav>
<main class="book" id="main"><header class="cover" id="cover"><div class="eyebrow">C# · .NET 8 · DBreeze</div><h1>{title}</h1><p class="lead">{subtitle}. {lead}</p><div class="tags"><span>net8.0</span><span>DBreeze 1.142.2026.1016-beta</span><span>{'Диск и RAM' if ru else 'Disk and RAM'}</span><span>{'Рабочие примеры' if ru else 'Runnable examples'}</span></div><p class="reading">{reading}</p></header>{contents}<footer class="footer">{footer}</footer></main></div><script>{JS}</script></body></html>'''
    (HERE / f"book-{language}.html").write_text(document, encoding="utf-8", newline="\n")
    return {"language": language, "chapters": len(chapters), "words_with_code": words, "bytes": len(document.encode("utf-8"))}


def main():
    from content_en import chapters as en
    from content_ru import chapters as ru
    editions = {"en": en(), "ru": ru()}
    if [item[0] for item in editions["en"]] != [item[0] for item in editions["ru"]]:
        raise ValueError("The language editions must have matching chapter anchors")
    stats = [build_book(language, chapters) for language, chapters in editions.items()]
    index = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Raft .NET 8 — human tutorials / книги для людей</title><style>{CSS}</style></head><body><main class="index"><div class="cover"><div class="eyebrow">C# · net8.0 · DBreeze</div><h1>Raft .NET 8</h1><p class="lead">Practical books for application developers.<br><span lang="ru">Практические книги для разработчиков приложений.</span></p><div class="editions"><a class="edition" href="book-en.html" lang="en"><strong>Read in English →</strong><p>Start a three-node cluster. Use C# APIs, disk and RAM storage, snapshots, membership and safe recovery.</p></a><a class="edition" href="book-ru.html" lang="ru"><strong>Читать на русском →</strong><p>Запуск трёх узлов, API на C#, диск и RAM, снимки, изменение состава и безопасное восстановление.</p></a></div><p class="reading">Offline HTML · No installation to read · Print to PDF<br><span lang="ru">Работает без интернета · Можно распечатать в PDF</span></p></div></main></body></html>'''
    (HERE / "index.html").write_text(index, encoding="utf-8", newline="\n")
    sources = sorted(path for path in EXAMPLES.iterdir()
                     if path.suffix in (".cs", ".csproj") or path.name == "packages.lock.json")
    manifest = {"format": 1, "date": "2026-11-14", "editions": stats, "source_sha256": {str(path.relative_to(REPO_ROOT)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}}
    (HERE / "book-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
