# ui_server.py
# Run:  uvicorn ui_server:app --host 127.0.0.1 --port 8000 --reload
# Requirements: fastapi, uvicorn, beautifulsoup4 (bs4 already in your repo)
# Reads ONLY from Redis using redis_controller.invoke_trigger and renders HTML (no JSON storage).

from typing import Dict, List, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from bs4 import BeautifulSoup
from datetime import datetime
import html
import re

# Your project's Redis adapter
from crawler.common.crawler_instance.crawler_services.redis_manager.redis_controller import redis_controller

app = FastAPI(title="News Collector UI", version="1.0")

# -------------------------
# Redis adapter (read-only)
# -------------------------
_rc = redis_controller()

def rget(key: str, default: str = "") -> str:
    try:
        val = _rc.invoke_trigger(1, [key, default, None])
        if val is None:
            return default
        return str(val)
    except Exception:
        return default

def rget_int(key: str, default: int = 0) -> int:
    try:
        val = rget(key, "")
        return int(val) if val not in ("", None) else default
    except Exception:
        return default

# -------------------------
# Source configuration
# -------------------------
SOURCES: Dict[str, Dict[str, str]] = {
    "thehackernews": {
        "label": "The Hacker News",
        "raw_index": "THN:raw_index",
        "proc_index": "THN:processed_index",
        "raw_base": "THN:raw",
        "proc_base": "THN:processed",
    },
    "hackread": {
        "label": "HackRead (Leaks & Affairs)",
        "raw_index": "HACKREAD:raw_index",
        "proc_index": "HACKREAD:processed_index",
        "raw_base": "HACKREAD:raw",
        "proc_base": "HACKREAD:processed",
    },
    "csocybercrime": {
        "label": "CSO Online (UK / Cybercrime)",
        "raw_index": "CSO:raw_index",
        "proc_index": "CSO:processed_index",
        "raw_base": "CSO:raw",
        "proc_base": "CSO:processed",
    }
}

# -------------------------
# Small HTML helpers
# -------------------------
CSS = """
<style>
:root { --bg:#0b1020; --panel:#121a33; --muted:#8ba0c6; --text:#e6ecff; --accent:#7aa2f7; --green:#76d39a; }
*{box-sizing:border-box}
body{margin:0; font-family:Inter,Segoe UI,Roboto,Arial,sans-serif; background:var(--bg); color:var(--text);}
a{color:var(--accent); text-decoration:none}
a:hover{text-decoration:underline}
.wrap{max-width:1200px; margin:0 auto; padding:24px}
.topbar{display:flex; gap:12px; align-items:center; margin-bottom:24px}
.badge{background:var(--panel); padding:6px 10px; border-radius:8px; color:var(--muted); font-size:13px;}
h1{margin:0 0 8px 0; font-size:24px}
h2{margin:24px 0 8px 0; font-size:18px}
.panel{background:var(--panel); border:1px solid #1e2b57; border-radius:12px; padding:16px; margin-bottom:16px}
.row{display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:center}
.grid{display:grid; grid-template-columns: 1fr 1fr; gap:12px}
small{color:var(--muted)}
.meta{display:flex; flex-wrap:wrap; gap:10px; margin-top:6px}
.meta span{font-size:12px; color:var(--muted); background:#0f1731; padding:4px 8px; border-radius:999px; border:1px solid #1c2a56}
.card-title{font-weight:600; font-size:16px}
.card-desc{color:#cbd6ff; margin-top:6px}
hr.sep{border:none; border-top:1px solid #20305f; margin:16px 0}
.controls{display:flex; gap:8px; align-items:center; margin:12px 0}
input[type=text]{background:#0f1731; color:var(--text); border:1px solid #1e2b57; padding:8px 10px; border-radius:8px; outline:none; width:280px}
select{background:#0f1731; color:var(--text); border:1px solid #1e2b57; padding:8px 10px; border-radius:8px; outline:none}
button{background:var(--accent); color:#081028; border:none; padding:8px 12px; border-radius:8px; cursor:pointer; font-weight:600}
button:hover{filter:brightness(1.05)}
.table{width:100%; border-collapse:collapse}
.table th,.table td{border-bottom:1px solid #1e2b57; padding:8px; text-align:left; vertical-align:top}
.kv{display:grid; grid-template-columns: 240px 1fr; gap:8px; }
.kv div{padding:8px 10px; background:#0f1731; border:1px solid #1e2b57; border-radius:8px}
.tag{display:inline-block; padding:4px 8px; border-radius:999px; background:#0f1731; border:1px solid #1e2b57; color:#a8c1ff; margin:2px 4px 0 0; font-size:12px}
.smallmuted{font-size:12px; color:var(--muted)}
.footer{margin-top:32px; color:var(--muted); font-size:12px}
</style>
"""

def page(title: str, body_html: str) -> str:
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
{CSS}
</head><body>
<div class="wrap">
  <div class="topbar">
    <a href="/"><strong>News Collector</strong></a>
    <span class="badge">Local UI</span>
  </div>
  {body_html}
  <div class="footer">Rendered from Redis (read-only) • No JSON persisted • {html.escape(datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'))}</div>
</div>
</body></html>"""

# -------------------------
# Data access (read-only)
# -------------------------
def split_index(val: str) -> List[str]:
    val = val or ""
    parts = [p for p in val.split("|") if p]
    return parts

def get_indices_counts() -> List[Tuple[str, int, int]]:
    rows = []
    for key, cfg in SOURCES.items():
        raw_ids = split_index(rget(cfg["raw_index"], ""))
        proc_ids = split_index(rget(cfg["proc_index"], ""))
        rows.append((key, len(raw_ids), len(proc_ids)))
    return rows

def load_raw_card(cfg: Dict[str, str], aid: str) -> Dict[str, object]:
    base = f'{cfg["raw_base"]}:{aid}'
    def gl(k, d=""): return rget(f"{base}:{k}", d)
    def gl_i(k, d=0): return rget_int(f"{base}:{k}", d)

    links = []
    n = gl_i("links_count", 0)
    for i in range(n):
        links.append(gl(f"links:{i}", ""))

    weblinks = []
    n = gl_i("weblink_count", 0)
    for i in range(n):
        weblinks.append(gl(f"weblink:{i}", ""))

    dumplinks = []
    n = gl_i("dumplink_count", 0)
    for i in range(n):
        dumplinks.append(gl(f"dumplink:{i}", ""))

    return {
        "aid": aid,
        "url": gl("url"),
        "title": gl("title"),
        "author": gl("author"),
        "date": gl("date"),
        "date_raw": gl("date_raw"),
        "description": gl("description"),
        "location": gl("location"),
        "content": gl("content"),
        "network_type": gl("network:type"),
        "seed_url": gl("seed_url"),
        "rendered": gl("rendered"),
        "scraped_at": gl("scraped_at"),
        "links": links,
        "weblink": weblinks,
        "dumplink": dumplinks,
        "content_html": gl("content_html"),
    }

def load_processed_card(cfg: Dict[str, str], aid: str) -> Dict[str, object]:
    base = f'{cfg["proc_base"]}:{aid}'
    def gl(k, d=""): return rget(f"{base}:{k}", d)
    def gl_i(k, d=0): return rget_int(f"{base}:{k}", d)

    # Try common NLP fields your pipeline writes
    entities = []
    ecount = gl_i("entities:count", 0)
    for i in range(ecount):
        ent = {
            "text": gl(f"entities:{i}:text"),
            "label": gl(f"entities:{i}:label"),
            "score": gl(f"entities:{i}:score"),
        }
        if ent["text"]:
            entities.append(ent)

    categories = []
    ccount = gl_i("categories:count", 0)
    for i in range(ccount):
        cat = {
            "label": gl(f"categories:{i}:label"),
            "score": gl(f"categories:{i}:score"),
        }
        if cat["label"]:
            categories.append(cat)

    links = []
    lcount = gl_i("links:count", 0)
    for i in range(lcount):
        links.append(gl(f"links:{i}", ""))

    return {
        "aid": aid,
        "url": gl("url"),
        "title": gl("title"),
        "author": gl("author"),
        "date": gl("date"),            # in your pipeline: ISO normalized
        "date_raw": gl("date_raw"),    # if present
        "published": gl("published"),
        "description": gl("description"),
        "summary": gl("summary"),
        "content": gl("content"),
        "seed_url": gl("seed_url"),
        "scraped_at": gl("scraped_at"),
        "entities": entities,
        "categories": categories,
        "links": links
    }

def fetch_list(source_key: str, view: str, page: int, per: int, q: str = "", sort: str = "time") -> Tuple[List[Dict[str, object]], int]:
    cfg = SOURCES[source_key]
    index_key = cfg["raw_index"] if view == "raw" else cfg["proc_index"]
    ids = split_index(rget(index_key, ""))

    # Prepare items: load just titles+dates for sorting/search
    items = []
    for aid in ids:
        if view == "raw":
            base = f'{cfg["raw_base"]}:{aid}'
            title = rget(f"{base}:title", "")
            date_raw = rget(f"{base}:date_raw", "")
            date_iso = rget(f"{base}:date", "")
            scraped = rget_int(f"{base}:scraped_at", 0)
        else:
            base = f'{cfg["proc_base"]}:{aid}'
            title = rget(f"{base}:title", "")
            date_raw = rget(f"{base}:date_raw", "")
            date_iso = rget(f"{base}:date", "")
            scraped = rget_int(f"{base}:scraped_at", 0)

        if q:
            if q.lower() not in (title or "").lower():
                continue

        items.append({
            "aid": aid,
            "title": title or "(No title)",
            "date": date_iso or date_raw or "",
            "scraped_at": scraped
        })

    # Sort
    if sort == "time":
        items.sort(key=lambda x: x.get("scraped_at", 0), reverse=True)
    else:
        items.sort(key=lambda x: (x.get("title") or "").lower())

    total = len(items)
    start = (page - 1) * per
    end = start + per
    page_items = items[start:end]

    # Hydrate a little more for cards (first two sentences)
    hydrated = []
    for row in page_items:
        aid = row["aid"]
        card = load_raw_card(cfg, aid) if view == "raw" else load_processed_card(cfg, aid)
        content = (card.get("summary") or card.get("description") or card.get("content") or "") if view == "processed" else (card.get("description") or card.get("content") or "")
        text = str(content or "")
        # take first ~200 chars
        snippet = text[:200] + ("..." if len(text) > 200 else "")
        hydrated.append({
            "aid": aid,
            "title": card.get("title") or "(No title)",
            "date": (card.get("published") or card.get("date") or card.get("date_raw") or ""),
            "snippet": snippet,
        })
    return hydrated, total

# -------------------------
# Routes
# -------------------------
@app.get("/", response_class=HTMLResponse)
def home():
    rows = get_indices_counts()
    cards_html = []
    for src_key, raw_count, proc_count in rows:
        label = SOURCES[src_key]["label"]
        cards_html.append(f"""
        <div class="panel">
          <div class="row">
            <div>
              <div class="card-title">{html.escape(label)}</div>
              <div class="meta">
                <span>raw: {raw_count}</span>
                <span>processed: {proc_count}</span>
              </div>
            </div>
            <div>
              <a href="/source/{src_key}?view=raw"><button>Open Raw</button></a>
              <a href="/source/{src_key}?view=processed"><button>Open Processed</button></a>
            </div>
          </div>
        </div>
        """)
    body = f"""
      <h1>Sources</h1>
      <div class="grid">
        {''.join(cards_html)}
      </div>
    """
    return page("News Collector UI", body)

@app.get("/source/{source_key}", response_class=HTMLResponse)
def list_source(source_key: str,
                view: str = Query("raw", pattern="^(raw|processed)$"),
                page_no: int = Query(1, ge=1),
                per: int = Query(12, ge=1, le=50),
                q: str = Query("", description="Search in titles"),
                sort: str = Query("time", pattern="^(time|title)$")):
    if source_key not in SOURCES:
        return RedirectResponse(url="/", status_code=302)

    cards, total = fetch_list(source_key, view, page_no, per, q, sort)
    total_pages = max(1, (total + per - 1) // per)
    base_url = f"/source/{source_key}?view={view}&per={per}&sort={sort}&q={html.escape(q)}"

    # controls
    controls = f"""
    <form class="controls" method="get" action="/source/{source_key}">
      <input type="hidden" name="view" value="{view}">
      <input type="text" name="q" value="{html.escape(q)}" placeholder="Search title...">
      <select name="per">
        <option value="12" {"selected" if per==12 else ""}>12</option>
        <option value="24" {"selected" if per==24 else ""}>24</option>
        <option value="36" {"selected" if per==36 else ""}>36</option>
      </select>
      <select name="sort">
        <option value="time" {"selected" if sort=="time" else ""}>Newest</option>
        <option value="title" {"selected" if sort=="title" else ""}>Title</option>
      </select>
      <select name="view">
        <option value="raw" {"selected" if view=="raw" else ""}>Raw</option>
        <option value="processed" {"selected" if view=="processed" else ""}>Processed</option>
      </select>
      <button type="submit">Apply</button>
    </form>
    """

    items_html = []
    for c in cards:
        items_html.append(f"""
        <div class="panel">
          <div class="card-title"><a href="/article/{source_key}/{c['aid']}?view={view}">{html.escape(c['title'] or '(No title)')}</a></div>
          <div class="meta"><span>{html.escape(str(c['date'] or ''))}</span></div>
          <div class="card-desc">{html.escape(c['snippet'] or '')}</div>
        </div>
        """)

    # pagination
    nav = []
    if page_no > 1:
        nav.append(f'<a href="{base_url}&page_no={page_no-1}"><button>Prev</button></a>')
    nav.append(f'<span class="badge">Page {page_no} / {total_pages}</span>')
    if page_no < total_pages:
        nav.append(f'<a href="{base_url}&page_no={page_no+1}"><button>Next</button></a>')

    body = f"""
      <h1>{html.escape(SOURCES[source_key]['label'])}</h1>
      <div class="meta">
        <span>Viewing: {html.escape(view)}</span>
        <a class="tag" href="/source/{source_key}?view={'processed' if view=='raw' else 'raw'}&q={html.escape(q)}">Switch to {'processed' if view=='raw' else 'raw'}</a>
      </div>
      {controls}
      {''.join(items_html) if items_html else '<div class="panel"><small>No items.</small></div>'}
      <div class="controls">{''.join(nav)}</div>
    """
    return page(f"{SOURCES[source_key]['label']} • {view}", body)

@app.get("/article/{source_key}/{aid}", response_class=HTMLResponse)
def show_article(source_key: str, aid: str, view: str = Query("raw", pattern="^(raw|processed)$")):
    if source_key not in SOURCES:
        return RedirectResponse(url="/", status_code=302)
    cfg = SOURCES[source_key]
    card = load_raw_card(cfg, aid) if view == "raw" else load_processed_card(cfg, aid)

    # build fields table
    def row(label: str, value: str) -> str:
        val = html.escape(str(value or ""))
        return f"<tr><th>{html.escape(label)}</th><td>{val}</td></tr>"

    meta_rows = []
    meta_rows.append(row("Title", card.get("title") or "(No title)"))
    meta_rows.append(row("Author", card.get("author") or ""))
    meta_rows.append(row("Date (ISO/processed)", card.get("date") or ""))
    meta_rows.append(row("Date (raw)", card.get("date_raw") or ""))
    meta_rows.append(row("Published", card.get("published") or ""))
    meta_rows.append(row("URL", card.get("url") or ""))
    meta_rows.append(row("Seed", card.get("seed_url") or ""))
    meta_rows.append(row("Scraped At (epoch)", card.get("scraped_at") or ""))

    extra_html = ""

    # Entities / Categories for processed view
    if view == "processed":
        ents = card.get("entities") or []
        if ents:
            pills = "".join([f'<span class="tag">{html.escape(e.get("label",""))}: {html.escape(e.get("text",""))}</span>' for e in ents[:40]])
            extra_html += f"<h2>Entities</h2><div>{pills}</div>"
        cats = card.get("categories") or []
        if cats:
            pills = "".join([f'<span class="tag">{html.escape(c.get("label",""))} ({html.escape(str(c.get("score","")))})</span>' for c in cats])
            extra_html += f"<h2>Categories</h2><div>{pills}</div>"

        if card.get("summary"):
            extra_html += f"<h2>Summary</h2><div class='panel'>{html.escape(card['summary'])}</div>"

    # Content
    content_block = ""
    if card.get("content_html"):
        # Show sanitized preview of HTML if the crawler saved it
        content_block = f"<h2>Content (HTML)</h2><div class='panel smallmuted'>Saved HTML present (truncated preview below)</div><div class='panel'><pre style='white-space:pre-wrap'>{html.escape(card['content_html'][:4000])}</pre></div>"
    elif card.get("content"):
        content_block = f"<h2>Content (text)</h2><div class='panel'><pre style='white-space:pre-wrap'>{html.escape(str(card['content'])[:10000])}</pre></div>"

    # Outbound links
    def list_of_links(label: str, lst: List[str]) -> str:
        if not lst: return ""
        items = "".join([f"<li><a href='{html.escape(u)}' target='_blank' rel='noreferrer noopener'>{html.escape(u)}</a></li>" for u in lst])
        return f"<h2>{html.escape(label)}</h2><div class='panel'><ul>{items}</ul></div>"

    links_html = ""
    if view == "raw":
        links_html += list_of_links("Links", card.get("links") or [])
        links_html += list_of_links("Web Links", card.get("weblink") or [])
        links_html += list_of_links("Dump Links", card.get("dumplink") or [])
    else:
        links_html += list_of_links("Links", card.get("links") or [])

    body = f"""
      <h1>{html.escape(SOURCES[source_key]['label'])}</h1>
      <div class="meta">
        <span>View: {html.escape(view)}</span>
        <a class="tag" href="/article/{source_key}/{aid}?view={'processed' if view=='raw' else 'raw'}">Switch to {'processed' if view=='raw' else 'raw'}</a>
        <a class="tag" href="/source/{source_key}?view={view}">Back to list</a>
      </div>

      <div class="panel">
        <table class="table">
          {''.join(meta_rows)}
        </table>
      </div>

      {extra_html}
      {content_block}
      {links_html}
    """
    return page(f"{SOURCES[source_key]['label']} • Article", body)
