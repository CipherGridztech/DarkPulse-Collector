# nlp_processor.py
# PURE REDIS (no JSON). Reads raw from Redis, writes processed to Redis.
# Also exposes process_record(rec) for in-process use by crawler.

import re
import time
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup

from transformers import pipeline
from sentence_transformers import SentenceTransformer

# import your redis controller (same one the crawler uses)
from crawler.common.crawler_instance.crawler_services.redis_manager.redis_controller import redis_controller

# --------- Config ----------
SUMMARIZER_MODEL = "facebook/bart-large-cnn"
ZERO_SHOT_MODEL = "facebook/bart-large-mnli"
NER_MODEL = "dslim/bert-base-NER"
EMBED_MODEL = "all-MiniLM-L6-v2"

CANDIDATE_LABELS = [
    "ransomware", "vulnerability", "data breach", "phishing",
    "malware", "scam", "policy", "research", "exposure", "other"
]
ZERO_SHOT_THRESHOLD = 0.30

RAW_INDEX_KEY = "THN:raw_index"
PROCESSED_INDEX_KEY = "THN:processed_index"

# --------- Utilities (text cleaning) ----------
def clean_html_text(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for bad in soup(["script", "style", "iframe", "aside", "noscript", "footer", "header", "form", "nav"]):
        bad.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r'[ \t\u00A0]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def clean_plain_text(s: str) -> str:
    s = (s or "")
    s = s.replace("\r", "\n")
    s = re.sub(r'[ \t\u00A0]+', ' ', s)
    s = re.sub(r'\n{3,}', '\n\n', s)
    return s.strip()

def clean_list_of_links(lst: List[str]) -> List[str]:
    out, seen = [], set()
    for u in lst or []:
        u = (u or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

def fix_bert_tokens(text: str) -> str:
    if not text:
        return ""
    t = text.replace(" ##", "").replace("##", "")
    t = re.sub(r"\s+", " ", t)
    return t.strip()

# --------- Date parsing (robust, preserves raw) ----------
MONTHS = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12
}

def _mm(mon: str) -> Optional[str]:
    mon = (mon or "").lower()
    if mon in MONTHS:
        return f"{MONTHS[mon]:02d}"
    return None

def _to_iso_date_safe(s: str) -> str:
    """
    Convert common news date strings to YYYY-MM-DD.
    If we can't be 100% sure, return "" and keep raw alongside.
    This avoids the "Oct 28 → 2025-10-02" kind of bugs.
    """
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", s, flags=re.IGNORECASE)

    # 1) Embedded ISO anywhere
    m = re.search(r"(20\d{2})-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|3[01])", s)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    # 2) "Mon 28, 2025" or "October 28, 2025"
    m = re.search(r"\b(" + "|".join(MONTHS.keys()) + r")\.?\s+(\d{1,2}),\s*(20\d{2})\b", s, flags=re.IGNORECASE)
    if m:
        mon, d, y = m.group(1), m.group(2), m.group(3)
        mm = _mm(mon)
        if mm:
            return f"{int(y):04d}-{mm}-{int(d):02d}"

    # 3) "28 Mon 2025" or "28 October 2025"
    m = re.search(r"\b(\d{1,2})\s+(" + "|".join(MONTHS.keys()) + r")\.?,?\s+(20\d{2})\b", s, flags=re.IGNORECASE)
    if m:
        d, mon, y = m.group(1), m.group(2), m.group(3)
        mm = _mm(mon)
        if mm:
            return f"{int(y):04d}-{mm}-{int(d):02d}"

    # 4) "Mon 28 2025" (no comma)
    m = re.search(r"\b(" + "|".join(MONTHS.keys()) + r")\.?\s+(\d{1,2})\s+(20\d{2})\b", s, flags=re.IGNORECASE)
    if m:
        mon, d, y = m.group(1), m.group(2), m.group(3)
        mm = _mm(mon)
        if mm:
            return f"{int(y):04d}-{mm}-{int(d):02d}"

    # 5) Slash style: 10/28/2025 or 28/10/2025 (ambiguous). We'll NOT guess — return "".
    # If you need handling, add a setting for locale.

    return ""  # be conservative

def _auto_summary_lengths(token_count: int) -> (int, int):
    if token_count < 40:
        return 56, 16
    if token_count < 120:
        return 96, 24
    if token_count < 240:
        return 140, 40
    return 180, 60

# --------- Load models ----------
print("Loading models (first run may be slow)...")
summarizer = pipeline("summarization", model=SUMMARIZER_MODEL)
classifier = pipeline("zero-shot-classification", model=ZERO_SHOT_MODEL)
ner = pipeline("ner", model=NER_MODEL, grouped_entities=True)
embedder = SentenceTransformer(EMBED_MODEL)
print("Models loaded.")

# --------- Core processing API (used by crawler inline) ----------
def process_record(rec: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Input: a Python dict constructed by crawler (NOT JSON).
    Returns: processed dict (caller may persist to Redis).
    """
    # ---- Required fields (cleaned) ----
    title = clean_plain_text(rec.get("title", ""))
    author = clean_plain_text(rec.get("author", ""))
    # preserve raw date EXACTLY as given (to avoid corruption), and compute ISO separately
    date_raw = clean_plain_text(rec.get("date", "")) or clean_plain_text(rec.get("published", ""))
    date_iso = _to_iso_date_safe(date_raw) if date_raw else ""

    description = clean_plain_text(rec.get("description", ""))
    location = clean_plain_text(rec.get("location", ""))
    content_raw = rec.get("content") or ""
    content = clean_plain_text(content_raw)
    links = clean_list_of_links(rec.get("links") or [])
    # pass network as-is if dict, else make a dict
    network = rec.get("network") if isinstance(rec.get("network"), dict) else {"type": str(rec.get("network") or "")}

    # If nothing to process, skip
    text_for_nlp = content or description or title
    if not text_for_nlp:
        return None

    # ---- Summarization (bounded) ----
    try:
        words = text_for_nlp.split()
        snippet_words = words[:900]
        approx_tokens = int(len(snippet_words) * 1.3)
        max_len, min_len = _auto_summary_lengths(approx_tokens)
        snippet = " ".join(snippet_words)
        summary_out = summarizer(snippet, max_length=max_len, min_length=min_len, truncation=True)
        summary = summary_out[0]["summary_text"].strip()
    except Exception:
        summary = (text_for_nlp[:250] + ("..." if len(text_for_nlp) > 250 else ""))

    # ---- NER (cleaned) ----
    try:
        ner_input = text_for_nlp[:1500]
        ner_out = ner(ner_input)
        entities = []
        for e in ner_out:
            word = fix_bert_tokens(e.get("word") or e.get("entity") or "")
            if not word:
                continue
            entities.append({
                "text": word,
                "label": e.get("entity_group") or e.get("entity"),
                "score": float(e.get("score", 0.0))
            })
    except Exception:
        entities = []

    # ---- Zero-shot classification ----
    try:
        cls = classifier(text_for_nlp[:1200], candidate_labels=CANDIDATE_LABELS, multi_label=True)
        labels = []
        for lab, score in zip(cls["labels"], cls["scores"]):
            if score >= ZERO_SHOT_THRESHOLD:
                labels.append({"label": lab, "score": float(score)})
    except Exception:
        labels = []

    # ---- Embedding ----
    try:
        emb = embedder.encode(text_for_nlp, show_progress_bar=False)
        embedding = emb.tolist() if hasattr(emb, "tolist") else list(emb)
    except Exception:
        embedding = []

    out = {
        # source/meta (mirrors crawler dict; nothing JSON)
        "url": rec.get("url"),
        "seed_url": rec.get("seed_url"),
        "rendered": bool(rec.get("rendered", True)),
        "scraped_at": rec.get("scraped_at"),

        # clean required fields
        "title": title,
        "author": author,

        # IMPORTANT: keep both
        "date_raw": date_raw,     # exact original
        "date": date_iso,         # normalized ISO if confidently parsed, else ""

        "description": description,
        "location": location,
        "links": links,
        "content": content,
        "network": network,

        # nlp
        "summary": summary,
        "entities": entities,
        "categories": labels,
        "embedding": embedding,

        # small raw slice for QA
        "raw_text_snippet": text_for_nlp[:4000]
    }
    return out

# --------- Redis I/O helpers (NO JSON) ----------
class _RedisIO:
    def __init__(self):
        self.r = redis_controller()

    def get(self, key: str, default: str = "") -> str:
        try:
            val = self.r.invoke_trigger(1, [key, default, None])
            return default if val is None else str(val)
        except Exception:
            return default

    def set(self, key: str, value: object, expiry: Optional[int] = None):
        self.r.invoke_trigger(2, [key, "" if value is None else str(value), expiry])

    def append_index(self, index_key: str, item_id: str):
        cur = self.get(index_key, "")
        parts = [p for p in cur.split("|") if p] if cur else []
        if item_id not in parts:
            parts.append(item_id)
            self.set(index_key, "|".join(parts), expiry=None)

    # ---- read raw record (constructed by crawler) ----
    def read_raw_rec(self, aid: str) -> Dict[str, Any]:
        base = f"THN:raw:{aid}"
        def g(suffix, default=""):
            return self.get(f"{base}:{suffix}", default)

        # lists
        def read_list(prefix: str) -> List[str]:
            n = int(self.get(f"{base}:{prefix}_count", "0") or 0)
            out = []
            for i in range(n):
                out.append(self.get(f"{base}:{prefix}:{i}", ""))
            return [x for x in out if x]

        return {
            "url": g("url"),
            "seed_url": g("seed_url"),
            "rendered": g("rendered", "1") == "1",
            "scraped_at": g("scraped_at"),

            "title": g("title"),
            "author": g("author"),
            "date": g("date"),               # raw stored by crawler (could be ISO already)
            "description": g("description"),
            "location": g("location"),
            "content": g("content"),
            "links": read_list("links"),
            "network": {"type": g("network:type")}
        }

    # ---- write processed (flatten dict/list -> per-field keys) ----
    def write_processed(self, aid: str, processed: Dict[str, Any]):
        base = f"THN:processed:{aid}"

        def write_obj(prefix: str, obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    write_obj(f"{prefix}:{k}", v)
            elif isinstance(obj, list):
                self.set(f"{prefix}:count", len(obj))
                for i, v in enumerate(obj):
                    write_obj(f"{prefix}:{i}", v)
            else:
                self.set(prefix, "" if obj is None else obj)

        write_obj(base, processed)
        self.append_index(PROCESSED_INDEX_KEY, aid)

# --------- Batch runner (optional CLI-less usage) ----------
def process_all_from_redis(limit: Optional[int] = None, sleep_ms: int = 0):
    """
    Iterate THN:raw_index, read raw records, run NLP, and save processed to Redis.
    No JSON anywhere.
    """
    rio = _RedisIO()
    idx = rio.get(RAW_INDEX_KEY, "")
    if not idx:
        print("[NLP] No raw index found.")
        return

    ids = [x for x in idx.split("|") if x]
    if limit is not None:
        ids = ids[:limit]

    print(f"[NLP] Records to process from Redis: {len(ids)}")

    for i, aid in enumerate(ids, 1):
        try:
            rec = rio.read_raw_rec(aid)
            processed = process_record(rec)
            if processed:
                rio.write_processed(aid, processed)

                # pretty log
                print("\n----------------------------------------")
                print(f"Date(raw): {processed.get('date_raw','')}")
                print(f"Date(iso): {processed.get('date','')}")
                print(f"title: {processed.get('title','')}")
                print(f"Author: {processed.get('author','')}")
                print(f"description: {processed.get('description','')[:300]}\n")
                print(f"seed url: {processed.get('seed_url','')}")
                print(f"dump url: {processed.get('url','')}")
                print("----------------------------------------\n")
        except Exception as ex:
            print(f"[NLP] ❌ Error processing AID={aid}: {ex}")

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    print("[NLP] ✅ Done.")

# --------- Optional: allow running directly (no JSON) ----------

