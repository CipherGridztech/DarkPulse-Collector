import hashlib, re, requests
from datetime import datetime, timezone
from typing import Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

class CrawlerBase:
    def __init__(self):
        self._redis = None
        self._proxy = {}
        self._chromium_exe = None

    # --- redis helpers (expects subclass to set self._redis) ---
    def _redis_get(self, key: str, default: str = "") -> str:
        try:
            val = self._redis.invoke_trigger(1, [key, default, None])
            return "" if val is None else str(val)
        except Exception:
            return default

    def _redis_set(self, key: str, value: object, expiry: Optional[int] = None):
        val = "" if value is None else str(value)
        try:
            self._redis.invoke_trigger(2, [key, val, expiry])
        except Exception:
            pass

    def _append_index(self, index_key: str, item_id: str):
        cur = self._redis_get(index_key, "")
        parts = [p for p in cur.split("|") if p] if cur else []
        if item_id not in parts:
            parts.append(item_id)
            self._redis_set(index_key, "|".join(parts), expiry=None)

    # --- small utilities ---
    @staticmethod
    def _sha1(text: str) -> str:
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _date_to_string(d) -> str:
        if d is None:
            return ""
        if isinstance(d, datetime):
            return d.strftime("%Y-%m-%d")
        return str(d)

    # --- generic processed writer ---
    def _store_processed_generic(self, base: str, processed: dict):
        def write_obj(prefix: str, obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    write_obj(f"{prefix}:{k}", v)
            elif isinstance(obj, list):
                self._redis_set(f"{prefix}:count", len(obj))
                for i, v in enumerate(obj):
                    write_obj(f"{prefix}:{i}", v)
            else:
                self._redis_set(prefix, "" if obj is None else obj)
        write_obj(base, processed)

    # --- generic raw card writer (per-field, mirrors original behaviour) ---
    def store_raw_card_generic(self, prefix: str, card) -> str:
        aid = self._sha1(card.m_url or (card.m_title or "") + str(datetime.now(timezone.utc).timestamp()))
        base = f"{prefix}:{aid}"
        for k, v in {
            "url": card.m_url,
            "title": card.m_title,
            "author": card.m_author,
            "date": self._date_to_string(card.m_leak_date),
            "description": card.m_description,
            "location": card.m_location or "",
            "content": card.m_content or "",
            "network:type": card.m_network,
            "seed_url": getattr(card, 'm_base_url', "" ) or self._get_seed_url(),
            "rendered": "1",
            "scraped_at": int(datetime.now(timezone.utc).timestamp())
        }.items():
            self._redis_set(f"{base}:{k}", v)
        # optional extras
        try:
            date_raw = (card.m_extra or {}).get("date_raw", "")
        except Exception:
            date_raw = ""
        try:
            content_html = (card.m_extra or {}).get("content_html", "")
        except Exception:
            content_html = ""
        self._redis_set(f"{base}:date_raw", date_raw)
        self._redis_set(f"{base}:content_html", content_html)
        # lists
        for name, lst in (("links", getattr(card, 'm_links', None)), ("weblink", getattr(card, 'm_weblink', None)), ("dumplink", getattr(card, 'm_dumplink', None))):
            lst = lst or []
            self._redis_set(f"{base}:{name}_count", len(lst))
            for i, link in enumerate(lst):
                self._redis_set(f"{base}:{name}:{i}", link)
        # append to raw index should be done by caller if desired
        return aid

    def _get_seed_url(self) -> str:
        # fallback; subclasses define property seed_url
        return getattr(self, 'seed_url', '')

    # --- generic NLP enrichment + processed storage ---
    def nlp_enrich_and_store_generic(self, cards: list, processed_index_key: str):
        # expects self._nlp to be set to the module providing process_record
        try:
            print(f"NLP enrichment on {len(cards)} records (generic)")
            for card in cards:
                try:
                    date_raw = (card.m_extra or {}).get("date_raw", "") if card.m_extra else ""
                except Exception:
                    date_raw = ""
                date_iso = self._date_to_string(card.m_leak_date)
                rec = {
                    "url": card.m_url, "title": card.m_title, "author": card.m_author,
                    "date": date_raw, "published": date_iso, "description": card.m_description,
                    "location": card.m_location, "links": card.m_links or [], "content": card.m_content,
                    "network": {"type": card.m_network}, "seed_url": getattr(card, 'm_base_url', self._get_seed_url()),
                    "rendered": True, "scraped_at": int(datetime.now(timezone.utc).timestamp())
                }
                try:
                    processed = self._nlp.process_record(rec)
                except Exception as e:
                    print("NLP processing failed for record:", e)
                    processed = None

                aid = self._sha1(card.m_url or card.m_title)
                if processed:
                    base = f"{processed_index_key.split(':')[0]}:processed:{aid}" if ':' in processed_index_key else f"processed:{aid}"
                    # write processed fields
                    self._store_processed_generic(base, processed)
                    self._append_index(processed_index_key, aid)
                    # echo some info (keeps original verbose output)
                    date_raw_out = str(processed.get("date_raw") or rec.get("date") or "")
                    date_iso_out = str(processed.get("date") or rec.get("published") or "")
                    title = str(processed.get("title") or rec.get("title") or "")
                    author = str(processed.get("author") or rec.get("author") or "")
                    description = str(processed.get("description") or (processed.get("summary") or ""))[:3000]
                    url = str(processed.get("url") or rec.get("url") or "")
                    seed = rec.get("seed_url") or self._get_seed_url()
                    print("\n----------------------------------------")
                    print(f"Date(raw): {date_raw_out}")
                    print(f"Date(iso): {date_iso_out}")
                    print(f"title: {title}")
                    print(f"Author: {author}")
                    print(f"description: {description}\n")
                    print(f"seed url: {seed}")
                    print(f"dump url: {url}")
                    print("----------------------------------------\n")
            print("NLP enrichment stored to Redis ✅ (generic)")
        except Exception as ex:
            print("⚠ NLP enrichment error (generic):", ex)

    # --- network helpers ---
    def _make_requests_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": "Collector/1.0 (+contact)"})
        server = (self._proxy or {}).get("server")
        if server:
            s.proxies.update({"http": server, "https": server})
        return s

    def _launch_browser(self, p, use_proxy: bool):
        launch_kwargs = {"headless": False}
        if self._chromium_exe:
            launch_kwargs["executable_path"] = self._chromium_exe
        if use_proxy and (self._proxy or {}).get("server"):
            launch_kwargs["proxy"] = {"server": self._proxy["server"]}
        browser = p.chromium.launch(**launch_kwargs)
        return browser, browser.new_context()

    # --- helpers to centralize Playwright pagination & visiting ---
    def collect_links_playwright(self, seed_url: str, max_pages: int, extract_links_fn, find_next_page_url_fn=None, use_proxy: bool = True, max_articles: Optional[int] = None):
        """Open seed with Playwright and collect article links using site-provided extractors.
        extract_links_fn(soup) should return an iterable of URLs. find_next_page_url_fn(soup) -> next_url or None.
        Returns a sorted visit_list (possibly truncated by max_articles).
        """
        all_links = set()
        with sync_playwright() as p:
            try:
                browser, context = self._launch_browser(p, use_proxy=use_proxy)
                page = context.new_page()
                page.goto(seed_url, timeout=60000, wait_until="load")
            except Exception:
                # retry without proxy
                try:
                    context.close(); browser.close()
                except Exception:
                    pass
                browser, context = self._launch_browser(p, use_proxy=False)
                page = context.new_page()
                page.goto(seed_url, timeout=60000, wait_until="load")

            current_url = seed_url
            for page_no in range(1, max_pages + 1):
                soup = BeautifulSoup(page.content(), "html.parser")
                try:
                    links = set(extract_links_fn(soup))
                except Exception:
                    links = set()
                all_links.update(links)
                if max_articles and len(all_links) >= max_articles:
                    break
                next_url = None
                if find_next_page_url_fn:
                    try:
                        next_url = find_next_page_url_fn(soup)
                    except Exception:
                        next_url = None
                if not next_url or next_url == current_url:
                    break
                current_url = next_url
                page.goto(current_url, timeout=60000, wait_until="load")

            try:
                page.close()
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

        visit_list = sorted(all_links)
        if max_articles:
            visit_list = visit_list[:max_articles]
        return visit_list

    def visit_links_playwright(self, visit_list: list, parse_article_fn, use_proxy: bool = True):
        """Open browser and visit each link in visit_list. parse_article_fn(page, link, idx, total) -> bool on success.
        Returns number of successfully processed articles.
        """
        collected = 0
        with sync_playwright() as p:
            try:
                browser, context = self._launch_browser(p, use_proxy=use_proxy)
                page = context.new_page()
            except Exception:
                try:
                    context.close(); browser.close()
                except Exception:
                    pass
                browser, context = self._launch_browser(p, use_proxy=False)
                page = context.new_page()

            total = len(visit_list)
            for idx, link in enumerate(visit_list, 1):
                try:
                    page.goto(link, timeout=60000, wait_until="load")
                    ok = parse_article_fn(page, link, idx, total)
                    if ok:
                        collected += 1
                except Exception:
                    # per-article errors should be handled by parse_article_fn where possible
                    continue

            try:
                page.close()
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

        return collected

    # --- parsing helpers ---
    @staticmethod
    def _is_date_like(text: str) -> bool:
        if not text:
            return False
        t = text.strip()
        patterns = [
            r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}$",
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$"
        ]
        return any(re.match(p, t, re.IGNORECASE) for p in patterns)

    @staticmethod
    def _parse_date(s: str):
        if not s:
            return None
        s = s.strip()
        s = re.sub(r"\bSept\b", "Sep", s, flags=re.IGNORECASE)
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%SZ",
                    "%B %d, %Y", "%b %d, %Y", "%d %b %Y", "%d %B %Y"):
            try:
                # return date object for compatibility with existing callers
                return datetime.strptime(s, fmt).date()
            except Exception:
                continue
        m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}", s, re.IGNORECASE)
        if m:
            try:
                return datetime.strptime(m.group(0), "%b %d, %Y").date()
            except Exception:
                try:
                    return datetime.strptime(m.group(0), "%B %d, %Y").date()
                except Exception:
                    return None
        return None
