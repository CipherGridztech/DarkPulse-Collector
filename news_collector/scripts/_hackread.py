import re
import hashlib
import requests
from abc import ABC
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Set
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from crawler.common.crawler_instance.local_interface_model.leak.leak_extractor_interface import leak_extractor_interface
from crawler.common.crawler_instance.local_shared_model.data_model import entity_model
from crawler.common.crawler_instance.local_shared_model.data_model import news_model
from crawler.common.crawler_instance.local_shared_model import RuleModel, FetchProxy, FetchConfig, ThreatType
from crawler.common.crawler_instance.crawler_services.redis_manager.redis_controller import redis_controller
from crawler.common.crawler_instance.crawler_services.shared.helper_method import helper_method
from crawler.common.dev_signature import developer_signature
from news_collector.scripts import nlp_processor as nlp


class _hackread(leak_extractor_interface, ABC):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(_hackread, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, developer_name: str = "Anonymous", developer_note: str = ""):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        self._card_data: List[news_model] = []
        self._entity_data: List[entity_model] = []
        self._redis = redis_controller()
        self._is_crawled = False
        self._proxy = {}
        self._developer_name = developer_name
        self._developer_note = developer_note
        self.callback = None

        # pagination limits
        self._max_pages: int = 5                 # category index pages to walk (1 = only seed)
        self._max_articles: Optional[int] = None # None = no cap

        # Master index keys (pipe-delimited strings, NOT JSON)
        self._raw_index_key = "HACKREAD:raw_index"
        self._processed_index_key = "HACKREAD:processed_index"

        # optional: path to local Chromium
        #self._chromium_exe = r"C:\Users\DELL\darkpulse\chromium-win64\chrome-win\chrome.exe"

        print("[HACKREAD] Initialized ✅ (pure Redis, no JSON)")

    # ------- lifecycle/config hooks --------
    def init_callback(self, callback=None):
        self.callback = callback
        print("[HACKREAD] Callback set")

    def set_proxy(self, proxy: dict):
        self._proxy = proxy or {}
        print(f"[HACKREAD] Proxy configured: {self._proxy}")

    def set_limits(self, max_pages: Optional[int] = None, max_articles: Optional[int] = None):
        if max_pages is not None and max_pages >= 1:
            self._max_pages = int(max_pages)
        if max_articles is not None and max_articles >= 1:
            self._max_articles = int(max_articles)
        print(f"[HACKREAD] Limits → pages={self._max_pages}, articles={self._max_articles or '∞'}")

    def reset_cache(self):
        print("[HACKREAD] Resetting crawl timestamp …")
        self._redis_set("HACKREAD:last_crawl", "", 60)

    # ------- required interface props -------
    @property
    def is_crawled(self) -> bool:
        return self._is_crawled

    @property
    def seed_url(self) -> str:
        return "https://hackread.com/category/hacking-news/leaks-affairs/"

    @property
    def base_url(self) -> str:
        return "https://hackread.com/"

    @property
    def rule_config(self) -> RuleModel:
        return RuleModel(
            m_threat_type=ThreatType.NEWS,
            m_fetch_proxy=FetchProxy.NONE,
            m_fetch_config=FetchConfig.REQUESTS,
            m_resoource_block=False
        )

    @property
    def card_data(self) -> List[news_model]:
        return self._card_data

    @property
    def entity_data(self) -> List[entity_model]:
        return self._entity_data

    def developer_signature(self) -> str:
        return developer_signature(self._developer_name, self._developer_note)

    def contact_page(self) -> str:
        return "https://hackread.com/contact-us/"

    # ------- minimal Redis helpers (NO JSON) ------------
    def _redis_get(self, key: str, default: str = "") -> str:
        try:
            val = self._redis.invoke_trigger(1, [key, default, None])
            if val is None:
                return default
            return str(val)
        except Exception:
            return default

    def _redis_set(self, key: str, value: object, expiry: Optional[int] = None):
        val = "" if value is None else str(value)
        self._redis.invoke_trigger(2, [key, val, expiry])

    def _append_index(self, index_key: str, item_id: str):
        cur = self._redis_get(index_key, "")
        parts = [p for p in cur.split("|") if p] if cur else []
        if item_id not in parts:
            parts.append(item_id)
            self._redis_set(index_key, "|".join(parts), expiry=None)

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

    # ------- store raw article (per-field keys) ---------
    def _store_raw_card(self, card: news_model) -> str:
        aid = self._sha1(card.m_url or (card.m_title or "") + str(datetime.now(timezone.utc).timestamp()))
        base = f"HACKREAD:raw:{aid}"

        # scalar fields
        self._redis_set(f"{base}:url", card.m_url)
        self._redis_set(f"{base}:title", card.m_title)
        self._redis_set(f"{base}:author", card.m_author)
        self._redis_set(f"{base}:date", self._date_to_string(card.m_leak_date))  # ISO/stringified
        # store human raw date & html if present in m_extra
        date_raw = ""
        content_html = ""
        try:
            date_raw = (card.m_extra or {}).get("date_raw", "")  # type: ignore
            content_html = (card.m_extra or {}).get("content_html", "")  # type: ignore
        except Exception:
            date_raw = ""
            content_html = ""
        self._redis_set(f"{base}:date_raw", date_raw)
        self._redis_set(f"{base}:content_html", content_html)

        self._redis_set(f"{base}:description", card.m_description)
        self._redis_set(f"{base}:location", card.m_location or "")
        self._redis_set(f"{base}:content", card.m_content or "")
        self._redis_set(f"{base}:network:type", card.m_network)
        self._redis_set(f"{base}:seed_url", self.seed_url)
        self._redis_set(f"{base}:rendered", "1")
        self._redis_set(f"{base}:scraped_at", int(datetime.now(timezone.utc).timestamp()))

        # lists (no JSON)
        links = card.m_links or []
        self._redis_set(f"{base}:links_count", len(links))
        for i, link in enumerate(links):
            self._redis_set(f"{base}:links:{i}", link)

        weblinks = card.m_weblink or []
        self._redis_set(f"{base}:weblink_count", len(weblinks))
        for i, link in enumerate(weblinks):
            self._redis_set(f"{base}:weblink:{i}", link)

        dumplinks = card.m_dumplink or []
        self._redis_set(f"{base}:dumplink_count", len(dumplinks))
        for i, link in enumerate(dumplinks):
            self._redis_set(f"{base}:dumplink:{i}", link)

        self._append_index(self._raw_index_key, aid)
        return aid

    # ------- store processed NLP output (generic flattener, no JSON) ----
    def _store_processed(self, aid: str, processed: dict):
        base = f"HACKREAD:processed:{aid}"

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
        self._append_index(self._processed_index_key, aid)

    # ------- HTTP session (fallback path) ---
    def _make_requests_session(self) -> requests.Session:
        print("[HACKREAD] Creating requests session …")
        s = requests.Session()
        s.headers.update({"User-Agent": "HackReadCollector/1.0 (+contact)"})
        server = (self._proxy or {}).get("server")
        if server:
            s.proxies.update({"http": server, "https": server})
            print(f"[HACKREAD] requests will use proxy: {server}")
        return s

    # ------- Playwright helpers -------------
    def _launch_browser(self, p, use_proxy: bool) -> Tuple[object, object]:
        launch_kwargs = {"headless": False}
        if self._chromium_exe:
            launch_kwargs["executable_path"] = self._chromium_exe
        if use_proxy and (self._proxy or {}).get("server"):
            launch_kwargs["proxy"] = {"server": self._proxy["server"]}
            print(f"[HACKREAD] Launching Chromium WITH proxy: {self._proxy['server']}")
        else:
            print("[HACKREAD] Launching Chromium WITHOUT proxy")
        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context()
        return browser, context

    # ------- author/date extraction ----------
    @staticmethod
    def _is_date_like(text: str) -> bool:
        if not text:
            return False
        t = text.strip()
        if re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}$", t):
            return True
        if re.match(r"^\d{4}-\d{2}-\d{2}$", t):
            return True
        if re.match(r"^\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$", t):
            return True
        return False

    def _extract_author_date(self, soup: BeautifulSoup) -> Tuple[str, str]:
        # HackRead typical:
        #   date: div.cs-meta-date   e.g., "October 28, 2025"
        #   author: span.cs-meta-author a, or a[rel='author']
        date_raw = ""
        author = ""

        date_el = soup.select_one("div.cs-meta-date, span.cs-meta-date, time.entry-date, time[datetime]")
        if date_el:
            date_raw = (date_el.get_text(strip=True) or date_el.get("datetime") or "").strip()
            # normalize to "Mon DD, YYYY" pattern if longer
            m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}", date_raw, re.IGNORECASE)
            if m:
                date_raw = m.group(0).title()

        a_el = soup.select_one("span.cs-meta-author a, .cs-meta-author a, a[rel='author'], span.author a")
        if a_el:
            author = a_el.get_text(strip=True)

        return author, date_raw

    # ------- index page helpers (pagination) ----
    def _extract_article_links_from_index(self, soup: BeautifulSoup) -> Set[str]:
        links: Set[str] = set()
        for a in soup.select("h2.cs-entry__title a"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(self.base_url, href)
            # accept only article pages under site
            if full.startswith(self.base_url):
                links.add(full)
        return links

    def _page_url(self, page_no: int) -> str:
        # seed ends with '/', HackRead pagination: /page/2/
        if page_no <= 1:
            return self.seed_url
        return urljoin(self.seed_url, f"page/{page_no}/")

    # ------- core crawling ------------------
    def run(self) -> dict:
        print("[HACKREAD] run() → Playwright first, then requests fallback")
        try:
            return self.parse_leak_data()
        except Exception as ex:
            print(f"[HACKREAD] Playwright failed ({ex}). Falling back to requests.")
            return self._run_with_requests()

    def parse_leak_data(self) -> dict:
        collected = 0
        all_links: Set[str] = set()

        with sync_playwright() as p:
            # open first page
            try:
                browser, context = self._launch_browser(p, use_proxy=True)
                page = context.new_page()
                first_url = self._page_url(1)
                print(f"[HACKREAD] Opening seed (proxy): {first_url}")
                page.goto(first_url, timeout=60000, wait_until="load")
            except Exception as ex:
                print(f"[HACKREAD] Proxy navigation failed: {ex}. Retrying without proxy …")
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass
                browser, context = self._launch_browser(p, use_proxy=False)
                page = context.new_page()
                first_url = self._page_url(1)
                print(f"[HACKREAD] Opening seed (no proxy): {first_url}")
                page.goto(first_url, timeout=60000, wait_until="load")

            # iterate category pages deterministically
            for page_no in range(1, self._max_pages + 1):
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                page_links = self._extract_article_links_from_index(soup)
                all_links.update(page_links)
                print(f"[HACKREAD] Index page {page_no}: +{len(page_links)} links (unique {len(all_links)})")

                if self._max_articles and len(all_links) >= self._max_articles:
                    break
                next_url = self._page_url(page_no + 1)
                if page_no >= self._max_pages:
                    break
                print(f"[HACKREAD] → Next Page: {next_url}")
                page.goto(next_url, timeout=60000, wait_until="load")

            visit_list = sorted(all_links)
            if self._max_articles:
                visit_list = visit_list[: self._max_articles]

            print(f"[HACKREAD] Visiting {len(visit_list)} articles after pagination")
            for idx, link in enumerate(visit_list, 1):
                try:
                    print(f"[HACKREAD] Visiting [{idx}/{len(visit_list)}]: {link}")
                    page.goto(link, timeout=60000, wait_until="load")
                    s = BeautifulSoup(page.content(), "html.parser")

                    # title
                    title_el = s.select_one("h1.cs-entry__title.cs-entry__title-line") or s.select_one("h1.cs-entry__title") or s.select_one("h1.entry-title")
                    title = title_el.get_text(strip=True) if title_el else "(No title)"

                    # subtitle / standfirst
                    subtitle_el = s.select_one(".cs-entry__subtitle")
                    subtitle = subtitle_el.get_text(strip=True) if subtitle_el else ""

                    # content (HTML + text)
                    entry_el = s.select_one(".entry-content")
                    content_html = str(entry_el) if entry_el else ""
                    content_text = entry_el.get_text(" ", strip=True) if entry_el else ""

                    full_content_text = f"{subtitle}\n\n{content_text}".strip() if subtitle else content_text
                    important_text = subtitle if subtitle else " ".join(content_text.split()[:150])

                    # author + date
                    author, date_raw = self._extract_author_date(s)
                    parsed_date = self._parse_date(date_raw)

                    card = news_model(
                        m_screenshot="",
                        m_title=title,
                        m_weblink=[link],
                        m_dumplink=[link],
                        m_url=link,
                        m_base_url=self.base_url,
                        m_content=full_content_text,
                        m_network=helper_method.get_network_type(self.base_url),
                        m_important_content=important_text,
                        m_content_type=["news"],
                        m_leak_date=parsed_date,
                        m_author=author,
                        m_description=important_text,
                        m_location="",
                        m_links=[link],
                        m_extra={"date_raw": date_raw, "content_html": content_html}
                    )
                    entity = entity_model(m_scrap_file=self.__class__.__name__, m_team="hackread")

                    self._card_data.append(card)
                    self._entity_data.append(entity)
                    aid = self._store_raw_card(card)

                    collected += 1
                    print(f"[HACKREAD] ✅ Parsed ({collected}/{len(visit_list)}): {title[:90]}")
                    print(f"[HACKREAD]    Author: {author or '(n/a)'} | Date: {date_raw or '(n/a)'} | AID: {aid}")

                except Exception as ex:
                    print(f"[HACKREAD] ❌ Error parsing article {link}: {ex}")
                    continue

            # close browser
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

        # NLP enrichment (stores processed per-field, no JSON)
        self._nlp_enrich_and_store()

        self._is_crawled = True
        print(f"[HACKREAD] ✅ Done. Collected={collected}")
        return {
            "seed_url": self.seed_url,
            "articles_collected": collected,
            "developer_signature": self.developer_signature()
        }

    def _run_with_requests(self) -> dict:
        print("[HACKREAD] Fallback: requests-based crawl")
        collected = 0
        session = self._make_requests_session()

        # paginate category pages deterministically
        all_links: Set[str] = set()
        for page_no in range(1, self._max_pages + 1):
            list_url = self._page_url(page_no)
            r = session.get(list_url, timeout=60)
            if r.status_code != 200:
                print(f"[HACKREAD] Stopped at page {page_no}, status {r.status_code}")
                break
            soup = BeautifulSoup(r.text, "html.parser")
            page_links = self._extract_article_links_from_index(soup)
            all_links.update(page_links)
            print(f"[HACKREAD] Index page {page_no} (requests): +{len(page_links)} links (unique {len(all_links)})")
            if self._max_articles and len(all_links) >= self._max_articles:
                break

        visit_list = sorted(all_links)
        if self._max_articles:
            visit_list = visit_list[: self._max_articles]

        print(f"[HACKREAD] Visiting {len(visit_list)} articles (requests mode)")
        for idx, link in enumerate(visit_list, 1):
            try:
                art = session.get(link, timeout=60)
                if art.status_code != 200:
                    continue
                s = BeautifulSoup(art.text, "html.parser")

                title_el = s.select_one("h1.cs-entry__title.cs-entry__title-line") or s.select_one("h1.cs-entry__title") or s.select_one("h1.entry-title")
                title = title_el.get_text(strip=True) if title_el else "(No title)"

                subtitle_el = s.select_one(".cs-entry__subtitle")
                subtitle = subtitle_el.get_text(strip=True) if subtitle_el else ""

                entry_el = s.select_one(".entry-content")
                content_html = str(entry_el) if entry_el else ""
                content_text = entry_el.get_text(" ", strip=True) if entry_el else ""

                full_content_text = f"{subtitle}\n\n{content_text}".strip() if subtitle else content_text
                important_text = subtitle if subtitle else " ".join(content_text.split()[:150])

                author, date_raw = self._extract_author_date(s)
                parsed_date = self._parse_date(date_raw)

                card = news_model(
                    m_screenshot="",
                    m_title=title,
                    m_weblink=[link],
                    m_dumplink=[link],
                    m_url=link,
                    m_base_url=self.base_url,
                    m_content=full_content_text,
                    m_network=helper_method.get_network_type(self.base_url),
                    m_important_content=important_text,
                    m_content_type=["news"],
                    m_leak_date=parsed_date,
                    m_author=author,
                    m_description=important_text,
                    m_location="",
                    m_links=[link],
                    m_extra={"date_raw": date_raw, "content_html": content_html}
                )
                entity = entity_model(m_scrap_file=self.__class__.__name__, m_team="hackread")

                self._card_data.append(card)
                self._entity_data.append(entity)
                aid = self._store_raw_card(card)

                collected += 1
                print(f"[HACKREAD] ✅ Parsed (requests) ({idx}/{len(visit_list)}): {title[:90]}")
                print(f"[HACKREAD]    Author: {author or '(n/a)'} | Date: {date_raw or '(n/a)'} | AID: {aid}")

            except Exception as ex:
                print(f"[HACKREAD] ❌ Error (requests) parsing {link}: {ex}")
                continue

        self._nlp_enrich_and_store()
        self._is_crawled = True
        print(f"[HACKREAD] ✅ Done (requests). Collected={collected}")
        return {
            "seed_url": self.seed_url,
            "articles_collected": collected,
            "developer_signature": self.developer_signature()
        }

    # ------- NLP (pure Redis, no JSON) ----
    def _nlp_enrich_and_store(self):
        try:
            print(f"[HACKREAD] NLP enrichment on {len(self._card_data)} records (no JSON)")
            for card in self._card_data:
                # raw human date + ISO
                date_raw = ""
                try:
                    date_raw = (card.m_extra or {}).get("date_raw", "")  # type: ignore
                except Exception:
                    date_raw = ""
                date_iso = self._date_to_string(card.m_leak_date)

                rec = {
                    "url": card.m_url,
                    "title": card.m_title,
                    "author": card.m_author,

                    # raw human → date, iso → published
                    "date": date_raw,
                    "published": date_iso,

                    "description": card.m_description,
                    "location": card.m_location,
                    "links": card.m_links or [],
                    "content": card.m_content,
                    "network": {"type": card.m_network},
                    "seed_url": self.seed_url,
                    "rendered": True,
                    "scraped_at": int(datetime.now(timezone.utc).timestamp())
                }
                try:
                    processed = nlp.process_record(rec)
                except Exception as e:
                    print("[HACKREAD] NLP processing failed for record:", e)
                    processed = None

                aid = self._sha1(card.m_url or card.m_title)

                if processed:
                    self._store_processed(aid, processed)
                    date_raw_out = str(processed.get("date_raw") or rec.get("date") or "")
                    date_iso_out = str(processed.get("date") or rec.get("published") or "")
                    title = str(processed.get("title") or rec.get("title") or "")
                    author = str(processed.get("author") or rec.get("author") or "")
                    description = str(processed.get("description") or (processed.get("summary") or ""))[:300]
                    url = str(processed.get("url") or rec.get("url") or "")
                    seed = rec.get("seed_url") or self.seed_url

                    print("\n----------------------------------------")
                    print(f"Date(raw): {date_raw_out}")
                    print(f"Date(iso): {date_iso_out}")
                    print(f"title: {title}")
                    print(f"Author: {author}")
                    print(f"description: {description}\n")
                    print(f"seed url: {seed}")
                    print(f"dump url: {url}")
                    print("----------------------------------------\n")

            print("[HACKREAD] NLP enrichment stored to Redis ✅ (no JSON)")

        except Exception as ex:
            print("[HACKREAD] ⚠ NLP enrichment error:", ex)

    # ------- date parsing -------------------
    @staticmethod
    def _parse_date(s: str):
        if not s:
            return None
        s = s.strip()
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                continue
        m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}", s)
        if m:
            try:
                return datetime.strptime(m.group(0), "%b %d, %Y").date()
            except Exception:
                pass
        return None
