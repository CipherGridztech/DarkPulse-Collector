import re
import json
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
from news_collector.scripts._crawler_base import CrawlerBase


class _csocybercrime(leak_extractor_interface, CrawlerBase, ABC):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(_csocybercrime, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, developer_name: str = "Anonymous", developer_note: str = ""):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        CrawlerBase.__init__(self)
        self._card_data: List[news_model] = []
        self._entity_data: List[entity_model] = []
        self._redis = redis_controller()
        self._is_crawled = False
        self._proxy = {}
        self._developer_name = developer_name
        self._developer_note = developer_note
        self.callback = None

        # Crawl limits
        self._max_pages: int = 5
        self._max_articles: Optional[int] = None

        # Master index keys (pipe-delimited strings, NOT JSON)
        self._raw_index_key = "CSO:raw_index"
        self._processed_index_key = "CSO:processed_index"

        # optional: path to local Chromium
       # self._chromium_exe = r"C:\Users\DELL\darkpulse\chromium-win64\chrome-win\chrome.exe"

        print("[CSO] Initialized ✅ (pure Redis, no JSON)")

    # ------- lifecycle/config --------
    def init_callback(self, callback=None):
        self.callback = callback
        print("[CSO] Callback set")

    def set_proxy(self, proxy: dict):
        self._proxy = proxy or {}
        print(f"[CSO] Proxy configured: {self._proxy}")

    def set_limits(self, max_pages: Optional[int] = None, max_articles: Optional[int] = None):
        if max_pages is not None and max_pages >= 1:
            self._max_pages = int(max_pages)
        if max_articles is not None and max_articles >= 1:
            self._max_articles = int(max_articles)
        print(f"[CSO] Limits → pages={self._max_pages}, articles={self._max_articles or '∞'}")

    def reset_cache(self):
        print("[CSO] Resetting crawl timestamp …")
        self._redis_set("CSO:last_crawl", "", 60)

    # ------- required interface props -------
    @property
    def is_crawled(self) -> bool:
        return self._is_crawled

    @property
    def seed_url(self) -> str:
        return "https://www.csoonline.com/uk/cybercrime/"

    @property
    def base_url(self) -> str:
        return "https://www.csoonline.com"

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
        return "https://www.csoonline.com/contact-us/"
    # redis and small helpers delegated to CrawlerBase

    # ------- store raw article (per-field keys) ---------
    def _store_raw_card(self, card: news_model) -> str:
        aid = self.store_raw_card_generic("CSO:raw", card)
        self._append_index(self._raw_index_key, aid)
        return aid

    # ------- store processed NLP output (generic flattener, no JSON) ----
    def _store_processed(self, aid: str, processed: dict):
        base = f"CSO:processed:{aid}"
        self._store_processed_generic(base, processed)
        self._append_index(self._processed_index_key, aid)

    # HTTP session / Playwright helpers delegated to CrawlerBase

    # ------- author/date extraction ----------
    @staticmethod
    def _is_date_like(text: str) -> bool:
        return CrawlerBase._is_date_like(text)

    def _extract_author_date(self, soup: BeautifulSoup) -> Tuple[str, str]:
        # Try (1) time/meta, (2) JSON-LD, (3) visible spans in the article hero/card info.
        date_raw = ""
        author = ""

        # (1) time/meta
        time_el = soup.select_one("time[datetime]")
        if time_el:
            date_raw = (time_el.get("datetime") or time_el.get_text(strip=True) or "").strip()

        if not date_raw:
            m_pub = soup.select_one("meta[property='article:published_time']")
            if m_pub and m_pub.get("content"):
                date_raw = m_pub.get("content").strip()

        if not date_raw:
            m_pub2 = soup.select_one("meta[name='pubdate']")
            if m_pub2 and m_pub2.get("content"):
                date_raw = m_pub2.get("content").strip()

        # (2) JSON-LD (datePublished/dateModified)
        if not date_raw:
            for script in soup.select("script[type='application/ld+json']"):
                try:
                    data = json.loads(script.string or "")
                except Exception:
                    continue
                # Handle list or object
                items = data if isinstance(data, list) else [data]
                for obj in items:
                    if not isinstance(obj, dict):
                        continue
                    cand = obj.get("datePublished") or obj.get("dateModified")
                    if cand and isinstance(cand, str):
                        date_raw = cand.strip()
                        break
                if date_raw:
                    break

        # (3) Visible spans in the hero/card area (your selector generalized)
        if not date_raw:
            for el in soup.select(
                "#primary div.card__info span, "
                ".article-hero .card__info span, "
                "div.card__info.card__info--light span, "
                ".card__info span"
            ):
                txt = el.get_text(strip=True)
                if txt and self._is_date_like(txt):
                    date_raw = txt
                    break

        # Normalize textual month variants (e.g., 'Sept' → 'Sep')
        if date_raw:
            date_raw = re.sub(r"\bSept\b", "Sep", date_raw, flags=re.IGNORECASE)
            m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}", date_raw, re.IGNORECASE)
            if m:
                date_raw = m.group(0).title()

        # Author
        a_el = soup.select_one("a[rel='author'], .byline a, span.byline a, .author a, span.author a, a[href*='/author/'], .card__info a[rel='author']")
        if a_el:
            author = a_el.get_text(strip=True)
        if not author:
            m_auth = soup.select_one("meta[name='author']")
            if m_auth and m_auth.get("content"):
                author = m_auth.get("content").strip()

        return author, date_raw

    # ------- index page helpers (pagination) ----
    def _extract_article_links_from_index(self, soup: BeautifulSoup) -> Set[str]:
        links: Set[str] = set()
        selectors = [
            "div.river-well.article h3 a",
            "h3 a[href*='/article/']",
            "a[href*='/cybercrime/']",
            ".content-listing a",
            "a[href*='/article/']"
        ]
        for sel in selectors:
            for el in soup.select(sel):
                href = el.get("href")
                if not href:
                    continue
                full = urljoin(self.base_url, href)
                if "/article/" in full and full.startswith(self.base_url):
                    links.add(full)
        return links

    def _page_url(self, page_no: int) -> str:
        if page_no <= 1:
            return self.seed_url
        return urljoin(self.seed_url, f"page/{page_no}/")

    # ------- core crawling ------------------
    def run(self) -> dict:
        print("[CSO] run() → Playwright first, then requests fallback")
        try:
            return self.parse_leak_data()
        except Exception as ex:
            print(f"[CSO] Playwright failed ({ex}). Falling back to requests.")
            return self._run_with_requests()

    def parse_leak_data(self) -> dict:
        print("[CSO] run() → Playwright first, then requests fallback (centralized)")
        # Collect links across deterministic pagination by invoking the helper per seed page
        all_links: Set[str] = set()
        for page_no in range(1, self._max_pages + 1):
            seed = self._page_url(page_no)
            links = self.collect_links_playwright(seed, 1, self._extract_article_links_from_index, None, use_proxy=True, max_articles=None)
            all_links.update(links)
            if self._max_articles and len(all_links) >= self._max_articles:
                break

        visit_list = sorted(all_links)
        if self._max_articles:
            visit_list = visit_list[: self._max_articles]

        print(f"[CSO] Visiting {len(visit_list)} articles after pagination")

        def _parse_and_store(page, link, idx, total):
            try:
                print(f"[CSO] Visiting [{idx}/{total}]: {link}")
                s = BeautifulSoup(page.content(), "html.parser")
                title_el = s.select_one("h1")
                title = title_el.get_text(strip=True) if title_el else "(No title)"
                entry_el = (
                    s.select_one("div.article-content")
                    or s.select_one("article .content")
                    or s.select_one("div.content")
                    or s.select_one("article")
                )
                content_html = str(entry_el) if entry_el else ""
                if entry_el:
                    for bad in entry_el.select("aside, nav, form, script, style, iframe"):
                        bad.extract()
                    content_html = str(entry_el)
                    content_text = entry_el.get_text(" ", strip=True)
                else:
                    paras = [p.get_text(" ", strip=True) for p in s.select("p")]
                    paras = [p for p in paras if p and len(p) > 25]
                    content_text = " ".join(paras[:8])
                if not content_text:
                    return False
                author, date_raw = self._extract_author_date(s)
                parsed_date = self._parse_date(date_raw)
                paragraphs = [p.strip() for p in content_text.split(". ") if p.strip()]
                lead = ". ".join(paragraphs[:2]) if paragraphs else content_text[:240]
                card = news_model(
                    m_screenshot="",
                    m_title=title,
                    m_weblink=[link],
                    m_dumplink=[link],
                    m_url=link,
                    m_base_url=self.base_url,
                    m_content=content_text,
                    m_network=helper_method.get_network_type(self.base_url),
                    m_important_content=lead,
                    m_content_type=["news"],
                    m_leak_date=parsed_date,
                    m_author=author,
                    m_description=lead,
                    m_location="",
                    m_links=[link],
                    m_extra={"date_raw": date_raw, "content_html": content_html}
                )
                entity = entity_model(
                    m_scrap_file=self.__class__.__name__,
                    m_team="CSO Cybercrime Section"
                )
                self._card_data.append(card)
                self._entity_data.append(entity)
                aid = self._store_raw_card(card)
                print(f"[CSO] ✅ Parsed: {title[:90]} | Author: {author or '(n/a)'} | Date: {date_raw or '(n/a)'} | AID: {aid}")
                return True
            except Exception as ex:
                print(f"[CSO] ❌ Error parsing article {link}: {ex}")
                return False

        collected = self.visit_links_playwright(visit_list, _parse_and_store, use_proxy=True)

        self._nlp_enrich_and_store()

        self._is_crawled = True
        print(f"[CSO] ✅ Done. Collected={collected}")
        return {
            "seed_url": self.seed_url,
            "articles_collected": collected,
            "developer_signature": self.developer_signature()
        }

    def _run_with_requests(self) -> dict:
        print("[CSO] Fallback: requests-based crawl")
        collected = 0
        session = self._make_requests_session()

        all_links: Set[str] = set()
        for page_no in range(1, self._max_pages + 1):
            list_url = self._page_url(page_no)
            r = session.get(list_url, timeout=60)
            if r.status_code != 200:
                print(f"[CSO] Stopped at page {page_no}, status {r.status_code}")
                break
            soup = BeautifulSoup(r.text, "html.parser")
            page_links = self._extract_article_links_from_index(soup)
            all_links.update(page_links)
            print(f"[CSO] Index page {page_no} (requests): +{len(page_links)} links (unique {len(all_links)})")
            if self._max_articles and len(all_links) >= self._max_articles:
                break

        visit_list = sorted(all_links)
        if self._max_articles:
            visit_list = visit_list[: self._max_articles]

        print(f"[CSO] Visiting {len(visit_list)} articles (requests mode)")
        for idx, link in enumerate(visit_list, 1):
            try:
                art = session.get(link, timeout=60)
                if art.status_code != 200:
                    continue
                s = BeautifulSoup(art.text, "html.parser")

                title_el = s.select_one("h1")
                title = title_el.get_text(strip=True) if title_el else "(No title)"

                entry_el = (
                    s.select_one("div.article-content")
                    or s.select_one("article .content")
                    or s.select_one("div.content")
                    or s.select_one("article")
                )
                content_html = str(entry_el) if entry_el else ""
                if entry_el:
                    for bad in entry_el.select("aside, nav, form, script, style, iframe"):
                        bad.extract()
                    content_html = str(entry_el)
                    content_text = entry_el.get_text(" ", strip=True)
                else:
                    paras = [p.get_text(" ", strip=True) for p in s.select("p")]
                    paras = [p for p in paras if p and len(p) > 25]
                    content_text = " ".join(paras[:8])

                if not content_text:
                    continue

                author, date_raw = self._extract_author_date(s)
                parsed_date = self._parse_date(date_raw)

                paragraphs = [p.strip() for p in content_text.split(". ") if p.strip()]
                lead = ". ".join(paragraphs[:2]) if paragraphs else content_text[:240]

                card = news_model(
                    m_screenshot="",
                    m_title=title,
                    m_weblink=[link],
                    m_dumplink=[link],
                    m_url=link,
                    m_base_url=self.base_url,
                    m_content=content_text,
                    m_network=helper_method.get_network_type(self.base_url),
                    m_important_content=lead,
                    m_content_type=["news"],
                    m_leak_date=parsed_date,
                    m_author=author,
                    m_description=lead,
                    m_location="",
                    m_links=[link],
                    m_extra={"date_raw": date_raw, "content_html": content_html}
                )
                entity = entity_model(
                    m_scrap_file=self.__class__.__name__,
                    m_team="CSO Cybercrime Section"
                )

                self._card_data.append(card)
                self._entity_data.append(entity)
                aid = self._store_raw_card(card)

                collected += 1
                print(f"[CSO] ✅ Parsed (requests) ({idx}/{len(visit_list)}): {title[:90]}")
                print(f"[CSO]    Author: {author or '(n/a)'} | Date: {date_raw or '(n/a)'} | AID: {aid}")

            except Exception as ex:
                print(f"[CSO] ❌ Error (requests) parsing article {link}: {ex}")
                continue

        self._nlp_enrich_and_store()
        self._is_crawled = True
        print(f"[CSO] ✅ Done (requests). Collected={collected}")
        return {
            "seed_url": self.seed_url,
            "articles_collected": collected,
            "developer_signature": self.developer_signature()
        }

    # ------- NLP (pure Redis, no JSON) ----
    def _nlp_enrich_and_store(self):
        self._nlp = nlp
        self.nlp_enrich_and_store_generic(self._card_data, self._processed_index_key)
    # use CrawlerBase._parse_date