import re
from abc import ABC
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Set, Callable
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from crawler.common.crawler_instance.local_interface_model.leak.leak_extractor_interface import leak_extractor_interface
from crawler.common.crawler_instance.local_shared_model.data_model import entity_model, news_model
from crawler.common.crawler_instance.local_shared_model import RuleModel, FetchProxy, FetchConfig, ThreatType
from crawler.common.crawler_instance.crawler_services.redis_manager.redis_controller import redis_controller
from crawler.common.crawler_instance.crawler_services.shared.helper_method import helper_method
from crawler.common.dev_signature import developer_signature
from news_collector.scripts import nlp_processor as nlp
from news_collector.scripts._crawler_base import CrawlerBase


class _thehackernews(leak_extractor_interface, CrawlerBase, ABC):
    _instance = None

    def __new__(cls, *a, **k):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
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

        self._max_pages: int = 5
        self._max_articles: Optional[int] = None
        self._raw_index_key = "THN:raw_index"
        self._processed_index_key = "THN:processed_index"

        print("[THN] Initialized ✅ (pure Redis, no JSON)")

    # lifecycle/config
    def init_callback(self, callback=None):
        self.callback = callback
        print("[THN] Callback set")

    def set_proxy(self, proxy: dict):
        self._proxy = proxy or {}
        print(f"[THN] Proxy configured: {self._proxy}")

    def set_limits(self, max_pages: Optional[int] = None, max_articles: Optional[int] = None):
        if max_pages is not None and max_pages >= 1:
            self._max_pages = int(max_pages)
        if max_articles is not None and max_articles >= 1:
            self._max_articles = int(max_articles)
        print(f"[THN] Limits → pages={self._max_pages}, articles={self._max_articles or '∞'}")

    def reset_cache(self):
        print("[THN] Resetting crawl timestamp …")
        self._redis_set("THN:last_crawl", "", 60)

    # interface props
    @property
    def is_crawled(self) -> bool:
        return self._is_crawled

    @property
    def seed_url(self) -> str:
        return "https://thehackernews.com/"

    @property
    def base_url(self) -> str:
        return "https://thehackernews.com/"

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
        return "https://thehackernews.com/p/submit-news.html"
    # storage (raw card handling remains site-specific)
    def _store_raw_card(self, card: news_model) -> str:
        aid = self.store_raw_card_generic("THN:raw", card)
        self._append_index(self._raw_index_key, aid)
        return aid
    def _store_processed(self, aid: str, processed: dict):
        # delegate to shared generic writer
        base = f"THN:processed:{aid}"
        self._store_processed_generic(base, processed)
        self._append_index(self._processed_index_key, aid)

    # requests session
    # requests/playwright/date helpers are provided by CrawlerBase

    def _extract_author_date(self, soup: BeautifulSoup) -> Tuple[str, str]:
        author, date_raw = "", ""
        # common container
        container = soup.select_one("div.clear.post-head span.p-author")
        if container:
            spans = [sp.get_text(strip=True) for sp in container.select("span.author") if sp.get_text(strip=True)]
            items = []
            seen = set()
            for x in spans:
                if x not in seen:
                    seen.add(x); items.append(x)
            for token in items:
                if not date_raw and self._is_date_like(token):
                    date_raw = token; continue
                if not author and not self._is_date_like(token) and token.lower() not in {"by", "-", "—"}:
                    author = token
        # fallback selectors
        if not author:
            a_meta = soup.select_one("span.vcard a[rel='author'], span[itemprop='name'], a[rel='author']")
            if a_meta:
                author = a_meta.get_text(strip=True)
        if not date_raw:
            for se in ("time[datetime]", "abbr.published", "span.date", "span.post-date"):
                el = soup.select_one(se)
                if el:
                    date_raw = (el.get("datetime") or el.get_text(strip=True) or "").strip()
                    break
        # tighten date to common form if present
        if date_raw:
            m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}", date_raw)
            if m:
                date_raw = m.group(0)
        return author, date_raw

    def _extract_article_links_from_index(self, soup: BeautifulSoup) -> Set[str]:
        links: Set[str] = set()
        selectors = ["a.story-link", "article h2 a", ".post-title a", "h2.post-title a", "a[href*='/20']", ".article-title a", "h3 a[href*='/']"]
        for sel in selectors:
            for tag in soup.select(sel):
                href = tag.get("href")
                if not href:
                    continue
                full = urljoin(self.base_url, href)
                if full.startswith(self.base_url) and "/20" in full and not any(bad in full for bad in ("tag", "search", "page", "/contact", "/p/", "/videos/", "/expert-insights/")):
                    links.add(full)
        return links

    def _find_next_page_url(self, soup: BeautifulSoup) -> Optional[str]:
        for a in soup.select("a"):
            txt = (a.get_text(strip=True) or "").lower()
            href = a.get("href") or ""
            if not href:
                continue
            if ("next page" in txt) or ("older" in txt):
                return urljoin(self.base_url, href)
        a = soup.select_one("a[href*='updated-max=']")
        return urljoin(self.base_url, a.get("href")) if a and a.get("href") else None

    # core crawl (public)
    def run(self) -> dict:
        print("[THN] run() → Playwright first, then requests fallback")
        try:
            return self.parse_leak_data()
        except Exception as ex:
            print(f"[THN] Playwright failed ({ex}). Falling back to requests.")
            return self._run_with_requests()

    def parse_leak_data(self) -> dict:
        print("[THN] run() → Playwright first, then requests fallback (centralized)")
        visit_list = self.collect_links_playwright(self.seed_url, self._max_pages, self._extract_article_links_from_index, self._find_next_page_url, use_proxy=True, max_articles=self._max_articles)
        print(f"[THN] Visiting {len(visit_list)} articles after pagination")

        def _parse_and_store(page, link, idx, total):
            try:
                print(f"[THN] Visiting [{idx}/{total}]: {link}")
                s = BeautifulSoup(page.content(), "html.parser")
                title_el = s.select_one("h1, .post-title, .entry-title, .article-title")
                title = title_el.get_text(strip=True) if title_el else "(No title)"
                author, date_raw = self._extract_author_date(s)
                content_tag = next((s.select_one(sel) for sel in ("div.articlebody", ".post-body", ".entry-content", ".article-content") if s.select_one(sel)), None)
                full_text = ""
                first_two_sentences = "Content not found."
                if content_tag:
                    full_text = content_tag.get_text(" ", strip=True).replace("\n", " ")
                    parts = re.split(r"(?<=[.!?])\s+", full_text)
                    first_two_sentences = " ".join(parts[:2]).strip() or first_two_sentences
                parsed_date = self._parse_date(date_raw)
                card = news_model(
                    m_screenshot="",
                    m_title=title,
                    m_weblink=[link],
                    m_dumplink=[link],
                    m_url=link,
                    m_base_url=self.base_url,
                    m_content=full_text,
                    m_network=helper_method.get_network_type(self.base_url),
                    m_important_content=first_two_sentences,
                    m_content_type=["news"],
                    m_leak_date=parsed_date,
                    m_author=author,
                    m_description=first_two_sentences,
                    m_location="",
                    m_links=[link],
                    m_extra={"date_raw": date_raw}
                )
                entity = entity_model(m_scrap_file=self.__class__.__name__, m_team="hackernews live")
                self._card_data.append(card)
                self._entity_data.append(entity)
                aid = self._store_raw_card(card)
                print(f"[THN] ✅ Parsed: {title[:80]} | Author: {author or '(n/a)'} | Date: {date_raw or '(n/a)'} | AID: {aid}")
                return True
            except Exception as ex:
                print(f"[THN] ❌ Error parsing article {link}: {ex}")
                return False

        collected = self.visit_links_playwright(visit_list, _parse_and_store, use_proxy=True)

        # delegate to base generic NLP enricher
        self._nlp = nlp
        self.nlp_enrich_and_store_generic(self._card_data, self._processed_index_key)

        self._is_crawled = True
        print(f"[THN] ✅ Done. Collected={collected}")
        return {"seed_url": self.seed_url, "articles_collected": collected, "developer_signature": self.developer_signature()}

    def _run_with_requests(self) -> dict:
        print("[THN] Fallback: requests-based crawl")
        collected = 0
        session = self._make_requests_session()
        all_links: Set[str] = set()
        current_url = self.seed_url

        for page_no in range(1, self._max_pages + 1):
            r = session.get(current_url, timeout=60)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            all_links.update(self._extract_article_links_from_index(soup))
            print(f"[THN] Index page {page_no} (requests): unique {len(all_links)}")
            if self._max_articles and len(all_links) >= self._max_articles:
                break
            next_url = self._find_next_page_url(soup)
            if not next_url or next_url == current_url:
                break
            current_url = next_url

        visit_list = sorted(all_links)[: self._max_articles] if self._max_articles else sorted(all_links)
        print(f"[THN] Visiting {len(visit_list)} articles (requests mode)")

        for idx, link in enumerate(visit_list, 1):
            try:
                art = session.get(link, timeout=60); art.raise_for_status()
                s = BeautifulSoup(art.text, "html.parser")
                title_el = s.select_one("h1, .post-title, .entry-title, .article-title")
                title = title_el.get_text(strip=True) if title_el else "(No title)"
                author, date_raw = self._extract_author_date(s)
                content_tag = next((s.select_one(sel) for sel in ("div.articlebody", ".post-body", ".entry-content", ".article-content") if s.select_one(sel)), None)
                full_text = ""
                first_two_sentences = "Content not found."
                if content_tag:
                    full_text = content_tag.get_text(" ", strip=True).replace("\n", " ")
                    parts = re.split(r"(?<=[.!?])\s+", full_text)
                    first_two_sentences = " ".join(parts[:2]).strip() or first_two_sentences
                parsed_date = self._parse_date(date_raw)
                card = news_model(
                    m_screenshot="", m_title=title, m_weblink=[link], m_dumplink=[link], m_url=link, m_base_url=self.base_url,
                    m_content=full_text, m_network=helper_method.get_network_type(self.base_url),
                    m_important_content=first_two_sentences, m_content_type=["news"], m_leak_date=parsed_date,
                    m_author=author, m_description=first_two_sentences, m_location="", m_links=[link], m_extra={"date_raw": date_raw}
                )
                entity = entity_model(m_scrap_file=self.__class__.__name__, m_team="hackernews live")
                self._card_data.append(card); self._entity_data.append(entity); aid = self._store_raw_card(card)
                collected += 1
                print(f"[THN] ✅ Parsed (requests) ({idx}/{len(visit_list)}): {title[:80]}")
                print(f"[THN]    Author: {author or '(n/a)'} | Date: {date_raw or '(n/a)'} | AID: {aid}")
            except Exception as ex:
                print(f"[THN] ❌ Error (requests) parsing {link}: {ex}")
                continue

        self._nlp_enrich_and_store()
        self._is_crawled = True
        print(f"[THN] ✅ Done (requests). Collected={collected}")
        return {"seed_url": self.seed_url, "articles_collected": collected, "developer_signature": self.developer_signature()}

    # NLP & date parsing (unchanged logic)
    def _nlp_enrich_and_store(self):
        # thin wrapper to keep original name for callers
        self._nlp = nlp
        self.nlp_enrich_and_store_generic(self._card_data, self._processed_index_key)
    # use CrawlerBase._parse_date