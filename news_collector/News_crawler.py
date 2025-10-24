import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import json
from nlp_processor import process_record

HEADERS = {"User-Agent": "DarkPulseBot/1.0"}

def extract_article(url):
    """Extract title and content from a given article URL."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"[!] Bad status ({r.status_code}) for {url}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract title
        title_el = soup.select_one("h1, title")
        title = title_el.get_text(strip=True) if title_el else "No title found"

        # Extract content
        content_el = None
        for sel in [".post-body", ".article-content", "article", ".td-post-content", ".entry-content"]:
            el = soup.select_one(sel)
            if el:
                content_el = el
                break

        if content_el:
            content = content_el.get_text(separator="\n", strip=True)
        else:
            content = soup.get_text(separator="\n", strip=True)

        # Show progress in console
        print(f"\n✅ Scraped: {title[:80]}...")
        print(f"📄 URL: {url}\n")
        print("-" * 60)

        return {"url": url, "title": title, "content": content}

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None


def run(urls_file="News_urls.txt", output_file="raw_articles.jsonl"):
    """Run crawler on all URLs from file."""
    try:
        with open(urls_file, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"⚠️ File not found: {urls_file}")
        return

    print(f"🚀 Starting DarkPulse crawler on {len(urls)} URLs...\n")

    for url in tqdm(urls, desc="Scraping in progress"):
        data = extract_article(url)
        if data:
            with open(output_file, "a", encoding="utf-8") as out:
                out.write(json.dumps(data, ensure_ascii=False) + "\n")
        time.sleep(1.0)  # polite delay for servers

    print("\n🎉 Done! All articles saved to:", output_file)


if __name__ == "__main__":
    run()
