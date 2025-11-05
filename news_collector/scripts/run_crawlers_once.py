
# run_crawlers_once.py
import os, sys, traceback

# --- ensure project root on sys.path ---
ROOT = os.path.dirname(os.path.abspath(__file__))  # C:\Users\DELL\darkpulse
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# sanity check
try:
    from news_collector.scripts import nlp_processor as nlp
    print(f"[BOOT] nlp_processor found at: {nlp.__file__}")
except Exception as e:
    print("[BOOT] nlp_processor not importable yet:", e)

# --- import crawlers ---
from news_collector.scripts._thehackernews import _thehackernews
from news_collector.scripts._hackread import _hackread
from news_collector.scripts._csocybercrime import _csocybercrime

def run_one(model, name, proxy=None, max_pages=None, max_articles=None):
    try:
        if proxy:
            model.set_proxy({"server": proxy})
        if max_pages or max_articles:
            model.set_limits(max_pages=max_pages, max_articles=max_articles)
        out = model.run()
        print(f"[OK] {name}: {out}")
    except Exception:
        print(f"[ERR] {name} crashed:\n{traceback.format_exc()}")

def main():
    proxy = "socks5://127.0.0.1:9150"   # set to None if you don't want a proxy
    max_pages = 3                       # keep small first time
    max_articles = None

    print("\n=== Running crawlers once ===")
    run_one(_thehackernews(), "thehackernews", proxy=proxy, max_pages=1, max_articles=1)
    run_one(_hackread(), "hackread", proxy=proxy, max_pages=1, max_articles=1)
    run_one(_csocybercrime(), "csocybercrime", proxy=proxy, max_pages=1, max_articles=1)
    print("=== Done ===\n")

if __name__ == "__main__":
    main()

