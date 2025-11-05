from crawler.request_manager import check_services_status, init_services
from crawler.request_parser import RequestParser
from news_collector.scripts._csocybercrime import _csocybercrime
from news_collector.scripts._hackread import _hackread
from news_collector.scripts._thehackernews import _thehackernews

print("[MAIN] Initializing crawler services ...")
init_services()
check_services_status()
print("[MAIN] Services ready ✅")

if __name__ == "__main__":
    print("[MAIN] Starting hackread crawler ...")
    parse_sample = _thehackernews()
    RequestParser(proxy={"server": "socks5://127.0.0.1:9150"}, model=parse_sample, reset_cache=True).parse()
