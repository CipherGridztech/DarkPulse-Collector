from urllib.parse import urlparse

class helper_method:
    @staticmethod
    def get_network_type(base_url: str) -> str:
        try:
            netloc = urlparse(base_url).netloc.lower()
            if netloc.endswith(".onion"):
                return "tor"
            scheme = urlparse(base_url).scheme.lower()
            if scheme in ("http", "https"):
                return "clearnet"
        except Exception:
            pass
        return "unknown"
