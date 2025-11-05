# crawler/request_manager.py
import os
import sys

def init_services():
    """
    Stub initializer for external services.
    Extend as needed (e.g., warm caches, ping redis, etc.).
    """
    return {
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}",
        "cwd": os.getcwd(),
    }

def check_services_status():
    """
    Print a simple readiness report (kept minimal on purpose).
    """
    print("=== Service Status ===")
    try:
        import requests  # noqa
        print("requests: OK")
    except Exception:
        print("requests: MISSING")

    try:
        import bs4  # noqa
        print("beautifulsoup4: OK")
    except Exception:
        print("beautifulsoup4: MISSING")

    try:
        import transformers  # noqa
        import sentence_transformers  # noqa
        print("NLP stack (transformers/sentence-transformers): OK")
    except Exception:
        print("NLP stack: MISSING (will still run crawler; NLP will attempt to load when imported)")

    print("======================")
