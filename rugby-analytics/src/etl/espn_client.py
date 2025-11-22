# src/etl/espn_client.py

import sys, os, time, random
from typing import Dict, Any, Optional
import requests

# Add project root to Python path (so we can import db.*, src.*, etc.)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv

load_dotenv()

USER_AGENT = os.getenv("ESPN_USER_AGENT", "RugbyAnalytics/1.0 (test)")
RATE_DELAY = float(os.getenv("ETL_RATE_LIMIT_DELAY", "0.6"))
MAX_RETRIES = int(os.getenv("ETL_MAX_RETRIES", "3"))

# ESPN rugby base path
BASE_RUGBY_PATH = "union"

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def safe_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Rate-limited GET with retries.
    Returns parsed JSON.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        # Add small jitter to avoid rate-limit detection
        time.sleep(RATE_DELAY + random.uniform(0, 0.05))

        resp = session.get(url, params=params, timeout=30)

        # Retry logic for temporary ESPN issues
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == MAX_RETRIES:
                resp.raise_for_status()

            backoff = min(2 ** (attempt - 1) * RATE_DELAY, 10) + random.uniform(0, 0.3)
            print(
                f"[WARN] ESPN {resp.status_code} on attempt {attempt}, "
                f"retrying in {backoff:.2f}s..."
            )
            time.sleep(backoff)
            continue

        # Other non-200 responses raise immediately
        resp.raise_for_status()
        return resp.json()

    # Should never happen
    raise RuntimeError("safe_get retry loop ended unexpectedly")


def fetch_rugby_scoreboard(league_slug: str, date_yyyymmdd: str) -> Dict[str, Any]:
    """
    Fetch rugby scoreboard for a given league (slug) and date.

    Example league slugs:
      - "sixnations"
      - "rugbyworldcup"
      - "the-rugby-championship"
    """

    url = f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/{BASE_RUGBY_PATH}/scoreboard"

    params = {
        "dates": date_yyyymmdd,
        "limit": 300,
        "lang": "en",
        "league": league_slug,
    }

    return safe_get(url, params)


def fetch_rugby_summary(league_slug: str, event_id: str) -> Dict[str, Any]:
    """
    Fetch a single match summary for a given rugby league and ESPN event ID.
    """

    url = f"https://site.web.api.espn.com/apis/site/v2/sports/rugby/{BASE_RUGBY_PATH}/summary"

    params = {
        "event": event_id,
        "lang": "en",
        "league": league_slug,
    }

    return safe_get(url, params)
