import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# If you want to push into raw_events, uncomment the next line
# and make sure src/etl/raw_events.py has upsert_raw_event(conn, endpoint, espn_event_id, payload)
# from src.etl.raw_events import upsert_raw_event
# from db.connection import engine

load_dotenv()

USER_AGENT = os.getenv("ESPN_USER_AGENT", "RugbyAnalytics/1.0 (single-game-test)")


def safe_get(url: str, params: dict | None = None, timeout: int = 30) -> dict:
    """Minimal safe GET wrapper with logging."""
    print("Requesting ESPN summary...")
    print(f"URL: {url}")
    if params:
        print(f"Params: {params}")

    resp = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": USER_AGENT})
    print(f"HTTP status: {resp.status_code}")
    resp.raise_for_status()
    return resp.json()


def main():
    # 🔁 STEP 1: Put your real event ID here (from DevTools)
    EVENT_ID = os.getenv("ESPN_TEST_EVENT_ID", "").strip()

    if not EVENT_ID:
        # Hard-code here if you don't want to use env var
        # EVENT_ID = "401547574"
        print(
            "Please set ESPN_TEST_EVENT_ID in your .env file or hard-code EVENT_ID "
            "in fetch_single_espn_game.py"
        )
        return

    # Rugby union summary endpoint pattern:
    # https://site.web.api.espn.com/apis/site/v2/sports/rugby/union/summary?event={EVENT_ID}
    base_url = "https://site.web.api.espn.com/apis/site/v2/sports/rugby/union/summary"
    params = {
        "event": EVENT_ID,
        "lang": "en",
    }

    try:
        data = safe_get(base_url, params=params)
    except requests.HTTPError as e:
        print("❌ HTTP error when calling ESPN:")
        print(e)
        return
    except Exception as e:
        print("❌ Unexpected error when calling ESPN:")
        print(repr(e))
        return

    # 🔍 STEP 2: Print a tiny human-readable summary
    try:
        header_competition = data.get("header", {}).get("competitions", [{}])[0]
        competitors = header_competition.get("competitors", [])

        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)

        home_name = home.get("team", {}).get("displayName") if home else "UNKNOWN"
        away_name = away.get("team", {}).get("displayName") if away else "UNKNOWN"

        home_score = home.get("score") if home else "?"
        away_score = away.get("score") if away else "?"

        status_desc = data.get("header", {}).get("competitions", [{}])[0].get("status", {}).get(
            "type", {}
        ).get("description", "Unknown")

        date_str = data.get("header", {}).get("competitions", [{}])[0].get("date")
        kickoff_utc = None
        if date_str:
            kickoff_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))

        print("\n=== ESPN Single Match Summary ===")
        print(f"Event ID: {EVENT_ID}")
        print(f"Match   : {home_name} {home_score} – {away_score} {away_name}")
        print(f"Status  : {status_desc}")
        if kickoff_utc is not None:
            print(f"Kickoff : {kickoff_utc.isoformat()} (UTC)")
        print("=================================\n")

    except Exception as e:
        print("⚠️ Could not parse a friendly summary from the JSON.")
        print("Raw error:", repr(e))

    # 💾 STEP 3: Save full JSON to disk for inspection
    out_dir = Path("data") / "raw" / "espn_single_test"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"summary_{EVENT_ID}_{ts}.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Full JSON saved to: {out_path}")

    # 🗄️ STEP 4 (optional): also stash in raw_events table
    # Uncomment this block if you want it in Postgres
    """
    from sqlalchemy import text

    with engine.begin() as conn:
        # if you have upsert_raw_event helper:
        # upsert_raw_event(conn, endpoint="summary", espn_event_id=EVENT_ID, payload=data)

        # Or do it inline:
        conn.execute(
            text(
                \"\"\"
                INSERT INTO raw_events (endpoint, espn_event_id, payload)
                VALUES (:endpoint, :espn_event_id, :payload::jsonb)
                ON CONFLICT (endpoint, espn_event_id) DO UPDATE
                SET payload = EXCLUDED.payload,
                    updated_at = NOW()
                \"\"\"
            ),
            {
                "endpoint": "summary",
                "espn_event_id": EVENT_ID,
                "payload": json.dumps(data),
            },
        )
        print("✅ Stored event in raw_events table.")
    """


if __name__ == "__main__":
    main()
