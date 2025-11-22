# scripts/test_insert_raw.py

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
import json

from sqlalchemy import text
from db.connection import engine


def main():
    payload = {
        "test": True,
        "message": "hello from python",
        "inserted_at": datetime.utcnow().isoformat(),
    }

    with engine.begin() as conn:
          conn.execute(
            text("""
                INSERT INTO raw_events (endpoint, espn_event_id, payload)
                VALUES (:endpoint, :espn_event_id, CAST(:payload AS jsonb))
                ON CONFLICT (endpoint, espn_event_id) DO NOTHING
            """),
            {
                "endpoint": "test",
                "espn_event_id": "test-1",
                "payload": json.dumps(payload),
            },
        )

    print("Inserted (or skipped if already there).")


if __name__ == "__main__":
    main()
