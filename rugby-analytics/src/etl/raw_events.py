# src/etl/raw_events.py

import sys, os, json
from typing import Any, Dict

from sqlalchemy import text

# Make sure root is on path so we can import db.connection
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db.connection import engine


def upsert_raw_event(endpoint: str, espn_event_id: str, payload: Dict[str, Any]) -> None:
    """
    Store a JSON payload in raw_events with a natural key (endpoint, espn_event_id).

    If the row exists, update payload + fetched_at.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw_events (endpoint, espn_event_id, payload)
                VALUES (:endpoint, :espn_event_id, CAST(:payload AS jsonb))
                ON CONFLICT (endpoint, espn_event_id)
                DO UPDATE SET
                    payload   = EXCLUDED.payload,
                    fetched_at = NOW();
                """
            ),
            {
                "endpoint": endpoint,
                "espn_event_id": str(espn_event_id),
                "payload": json.dumps(payload),
            },
        )
