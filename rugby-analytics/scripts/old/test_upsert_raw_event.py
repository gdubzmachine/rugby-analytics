# scripts/test_upsert_raw_event.py

import sys, os
from datetime import datetime

# ensure project root on path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl.raw_events import upsert_raw_event


def main():
    payload = {
        "source": "unit-test",
        "message": "upsert_raw_event helper works",
        "inserted_at": datetime.utcnow().isoformat(),
    }

    upsert_raw_event(
        endpoint="test_helper",
        espn_event_id="demo-1",
        payload=payload,
    )

    print("Helper call completed.")


if __name__ == "__main__":
    main()
