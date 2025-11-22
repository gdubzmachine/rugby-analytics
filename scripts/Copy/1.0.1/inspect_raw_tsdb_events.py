#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
inspect_raw_tsdb_events.py
--------------------------

Utility script to inspect / export raw TSDB event JSON that was ingested
into raw_tsdb_events by scripts.ingest_match_events_raw.

It:

  - Joins raw_tsdb_events to matches + leagues so you can filter by
    TSDB league id and/or season.
  - Supports both `payload` (JSONB) and legacy `raw_json` columns,
    preferring `payload` if present.
  - Writes one JSON file per event into ./data/, e.g.:

        data/raw_event_661004.json

Usage examples (from C:\\rugby-analytics):

    # Export 10 URC events (TSDB league 4446)
    python -m scripts.inspect_raw_tsdb_events ^
        --only-tsdb-league 4446 ^
        --limit 10 -v

    # Export 5 events for a specific season label (e.g. '2023-2024')
    python -m scripts.inspect_raw_tsdb_events ^
        --only-tsdb-league 4446 ^
        --season-label 2023-2024 ^
        --limit 5 -v
"""

import os
import sys
import json
from typing import Any, Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Make sure project root is importable
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# DB imports
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print(
        "Missing dependency: psycopg2-binary (pip install psycopg2-binary)",
        file=sys.stderr,
    )
    sys.exit(1)

# Optional db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _get_conn():
    """
    Get DB connection, preferring db.connection.get_db_connection().
    """
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing. "
            "Set DATABASE_URL in .env or implement db/connection.get_db_connection()."
        )
    return psycopg2.connect(dsn)


def _get_raw_event_columns(cur) -> Set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'raw_tsdb_events'
        """
    )
    return {r[0] for r in cur.fetchall()}


def _load_raw_events(
    cur,
    only_tsdb_league: Optional[str],
    season_label: Optional[str],
    limit: Optional[int],
    verbose: bool,
) -> List[Dict[str, Any]]:
    """
    Load raw events joined to matches + leagues + seasons, with optional filters.
    """
    sql = """
        SELECT
            rte.tsdb_event_id,
            rte.payload,
            rte.raw_json,
            rte.fetched_at,
            m.match_id,
            l.tsdb_league_id,
            l.name AS league_name,
            s.label AS season_label,
            s.year
        FROM raw_tsdb_events rte
        JOIN matches m
          ON m.tsdb_event_id = rte.tsdb_event_id
        JOIN leagues l
          ON l.league_id = m.league_id
        JOIN seasons s
          ON s.season_id = m.season_id
        WHERE 1=1
    """
    params: List[Any] = []

    if only_tsdb_league:
        sql += " AND l.tsdb_league_id = %s"
        params.append(only_tsdb_league)

    if season_label:
        sql += " AND s.label = %s"
        params.append(season_label)

    sql += " ORDER BY l.tsdb_league_id::TEXT, s.year, rte.tsdb_event_id::TEXT"

    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()

    results: List[Dict[str, Any]] = []
    for r in rows:
        results.append(
            {
                "tsdb_event_id": str(r["tsdb_event_id"]),
                "payload": r.get("payload"),
                "raw_json": r.get("raw_json"),
                "fetched_at": r["fetched_at"],
                "match_id": r["match_id"],
                "tsdb_league_id": str(r["tsdb_league_id"]),
                "league_name": r["league_name"],
                "season_label": r["season_label"],
                "year": r["year"],
            }
        )

    if verbose:
        print(f"[INFO] Loaded {len(results)} raw_tsdb_events rows to export")
    return results


def _choose_payload(row: Dict[str, Any], cols: Set[str]) -> Optional[Dict[str, Any]]:
    """
    Choose which column to treat as the JSON payload:
      - If payload exists and is not None, use that.
      - Else if raw_json exists and is not None, use that.
    """
    if "payload" in cols and row.get("payload") is not None:
        return row["payload"]
    if "raw_json" in cols and row.get("raw_json") is not None:
        return row["raw_json"]
    return None


def _export_events(
    events: List[Dict[str, Any]],
    cols: Set[str],
    out_dir: str,
    verbose: bool,
) -> None:
    """
    Write one JSON file per event: raw_event_<idEvent>.json
    """
    os.makedirs(out_dir, exist_ok=True)
    count_written = 0
    count_skipped = 0

    for row in events:
        tsdb_event_id = row["tsdb_event_id"]
        payload = _choose_payload(row, cols)
        if payload is None:
            if verbose:
                print(f"[WARN] No JSON payload for event {tsdb_event_id}, skipping")
            count_skipped += 1
            continue

        # Some DB drivers might return JSONB as string, ensure dict
        if isinstance(payload, str):
            try:
                payload_obj = json.loads(payload)
            except Exception:
                payload_obj = {"_raw": payload}
        else:
            payload_obj = payload

        fname = f"raw_event_{tsdb_event_id}.json"
        fpath = os.path.join(out_dir, fname)

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(payload_obj, f, indent=2, sort_keys=True)

        count_written += 1
        if verbose:
            print(f"[WRITE] {fpath}")

    if verbose:
        print(
            f"[INFO] Export complete: written={count_written}, skipped={count_skipped}"
        )


def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Export sample raw_tsdb_events payloads to JSON files under ./data/."
    )
    parser.add_argument(
        "--only-tsdb-league",
        help="Optional TSDB league id filter (e.g. 4446 for URC).",
    )
    parser.add_argument(
        "--season-label",
        help="Optional season label filter (e.g. '2023-2024').",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max number of events to export (default: 10).",
    )
    parser.add_argument(
        "--out-dir",
        default="data",
        help="Output directory for JSON files (default: ./data).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    verbose = args.verbose

    conn = _get_conn()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        cols = _get_raw_event_columns(cur)
        if verbose:
            print(f"[INFO] raw_tsdb_events columns: {sorted(cols)}")

        events = _load_raw_events(
            cur,
            only_tsdb_league=args.only_tsdb_league,
            season_label=args.season_label,
            limit=args.limit,
            verbose=verbose,
        )

        _export_events(events, cols, args.out_dir, verbose=verbose)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
