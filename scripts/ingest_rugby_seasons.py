#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_rugby_seasons.py
-----------------------

Fetch seasons from TheSportsDB for all rugby leagues in your DB
and upsert them into the `seasons` table.

Assumptions about your schema:

  leagues (
      league_id      BIGSERIAL PRIMARY KEY,
      name           TEXT,
      short_name     TEXT,
      slug           TEXT,
      country_code   CHAR(2),
      sport_id       BIGINT,
      tsdb_league_id TEXT UNIQUE,
      created_at     TIMESTAMPTZ,
      updated_at     TIMESTAMPTZ
  )

  seasons (
      season_id       BIGSERIAL PRIMARY KEY,
      league_id       BIGINT REFERENCES leagues(league_id) ON DELETE CASCADE,
      year            INTEGER,
      label           TEXT,
      start_date      DATE,
      end_date        DATE,
      tsdb_season_key TEXT,
      created_at      TIMESTAMPTZ,
      updated_at      TIMESTAMPTZ,
      UNIQUE (league_id, year)
  )

What this script does:

- Uses TSDB v1 API with key from .env:
    THESPORTSDB_API_KEY=752396   (or your real key)
- For each league with leagues.tsdb_league_id IS NOT NULL:
    * Calls search_all_seasons.php?id={tsdb_league_id}
    * For each season (strSeason):
        - year = int(strSeason[:4]) when possible
        - label = strSeason
        - tsdb_season_key = strSeason
    * Upserts into seasons using:
        INSERT ... ON CONFLICT (league_id, year) DO UPDATE ...

- Optional: writes CSV to ./data/rugby_seasons_catalog.csv

Usage (from C:\rugby-analytics):

  # All leagues with tsdb_league_id
  python .\scripts\ingest_rugby_seasons.py --write-csv -v

  # Only one TSDB league (e.g. URC = 4446)
  python .\scripts\ingest_rugby_seasons.py --only-tsdb-league 4446 --write-csv -v
"""

import os
import sys
import csv
import time
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Ensure project ROOT on sys.path so db.* is importable
# ---------------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2-binary (pip install psycopg2-binary)", file=sys.stderr)
    sys.exit(1)

# Try to use your existing helper if present
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _session_with_retries() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get_json_with_backoff(
    session: requests.Session,
    base_url: str,
    endpoint: str,
    params: Dict[str, Any],
    max_retries: int = 4,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Simple GET with exponential backoff on 429/5xx.
    """
    url = f"{base_url}/{endpoint}"
    delay = 0.8
    for attempt in range(1, max_retries + 1):
        resp = session.get(url, params=params, timeout=45)
        if resp.status_code in (429, 500, 502, 503, 504):
            if verbose:
                print(
                    f"[TSDB] HTTP {resp.status_code} on {endpoint} {params} "
                    f"(attempt {attempt}/{max_retries}); sleeping {delay:.1f}s…",
                    file=sys.stderr,
                )
            time.sleep(delay)
            delay *= 1.7
            continue
        resp.raise_for_status()
        try:
            return resp.json() or {}
        except ValueError:
            return {}
    resp.raise_for_status()
    return {}


# ---------------------------------------------------------------------------
# Small utils
# ---------------------------------------------------------------------------
def _parse_year_from_season(str_season: str) -> Optional[int]:
    """
    Try to parse an integer year from a TSDB strSeason like:
      '2025-2026', '2019', '2011-2012', etc.
    We use the first 4 chars.
    """
    if not str_season:
        return None
    s = str_season.strip()
    if len(s) < 4:
        return None
    try:
        return int(s[:4])
    except Exception:
        return None


def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_seasons_csv(rows: List[Dict[str, Any]]) -> str:
    out_dir = _ensure_data_dir()
    path = os.path.join(out_dir, "rugby_seasons_catalog.csv")

    cols = [
        "league_id",
        "tsdb_league_id",
        "league_name",
        "strSeason",
        "year",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[OK] Wrote seasons catalog CSV: {path}")
    return path


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _get_conn():
    """
    Get a psycopg2 connection, preferring db.connection.get_db_connection().
    """
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing. "
            "Set DATABASE_URL in .env or create db/connection.py with get_db_connection()."
        )
    return psycopg2.connect(dsn)


def _load_leagues(cur, only_tsdb_league: Optional[str] = None, verbose: bool = False) -> List[Dict[str, Any]]:
    """
    Load all leagues that have a tsdb_league_id, or just a single one.
    """
    if only_tsdb_league:
        cur.execute(
            """
            SELECT league_id, name, tsdb_league_id
            FROM leagues
            WHERE tsdb_league_id = %s
            ORDER BY league_id
            """,
            (only_tsdb_league,),
        )
    else:
        cur.execute(
            """
            SELECT league_id, name, tsdb_league_id
            FROM leagues
            WHERE tsdb_league_id IS NOT NULL
            ORDER BY league_id
            """
        )
    rows = cur.fetchall()
    leagues: List[Dict[str, Any]] = []
    for r in rows:
        leagues.append(
            {
                "league_id": int(r["league_id"]),
                "name": r["name"],
                "tsdb_league_id": str(r["tsdb_league_id"]),
            }
        )
    if verbose:
        print(f"[INFO] Found {len(leagues)} league(s) with tsdb_league_id")
    return leagues


def _upsert_season(
    cur,
    league_id: int,
    year: int,
    label: str,
    tsdb_season_key: str,
    verbose: bool = False,
) -> Tuple[bool, bool]:
    """
    Upsert a season row by (league_id, year).
    Returns (inserted, updated).
    """
    # Use ON CONFLICT (league_id, year)
    if verbose:
        print(f"  [UPSERT] league_id={league_id}, year={year}, label='{label}', tsdb_season_key='{tsdb_season_key}'")

    cur.execute(
        """
        INSERT INTO seasons (
            league_id,
            year,
            label,
            start_date,
            end_date,
            tsdb_season_key,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s,
            NULL, NULL,
            %s,
            NOW(), NOW()
        )
        ON CONFLICT (league_id, year)
        DO UPDATE SET
            label = EXCLUDED.label,
            tsdb_season_key = EXCLUDED.tsdb_season_key,
            updated_at = NOW()
        RETURNING (xmax = 0) AS inserted
        """,
        (league_id, year, label, tsdb_season_key),
    )
    row = cur.fetchone()
    inserted = bool(row[0])
    updated = not inserted
    return inserted, updated


# ---------------------------------------------------------------------------
# TSDB helper for seasons
# ---------------------------------------------------------------------------
def _get_seasons_for_league(
    session: requests.Session,
    api_key: str,
    tsdb_league_id: str,
    verbose: bool = False,
) -> List[str]:
    """
    Call search_all_seasons.php?id={tsdb_league_id} and return list of strSeason strings.
    """
    base = _tsdb_base(api_key)
    data = _get_json_with_backoff(
        session,
        base,
        "search_all_seasons.php",
        {"id": tsdb_league_id},
        verbose=verbose,
    )
    seasons = data.get("seasons") or []
    out: List[str] = []
    for s in seasons:
        lbl = (s.get("strSeason") or "").strip()
        if not lbl:
            continue
        out.append(lbl)
    if verbose:
        print(f"[TSDB] search_all_seasons id={tsdb_league_id} -> {len(out)} seasons")
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest seasons from TSDB for all leagues with tsdb_league_id into seasons table."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "123"),
        help="TSDB v1 API key (default: THESPORTSDB_API_KEY or '123').",
    )
    parser.add_argument(
        "--only-tsdb-league",
        help="If set, only ingest seasons for this TSDB league id (e.g. 4446 for URC).",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write seasons catalog CSV to ./data/rugby_seasons_catalog.csv.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    api_key = args.api_key
    verbose = args.verbose

    if verbose:
        shown = api_key if len(api_key) <= 4 else api_key[:2] + "***" + api_key[-2:]
        print(f"[INFO] Using TSDB API key: '{shown}'")

    session = _session_with_retries()
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        leagues = _load_leagues(cur, only_tsdb_league=args.only_tsdb_league, verbose=verbose)
        if not leagues:
            raise SystemExit("No leagues with tsdb_league_id found in DB. Run leagues ingest first.")

        catalog_rows: List[Dict[str, Any]] = []
        total_inserted = 0
        total_updated = 0
        total_skipped = 0

        for idx, lg in enumerate(leagues, start=1):
            league_id_db = lg["league_id"]
            league_name = lg["name"]
            tsdb_league_id = lg["tsdb_league_id"]

            if verbose:
                print(
                    f"\n[LEAGUE {idx}/{len(leagues)}] league_id={league_id_db}, "
                    f"tsdb_league_id={tsdb_league_id}, name='{league_name}'"
                )

            str_seasons = _get_seasons_for_league(session, api_key, tsdb_league_id, verbose=verbose)
            if not str_seasons:
                if verbose:
                    print(f"[WARN] No seasons returned for tsdb_league_id={tsdb_league_id}")
                continue

            for s_lbl in str_seasons:
                year = _parse_year_from_season(s_lbl)
                if year is None:
                    if verbose:
                        print(f"  [SKIP] Could not parse year from strSeason='{s_lbl}'")
                    total_skipped += 1
                    continue

                inserted, updated = _upsert_season(
                    cur,
                    league_id=league_id_db,
                    year=year,
                    label=s_lbl,
                    tsdb_season_key=s_lbl,
                    verbose=verbose,
                )
                total_inserted += int(inserted)
                total_updated += int(updated)

                catalog_rows.append(
                    {
                        "league_id": league_id_db,
                        "tsdb_league_id": tsdb_league_id,
                        "league_name": league_name,
                        "strSeason": s_lbl,
                        "year": year,
                    }
                )

        conn.commit()
        print(
            f"[DONE] Seasons ingest complete. Inserted={total_inserted}, "
            f"Updated={total_updated}, Skipped={total_skipped}"
        )

        if args.write_csv:
            _write_seasons_csv(catalog_rows)

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Ingestion failed, rolled back transaction: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
