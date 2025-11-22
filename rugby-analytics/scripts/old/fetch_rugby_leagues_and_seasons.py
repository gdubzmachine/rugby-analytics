#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
fetch_rugby_leagues_and_seasons.py
----------------------------------

Ingest a curated list of rugby leagues into your DB and populate seasons
for each league using TheSportsDB.

Tables touched (per your schema):

- sports:
    * ensure a row exists with code='rugby_union', name='Rugby Union'

- leagues:
    * add tsdb_league_id BIGINT if not present
    * upsert rows for the configured rugby leagues
      - if a league already exists (by tsdb_league_id OR name/short_name),
        we attach tsdb_league_id to that row instead of inserting a duplicate

- seasons:
    * add tsdb_season_key TEXT if not present
    * for each league, call search_all_seasons.php?id={idLeague}
    * create seasons with:
        - league_id (FK)
        - year (start year from strSeason, e.g. '2024-2025' -> 2024)
        - label (strSeason)
        - tsdb_season_key (strSeason)
        - start_date, end_date left NULL for now

Usage
-----
From project root:

    python .\scripts\fetch_rugby_leagues_and_seasons.py --write-csv -v
"""

import os
import re
import sys
import csv
import time
from typing import Any, Dict, List, Optional

# --- HTTP client -------------------------------------------------------------
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)

# --- DB client: psycopg v3 preferred, fallback to psycopg2 -------------------
try:
    import psycopg  # type: ignore
    _PSYCOPG_VERSION = 3
except ImportError:
    try:
        import psycopg2 as psycopg  # type: ignore
        _PSYCOPG_VERSION = 2
    except ImportError:
        print(
            "Missing dependency: psycopg or psycopg2-binary\n"
            "Install one of:\n"
            "  pip install psycopg[binary]\n"
            "  or\n"
            "  pip install psycopg2-binary",
            file=sys.stderr,
        )
        sys.exit(1)


# --- Config: your curated rugby leagues -------------------------------------
RUGBY_LEAGUES: List[Dict[str, str]] = [
    # Division 1 / domestic & club
    {"idLeague": "5370", "label": "Commonwealth Games 7s Rugby"},
    {"idLeague": "5069", "label": "Currie Cup"},
    {"idLeague": "5166", "label": "English Premier 15s"},
    {"idLeague": "5167", "label": "Japan Rugby League One"},
    {"idLeague": "5070", "label": "Major League Rugby"},
    {"idLeague": "5168", "label": "Romanian SuperLiga"},
    {"idLeague": "5480", "label": "Rugby Union Club Friendlies"},
    {"idLeague": "5479", "label": "Rugby Union International Friendlies"},
    {"idLeague": "5170", "label": "Spanish División de Honor"},
    {"idLeague": "5169", "label": "Super Liga Americana"},
    {"idLeague": "5701", "label": "Super Rugby Aus"},
    {"idLeague": "5165", "label": "URBA Top 13"},

    # Competitions / cups / internationals
    {"idLeague": "4984", "label": "Autumn Nations Cup"},
    {"idLeague": "5512", "label": "British and Irish Lions Tours"},
    {"idLeague": "5695", "label": "English Prem Rugby Cup"},
    {"idLeague": "5418", "label": "European Rugby Challenge Cup"},
    {"idLeague": "4550", "label": "European Rugby Champions Cup"},
    {"idLeague": "5037", "label": "Olympics 7s Rugby"},
    {"idLeague": "4985", "label": "Pacific Nations Cup"},
    {"idLeague": "4986", "label": "Rugby Championship"},
    {"idLeague": "4983", "label": "Rugby Europe Championship"},
    {"idLeague": "4574", "label": "Rugby World Cup"},
    {"idLeague": "4714", "label": "Six Nations Championship"},
    {"idLeague": "5082", "label": "Six Nations Under 20s Championship"},
    {"idLeague": "5563", "label": "Six Nations Women"},
    {"idLeague": "5682", "label": "Womens Rugby World Cup"},
]


# --- Env / utils -------------------------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass  # optional


def _clean(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = x.strip()
    return x or None


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower())
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")
    return slug or "league"


# --- TheSportsDB helpers -----------------------------------------------------
def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _lookup_league(api_key: str, id_league: str) -> Optional[Dict[str, Any]]:
    url = f"{_tsdb_base(api_key)}/lookupleague.php"
    resp = requests.get(url, params={"id": id_league}, timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    leagues = payload.get("leagues") or []
    return leagues[0] if leagues else None


def _fetch_seasons(api_key: str, id_league: str) -> List[Dict[str, Any]]:
    url = f"{_tsdb_base(api_key)}/search_all_seasons.php"
    resp = requests.get(url, params={"id": id_league}, timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    return payload.get("seasons") or []


def _parse_season_year(str_season: str) -> Optional[int]:
    s = (str_season or "").strip()
    if not s:
        return None
    m = re.match(r"^(\d{4})", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


# --- DB helpers --------------------------------------------------------------
def _get_db_connection():
    try:
        import db  # type: ignore
        for fn_name in ("get_connection", "get_db", "connect"):
            if hasattr(db, fn_name):
                return getattr(db, fn_name)()
    except Exception:
        pass

    dsn = os.getenv("DATABASE_URL") or os.getenv("PG_DSN")
    if dsn:
        return psycopg.connect(dsn)

    params = {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "dbname": os.getenv("PGDATABASE"),
    }
    if params["host"]:
        return psycopg.connect(**params)

    raise RuntimeError(
        "No DB connection available. Provide a db module or set DATABASE_URL / PG* env vars."
    )


def _ensure_sport_rugby_union(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sports (code, name)
            VALUES ('rugby_union', 'Rugby Union')
            ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
            RETURNING sport_id
            """
        )
        row = cur.fetchone()
    conn.commit()
    return int(row[0])


def _ensure_league_columns(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE leagues ADD COLUMN IF NOT EXISTS tsdb_league_id BIGINT")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS leagues_tsdb_league_id_uk "
            "ON leagues(tsdb_league_id)"
        )
    conn.commit()


def _ensure_season_columns(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE seasons ADD COLUMN IF NOT EXISTS tsdb_season_key TEXT")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS seasons_league_id_tsdb_season_key_idx "
            "ON seasons(league_id, tsdb_season_key)"
        )
    conn.commit()


def _find_existing_league_by_names(conn, candidates: List[str]) -> Optional[int]:
    """
    Try to find an existing league row by matching any candidate against
    leagues.name or leagues.short_name (case-insensitive).
    """
    normed = [c for c in { (c or "").strip() for c in candidates } if c]
    if not normed:
        return None
    with conn.cursor() as cur:
        for nm in normed:
            cur.execute(
                """
                SELECT league_id
                  FROM leagues
                 WHERE LOWER(name) = LOWER(%s)
                    OR LOWER(short_name) = LOWER(%s)
                 LIMIT 1
                """,
                (nm, nm),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])
    return None


def _upsert_league(
    conn,
    league_meta: Optional[Dict[str, Any]],
    id_league: str,
    fallback_label: str,
    rugby_union_sport_id: int,
) -> int:
    """
    Upsert a league row and return league_id (PK).

    Mapping:
      tsdb_league_id      <- idLeague
      name                <- strLeague or fallback_label
      short_name          <- strLeagueAlternate or same as name
      slug                <- slugified name
      country_code        <- ISO-2 ONLY (if TSDB strCountry is exactly 2 letters),
                             otherwise NULL to avoid char(2) truncation.
      sport_id            <- rugby_union_sport_id

    Logic:
      1) If a row with this tsdb_league_id exists, update it.
      2) Else, if a row with matching name or short_name exists, attach tsdb_league_id to that row.
      3) Else, insert a new league row; if short_name collides, update that row (ON CONFLICT).
    """
    tsdb_league_id = int(id_league)
    str_league = _clean(league_meta.get("strLeague")) if league_meta else None
    str_alt = _clean(league_meta.get("strLeagueAlternate")) if league_meta else None
    raw_country = _clean(league_meta.get("strCountry")) if league_meta else None

    # country_code is char(2)
    country_code = None
    if raw_country and len(raw_country) == 2:
        country_code = raw_country.upper()

    name = str_league or fallback_label
    short_name = str_alt or name
    slug = _slugify(name)

    with conn.cursor() as cur:
        # 1) If a row with this tsdb_league_id exists -> update
        cur.execute("SELECT league_id FROM leagues WHERE tsdb_league_id = %s", (tsdb_league_id,))
        row = cur.fetchone()
        if row:
            league_id = int(row[0])
            cur.execute(
                """
                UPDATE leagues
                   SET name = %s,
                       short_name = %s,
                       slug = %s,
                       country_code = %s,
                       sport_id = %s
                 WHERE league_id = %s
                """,
                (name, short_name, slug, country_code, rugby_union_sport_id, league_id),
            )
            return league_id

        # 2) Try to find an existing row by name or short_name
        existing_id = _find_existing_league_by_names(conn, [name, short_name, fallback_label])
        if existing_id is not None:
            cur.execute(
                """
                UPDATE leagues
                   SET tsdb_league_id = %s,
                       name = %s,
                       short_name = %s,
                       slug = %s,
                       country_code = %s,
                       sport_id = %s
                 WHERE league_id = %s
                """,
                (tsdb_league_id, name, short_name, slug, country_code, rugby_union_sport_id, existing_id),
            )
            return existing_id

        # 3) Insert new row; if short_name collides, update that existing row instead
        cur.execute(
            """
            INSERT INTO leagues (name, short_name, slug, country_code, sport_id, tsdb_league_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT unique_short_name DO UPDATE
               SET name = EXCLUDED.name,
                   slug = EXCLUDED.slug,
                   country_code = EXCLUDED.country_code,
                   sport_id = EXCLUDED.sport_id,
                   tsdb_league_id = EXCLUDED.tsdb_league_id
            RETURNING league_id
            """,
            (name, short_name, slug, country_code, rugby_union_sport_id, tsdb_league_id),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row[0])


def _upsert_season(conn, league_id: int, tsdb_season_key: str) -> int:
    label = tsdb_season_key.strip()
    year = _parse_season_year(label)
    with conn.cursor() as cur:
        if year is not None:
            cur.execute(
                "SELECT season_id FROM seasons WHERE league_id = %s AND year = %s",
                (league_id, year),
            )
            row = cur.fetchone()
            if row:
                season_id = int(row[0])
                cur.execute(
                    """
                    UPDATE seasons
                       SET label = COALESCE(%s, label),
                           tsdb_season_key = COALESCE(%s, tsdb_season_key)
                     WHERE season_id = %s
                    """,
                    (label, label, season_id),
                )
                return season_id

        cur.execute(
            """
            INSERT INTO seasons (league_id, year, label, tsdb_season_key)
            VALUES (%s, %s, %s, %s)
            RETURNING season_id
            """,
            (league_id, year, label, label),
        )
        row = cur.fetchone()
    return int(row[0])


def _write_csv(leagues: List[Dict[str, Any]], seasons_map: Dict[str, List[str]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    cols = ["tsdb_league_id", "label", "api_strLeague", "api_strCountry", "season_keys"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for lg in leagues:
            lid = lg["idLeague"]
            season_keys = seasons_map.get(lid, [])
            writer.writerow(
                {
                    "tsdb_league_id": lid,
                    "label": lg["label"],
                    "api_strLeague": lg.get("api_strLeague"),
                    "api_strCountry": lg.get("api_strCountry"),
                    "season_keys": ", ".join(season_keys),
                }
            )


# --- Main --------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest configured rugby leagues + seasons from TheSportsDB into your DB."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "1"),
        help="TheSportsDB API key (default: 1 = public test key).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.5,
        help="Seconds to sleep between league API calls (default: 1.5).",
    )
    parser.add_argument(
        "--write-csv",
        dest="write_csv",
        action="store_true",
        help="Write CSV summary to ./data/rugby_leagues_and_seasons.csv",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    api_key = args.api_key
    sleep_seconds = max(args.sleep_seconds, 0.0)

    if args.verbose:
        print(f"[INFO] Using TheSportsDB key={api_key!r}")
        print(f"[INFO] Sleep between league calls: {sleep_seconds:.2f}s")
        print(f"[INFO] Leagues configured: {len(RUGBY_LEAGUES)}")

    conn = _get_db_connection()
    try:
        rugby_sport_id = _ensure_sport_rugby_union(conn)
        _ensure_league_columns(conn)
        _ensure_season_columns(conn)

        ingested_leagues: List[Dict[str, Any]] = []
        seasons_by_league_id: Dict[str, List[str]] = {}

        for idx, lg in enumerate(RUGBY_LEAGUES, start=1):
            id_league = lg["idLeague"]
            fallback_label = lg["label"]
            if args.verbose:
                print(f"[INFO] League {idx}/{len(RUGBY_LEAGUES)}: id={id_league} label={fallback_label!r}")

            # 1) Fetch league metadata
            try:
                league_meta = _lookup_league(api_key, id_league)
            except requests.HTTPError as e:
                print(f"[WARN] HTTP error for league id={id_league}: {e}", file=sys.stderr)
                league_meta = None

            api_strLeague = _clean(league_meta.get("strLeague")) if league_meta else None
            api_strCountry = _clean(league_meta.get("strCountry")) if league_meta else None

            # 2) Upsert league in DB (robust against duplicates)
            league_id = _upsert_league(
                conn=conn,
                league_meta=league_meta,
                id_league=id_league,
                fallback_label=fallback_label,
                rugby_union_sport_id=rugby_sport_id,
            )

            if args.verbose:
                print(f"[INFO]  -> league_id={league_id} (DB)")

            # 3) Fetch & upsert seasons
            try:
                seasons = _fetch_seasons(api_key, id_league)
            except requests.HTTPError as e:
                print(f"[WARN] HTTP error fetching seasons for league id={id_league}: {e}", file=sys.stderr)
                seasons = []

            season_keys: List[str] = []
            created_or_updated = 0
            for s in seasons:
                str_season = _clean(s.get("strSeason"))
                if not str_season:
                    continue
                _upsert_season(conn, league_id, str_season)
                created_or_updated += 1
                season_keys.append(str_season)

            if args.verbose:
                print(f"[INFO]  -> seasons upserted for league_id={league_id}: {created_or_updated}")

            ingested_leagues.append(
                {
                    "idLeague": id_league,
                    "label": fallback_label,
                    "api_strLeague": api_strLeague,
                    "api_strCountry": api_strCountry,
                }
            )
            seasons_by_league_id[id_league] = season_keys

            if sleep_seconds > 0 and idx < len(RUGBY_LEAGUES):
                time.sleep(sleep_seconds)

        conn.commit()
        print("[OK] Rugby leagues + seasons ingest complete.")

    finally:
        try:
            conn.close()
        except Exception:
            pass

    if args.write_csv:
        out_csv = os.path.join(os.getcwd(), "data", "rugby_leagues_and_seasons.csv")
        _write_csv(ingested_leagues, seasons_by_league_id, out_csv)
        print(f"[OK] Wrote CSV summary: {out_csv}")


if __name__ == "__main__":
    main()
