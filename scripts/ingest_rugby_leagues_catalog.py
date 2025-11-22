#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_rugby_leagues_catalog.py
--------------------------------

High-level catalog ingest for rugby leagues from TheSportsDB.

Goal:
- Populate your `leagues` table with key rugby competitions, split into:
  * "division1"   -> domestic / club leagues
  * "international" -> international & cups

What it does:
- Uses TSDB v1 API with key from .env:
    THESPORTSDB_API_KEY=752396
- For each configured league ID:
    * Calls lookupleague.php?id={idLeague}
    * Reads strLeague, strLeagueAlternate, strCountry, strSport
    * Derives:
        - name        -> leagues.name
        - short_name  -> leagues.short_name (uniqueness-safe)
        - slug        -> leagues.slug
        - country_code -> leagues.country_code (CHAR(2), mapped or NULL)
        - tsdb_league_id
        - sport_id    -> inferred from sports table (rugby_union / rugby)

- Upserts into `leagues`:
    leagues (
        league_id      BIGSERIAL PK,
        name           TEXT,
        short_name     TEXT,
        slug           TEXT,
        country_code   CHAR(2),
        sport_id       BIGINT REFERENCES sports,
        tsdb_league_id TEXT UNIQUE,
        created_at     TIMESTAMPTZ,
        updated_at     TIMESTAMPTZ
    )

- Optionally writes a CSV summary to ./data/rugby_leagues_catalog.csv.

Usage (from C:\rugby-analytics):

  # Ingest all configured Division 1 + International leagues
  python .\scripts\ingest_rugby_leagues_catalog.py --write-csv -v

  # Only division1
  python .\scripts\ingest_rugby_leagues_catalog.py --only division1 --write-csv -v

  # Only international
  python .\scripts\ingest_rugby_leagues_catalog.py --only international -v
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
# Config: Division 1 & International rugby league IDs (from TSDB)
# ---------------------------------------------------------------------------

DIVISION1_LEAGUES: Dict[str, str] = {
    # idLeague: label
    "5370": "Commonwealth Games 7s Rugby",
    "5069": "Currie Cup",
    "5166": "English Premier 15s",
    "5167": "Japan Rugby League One",
    "5070": "Major League Rugby",
    "5168": "Romanian SuperLiga",
    "5480": "Rugby Union Club Friendlies",
    "5479": "Rugby Union International Friendlies",
    "5170": "Spanish División de Honor",
    "5169": "Super Liga Americana",
    "5701": "Super Rugby Aus",
    "5165": "URBA Top 13",
    # URC is technically club/int'l hybrid, but you've already ingested it as URC:
    "4446": "United Rugby Championship",
}

INTERNATIONAL_LEAGUES: Dict[str, str] = {
    "4984": "Autumn Nations Cup",
    "5512": "British and Irish Lions Tours",
    "5695": "English Prem Rugby Cup",
    "5418": "European Rugby Challenge Cup",
    "4550": "European Rugby Champions Cup",
    "5037": "Olympics 7s Rugby",
    "4985": "Pacific Nations Cup",
    "4986": "Rugby Championship",
    "4983": "Rugby Europe Championship",
    "4574": "Rugby World Cup",
    "4714": "Six Nations Championship",
    "5082": "Six Nations Under 20s Championship",
    "5563": "Six Nations Women",
    "5682": "Womens Rugby World Cup",
}


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
# Util helpers
# ---------------------------------------------------------------------------
def _slugify(name: str) -> str:
    out: List[str] = []
    prev_dash = False
    for ch in name.lower():
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append("-")
                prev_dash = True
    slug = "".join(out).strip("-")
    return slug or "league"


def _to_iso2(country: Optional[str]) -> Optional[str]:
    """
    Map TSDB country strings to ISO-2 where possible.
    Anything unknown returns None (to avoid CHAR(2) overflow).
    """
    if not country:
        return None
    c = country.strip().lower()
    mapping = {
        "england": "GB",
        "scotland": "GB",
        "wales": "GB",
        "northern ireland": "GB",
        "united kingdom": "GB",
        "ireland": "IE",
        "france": "FR",
        "italy": "IT",
        "spain": "ES",
        "portugal": "PT",
        "romania": "RO",
        "argentina": "AR",
        "japan": "JP",
        "australia": "AU",
        "new zealand": "NZ",
        "south africa": "ZA",
        "united states": "US",
        "usa": "US",
        "canada": "CA",
        "fiji": "FJ",
        "samoa": "WS",
        "tonga": "TO",
        "georgia": "GE",
        "worldwide": None,
        "international": None,
        "europe": None,
        "world": None,
    }
    return mapping.get(c, None)


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


def _get_rugby_sport_id(cur) -> int:
    """
    Resolve sport_id for rugby (prefers code='rugby_union', falls back to name ILIKE 'rugby%').
    """
    cur.execute(
        """
        SELECT sport_id
        FROM sports
        WHERE code = 'rugby_union'
           OR LOWER(name) LIKE 'rugby%%'
        ORDER BY (code = 'rugby_union') DESC, name ASC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            "No rugby sport found in sports table. Expected row with code='rugby_union' "
            "or name ILIKE 'rugby%'."
        )
    return int(row[0])


def _ensure_unique_short_name(cur, candidate: str) -> str:
    """
    Ensure short_name is unique. If candidate already exists for another league,
    append ' (TSDB {id})' at call site. Here we only check existence.
    """
    cur.execute(
        """
        SELECT 1
        FROM leagues
        WHERE short_name = %s
        LIMIT 1
        """,
        (candidate,),
    )
    row = cur.fetchone()
    return candidate if not row else candidate  # we modify at caller if needed


def _upsert_league(
    cur,
    tsdb_league_id: str,
    name: str,
    short_name: Optional[str],
    country_code: Optional[str],
    sport_id: int,
    category: str,
    verbose: bool = False,
) -> Tuple[bool, bool]:
    """
    Upsert a league row by tsdb_league_id (primary) and fallback to (name/short_name).
    Returns (inserted, updated).
    """

    name = name.strip()
    short = (short_name or name).strip()
    slug = _slugify(name)

    # 1) Try by tsdb_league_id
    cur.execute(
        """
        SELECT league_id
        FROM leagues
        WHERE tsdb_league_id = %s
        """,
        (tsdb_league_id,),
    )
    row = cur.fetchone()
    if row:
        league_id = int(row[0])
        if verbose:
            print(f"  [UPDATE] league_id={league_id} (tsdb_league_id={tsdb_league_id}) -> {name}")
        cur.execute(
            """
            UPDATE leagues
               SET name = %s,
                   short_name = %s,
                   slug = %s,
                   country_code = %s,
                   sport_id = %s,
                   updated_at = NOW()
             WHERE league_id = %s
            """,
            (name, short, slug, country_code, sport_id, league_id),
        )
        return (False, True)

    # 2) Try by name / short_name
    cur.execute(
        """
        SELECT league_id
        FROM leagues
        WHERE LOWER(name) = LOWER(%s)
           OR LOWER(short_name) = LOWER(%s)
        LIMIT 1
        """,
        (name, short),
    )
    row = cur.fetchone()
    if row:
        league_id = int(row[0])
        if verbose:
            print(f"  [ATTACH] existing league_id={league_id} to tsdb_league_id={tsdb_league_id} ({name})")
        cur.execute(
            """
            UPDATE leagues
               SET tsdb_league_id = %s,
                   name = %s,
                   short_name = %s,
                   slug = %s,
                   country_code = %s,
                   sport_id = %s,
                   updated_at = NOW()
             WHERE league_id = %s
            """,
            (tsdb_league_id, name, short, slug, country_code, sport_id, league_id),
        )
        return (False, True)

    # 3) Insert new
    if verbose:
        print(f"  [INSERT] new league '{name}' (tsdb_league_id={tsdb_league_id}, category={category})")

    cur.execute(
        """
        INSERT INTO leagues (
            name,
            short_name,
            slug,
            country_code,
            sport_id,
            tsdb_league_id,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s,
            %s,
            %s,
            %s,
            NOW(), NOW()
        )
        """,
        (name, short, slug, country_code, sport_id, tsdb_league_id),
    )
    return (True, False)


# ---------------------------------------------------------------------------
# TSDB helpers
# ---------------------------------------------------------------------------
def _get_league_from_tsdb(
    session: requests.Session,
    api_key: str,
    tsdb_league_id: str,
    verbose: bool = False,
) -> Optional[Dict[str, Any]]:
    base = _tsdb_base(api_key)
    data = _get_json_with_backoff(
        session,
        base,
        "lookupleague.php",
        {"id": tsdb_league_id},
        verbose=verbose,
    )
    leagues = data.get("leagues") or []
    if not leagues:
        if verbose:
            print(f"[WARN] TSDB lookupleague: no league for id={tsdb_league_id}")
        return None
    return leagues[0]


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------
def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_catalog_csv(rows: List[Dict[str, Any]]) -> str:
    out_dir = _ensure_data_dir()
    path = os.path.join(out_dir, "rugby_leagues_catalog.csv")

    cols = [
        "category",
        "tsdb_league_id",
        "strLeague",
        "strLeagueAlternate",
        "strCountry",
        "strSport",
        "strCurrentSeason",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[OK] Wrote leagues catalog CSV: {path}")
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Ingest Division 1 and International rugby leagues (TSDB) into leagues table."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("THESPORTSDB_API_KEY", "123"),
        help="TSDB v1 API key (default: THESPORTSDB_API_KEY or '123').",
    )
    parser.add_argument(
        "--only",
        choices=["division1", "international"],
        default=None,
        help="If set, only ingest this category.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write catalog CSV to ./data/rugby_leagues_catalog.csv.",
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

    # Decide which categories to process
    categories: List[str]
    if args.only:
        categories = [args.only]
    else:
        categories = ["division1", "international"]

    if verbose:
        print(f"[INFO] Categories to ingest: {categories}")

    # Build full list of (category, tsdb_league_id, label)
    work_list: List[Tuple[str, str, str]] = []
    if "division1" in categories:
        for lid, label in DIVISION1_LEAGUES.items():
            work_list.append(("division1", lid, label))
    if "international" in categories:
        for lid, label in INTERNATIONAL_LEAGUES.items():
            work_list.append(("international", lid, label))

    if verbose:
        print("[INFO] Leagues configured:")
        for cat, lid, label in work_list:
            print(f"  [{cat}] id={lid} label='{label}'")

    session = _session_with_retries()
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    catalog_rows: List[Dict[str, Any]] = []

    try:
        sport_id = _get_rugby_sport_id(cur)
        if verbose:
            print(f"[INFO] Using sport_id={sport_id} for rugby leagues")

        total_inserted = 0
        total_updated = 0

        for category, tsdb_league_id, label in work_list:
            if verbose:
                print(f"\n[LEAGUE] category={category}, tsdb_league_id={tsdb_league_id}, label='{label}'")

            league = _get_league_from_tsdb(session, api_key, tsdb_league_id, verbose=verbose)
            if not league:
                print(f"[WARN] Skipping tsdb_league_id={tsdb_league_id} (no TSDB data).")
                continue

            strLeague = (league.get("strLeague") or label).strip()
            strAlt = (league.get("strLeagueAlternate") or "").strip()
            strCountry = (league.get("strCountry") or "").strip() or None
            strSport = (league.get("strSport") or "").strip()
            strCurrentSeason = (league.get("strCurrentSeason") or "").strip()

            # Pick short_name = alt or name
            short_name = strAlt or strLeague
            country_code = _to_iso2(strCountry)

            if verbose:
                print(
                    f"  TSDB: name='{strLeague}', alt='{strAlt}', country='{strCountry}', "
                    f"sport='{strSport}', currentSeason='{strCurrentSeason}', iso2={country_code}"
                )

            inserted, updated = _upsert_league(
                cur,
                tsdb_league_id=tsdb_league_id,
                name=strLeague,
                short_name=short_name,
                country_code=country_code,
                sport_id=sport_id,
                category=category,
                verbose=verbose,
            )
            total_inserted += int(inserted)
            total_updated += int(updated)

            catalog_rows.append(
                {
                    "category": category,
                    "tsdb_league_id": tsdb_league_id,
                    "strLeague": strLeague,
                    "strLeagueAlternate": strAlt,
                    "strCountry": strCountry or "",
                    "strSport": strSport,
                    "strCurrentSeason": strCurrentSeason,
                }
            )

        conn.commit()
        print(
            f"[DONE] Rugby leagues catalog ingest complete. "
            f"Inserted={total_inserted}, Updated={total_updated}"
        )

        if args.write_csv:
            _write_catalog_csv(catalog_rows)

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Ingestion failed, rolled back transaction: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
