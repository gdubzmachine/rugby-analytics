#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
seed_positions_and_backfill_players.py
--------------------------------------

What this does
==============
1) Ensures the `position_category` enum exists with values: 'forward','back','other'.
2) Ensures the `positions` table exists with columns:
      position_id BIGSERIAL PK,
      code TEXT UNIQUE,
      name TEXT,
      category position_category NOT NULL,
      number_min INT NULL,
      number_max INT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
3) Seeds canonical Rugby Union positions (idempotent upsert by `code`).
4) Builds a robust mapping from TheSportsDB `strPosition` text → your canonical `positions` (via `code`).
5) For all **URC** teams (idLeague=4446), fetches all players via `lookup_all_players.php`,
   maps their `strPosition`, and updates `players.preferred_position_id` by matching on `players.tsdb_player_id`.

Notes
=====
- TheSportsDB does **not** provide a numeric "position id" for rugby. It sends text in `strPosition`.
  This script normalizes TSDB texts → your canonical positions, and the DB's `position_id` becomes the stable ID.
- Requires your `players` table to have `tsdb_player_id` (TEXT UNIQUE). If missing, we add it.
- Uses THESPORTSDB_API_KEY from .env (premium key recommended).
- Writes CSV snapshots into ./data:
    - positions_lookup.csv (your canonical positions and DB ids)
    - tsdb_position_mapping.csv (TSDB text → canonical code + DB id resolved)

Usage
=====
# Seed positions and backfill all URC players' preferred_position_id
python .\scripts\seed_positions_and_backfill_players.py -v --write-csv

# Only seed positions, skip player backfill
python .\scripts\seed_positions_and_backfill_players.py --no-backfill -v

# Different league (if you want to backfill another rugby league)
python .\scripts\seed_positions_and_backfill_players.py --league-id 4550 -v
"""

import os
import sys
import csv
import time
from typing import Any, Dict, List, Optional, Tuple, Set

# HTTP
try:
    import requests
except ImportError:
    print("Missing dependency: requests (pip install requests)", file=sys.stderr)
    sys.exit(1)

# DB
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except ImportError:
    print("Missing dependency: psycopg2 (pip install psycopg2)", file=sys.stderr)
    sys.exit(1)

# Prefer your project DB helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------- Env / HTTP helpers ----------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def _tsdb_base(api_key: str) -> str:
    return f"https://www.thesportsdb.com/api/v1/json/{api_key}"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "rugby-analytics/positions-backfill"})
    return s


def _get_json_with_backoff(sess: requests.Session, url: str, params: Dict[str, Any], verbose: bool=False,
                           max_retries: int=5, timeout: int=45) -> Dict[str, Any]:
    delay = 0.6
    for attempt in range(1, max_retries + 1):
        r = sess.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            if verbose:
                print(f"[WARN] 429 {url} {params} attempt {attempt}/{max_retries}; sleep {delay:.1f}s", file=sys.stderr)
            time.sleep(delay)
            delay *= 1.8
            continue
        r.raise_for_status()
        try:
            return r.json() or {}
        except Exception:
            return {}
    r.raise_for_status()
    return {}


# ---------------- TheSportsDB calls ----------------
def _lookup_league(sess: requests.Session, key: str, league_id: str, verbose: bool=False) -> Dict[str, Any]:
    url = f"{_tsdb_base(key)}/lookupleague.php"
    return _get_json_with_backoff(sess, url, {"id": league_id}, verbose=verbose).get("leagues", [{}])[0] or {}


def _events_for_season(sess: requests.Session, key: str, league_id: str, season: str, verbose: bool=False) -> List[Dict[str, Any]]:
    url = f"{_tsdb_base(key)}/eventsseason.php"
    data = _get_json_with_backoff(sess, url, {"id": league_id, "s": season}, verbose=verbose)
    events = data.get("events") or []
    return [e for e in events if (e.get("strSport") or "").lower().startswith("rugby")]


def _lookup_all_players(sess: requests.Session, key: str, team_id: str, verbose: bool=False) -> List[Dict[str, Any]]:
    url = f"{_tsdb_base(key)}/lookup_all_players.php"
    data = _get_json_with_backoff(sess, url, {"id": team_id}, verbose=verbose)
    players = data.get("player") or []
    return [p for p in players if (p.get("strSport") or "").lower().startswith("rugby")]


# ---------------- Canonical Rugby positions ----------------
# Shirt number ranges reflect Rugby Union conventions. Generic buckets (Prop, Lock, Flanker, Centre, Winger, Back Row)
# carry sensible [min,max] spans. Specific slots use exact numbers.
_CANONICAL_POSITIONS: List[Dict[str, Any]] = [
    # Forwards (1–8)
    {"code": "RU_LOOSEHEAD_PROP",   "name": "Loosehead Prop",   "category": "forward", "min": 1,  "max": 1},
    {"code": "RU_HOOKER",           "name": "Hooker",           "category": "forward", "min": 2,  "max": 2},
    {"code": "RU_TIGHTHEAD_PROP",   "name": "Tighthead Prop",   "category": "forward", "min": 3,  "max": 3},
    {"code": "RU_PROP",             "name": "Prop",             "category": "forward", "min": 1,  "max": 3},
    {"code": "RU_LOCK",             "name": "Lock",             "category": "forward", "min": 4,  "max": 5},
    {"code": "RU_BLINDSIDE_FLANKER","name": "Blindside Flanker","category": "forward", "min": 6,  "max": 6},
    {"code": "RU_OPENSIDE_FLANKER", "name": "Openside Flanker", "category": "forward", "min": 7,  "max": 7},
    {"code": "RU_FLANKER",          "name": "Flanker",          "category": "forward", "min": 6,  "max": 7},
    {"code": "RU_NUMBER_8",         "name": "Number 8",         "category": "forward", "min": 8,  "max": 8},
    {"code": "RU_BACK_ROW",         "name": "Back Row",         "category": "forward", "min": 6,  "max": 8},

    # Backs (9–15)
    {"code": "RU_SCRUM_HALF",       "name": "Scrum-half",       "category": "back",    "min": 9,  "max": 9},
    {"code": "RU_FLY_HALF",         "name": "Fly-half",         "category": "back",    "min": 10, "max": 10},
    {"code": "RU_INSIDE_CENTRE",    "name": "Inside Centre",    "category": "back",    "min": 12, "max": 12},
    {"code": "RU_OUTSIDE_CENTRE",   "name": "Outside Centre",   "category": "back",    "min": 13, "max": 13},
    {"code": "RU_CENTRE",           "name": "Centre",           "category": "back",    "min": 12, "max": 13},
    {"code": "RU_LEFT_WING",        "name": "Left Wing",        "category": "back",    "min": 11, "max": 11},
    {"code": "RU_RIGHT_WING",       "name": "Right Wing",       "category": "back",    "min": 14, "max": 14},
    {"code": "RU_WINGER",           "name": "Winger",           "category": "back",    "min": 11, "max": 14},
    {"code": "RU_FULLBACK",         "name": "Fullback",         "category": "back",    "min": 15, "max": 15},

    # Utilities / generic
    {"code": "RU_UTILITY_FORWARD",  "name": "Utility Forward",  "category": "forward", "min": None, "max": None},
    {"code": "RU_UTILITY_BACK",     "name": "Utility Back",     "category": "back",    "min": None, "max": None},
]

# TheSportsDB strPosition → canonical code
_TSDB_POSITION_MAP: Dict[str, str] = {
    # Forwards
    "prop": "RU_PROP",
    "loosehead prop": "RU_LOOSEHEAD_PROP",
    "tighthead prop": "RU_TIGHTHEAD_PROP",
    "hooker": "RU_HOOKER",
    "lock": "RU_LOCK",
    "second row": "RU_LOCK",
    "flanker": "RU_FLANKER",
    "openside flanker": "RU_OPENSIDE_FLANKER",
    "blindside flanker": "RU_BLINDSIDE_FLANKER",
    "back row": "RU_BACK_ROW",
    "number 8": "RU_NUMBER_8",
    "no 8": "RU_NUMBER_8",
    "no. 8": "RU_NUMBER_8",
    "no8": "RU_NUMBER_8",
    "utility forward": "RU_UTILITY_FORWARD",

    # Backs
    "scrum-half": "RU_SCRUM_HALF",
    "scrum half": "RU_SCRUM_HALF",
    "scrumhalf": "RU_SCRUM_HALF",
    "half back": "RU_SCRUM_HALF",
    "halfback": "RU_SCRUM_HALF",
    "fly-half": "RU_FLY_HALF",
    "fly half": "RU_FLY_HALF",
    "stand-off": "RU_FLY_HALF",
    "stand off": "RU_FLY_HALF",
    "first five": "RU_FLY_HALF",
    "inside centre": "RU_INSIDE_CENTRE",
    "inside center": "RU_INSIDE_CENTRE",
    "outside centre": "RU_OUTSIDE_CENTRE",
    "outside center": "RU_OUTSIDE_CENTRE",
    "centre": "RU_CENTRE",
    "center": "RU_CENTRE",
    "wing": "RU_WINGER",
    "winger": "RU_WINGER",
    "left wing": "RU_LEFT_WING",
    "right wing": "RU_RIGHT_WING",
    "fullback": "RU_FULLBACK",
    "full-back": "RU_FULLBACK",
    "full back": "RU_FULLBACK",
    "utility back": "RU_UTILITY_BACK",
}


# ---------------- Utilities ----------------
def _norm_pos_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return " ".join(s.strip().lower().split())


def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_positions_csv(cur) -> str:
    out_dir = _ensure_data_dir()
    path = os.path.join(out_dir, "positions_lookup.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["position_id", "code", "name", "category", "number_min", "number_max"])
        cur.execute("SELECT position_id, code, name, category::text, number_min, number_max FROM positions ORDER BY position_id")
        for row in cur.fetchall():
            w.writerow(row)
    return path


def _write_tsdb_map_csv(code_to_id: Dict[str, int]) -> str:
    out_dir = _ensure_data_dir()
    path = os.path.join(out_dir, "tsdb_position_mapping.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tsdb_strPosition", "canonical_code", "position_id"])
        for tsdb_text, code in sorted(_TSDB_POSITION_MAP.items()):
            w.writerow([tsdb_text, code, code_to_id.get(code)])
    return path


# ---------------- DB helpers ----------------
def _get_conn():
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set and db.connection.get_db_connection() not available.")
    return psycopg2.connect(dsn)


def _ensure_enum_and_table(cur, verbose: bool=False) -> None:
    # Ensure enum
    cur.execute("""
        SELECT 1
        FROM pg_type t
        WHERE t.typname = 'position_category'
        LIMIT 1
    """)
    if not cur.fetchone():
        if verbose:
            print("[INFO] Creating enum type position_category ...")
        cur.execute("CREATE TYPE position_category AS ENUM ('forward','back','other');")

    # Ensure table
    cur.execute("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema='public' AND table_name='positions'
        LIMIT 1
    """)
    exists = cur.fetchone() is not None
    if not exists:
        if verbose:
            print("[INFO] Creating table positions ...")
        cur.execute("""
            CREATE TABLE positions (
                position_id BIGSERIAL PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                category position_category NOT NULL,
                number_min INT NULL,
                number_max INT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
    else:
        # Ensure required columns exist
        for col, ddl in [
            ("code",        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS code TEXT;"),
            ("name",        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS name TEXT;"),
            ("category",    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS category position_category;"),
            ("number_min",  "ALTER TABLE positions ADD COLUMN IF NOT EXISTS number_min INT;"),
            ("number_max",  "ALTER TABLE positions ADD COLUMN IF NOT EXISTS number_max INT;"),
            ("created_at",  "ALTER TABLE positions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();"),
            ("updated_at",  "ALTER TABLE positions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();"),
        ]:
            cur.execute(f"""
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='positions' AND column_name=%s
            """, (col,))
            if not cur.fetchone():
                if verbose:
                    print(f"[INFO] Adding positions.{col} ...")
                cur.execute(ddl)
        # Ensure unique index on code
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname='public' AND tablename='positions' AND indexname='uniq_positions_code'
                ) THEN
                    CREATE UNIQUE INDEX uniq_positions_code ON positions(code);
                END IF;
            END; $$;
        """)


def _ensure_players_tsdb_column(cur, verbose: bool=False) -> None:
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='players' AND column_name='tsdb_player_id' LIMIT 1
    """)
    if cur.fetchone():
        return
    if verbose:
        print("[INFO] Adding players.tsdb_player_id (TEXT) + unique index")
    cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS tsdb_player_id TEXT;")
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname='public' AND tablename='players' AND indexname='uniq_players_tsdb_player_id'
            ) THEN
                CREATE UNIQUE INDEX uniq_players_tsdb_player_id ON players(tsdb_player_id);
            END IF;
        END; $$;
    """)


def _upsert_positions(cur, verbose: bool=False) -> Dict[str, int]:
    """
    Upsert canonical positions by code. Return map code -> position_id.
    """
    code_to_id: Dict[str, int] = {}
    for pos in _CANONICAL_POSITIONS:
        code = pos["code"]
        name = pos["name"]
        category = pos["category"]  # 'forward'|'back'|'other'
        nmin = pos["min"]
        nmax = pos["max"]
        if verbose:
            print(f"[INFO] Upsert position {code} ({name})")
        cur.execute(
            """
            INSERT INTO positions (code, name, category, number_min, number_max, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (code) DO UPDATE
               SET name = EXCLUDED.name,
                   category = EXCLUDED.category,
                   number_min = EXCLUDED.number_min,
                   number_max = EXCLUDED.number_max,
                   updated_at = NOW()
            RETURNING position_id
            """,
            (code, name, category, nmin, nmax),
        )
        pid = cur.fetchone()[0]
        code_to_id[code] = pid
    return code_to_id


def _lookup_league_current_season(sess, key: str, league_id: str, verbose: bool=False) -> str:
    meta = _lookup_league(sess, key, league_id, verbose=verbose)
    season = (meta.get("strCurrentSeason") or "").strip()
    if not season:
        raise SystemExit("League has no strCurrentSeason; cannot backfill players.")
    return season


def _collect_team_ids_from_events(sess, key: str, league_id: str, season: str, verbose: bool=False) -> List[str]:
    events = _events_for_season(sess, key, league_id, season, verbose=verbose)
    s: Set[str] = set()
    for e in events:
        hid = (e.get("idHomeTeam") or "").strip()
        aid = (e.get("idAwayTeam") or "").strip()
        if hid: s.add(hid)
        if aid: s.add(aid)
    return sorted(s)


def _resolve_position_id_for_tsdb(text: Optional[str], code_to_id: Dict[str, int]) -> Optional[int]:
    t = _norm_pos_text(text)
    if not t:
        return None
    code = _TSDB_POSITION_MAP.get(t)
    if not code:
        return None
    return code_to_id.get(code)


def _backfill_players_positions(
    cur,
    sess: requests.Session,
    key: str,
    league_id: str,
    code_to_id: Dict[str, int],
    sleep_seconds: float = 0.4,
    verbose: bool=False,
) -> Tuple[int, int]:
    """
    For each team in the league's current season, fetch players and update
    players.preferred_position_id by matching tsdb_player_id.
    Returns (updated_count, skipped_count)
    """
    season = _lookup_league_current_season(sess, key, league_id, verbose=verbose)
    team_ids = _collect_team_ids_from_events(sess, key, league_id, season, verbose=verbose)
    if verbose:
        print(f"[INFO] Season {season}: team_ids={team_ids}")

    updated = 0
    skipped = 0

    for idx, tid in enumerate(team_ids, start=1):
        if verbose:
            print(f"[INFO] ({idx}/{len(team_ids)}) lookup_all_players for team_id={tid}")
        players = _lookup_all_players(sess, key, tid, verbose=verbose)
        for p in players:
            tsdb_pid = (p.get("idPlayer") or "").strip()
            if not tsdb_pid:
                skipped += 1
                continue
            pos_id = _resolve_position_id_for_tsdb(p.get("strPosition"), code_to_id)
            if not pos_id:
                skipped += 1
                continue
            # Update by tsdb id
            cur.execute(
                """
                UPDATE players
                   SET preferred_position_id = %s,
                       updated_at = NOW()
                 WHERE tsdb_player_id = %s
                """,
                (pos_id, tsdb_pid),
            )
            if cur.rowcount > 0:
                updated += cur.rowcount
            else:
                # player not in DB yet -> skip
                skipped += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return updated, skipped


# ---------------- Main ----------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    ap = argparse.ArgumentParser(description="Seed rugby positions and backfill players.preferred_position_id from TheSportsDB.")
    ap.add_argument("--league-id", default="4446", help="TSDB idLeague (URC=4446)")
    ap.add_argument("--api-key", default=os.getenv("THESPORTSDB_API_KEY", "752396"),
                    help="TheSportsDB API key (v1)")
    ap.add_argument("--sleep-seconds", type=float, default=0.4, help="Sleep between team calls during backfill")
    ap.add_argument("--no-backfill", action="store_true", help="Only seed positions; skip updating players")
    ap.add_argument("--write-csv", action="store_true", help="Write positions and mapping CSV snapshots")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = ap.parse_args()

    league_id = str(args.league_id)
    key = args.api_key
    shown = key if len(key) <= 4 else key[:2] + "***" + key[-2:]
    verbose = args.verbose

    if verbose:
        print(f"[INFO] Using TSDB key '{shown}', league_id={league_id}")

    # DB
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        # Ensure schema surface
        _ensure_enum_and_table(cur, verbose=verbose)
        _ensure_players_tsdb_column(cur, verbose=verbose)

        # Seed / upsert canonical positions
        code_to_id = _upsert_positions(cur, verbose=verbose)
        conn.commit()

        if verbose:
            print(f"[INFO] Seeded/ensured {len(code_to_id)} positions")

        # CSV snapshots
        if args.write_csv:
            p_csv = _write_positions_csv(cur)
            m_csv = _write_tsdb_map_csv(code_to_id)
            print(f"[OK] Wrote positions CSV: {p_csv}")
            print(f"[OK] Wrote TSDB→canonical mapping CSV: {m_csv}")

        # Optional: backfill players
        if not args.no_backfill:
            sess = _session()
            updated, skipped = _backfill_players_positions(
                cur, sess, key, league_id, code_to_id, sleep_seconds=args.sleep_seconds, verbose=verbose
            )
            conn.commit()
            print(f"[DONE] Backfill complete → players updated={updated}, skipped={skipped}")
        else:
            print("[DONE] Positions seeded; player backfill skipped (--no-backfill).")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Failed; rolled back: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
