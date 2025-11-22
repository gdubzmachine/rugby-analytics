#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
ingest_urc_players.py
---------------------

Ingest all URC players into your `players` table by:
  1) Getting URC team IDs from the season schedule (eventsseason.php)
  2) For each team, calling lookup_all_players.php?id={idTeam}
  3) Upserting players into DB using tsdb_player_id as the external key

Notes
=====
- Uses TheSportsDB V1 key from .env:
    THESPORTSDB_API_KEY=xxxxxxxx
- Default league: URC idLeague=4446
- Filters to rugby only (defensive)
- Optionally writes a CSV snapshot to ./data

DB expectations
===============
players table (at minimum):
  - player_id (PK)
  - full_name TEXT
  - first_name TEXT NULL
  - last_name TEXT NULL
  - date_of_birth DATE NULL
  - nationality TEXT NULL
  - preferred_position_id INT NULL (FK to positions)  <-- we *try* to map
  - espn_player_id TEXT NULL UNIQUE
  - tsdb_player_id TEXT NULL UNIQUE (this script ensures it exists)
  - created_at, updated_at TIMESTAMPTZ

positions table:
  - position_id (PK)
  - code TEXT UNIQUE
  - name TEXT
  - category TEXT ('forward' | 'back' | 'other')
  - number_min INT NULL, number_max INT NULL

This script will create `players.tsdb_player_id` if it doesn't exist.
It will also upsert positions on-demand (simple mapping).

Usage
=====
# Default: pull current URC season’s team list and ingest all players
python .\scripts\ingest_urc_players.py -v --write-csv

# If you want to also scan one extra previous season to be safe:
python .\scripts\ingest_urc_players.py --max-seasons-back 2 -v --write-csv
"""

import os
import sys
import csv
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

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

# Try project DB helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# --------------------- Env / HTTP helpers ---------------------
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
    adapter = requests.adapters.HTTPAdapter(max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get_with_backoff(
    sess: requests.Session,
    url: str,
    params: Dict[str, Any],
    max_retries: int = 5,
    verbose: bool = False,
) -> requests.Response:
    delay = 0.6
    for attempt in range(1, max_retries + 1):
        r = sess.get(url, params=params, timeout=45)
        if r.status_code == 429:
            if verbose:
                print(f"[WARN] 429 Too Many Requests (attempt {attempt}/{max_retries}) "
                      f"-> sleeping {delay:.1f}s …", file=sys.stderr)
            time.sleep(delay)
            delay *= 1.8
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r


# --------------------- TSDB calls ---------------------
def _lookup_league(sess: requests.Session, key: str, league_id: str, verbose: bool=False) -> Dict[str, Any]:
    url = f"{_tsdb_base(key)}/lookupleague.php"
    r = _get_with_backoff(sess, url, {"id": league_id}, verbose=verbose)
    data = r.json() or {}
    leagues = data.get("leagues") or []
    return leagues[0] if leagues else {}


def _events_for_season(sess: requests.Session, key: str, league_id: str, season: str, verbose: bool=False) -> List[Dict[str, Any]]:
    url = f"{_tsdb_base(key)}/eventsseason.php"
    r = _get_with_backoff(sess, url, {"id": league_id, "s": season}, verbose=verbose)
    data = r.json() or {}
    events = data.get("events") or []
    return [e for e in events if (e.get("strSport") or "").lower().startswith("rugby")]


def _lookup_all_players(sess: requests.Session, key: str, team_id: str, verbose: bool=False) -> List[Dict[str, Any]]:
    """
    lookup_all_players.php returns key 'player'
    """
    url = f"{_tsdb_base(key)}/lookup_all_players.php"
    try:
        r = _get_with_backoff(sess, url, {"id": team_id}, verbose=verbose)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            if verbose:
                print(f"[WARN] lookup_all_players 404 for idTeam={team_id}", file=sys.stderr)
            return []
        raise
    data = r.json() or {}
    players = data.get("player") or []
    # Keep only rugby
    return [p for p in players if (p.get("strSport") or "").lower().startswith("rugby")]


# --------------------- Utilities ---------------------
def _season_back(label: str) -> str:
    s = (label or "").strip()
    if len(s) >= 9 and s[4] in "-/":
        try:
            y = int(s[:4]) - 1
            return f"{y}-{y+1}"
        except Exception:
            pass
    try:
        y = int(s[:4]) - 1
        return str(y)
    except Exception:
        return s


def _parse_date(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    s = d.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _split_name(full_name: str) -> Tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], None
    return " ".join(parts[:-1]), parts[-1]


def _ensure_data_dir() -> str:
    out = os.path.join(os.getcwd(), "data")
    os.makedirs(out, exist_ok=True)
    return out


def _write_players_csv(rows: List[Dict[str, Any]], league_id: str, season: str) -> str:
    out_dir = _ensure_data_dir()
    path = os.path.join(out_dir, f"urc_players_{league_id}_{season}.csv")
    cols = [
        "idPlayer", "strPlayer", "strPosition", "strNationality", "dateBorn",
        "idTeam", "strTeam", "strSport"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in rows:
            w.writerow({
                "idPlayer": p.get("idPlayer"),
                "strPlayer": p.get("strPlayer"),
                "strPosition": p.get("strPosition"),
                "strNationality": p.get("strNationality"),
                "dateBorn": p.get("dateBorn"),
                "idTeam": p.get("idTeam"),
                "strTeam": p.get("strTeam"),
                "strSport": p.get("strSport"),
            })
    return path


# ---- Position normalization / classification ----
_FORWARD = {"prop", "hooker", "lock", "second row", "flanker", "back row", "number 8", "no 8", "no. 8", "no8"}
_BACK    = {"scrum-half", "scrum half", "halfback", "scrumhalf", "fly-half", "stand-off", "stand off",
            "first five", "centre", "center", "wing", "winger", "fullback", "full-back", "full back"}


def _norm_position_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = " ".join(s.strip().lower().split())
    # normalize some aliases
    aliases = {
        "second row": "lock",
        "half back": "scrum-half",
        "halfback": "scrum-half",
        "scrumhalf": "scrum-half",
        "stand off": "fly-half",
        "stand-off": "fly-half",
        "first five": "fly-half",
        "full back": "fullback",
        "full-back": "fullback",
        "center": "centre",
        "wing": "winger",
        "no 8": "number 8",
        "no. 8": "number 8",
    }
    return aliases.get(t, t)


def _position_category(norm: Optional[str]) -> str:
    if not norm:
        return "other"
    if norm in _FORWARD:
        return "forward"
    if norm in _BACK:
        return "back"
    return "other"


def _position_code(norm: Optional[str]) -> Optional[str]:
    if not norm:
        return None
    # RU_ + upper snake
    code = "RU_" + "".join(ch if ch.isalnum() else "_" for ch in norm.upper())
    code = "_".join(filter(None, code.split("_")))
    return code[:48]  # keep it short enough to be safe


# --------------------- DB helpers ---------------------
def _get_conn():
    if get_db_connection is not None:
        return get_db_connection()  # type: ignore
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("No db.connection.get_db_connection and no DATABASE_URL.")
    return psycopg2.connect(dsn)


def _ensure_players_tsdb_column(cur, verbose: bool=False) -> None:
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='players' AND column_name='tsdb_player_id' LIMIT 1;
    """)
    if cur.fetchone():
        return
    if verbose:
        print("[INFO] Adding players.tsdb_player_id (TEXT) with unique index")
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


def _ensure_position(cur, raw_position: Optional[str], verbose: bool=False) -> Optional[int]:
    """
    Find or create a position row that matches the given text.
    Returns position_id or None.
    """
    norm = _norm_position_text(raw_position)
    if not norm:
        return None
    code = _position_code(norm)
    cat = _position_category(norm)
    # Try by code first
    cur.execute("SELECT position_id FROM positions WHERE code = %s", (code,))
    row = cur.fetchone()
    if row:
        return row[0]
    # Try by name (case-insensitive)
    cur.execute("SELECT position_id FROM positions WHERE LOWER(name) = LOWER(%s) LIMIT 1", (norm,))
    row = cur.fetchone()
    if row:
        return row[0]
    # Insert new position
    if verbose:
        print(f"[INFO] Creating new position: code={code}, name={norm}, category={cat}")
    cur.execute(
        """
        INSERT INTO positions (code, name, category, number_min, number_max, created_at, updated_at)
        VALUES (%s, %s, %s, NULL, NULL, NOW(), NOW())
        RETURNING position_id
        """,
        (code, norm, cat),
    )
    return cur.fetchone()[0]


def _upsert_player(cur, p: Dict[str, Any], verbose: bool=False) -> str:
    """
    Upsert precedence:
      1) tsdb_player_id
      2) (full_name + date_of_birth) match to attach id
      3) (full_name) only, if DOB not available
    Returns: "inserted" | "updated_by_tsdb_id" | "matched_by_name_dob" | "matched_by_name" | "skipped"
    """
    tsdb_id = (p.get("idPlayer") or "").strip()
    full_name = (p.get("strPlayer") or "").strip()
    if not tsdb_id or not full_name:
        return "skipped"

    dob = _parse_date(p.get("dateBorn"))
    nat = (p.get("strNationality") or "").strip() or None
    pos_text = (p.get("strPosition") or "").strip()
    first, last = _split_name(full_name)
    # Resolve / create position
    position_id = _ensure_position(cur, pos_text, verbose=verbose)

    # 1) by tsdb_player_id
    cur.execute("SELECT player_id FROM players WHERE tsdb_player_id = %s", (tsdb_id,))
    row = cur.fetchone()
    if row:
        player_id = row[0]
        if verbose:
            print(f"  [DB] update player_id={player_id} via tsdb_player_id={tsdb_id}")
        cur.execute(
            """
            UPDATE players
               SET full_name = %s,
                   first_name = %s,
                   last_name = %s,
                   date_of_birth = %s,
                   nationality = %s,
                   preferred_position_id = %s,
                   updated_at = NOW()
             WHERE player_id = %s
            """,
            (full_name, first, last, dob, nat, position_id, player_id),
        )
        return "updated_by_tsdb_id"

    # 2) by (full_name + dob)
    if dob:
        cur.execute(
            """
            SELECT player_id
              FROM players
             WHERE LOWER(full_name) = LOWER(%s)
               AND date_of_birth = %s
            LIMIT 1
            """,
            (full_name, dob),
        )
        row = cur.fetchone()
        if row:
            player_id = row[0]
            if verbose:
                print(f"  [DB] attach tsdb_player_id={tsdb_id} to player_id={player_id} (name+dob)")
            cur.execute(
                """
                UPDATE players
                   SET tsdb_player_id = %s,
                       first_name = COALESCE(first_name, %s),
                       last_name  = COALESCE(last_name, %s),
                       nationality = COALESCE(nationality, %s),
                       preferred_position_id = COALESCE(preferred_position_id, %s),
                       updated_at = NOW()
                 WHERE player_id = %s
                """,
                (tsdb_id, first, last, nat, position_id, player_id),
            )
            return "matched_by_name_dob"

    # 3) by (full_name) only
    cur.execute(
        "SELECT player_id FROM players WHERE LOWER(full_name) = LOWER(%s) LIMIT 1",
        (full_name,),
    )
    row = cur.fetchone()
    if row:
        player_id = row[0]
        if verbose:
            print(f"  [DB] attach tsdb_player_id={tsdb_id} to player_id={player_id} (name only)")
        cur.execute(
            """
            UPDATE players
               SET tsdb_player_id = %s,
                   first_name = COALESCE(first_name, %s),
                   last_name  = COALESCE(last_name, %s),
                   date_of_birth = COALESCE(date_of_birth, %s),
                   nationality = COALESCE(nationality, %s),
                   preferred_position_id = COALESCE(preferred_position_id, %s),
                   updated_at = NOW()
             WHERE player_id = %s
            """,
            (tsdb_id, first, last, dob, nat, position_id, player_id),
        )
        return "matched_by_name"

    # Insert new
    if verbose:
        print(f"  [DB] insert new player '{full_name}' (tsdb_player_id={tsdb_id})")
    cur.execute(
        """
        INSERT INTO players (
            full_name, first_name, last_name, date_of_birth, nationality,
            preferred_position_id, espn_player_id, tsdb_player_id,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, NULL, %s,
            NOW(), NOW()
        )
        """,
        (full_name, first, last, dob, nat, position_id, tsdb_id),
    )
    return "inserted"


# --------------------- Main ---------------------
def main() -> None:
    import argparse

    _load_dotenv_if_available()

    ap = argparse.ArgumentParser(description="Ingest all URC players into Postgres.")
    ap.add_argument("--league-id", default="4446", help="TheSportsDB league id (URC=4446)")
    ap.add_argument("--api-key", default=os.getenv("THESPORTSDB_API_KEY", "752396"),
                    help="TheSportsDB V1 API key")
    ap.add_argument("--sleep-seconds", type=float, default=0.65,
                    help="Sleep between team/player calls (default 0.65s)")
    ap.add_argument("--max-seasons-back", type=int, default=1,
                    help="Scan current season plus N previous if needed (default 1)")
    ap.add_argument("--write-csv", action="store_true",
                    help="Write a single CSV snapshot of all players")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = ap.parse_args()

    key = args.api_key
    league_id = str(args.league_id)
    sleep_s = max(0.0, args.sleep_seconds)
    verbose = args.verbose
    max_back = max(1, args.max_seasons_back)

    if verbose:
        shown = key if len(key) <= 4 else key[:2] + "***" + key[-2:]
        print(f"[INFO] Using TSDB key '{shown}' | league_id={league_id}")

    sess = _session()

    # 1) League + current season
    league_meta = _lookup_league(sess, key, league_id, verbose=verbose)
    league_name = (league_meta.get("strLeague") or f"league-{league_id}").strip()
    current_season = (league_meta.get("strCurrentSeason") or "").strip()

    if verbose:
        print(f"[INFO] League: {league_name} | strCurrentSeason={current_season!r}")

    if not current_season:
        raise SystemExit("League meta missing strCurrentSeason; cannot proceed.")

    # 2) Build seasons to scan (current + N-1 previous)
    seasons: List[str] = []
    s = current_season
    for _ in range(max_back):
        seasons.append(s)
        s = _season_back(s)
    if verbose:
        print(f"[INFO] Seasons to scan for team IDs: {seasons}")

    # 3) Collect URC team IDs from events
    team_ids: List[str] = []
    seen: Set[str] = set()
    season_used = None

    for idx, season in enumerate(seasons, start=1):
        if verbose:
            print(f"[INFO] ({idx}/{len(seasons)}) Fetching events for season={season}")
        events = _events_for_season(sess, key, league_id, season, verbose=verbose)
        for e in events:
            hid = (e.get("idHomeTeam") or "").strip()
            aid = (e.get("idAwayTeam") or "").strip()
            if hid and hid not in seen:
                seen.add(hid); team_ids.append(hid)
            if aid and aid not in seen:
                seen.add(aid); team_ids.append(aid)
        if team_ids:
            season_used = season
            break

    if not team_ids:
        raise SystemExit("Could not derive any URC team IDs from schedule. Check API key/league_id.")

    if verbose:
        print(f"[INFO] Found {len(team_ids)} team IDs (season={season_used}): {sorted(team_ids)}")

    # 4) For each team → fetch players
    all_players: List[Dict[str, Any]] = []
    for i, tid in enumerate(team_ids, start=1):
        if verbose:
            print(f"[INFO] ({i}/{len(team_ids)}) lookup_all_players for idTeam={tid}")
        players = _lookup_all_players(sess, key, tid, verbose=verbose)
        if verbose:
            print(f"[INFO]   -> {len(players)} rugby players")
        all_players.extend(players)
        if sleep_s:
            time.sleep(sleep_s)

    if verbose:
        print(f"[INFO] Total rugby players gathered: {len(all_players)}")

    # Optional CSV snapshot
    if args.write_csv:
        season_for_csv = season_used or current_season
        path = _write_players_csv(all_players, league_id, season_for_csv)
        print(f"[OK] Wrote CSV snapshot: {path}")

    # 5) Upsert into DB
    conn = _get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        _ensure_players_tsdb_column(cur, verbose=verbose)

        inserted = 0
        updated_by_tsdb_id = 0
        matched_by_name_dob = 0
        matched_by_name = 0
        skipped = 0

        for p in all_players:
            outcome = _upsert_player(cur, p, verbose=verbose)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated_by_tsdb_id":
                updated_by_tsdb_id += 1
            elif outcome == "matched_by_name_dob":
                matched_by_name_dob += 1
            elif outcome == "matched_by_name":
                matched_by_name += 1
            else:
                skipped += 1

        conn.commit()
        print(
            "[OK] Players upsert complete -> "
            f"inserted={inserted}, updated_by_tsdb_id={updated_by_tsdb_id}, "
            f"matched_by_name_dob={matched_by_name_dob}, matched_by_name={matched_by_name}, "
            f"skipped={skipped}"
        )

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Ingestion failed; rolled back: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
