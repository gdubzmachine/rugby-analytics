#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ingest_positions_from_players.py
--------------------------------

Populate the `positions` table based on distinct position text values
found in the `players.tsdb_position_text` column, and optionally link
players to positions via players.preferred_position_id.

Assumptions:
- Table `players` has:
    - tsdb_position_text TEXT (raw TSDB string, e.g. 'Prop', 'Scrum-half')
    - preferred_position_id BIGINT NULL (FK → positions.position_id)
- Table `positions` has:
    - position_id (PK, BIGSERIAL)
    - code (UNIQUE, e.g. 'PROP', 'LOCK', 'SCRUM_HALF')
    - name (human readable)
    - category (ENUM or TEXT: 'forward', 'back', 'other')
    - number_min, number_max (INT, nullable)
    - created_at, updated_at (TIMESTAMPTZ)

Usage:
    python .\\scripts\\ingest_positions_from_players.py --update-players --write-csv -v

Flags:
    --update-players   Also set players.preferred_position_id based on mapping
    --write-csv        Write ./data/positions_from_players.csv snapshot
    -v / --verbose     Verbose logging
"""

import os
import sys
import csv
from typing import Dict, Optional, Tuple, Any, List

# ---------------------------------------------
# Ensure project root on sys.path
# ---------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------
# DB imports
# ---------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import DictCursor
except Exception:
    print("Missing psycopg2. Install: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# Optional db helper
try:
    from db.connection import get_db_connection  # type: ignore
except Exception:
    get_db_connection = None  # type: ignore


# ---------------------------------------------
# Env helper (to read .env → environment)
# ---------------------------------------------
def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # If python-dotenv is not installed, just ignore;
        # environment variables may already be set.
        pass


# ---------------------------------------------
# DB connection helper
# ---------------------------------------------
def _get_conn():
    """
    Prefer db.connection.get_db_connection(), else DATABASE_URL.
    """
    if get_db_connection is not None:
        return get_db_connection()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set and db.connection.get_db_connection() missing. "
            "Set DATABASE_URL or create db/connection.py with get_db_connection()."
        )
    return psycopg2.connect(dsn)


# ---------------------------------------------
# Position mapping logic
# ---------------------------------------------

def _slugify(text: str) -> str:
    out: List[str] = []
    prev_dash = False
    for ch in text.upper():
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append("_")
                prev_dash = True
    slug = "".join(out).strip("_")
    return slug or "POSITION"


def _normalize_pos_text(pos: str) -> str:
    return pos.strip().lower()


# Basic rugby-union position mapping.
# Keys are lowercased TSDB position strings.
# Values: (code, name, category, number_min, number_max)
_POSITION_MAP: Dict[str, Tuple[str, str, str, Optional[int], Optional[int]]] = {
    # Front row / props / hooker
    "prop": ("PROP", "Prop", "forward", 1, 3),
    "loosehead prop": ("LHP", "Loosehead Prop", "forward", 1, 1),
    "tighthead prop": ("THP", "Tighthead Prop", "forward", 3, 3),
    "hooker": ("HK", "Hooker", "forward", 2, 2),

    # Locks
    "lock": ("LOCK", "Lock", "forward", 4, 5),
    "second row": ("LOCK", "Lock", "forward", 4, 5),

    # Back row
    "back row": ("BACK_ROW", "Back Row", "forward", 6, 8),
    "flanker": ("FLANKER", "Flanker", "forward", 6, 7),
    "openside flanker": ("OSF", "Openside Flanker", "forward", 7, 7),
    "blindside flanker": ("BSF", "Blindside Flanker", "forward", 6, 6),
    "number 8": ("NO8", "Number 8", "forward", 8, 8),

    # Half-backs
    "scrum-half": ("SCRUM_HALF", "Scrum-half", "back", 9, 9),
    "scrum half": ("SCRUM_HALF", "Scrum-half", "back", 9, 9),
    "half back": ("SCRUM_HALF", "Scrum-half", "back", 9, 9),
    "fly-half": ("FLY_HALF", "Fly-half", "back", 10, 10),
    "fly half": ("FLY_HALF", "Fly-half", "back", 10, 10),
    "outside half": ("FLY_HALF", "Fly-half", "back", 10, 10),

    # Centres
    "centre": ("CENTRE", "Centre", "back", 12, 13),
    "center": ("CENTRE", "Centre", "back", 12, 13),
    "inside centre": ("IC", "Inside Centre", "back", 12, 12),
    "inside center": ("IC", "Inside Centre", "back", 12, 12),
    "outside centre": ("OC", "Outside Centre", "back", 13, 13),
    "outside center": ("OC", "Outside Centre", "back", 13, 13),

    # Wings
    "wing": ("WING", "Wing", "back", 11, 14),
    "winger": ("WING", "Wing", "back", 11, 14),
    "left wing": ("LW", "Left Wing", "back", 11, 11),
    "right wing": ("RW", "Right Wing", "back", 14, 14),

    # Fullback
    "fullback": ("FULLBACK", "Fullback", "back", 15, 15),
    "full-back": ("FULLBACK", "Fullback", "back", 15, 15),

    # Utility / generic
    "utility back": ("UTILITY_BACK", "Utility Back", "back", None, None),
    "back": ("BACK", "Back", "back", None, None),
    "forward": ("FORWARD", "Forward", "forward", None, None),

    # Combo examples
    "hooker / prop": ("FRONT_ROW", "Front Row", "forward", 1, 3),
    "prop / hooker": ("FRONT_ROW", "Front Row", "forward", 1, 3),
}


def map_position_text(pos_text: str) -> Tuple[str, str, str, Optional[int], Optional[int]]:
    """
    Given a raw tsdb_position_text, return a tuple:
        (code, name, category, number_min, number_max)

    If we don't have a hard-coded mapping, create a generic code and
    mark category as 'other'.
    """
    norm = _normalize_pos_text(pos_text)
    if norm in _POSITION_MAP:
        return _POSITION_MAP[norm]

    # Heuristic: classify forwards vs backs
    cat = "other"
    if any(x in norm for x in ["prop", "hooker", "lock", "flanker", "back row", "number 8", "forward"]):
        cat = "forward"
    elif any(x in norm for x in ["wing", "fullback", "centre", "center", "half", "scrum"]):
        cat = "back"

    code = _slugify(pos_text)
    name = pos_text.strip() or "Unknown"
    return code, name, cat, None, None


# ---------------------------------------------
# Positions upsert + linking players
# ---------------------------------------------

def _get_distinct_position_texts(cur, verbose: bool = False) -> List[str]:
    cur.execute(
        """
        SELECT DISTINCT tsdb_position_text
        FROM players
        WHERE tsdb_position_text IS NOT NULL
          AND tsdb_position_text <> ''
        ORDER BY tsdb_position_text
        """
    )
    rows = cur.fetchall()
    pos_texts = [r[0] for r in rows if r[0]]
    if verbose:
        print(f"[INFO] Found {len(pos_texts)} distinct tsdb_position_text values")
    return pos_texts


def _upsert_position(
    cur,
    code: str,
    name: str,
    category: str,
    number_min: Optional[int],
    number_max: Optional[int],
    verbose: bool = False,
) -> int:
    """
    Upsert into positions using `code` as the natural key.
    Returns position_id.
    """
    cur.execute(
        "SELECT position_id FROM positions WHERE code = %s",
        (code,),
    )
    row = cur.fetchone()
    if row:
        position_id = row[0]
        if verbose:
            print(f"  [UPDATE] position_id={position_id} code={code}")
        cur.execute(
            """
            UPDATE positions
            SET name = %s,
                category = %s,
                number_min = %s,
                number_max = %s,
                updated_at = NOW()
            WHERE position_id = %s
            """,
            (name, category, number_min, number_max, position_id),
        )
        return position_id

    if verbose:
        print(f"  [INSERT] code={code} name={name} category={category}")
    cur.execute(
        """
        INSERT INTO positions (
            code,
            name,
            category,
            number_min,
            number_max,
            created_at,
            updated_at
        )
        VALUES (%s,%s,%s,%s,%s, NOW(), NOW())
        RETURNING position_id
        """,
        (code, name, category, number_min, number_max),
    )
    return cur.fetchone()[0]


def _link_players_to_position(
    cur,
    tsdb_pos_text: str,
    position_id: int,
    verbose: bool = False,
) -> int:
    """
    Update players.preferred_position_id for players with this tsdb_position_text.
    Returns number of updated rows.
    """
    cur.execute(
        """
        UPDATE players
        SET preferred_position_id = %s,
            updated_at = NOW()
        WHERE tsdb_position_text = %s
          AND (preferred_position_id IS NULL OR preferred_position_id <> %s)
        """,
        (position_id, tsdb_pos_text, position_id),
    )
    updated = cur.rowcount
    if verbose and updated:
        print(f"    [PLAYERS] updated {updated} rows for position_id={position_id}")
    return updated


def _write_csv_snapshot(rows: List[Dict[str, Any]]) -> str:
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", "positions_from_players.csv")
    cols = ["code", "name", "category", "number_min", "number_max"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path


# ---------------------------------------------
# Main
# ---------------------------------------------

def main() -> None:
    import argparse

    # Load .env so DATABASE_URL is available
    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Populate `positions` from players.tsdb_position_text and optionally link players."
    )
    parser.add_argument(
        "--update-players",
        action="store_true",
        help="Also set players.preferred_position_id based on the mapping.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write ./data/positions_from_players.csv snapshot.",
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
        pos_texts = _get_distinct_position_texts(cur, verbose=verbose)

        created_or_updated: List[Dict[str, Any]] = []
        total_players_linked = 0

        for pos_text in pos_texts:
            if verbose:
                print(f"[POS] Handling raw position text: {pos_text!r}")

            code, name, category, num_min, num_max = map_position_text(pos_text)
            position_id = _upsert_position(cur, code, name, category, num_min, num_max, verbose=verbose)

            created_or_updated.append(
                {
                    "code": code,
                    "name": name,
                    "category": category,
                    "number_min": num_min,
                    "number_max": num_max,
                }
            )

            if args.update_players:
                updated = _link_players_to_position(cur, pos_text, position_id, verbose=verbose)
                total_players_linked += updated

        conn.commit()

        print(f"[DONE] Positions upserted: {len(created_or_updated)}")
        if args.update_players:
            print(f"[DONE] Players linked to positions: {total_players_linked}")

        if args.write_csv:
            path = _write_csv_snapshot(created_or_updated)
            print(f"[OK] Wrote CSV snapshot: {path}")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Failed, rolled back transaction: {exc}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
