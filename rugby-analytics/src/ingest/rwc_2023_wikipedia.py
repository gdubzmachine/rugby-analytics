import sys
from pathlib import Path
from io import StringIO

import requests
import pandas as pd

# --- Ensure project root is on sys.path ---
ROOT_DIR = Path(__file__).resolve().parents[2]  # .../rugby-analytics
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

RWC_2023_URL = "https://en.wikipedia.org/wiki/2023_Rugby_World_Cup"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}


def fetch_tables():
    print(f"Requesting: {RWC_2023_URL}")
    resp = requests.get(RWC_2023_URL, headers=HEADERS, timeout=30)
    print("HTTP status code:", resp.status_code)
    resp.raise_for_status()

    html = resp.text
    tables = pd.read_html(StringIO(html))
    print(f"Found {len(tables)} tables on the page")
    return tables


def looks_like_match_table(df: pd.DataFrame) -> bool:
    """
    Heuristic: a match table should have:
    - a date-ish column
    - at least one column mentioning 'Team' or 'Nation'
    - some kind of score/result column
    - ideally a venue column
    """
    cols = [str(c) for c in df.columns]

    has_team = any("Team" in c or "Nation" in c for c in cols)
    has_score = any("Score" in c or "Result" in c for c in cols)
    has_date = any("Date" in c for c in cols)

    return has_team and has_score and has_date


def parse_score(score_str: str):
    """
    Parse a score string like '18–3' or '18-3' into (18, 3).
    If parsing fails, return (None, None).
    """
    if not isinstance(score_str, str):
        return None, None

    # Normalise dash types
    s = score_str.replace("–", "-").strip()
    if "-" not in s:
        return None, None

    left, right = s.split("-", 1)
    try:
        home = int(left.strip())
        away = int(right.strip())
        return home, away
    except ValueError:
        return None, None


def clean_match_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Try to standardise one match table to:
    match_date, home_team, away_team, home_score, away_score, venue
    """
    cols = [str(c) for c in df.columns]
    col_map = {str(c): c for c in df.columns}

    # Find key columns by fuzzy name
    date_col_name = next(c for c in cols if "Date" in c)
    team_cols = [c for c in cols if "Team" in c or "Nation" in c]

    if len(team_cols) < 2:
        # Sometimes team columns are under a MultiIndex or weird structure;
        # for now, bail out gracefully
        print("  ! Not enough team columns, skipping this table.")
        return pd.DataFrame()

    score_col_name = next(c for c in cols if "Score" in c or "Result" in c)
    venue_col_name = next((c for c in cols if "Venue" in c or "Stadium" in c), None)

    home_col = team_cols[0]
    away_col = team_cols[1]

    # Build a slim dataframe
    keep_cols = [date_col_name, home_col, score_col_name, away_col]
    if venue_col_name:
        keep_cols.append(venue_col_name)

    slim = df[[col_map[c] for c in keep_cols]].copy()

    # Standardise column names
    rename_map = {
        col_map[date_col_name]: "match_date",
        col_map[home_col]: "home_team",
        col_map[away_col]: "away_team",
        col_map[score_col_name]: "score_raw",
    }
    if venue_col_name:
        rename_map[col_map[venue_col_name]] = "venue"

    slim = slim.rename(columns=rename_map)

    # Parse scores
    slim["home_score"], slim["away_score"] = zip(
        *slim["score_raw"].map(parse_score)
    )

    # Clean up date to string for now (we can parse to proper dates later)
    slim["match_date"] = slim["match_date"].astype(str).str.strip()

    # Drop rows without scores or teams
    slim = slim.dropna(subset=["home_team", "away_team"])

    return slim[["match_date", "home_team", "away_team", "home_score", "away_score"]
                + (["venue"] if "venue" in slim.columns else [])]


def main():
    tables = fetch_tables()

    match_tables = []
    for i, df in enumerate(tables):
        if looks_like_match_table(df):
            print(f"\n=== Table {i} looks like a match table ===")
            cleaned = clean_match_table(df)
            if not cleaned.empty:
                print(cleaned.head(5))
                match_tables.append(cleaned)
            else:
                print("  (cleaned version is empty)")

    if not match_tables:
        print("\nNo match tables detected with current heuristic.")
        return

    # Combine all detected match tables into one big dataframe
    all_matches = pd.concat(match_tables, ignore_index=True).drop_duplicates()

    print("\n\n=== COMBINED MATCHES SAMPLE (first 20 rows) ===")
    print(all_matches.head(20))


if __name__ == "__main__":
    main()
