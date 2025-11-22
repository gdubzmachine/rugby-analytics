import sys
from pathlib import Path
from io import StringIO
from datetime import date

import requests
import pandas as pd
from sqlalchemy import text

# --- Ensure project root (rugby-analytics) is on sys.path ---
# File is .../rugby-analytics/src/ingest/load_rankings_to_db.py
ROOT_DIR = Path(__file__).resolve().parents[2]  # parents[0]=ingest, [1]=src, [2]=rugby-analytics
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db.connection import get_engine


WIKI_URL = "https://en.wikipedia.org/wiki/World_Rugby_Rankings"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}


def fetch_rankings_df() -> pd.DataFrame:
    print(f"Requesting: {WIKI_URL}")
    resp = requests.get(WIKI_URL, headers=HEADERS, timeout=30)
    print("HTTP status code:", resp.status_code)
    resp.raise_for_status()

    html = resp.text
    tables = pd.read_html(StringIO(html))
    print(f"Found {len(tables)} tables on the page")

    # Pick first table that has Rank + Team
    rankings = None
    for i, df in enumerate(tables):
        cols = [str(c) for c in df.columns]
        print(f"Table {i} columns: {cols}")
        if any("Rank" in c for c in cols) and any("Team" in c for c in cols):
            print(f"\n✅ Using table #{i} as the current rankings table")
            rankings = df
            break

    if rankings is None:
        raise RuntimeError("Could not find rankings table with Rank + Team")

    # Normalise columns
    col_map = {str(c): c for c in rankings.columns}
    rank_col = next(c for c in col_map if "Rank" in c)
    team_col = next(c for c in col_map if "Team" in c)
    points_col = next(c for c in col_map if "Point" in c or "Rating" in c)

    df_clean = rankings[[col_map[rank_col], col_map[team_col], col_map[points_col]]].copy()
    df_clean.columns = ["ranking_position", "team_name", "ranking_points"]

    df_clean["ranking_position"] = df_clean["ranking_position"].astype(int)
    df_clean["ranking_points"] = df_clean["ranking_points"].astype(float)

    print("\n=== Cleaned rankings sample ===")
    print(df_clean.head(10))

    return df_clean


def test_insert_team(engine):
    print("\n[TEST] Inserting a single dummy team 'Test XV'...")
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO teams (name)
                VALUES ('Test XV')
                ON CONFLICT (name) DO NOTHING
            """)
        )
    print("[TEST] Insert done. Check in pgAdmin if 'Test XV' appears in teams.")


def upsert_rankings(engine, df: pd.DataFrame, ranking_date: date):
    with engine.begin() as conn:
        team_names = df["team_name"].unique()
        print(f"\nEnsuring {len(team_names)} teams exist in teams table...")

        # Ensure teams exist
        for name in team_names:
            conn.execute(
                text("""
                    INSERT INTO teams (name)
                    VALUES (:name)
                    ON CONFLICT (name) DO NOTHING
                """),
                {"name": name}
            )

        print(f"Upserting {len(df)} ranking rows for date {ranking_date}...")
        for _, row in df.iterrows():
            team_name = row["team_name"]
            position = int(row["ranking_position"])
            points = float(row["ranking_points"])

            # Look up team_id
            team_id = conn.execute(
                text("SELECT team_id FROM teams WHERE name = :name"),
                {"name": team_name}
            ).scalar_one()

            conn.execute(
                text("""
                    INSERT INTO rankings (ranking_date, team_id, ranking_points, ranking_position)
                    VALUES (:ranking_date, :team_id, :points, :position)
                    ON CONFLICT (ranking_date, team_id)
                    DO UPDATE SET
                        ranking_points = EXCLUDED.ranking_points,
                        ranking_position = EXCLUDED.ranking_position
                """),
                {
                    "ranking_date": ranking_date,
                    "team_id": team_id,
                    "points": points,
                    "position": position,
                }
            )

    print("\n Rankings upsert complete.")
    


def main():
    engine = get_engine()
    print("Connected to DB engine.")

    # 1) Test insert dummy team
    test_insert_team(engine)

    # 2) Fetch rankings from Wikipedia
    rankings_df = fetch_rankings_df()

    # 3) Upsert rankings
    today = date.today()
    print(f"\nUsing ranking_date = {today}")
    upsert_rankings(engine, rankings_df, today)


if __name__ == "__main__":
    main()
