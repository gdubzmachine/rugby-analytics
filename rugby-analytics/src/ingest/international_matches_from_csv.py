import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# --- Ensure project root is on sys.path ---
ROOT_DIR = Path(__file__).resolve().parents[2]  # .../rugby-analytics
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db.connection import get_engine  # noqa: E402


CSV_PATH = ROOT_DIR / "data" / "rugby_matches_full.csv"


def load_matches_from_csv() -> pd.DataFrame:
    print(f"Loading matches from: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, encoding="latin1")

    expected_cols = {
        "match_date",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "venue",
        "competition_name",
        "competition_short_name",
        "level",
        "region",
        "source_tag",
    }
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {missing}")

    print("\nSample from CSV:")
    print(df.head(10))

    # Ensure types
    df["match_date"] = pd.to_datetime(df["match_date"], format="%Y-%m-%d").dt.date
    df["home_score"] = df["home_score"].astype(int)
    df["away_score"] = df["away_score"].astype(int)
    df["venue"] = df["venue"].astype(str).str.strip()
    df["competition_name"] = df["competition_name"].astype(str).str.strip()
    df["competition_short_name"] = df["competition_short_name"].astype(str).str.strip()
    df["level"] = df["level"].astype(str).str.strip()
    df["region"] = df["region"].astype(str).str.strip()
    df["source_tag"] = df["source_tag"].astype(str).str.strip()

    return df


def ensure_competition(conn, name: str, short_name: str, level: str, region: str, source: str) -> int:
    """
    Ensure a row exists in competitions for the given name and return its competition_id.
    """
    conn.execute(
        text("""
            INSERT INTO competitions (name, short_name, level, region, source)
            VALUES (:name, :short_name, :level, :region, :source)
            ON CONFLICT (name) DO UPDATE
            SET short_name = EXCLUDED.short_name,
                level      = EXCLUDED.level,
                region     = EXCLUDED.region,
                source     = EXCLUDED.source
        """),
        {
            "name": name,
            "short_name": short_name,
            "level": level,
            "region": region,
            "source": source,
        },
    )

    comp_id = conn.execute(
        text("SELECT competition_id FROM competitions WHERE name = :name"),
        {"name": name},
    ).scalar_one()

    return comp_id


def ensure_team(conn, team_name: str) -> int:
    """
    Ensure a row exists in teams for the given name and return team_id.
    """
    conn.execute(
        text("""
            INSERT INTO teams (name)
            VALUES (:name)
            ON CONFLICT (name) DO NOTHING
        """),
        {"name": team_name},
    )

    team_id = conn.execute(
        text("SELECT team_id FROM teams WHERE name = :name"),
        {"name": team_name},
    ).scalar_one()

    return team_id


def upsert_matches(engine, df: pd.DataFrame):
    with engine.begin() as conn:
        # Cache competition IDs to avoid re-querying for every row
        competition_cache: dict[str, int] = {}

        print(f"\nUpserting {len(df)} matches from master CSV...")
        for _, row in df.iterrows():
            comp_key = row["competition_name"]

            if comp_key not in competition_cache:
                competition_id = ensure_competition(
                    conn,
                    name=row["competition_name"],
                    short_name=row["competition_short_name"],
                    level=row["level"],
                    region=row["region"],
                    source=row["source_tag"],
                )
                competition_cache[comp_key] = competition_id
            else:
                competition_id = competition_cache[comp_key]

            home_team_name = row["home_team"]
            away_team_name = row["away_team"]

            home_team_id = ensure_team(conn, home_team_name)
            away_team_id = ensure_team(conn, away_team_name)

            conn.execute(
                text("""
                    INSERT INTO matches (
                        match_date,
                        home_team_id,
                        away_team_id,
                        home_score,
                        away_score,
                        competition_id,
                        venue,
                        source,
                        status
                    )
                    VALUES (
                        :match_date,
                        :home_team_id,
                        :away_team_id,
                        :home_score,
                        :away_score,
                        :competition_id,
                        :venue,
                        :source,
                        :status
                    )
                    ON CONFLICT (match_date, home_team_id, away_team_id, competition_id)
                    DO UPDATE SET
                        home_score = EXCLUDED.home_score,
                        away_score = EXCLUDED.away_score,
                        venue      = EXCLUDED.venue,
                        status     = EXCLUDED.status,
                        source     = EXCLUDED.source
                """),
                {
                    "match_date": row["match_date"],
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "home_score": int(row["home_score"]),
                    "away_score": int(row["away_score"]),
                    "competition_id": competition_id,
                    "venue": row["venue"],
                    "source": row["source_tag"],
                    "status": "finished",
                }
            )

    print("\n International matches upsert complete.")


def main():
    engine = get_engine()
    df = load_matches_from_csv()
    upsert_matches(engine, df)


if __name__ == "__main__":
    main()
