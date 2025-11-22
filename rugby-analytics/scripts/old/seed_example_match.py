# scripts/seed_example_match.py

import sys, os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from db.connection import engine


def get_or_create_league(conn, sport_code: str, league_name: str, short_name: str | None = None):
    # Get sport_id for rugby_union (we seeded this in the schema)
    sport_id = conn.execute(
        text("SELECT sport_id FROM sports WHERE code = :code"),
        {"code": sport_code},
    ).scalar_one()

    league_id = conn.execute(
        text("SELECT league_id FROM leagues WHERE name = :name"),
        {"name": league_name},
    ).scalar_one_or_none()

    if league_id is None:
        league_id = conn.execute(
            text(
                """
                INSERT INTO leagues (sport_id, name, short_name)
                VALUES (:sport_id, :name, :short_name)
                RETURNING league_id
                """
            ),
            {"sport_id": sport_id, "name": league_name, "short_name": short_name},
        ).scalar_one()

    return league_id


def get_or_create_season(conn, league_id: int, year: int):
    season_id = conn.execute(
        text(
            """
            SELECT season_id FROM seasons
            WHERE league_id = :league_id AND year = :year
            """
        ),
        {"league_id": league_id, "year": year},
    ).scalar_one_or_none()

    if season_id is None:
        season_id = conn.execute(
            text(
                """
                INSERT INTO seasons (league_id, year, label, start_date, end_date)
                VALUES (:league_id, :year, :label, :start_date, :end_date)
                RETURNING season_id
                """
            ),
            {
                "league_id": league_id,
                "year": year,
                "label": f"{year} Season",
                "start_date": datetime(year, 1, 1).date(),
                "end_date": datetime(year, 12, 31).date(),
            },
        ).scalar_one()

    return season_id


def get_or_create_team(conn, name: str, short_name: str | None = None, country: str | None = None):
    team_id = conn.execute(
        text("SELECT team_id FROM teams WHERE name = :name"),
        {"name": name},
    ).scalar_one_or_none()

    if team_id is None:
        team_id = conn.execute(
            text(
                """
                INSERT INTO teams (name, short_name, country)
                VALUES (:name, :short_name, :country)
                RETURNING team_id
                """
            ),
            {"name": name, "short_name": short_name, "country": country},
        ).scalar_one()

    return team_id


def get_or_create_venue(conn, name: str, city: str | None = None, country: str | None = None):
    venue_id = conn.execute(
        text("SELECT venue_id FROM venues WHERE name = :name"),
        {"name": name},
    ).scalar_one_or_none()

    if venue_id is None:
        venue_id = conn.execute(
            text(
                """
                INSERT INTO venues (name, city, country)
                VALUES (:name, :city, :country)
                RETURNING venue_id
                """
            ),
            {"name": name, "city": city, "country": country},
        ).scalar_one()

    return venue_id


def main():
    # Example match: Wales 10–34 Ireland, 2023-02-04, Six Nations @ Millennium Stadium (Cardiff)
    match_date = datetime(2023, 2, 4, 14, 15)  # arbitrary kickoff time

    with engine.begin() as conn:
        league_id = get_or_create_league(conn, "rugby_union", "Six Nations", "SN")
        season_id = get_or_create_season(conn, league_id, 2023)

        home_team_id = get_or_create_team(conn, "Wales", country="Wales")
        away_team_id = get_or_create_team(conn, "Ireland", country="Ireland")

        venue_id = get_or_create_venue(
            conn,
            "Millennium Stadium",
            city="Cardiff",
            country="Wales",
        )

        # Avoid duplicate insert if we run this script multiple times
        existing_match_id = conn.execute(
            text(
                """
                SELECT match_id
                FROM matches
                WHERE season_id = :season_id
                  AND home_team_id = :home_team_id
                  AND away_team_id = :away_team_id
                  AND kickoff_utc = :kickoff_utc
                """
            ),
            {
                "season_id": season_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "kickoff_utc": match_date,
            },
        ).scalar_one_or_none()

        if existing_match_id:
            print(f"Match already exists with id {existing_match_id}")
            return

        match_id = conn.execute(
            text(
                """
                INSERT INTO matches (
                    league_id, season_id, espn_event_id,
                    status, kickoff_utc, round_label, stage,
                    venue_id, home_team_id, away_team_id,
                    home_score, away_score, attendance, source
                )
                VALUES (
                    :league_id, :season_id, :espn_event_id,
                    'final', :kickoff_utc, :round_label, :stage,
                    :venue_id, :home_team_id, :away_team_id,
                    :home_score, :away_score, :attendance, 'manual'
                )
                RETURNING match_id
                """
            ),
            {
                "league_id": league_id,
                "season_id": season_id,
                "espn_event_id": None,
                "kickoff_utc": match_date,
                "round_label": "Round 1",
                "stage": "Six Nations 2023",
                "venue_id": venue_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_score": 10,
                "away_score": 34,
                "attendance": 73000,
            },
        ).scalar_one()

        print(f"Inserted example match with id {match_id}")


if __name__ == "__main__":
    main()
