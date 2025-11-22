# scripts/seed_example_match_events.py

import sys, os
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import engine


def get_example_match(conn):
    """Fetch our Wales vs Ireland Six Nations 2023 match."""
    row = conn.execute(
        text(
            """
            SELECT m.match_id,
                   ht.team_id AS home_team_id,
                   at.team_id AS away_team_id,
                   ht.name    AS home_team_name,
                   at.name    AS away_team_name,
                   m.home_score,
                   m.away_score
            FROM matches m
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            JOIN seasons s ON m.season_id = s.season_id
            JOIN leagues l ON m.league_id = l.league_id
            WHERE l.name = :league_name
              AND s.year = :season_year
              AND ht.name = :home_name
              AND at.name = :away_name
            ORDER BY m.match_id DESC
            LIMIT 1
            """
        ),
        {
            "league_name": "Six Nations",
            "season_year": 2023,
            "home_name": "Wales",
            "away_name": "Ireland",
        },
    ).mappings().first()
    return row


def get_player_id(conn, full_name: str):
    return conn.execute(
        text("SELECT player_id FROM players WHERE full_name = :full_name"),
        {"full_name": full_name},
    ).scalar_one_or_none()


def events_already_exist(conn, match_id: int) -> bool:
    return (
        conn.execute(
            text("SELECT 1 FROM match_events WHERE match_id = :match_id LIMIT 1"),
            {"match_id": match_id},
        ).scalar_one_or_none()
        is not None
    )


def main():
    with engine.begin() as conn:
        match_row = get_example_match(conn)
        if not match_row:
            print("Example match not found. Run seed_example_match.py first.")
            return

        match_id = match_row["match_id"]
        home_team_id = match_row["home_team_id"]
        away_team_id = match_row["away_team_id"]

        print(
            f"Using match_id={match_id} ({match_row['home_team_name']} vs {match_row['away_team_name']})"
        )

        if events_already_exist(conn, match_id):
            print("match_events already exist for this match. Skipping insert.")
            return

        # Try to use our example players if they exist
        wales_9_id = get_player_id(conn, "Example Wales Scrum-half")
        ireland_10_id = get_player_id(conn, "Example Ireland Fly-half")

        # Simple event timeline:
        #  10' PEN to Wales (3 pts)
        #  25' TRY to Ireland (5 pts)
        #  27' CON to Ireland (2 pts)
        #  50' TRY to Ireland (5 pts)
        #  60' PEN to Ireland (3 pts)

        events = [
            {
                "minute": 10,
                "second": 0,
                "period": 1,
                "team_id": home_team_id,
                "player_id": None,  # unknown kicker
                "event_type": "PEN",
                "points": 3,
                "description": "Penalty goal to Wales",
            },
            {
                "minute": 25,
                "second": 0,
                "period": 1,
                "team_id": away_team_id,
                "player_id": ireland_10_id,
                "event_type": "TRY",
                "points": 5,
                "description": "Try to Ireland (fly-half support line)",
            },
            {
                "minute": 27,
                "second": 0,
                "period": 1,
                "team_id": away_team_id,
                "player_id": ireland_10_id,
                "event_type": "CON",
                "points": 2,
                "description": "Conversion by Ireland fly-half",
            },
            {
                "minute": 50,
                "second": 0,
                "period": 2,
                "team_id": away_team_id,
                "player_id": ireland_10_id,
                "event_type": "TRY",
                "points": 5,
                "description": "Second-half try to Ireland",
            },
            {
                "minute": 60,
                "second": 0,
                "period": 2,
                "team_id": away_team_id,
                "player_id": ireland_10_id,
                "event_type": "PEN",
                "points": 3,
                "description": "Penalty goal to Ireland",
            },
        ]

        for ev in events:
            conn.execute(
                text(
                    """
                    INSERT INTO match_events (
                        match_id,
                        event_time,
                        event_second,
                        period,
                        team_id,
                        player_id,
                        assist_player_id,
                        event_type,
                        points,
                        description
                    )
                    VALUES (
                        :match_id,
                        :event_time,
                        :event_second,
                        :period,
                        :team_id,
                        :player_id,
                        NULL,
                        :event_type,
                        :points,
                        :description
                    )
                    """
                ),
                {
                    "match_id": match_id,
                    "event_time": ev["minute"],
                    "event_second": ev["second"],
                    "period": ev["period"],
                    "team_id": ev["team_id"],
                    "player_id": ev["player_id"],
                    "event_type": ev["event_type"],
                    "points": ev["points"],
                    "description": ev["description"],
                },
            )

        print(f"Inserted {len(events)} match_events rows for this match.")


if __name__ == "__main__":
    main()
