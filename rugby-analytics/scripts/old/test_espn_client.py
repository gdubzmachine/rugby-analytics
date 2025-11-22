# scripts/test_espn_client.py

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl.espn_client import fetch_rugby_scoreboard, fetch_rugby_summary


def main():
    print("Starting test_espn_client...")

    # Try Six Nations opening weekend 2023.
    league_slug = "sixnations"
    date = "20230204"

    print(f"Fetching scoreboard for league '{league_slug}' on {date}...")

    try:
        sb = fetch_rugby_scoreboard(league_slug, date)
        print("Scoreboard fetched OK.")
    except Exception as e:
        print("ERROR while fetching scoreboard:")
        import traceback
        traceback.print_exc()
        return

    # Print some basic info so we can see structure
    print("\nTop-level scoreboard keys:", list(sb.keys()))
    events = sb.get("events") or []
    print("Number of events in scoreboard:", len(events))

    if not events:
        print("No events returned; we may need to adjust league slug or date.")
        return

    first_event_id = events[0].get("id")
    print(f"\nFirst event id: {first_event_id}")
    print("Fetching summary for that event...")

    try:
        summ = fetch_rugby_summary(league_slug, first_event_id)
        print("Summary fetched OK.")
    except Exception as e:
        print("ERROR while fetching summary:")
        import traceback
        traceback.print_exc()
        return

    print("\nSummary top-level keys:", list(summ.keys()))
    print("Done test_espn_client.")


if __name__ == "__main__":
    main()
