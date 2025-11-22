# scripts/discover_espn_rugby.py
import sys, os, json
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
USER_AGENT = os.getenv("ESPN_USER_AGENT", "RugbyAnalytics/1.0 (discovery)")

def safe_get(session, url, params=None, timeout=30):
    """Safe request with error handling"""
    print(f"Requesting: {url}")
    try:
        resp = session.get(url, params=params, timeout=timeout)
        print(f"Status code: {resp.status_code}")
        if resp.status_code == 404:
            print("❌ 404: Endpoint not found. Check URL.")
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def main():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # Step 1: Discover ALL sports (find rugby)
    sports_url = "https://sports.core.api.espn.com/v2/sports"
    sports_data = safe_get(session, sports_url)
    if not sports_data:
        print("Failed to fetch sports. Exiting.")
        return

    # Find rugby sports
    rugby_sports = [
        s for s in sports_data.get("items", [])
        if "rugby" in s.get("name", "").lower()
    ]
    print(f"\nFound {len(rugby_sports)} rugby sports:")
    for sport in rugby_sports:
        print(f" - {sport.get('name')} (ID: {sport.get('id')}, slug: {sport.get('slug')})")

    if not rugby_sports:
        print("No rugby sports found. ESPN might not categorize it as expected.")
        return

    # Step 2: Get leagues under rugby (use first rugby sport, usually 'rugby')
    rugby_sport = rugby_sports[0]  # Assume first is union
    leagues_url = f"https://sports.core.api.espn.com/v2/sports/{rugby_sport.get('slug')}/leagues"
    leagues_data = safe_get(session, leagues_url)
    if not leagues_data:
        print("Failed to fetch leagues. Trying alternative...")
        # Fallback: Direct leagues endpoint
        leagues_url_fallback = "https://site.api.espn.com/apis/site/v2/sports/rugby/leagues"
        leagues_data = safe_get(session, leagues_url_fallback)

    leagues = leagues_data.get("items", []) if leagues_data else []
    print(f"\nFound {len(leagues)} leagues under rugby:")
    for lg in leagues[:10]:  # Top 10
        lid = lg.get("id")
        lname = lg.get("name")
        slug = lg.get("slug")
        print(f" - ID={lid}, Name={lname}, Slug={slug}")

    # Step 3: Test a real rugby league (Six Nations, ID=13)
    six_nations_url = "https://site.api.espn.com/apis/site/v2/sports/rugby/13/scoreboard"
    today = datetime.now().strftime("%Y%m%d")
    params = {"dates": today}  # Today's matches
    scoreboard_data = safe_get(session, six_nations_url, params)
    if scoreboard_data:
        events = scoreboard_data.get("events", [])
        print(f"\nSix Nations today ({today}): {len(events)} events")
        for event in events:
            print(f" - {event.get('name')} at {event.get('date')[:10]}")
    else:
        print("No scoreboard data for Six Nations today (normal if no matches).")

    # Step 4: Save full JSON
    out_dir = os.path.dirname(__file__)
    with open(os.path.join(out_dir, "espn_rugby_discovery.json"), "w", encoding="utf-8") as f:
        json.dump({
            "sports": sports_data,
            "leagues": leagues_data,
            "six_nations_sample": scoreboard_data
        }, f, indent=2)
    print(f"\n✅ Full JSON saved to: espn_rugby_discovery.json")

if __name__ == "__main__":
    main()