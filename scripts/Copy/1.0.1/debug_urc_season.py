#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
debug_urc_season.py

Debug helper:
- Confirms we can import scr.ingest.tsdb_client
- Prints what callables exist in that module
- If the expected functions exist, calls them for URC (4446)
"""

import os
import sys

print("[DEBUG] debug_urc_season.py starting")

# ---------------------------------------------------------------------------
# Ensure project root is on PYTHONPATH so `scr` imports work
# ---------------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(f"[DEBUG] Computed project ROOT = {ROOT}")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
    print("[DEBUG] Inserted ROOT into sys.path")
else:
    print("[DEBUG] ROOT already in sys.path")

print("[DEBUG] sys.path[0:5] =", sys.path[:5])

# ---------------------------------------------------------------------------
# Import the module (not individual functions)
# ---------------------------------------------------------------------------
try:
    import scr.ingest.tsdb_client as tsdb_client
    print("[DEBUG] Successfully imported scr.ingest.tsdb_client as tsdb_client")
except Exception as e:
    print("[ERROR] Failed to import scr.ingest.tsdb_client:", repr(e))
    sys.exit(1)

print("[DEBUG] Names in tsdb_client:", [
    n for n in dir(tsdb_client) if not n.startswith("_")
])

URC_LEAGUE_ID = "4446"  # United Rugby Championship


def main() -> None:
    print("[DEBUG] Entering main()")

    # Check expected functions exist
    missing = [
        name for name in (
            "get_league_meta",
            "get_current_season_label",
            "get_events_for_season_rugby",
        )
        if not hasattr(tsdb_client, name)
    ]
    if missing:
        print("[ERROR] tsdb_client is missing expected functions:", missing)
        print("[HINT] Check scr/ingest/tsdb_client.py content matches what we expect.")
        return

    get_league_meta = tsdb_client.get_league_meta
    get_current_season_label = tsdb_client.get_current_season_label
    get_events_for_season_rugby = tsdb_client.get_events_for_season_rugby

    # 1) League metadata
    try:
        print(f"[DEBUG] Calling get_league_meta({URC_LEAGUE_ID!r}) ...")
        league = get_league_meta(URC_LEAGUE_ID, verbose=True)
    except Exception as e:
        print(f"[ERROR] get_league_meta failed: {e!r}")
        return

    name = league.get("strLeague")
    print(f"[INFO] League name: {name!r}")

    # 2) Current season label
    try:
        print(f"[DEBUG] Calling get_current_season_label({URC_LEAGUE_ID!r}) ...")
        current_season = get_current_season_label(URC_LEAGUE_ID, verbose=True)
    except Exception as e:
        print(f"[ERROR] get_current_season_label failed: {e!r}")
        return

    print(f"[INFO] Current TSDB season label: {current_season!r}")

    if not current_season:
        print("[WARN] No current season label found on TSDB; nothing more to do.")
        return

    # 3) Events for that season
    try:
        print(
            f"[DEBUG] Calling get_events_for_season_rugby({URC_LEAGUE_ID!r}, "
            f"{current_season!r}) ..."
        )
        events = get_events_for_season_rugby(URC_LEAGUE_ID, current_season, verbose=True)
    except Exception as e:
        print(f"[ERROR] get_events_for_season_rugby failed: {e!r}")
        return

    print(f"[INFO] Number of rugby events in {current_season}: {len(events)}")

    if events:
        sample = events[0]
        print("[DEBUG] Sample event idEvent:", sample.get("idEvent"))
        print(
            "[DEBUG] Sample event teams:",
            sample.get("strHomeTeam"),
            "vs",
            sample.get("strAwayTeam"),
        )

    print("[DEBUG] main() finished")


if __name__ == "__main__":
    print("[DEBUG] __main__ entry reached")
    main()
    print("[DEBUG] Script finished")
