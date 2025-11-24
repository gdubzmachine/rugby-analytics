@app.get(
    "/headtohead/{tsdb_league_id}",
    response_model=HeadToHeadResponse,
)
def head_to_head(
    tsdb_league_id: int = Query(
        ...,
        description=(
            "External TSDB league id. Use 0 for 'all leagues' mode "
            "with club alias groups."
        ),
    ),
    team_a: str = Query(..., description="Team A name (fuzzy match)."),
    team_b: str = Query(..., description="Team B name (fuzzy match)."),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="How many recent matches to include in the history.",
    ),
) -> HeadToHeadResponse:
    """
    Head-to-head stats between two teams.

    If tsdb_league_id == 0, we allow matches across all leagues and
    use alias groups to unify clubs across competitions. In that mode
    we match by team name patterns (ILIKE) rather than numeric IDs.
    """
    league = None
    league_id = None
    league_name = None

    if tsdb_league_id != 0:
        league = resolve_league_by_tsdb(tsdb_league_id)
        if not league:
            raise HTTPException(status_code=404, detail="League not found")
        league_id = league["id"]
        league_name = league["name"]

    # Resolve canonical teams for display
    if tsdb_league_id == 0:
        team_a_row = resolve_team_global(team_a)
        team_b_row = resolve_team_global(team_b)
    else:
        team_a_row = resolve_team_in_league(league_id, team_a)
        team_b_row = resolve_team_in_league(league_id, team_b)

    if not team_a_row:
        raise HTTPException(status_code=404, detail=f"Team A not found: {team_a}")
    if not team_b_row:
        raise HTTPException(status_code=404, detail=f"Team B not found: {team_b}")

    team_a_id = team_a_row["id"]
    team_b_id = team_b_row["id"]

    # Build alias-aware patterns/tokens
    from h2h_helpers import build_name_patterns, build_alias_tokens  # local import to avoid cycles

    team_a_patterns = build_name_patterns(team_a_row["name"])
    team_b_patterns = build_name_patterns(team_b_row["name"])
    team_a_tokens = build_alias_tokens(team_a_row["name"])
    team_b_tokens = build_alias_tokens(team_b_row["name"])

    # ---- Played matches ----
    if tsdb_league_id == 0:
        # All leagues, alias-aware name matching
        rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                m.home_score,
                m.away_score,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (h.name ILIKE ANY(%s) AND a.name ILIKE ANY(%s)) OR
                (h.name ILIKE ANY(%s) AND a.name ILIKE ANY(%s))
              )
            ORDER BY m.kickoff_utc DESC
            LIMIT %s
            """,
            (team_a_patterns, team_b_patterns, team_b_patterns, team_a_patterns, limit),
        )
    else:
        # Single league, plain IDs but still stats will use alias tokens
        rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                m.home_score,
                m.away_score,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (m.home_team_id = %s AND m.away_team_id = %s) OR
                (m.home_team_id = %s AND m.away_team_id = %s)
              )
              AND m.league_id = %s
            ORDER BY m.kickoff_utc DESC
            LIMIT %s
            """,
            (team_a_id, team_b_id, team_b_id, team_a_id, league_id, limit),
        )

    last_matches = [build_match_summary_row(r) for r in rows]

    # ---- Upcoming fixtures ----
    if tsdb_league_id == 0:
        upcoming_rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (h.name ILIKE ANY(%s) AND a.name ILIKE ANY(%s)) OR
                (h.name ILIKE ANY(%s) AND a.name ILIKE ANY(%s))
              )
              AND m.kickoff_utc >= NOW()
            ORDER BY m.kickoff_utc ASC
            """,
            (team_a_patterns, team_b_patterns, team_b_patterns, team_a_patterns),
        )
    else:
        upcoming_rows = fetch_all(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_utc,
                h.name AS home_team,
                a.name AS away_team,
                v.name AS venue,
                l.name AS league,
                s.label AS season
            FROM matches m
            JOIN teams h
              ON h.id = m.home_team_id
            JOIN teams a
              ON a.id = m.away_team_id
            LEFT JOIN venues v
              ON v.id = m.venue_id
            LEFT JOIN seasons s
              ON s.id = m.season_id
            LEFT JOIN leagues l
              ON l.id = m.league_id
            WHERE
              (
                (m.home_team_id = %s AND m.away_team_id = %s) OR
                (m.home_team_id = %s AND m.away_team_id = %s)
              )
              AND m.league_id = %s
              AND m.kickoff_utc >= NOW()
            ORDER BY m.kickoff_utc ASC
            """,
            (team_a_id, team_b_id, team_b_id, team_a_id, league_id),
        )

    upcoming_fixtures = [build_fixture_summary_row(r) for r in upcoming_rows]

    # Stats (uses alias tokens so Bulls/Blue Bulls and Stormers/WP are grouped)
    stats = compute_head_to_head_stats(
        last_matches,
        team_a_row["name"],
        team_b_row["name"],
        team_a_tokens,
        team_b_tokens,
    )

    return HeadToHeadResponse(
        league_id=league_id,
        league_name=league_name,
        tsdb_league_id=tsdb_league_id,
        team_a_id=team_a_id,
        team_b_id=team_b_id,
        team_a_name=team_a_row["name"],
        team_b_name=team_b_row["name"],
        total_matches=stats["total"],
        team_a_wins=stats["team_a_wins"],
        team_b_wins=stats["team_b_wins"],
        draws=stats["draws"],
        team_a_win_rate=stats["team_a_rate"],
        team_b_win_rate=stats["team_b_rate"],
        draws_rate=stats["draw_rate"],
        current_streak=stats["current_streak"],
        last_matches=last_matches,
        upcoming_fixtures=upcoming_fixtures,
    )
