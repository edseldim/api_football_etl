-- Promote the unprefixed staging tables loaded by FootballETL into the
-- corresponding production tables. PostgresConnector.run_sql_file executes all
-- statements in one transaction, so any failure rolls back the complete load.

-- INSERT INTO prod_argentina_available_leagues (
--     league_id,
--     league_name,
--     league_type,
--     league_logo,
--     country_name,
--     country_code,
--     country_flag,
--     season_year,
--     season_start,
--     season_end,
--     season_current,
--     coverage_fixtures_events,
--     coverage_fixtures_lineups,
--     coverage_fixtures_statistics_fixtures,
--     coverage_fixtures_statistics_players,
--     coverage_standings,
--     coverage_players,
--     coverage_top_scorers,
--     coverage_top_assists,
--     coverage_top_cards,
--     coverage_injuries,
--     coverage_predictions,
--     coverage_odds
-- )
-- SELECT
--     league_id,
--     league_name,
--     league_type,
--     league_logo,
--     country_name,
--     country_code,
--     country_flag,
--     season_year,
--     CAST(season_start AS DATE),
--     CAST(season_end AS DATE),
--     season_current,
--     coverage_fixtures_events,
--     coverage_fixtures_lineups,
--     coverage_fixtures_statistics_fixtures,
--     coverage_fixtures_statistics_players,
--     coverage_standings,
--     coverage_players,
--     coverage_top_scorers,
--     coverage_top_assists,
--     coverage_top_cards,
--     coverage_injuries,
--     coverage_predictions,
--     coverage_odds
-- FROM public.argentina_available_leagues;

INSERT INTO prod_match_events (
    fixture_id,
    minute,
    extra,
    team_id,
    team_name,
    player_id,
    player_name,
    assist_id,
    assist_name,
    event_type,
    detail,
    comments
)
SELECT
    fixture_id,
    minute,
    extra,
    team_id,
    team_name,
    player_id,
    player_name,
    assist_id,
    assist_name,
    event_type,
    detail,
    comments
FROM public.match_events;

INSERT INTO prod_match_lineups (
    fixture_id,
    team_id,
    team_name,
    formation,
    is_starting,
    player_id,
    player_name,
    player_number,
    player_position,
    grid
)
SELECT
    fixture_id,
    team_id,
    team_name,
    formation,
    is_starting,
    player_id,
    player_name,
    player_number,
    player_position,
    grid
FROM public.match_lineups;

INSERT INTO prod_match_player_stats (
    games_minutes,
    games_number,
    games_position,
    games_rating,
    games_captain,
    games_substitute,
    offsides,
    shots_total,
    shots_on,
    goals_total,
    goals_conceded,
    goals_assists,
    goals_saves,
    passes_total,
    passes_key,
    passes_accuracy,
    tackles_total,
    tackles_blocks,
    tackles_interceptions,
    duels_total,
    duels_won,
    dribbles_attempts,
    dribbles_success,
    dribbles_past,
    fouls_drawn,
    fouls_committed,
    cards_yellow,
    cards_red,
    penalty_won,
    penalty_commited,
    penalty_scored,
    penalty_missed,
    penalty_saved,
    fixture_id,
    team_id,
    team_name,
    player_id,
    player_name
)
SELECT
    games_minutes,
    games_number,
    games_position,
    games_rating,
    games_captain,
    games_substitute,
    offsides,
    shots_total,
    shots_on,
    goals_total,
    goals_conceded,
    goals_assists,
    goals_saves,
    passes_total,
    passes_key,
    passes_accuracy,
    tackles_total,
    tackles_blocks,
    tackles_interceptions,
    duels_total,
    duels_won,
    dribbles_attempts,
    dribbles_success,
    dribbles_past,
    fouls_drawn,
    fouls_committed,
    cards_yellow,
    cards_red,
    penalty_won,
    penalty_commited,
    penalty_scored,
    penalty_missed,
    penalty_saved,
    fixture_id,
    team_id,
    team_name,
    player_id,
    player_name
FROM public.match_player_stats;

INSERT INTO prod_match_players (
    fixture_id,
    team_id,
    team_name,
    player_id,
    player_name,
    player_photo
)
SELECT
    fixture_id,
    team_id,
    team_name,
    player_id,
    player_name,
    player_photo
FROM public.match_players;

INSERT INTO prod_match_scores (
    fixture_id,
    score_type,
    home,
    away
)
SELECT
    fixture_id,
    score_type,
    home,
    away
FROM public.match_scores;

INSERT INTO prod_match_summary (
    fixture_id,
    referee,
    timezone,
    date,
    timestamp,
    venue_id,
    venue_name,
    venue_city,
    status_long,
    status_short,
    status_elapsed,
    status_extra,
    league_id,
    league_name,
    league_country,
    league_season,
    league_round,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    home_winner,
    away_winner
)
SELECT
    fixture_id,
    referee,
    timezone,
    CAST(date AS TIMESTAMPTZ),
    timestamp,
    venue_id,
    venue_name,
    venue_city,
    status_long,
    status_short,
    status_elapsed,
    status_extra,
    league_id,
    league_name,
    league_country,
    league_season,
    league_round,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    home_winner,
    away_winner
FROM public.match_summary;

INSERT INTO prod_match_team_stats (
    fixture_id,
    team_id,
    team_name,
    stat_type,
    stat_value
)
SELECT
    fixture_id,
    team_id,
    team_name,
    stat_type,
    stat_value
FROM public.match_team_stats;

INSERT INTO prod_match_teams (
    fixture_id,
    side,
    team_id,
    team_name,
    team_logo,
    winner
)
SELECT
    fixture_id,
    side,
    team_id,
    team_name,
    team_logo,
    winner
FROM public.match_teams;

INSERT INTO prod_teams_coaches (
    fixture_id,
    team_id,
    team_name,
    coach_id,
    coach_name
)
SELECT
    fixture_id,
    team_id,
    team_name,
    coach_id,
    coach_name
FROM public.teams_coaches;

ANALYZE prod_argentina_available_leagues;
ANALYZE prod_match_events;
ANALYZE prod_match_lineups;
ANALYZE prod_match_player_stats;
ANALYZE prod_match_players;
ANALYZE prod_match_scores;
ANALYZE prod_match_summary;
ANALYZE prod_match_team_stats;
ANALYZE prod_match_teams;
ANALYZE prod_teams_coaches;