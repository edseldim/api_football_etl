-- Production tables derived from the undated Parquet files in data/.
-- Nullable Parquet numeric identifiers remain nullable here; no inferred primary
-- keys are added because the source files do not establish row uniqueness.
DROP TABLE IF EXISTS prod_argentina_available_leagues;
CREATE TABLE IF NOT EXISTS prod_argentina_available_leagues (
    league_id BIGINT,
    league_name TEXT,
    league_type TEXT,
    league_logo TEXT,
    country_name TEXT,
    country_code TEXT,
    country_flag TEXT,
    season_year BIGINT,
    season_start DATE,
    season_end DATE,
    season_current BOOLEAN,
    coverage_fixtures_events BOOLEAN,
    coverage_fixtures_lineups BOOLEAN,
    coverage_fixtures_statistics_fixtures BOOLEAN,
    coverage_fixtures_statistics_players BOOLEAN,
    coverage_standings BOOLEAN,
    coverage_players BOOLEAN,
    coverage_top_scorers BOOLEAN,
    coverage_top_assists BOOLEAN,
    coverage_top_cards BOOLEAN,
    coverage_injuries BOOLEAN,
    coverage_predictions BOOLEAN,
    coverage_odds BOOLEAN
);
DROP TABLE IF EXISTS prod_match_events;
CREATE TABLE IF NOT EXISTS prod_match_events (
    fixture_id BIGINT,
    minute BIGINT,
    extra DOUBLE PRECISION,
    team_id BIGINT,
    team_name TEXT,
    player_id DOUBLE PRECISION,
    player_name TEXT,
    assist_id DOUBLE PRECISION,
    assist_name TEXT,
    event_type TEXT,
    detail TEXT,
    comments TEXT
);
DROP TABLE IF EXISTS prod_match_lineups;
CREATE TABLE IF NOT EXISTS prod_match_lineups (
    fixture_id BIGINT,
    team_id BIGINT,
    team_name TEXT,
    formation TEXT,
    is_starting BOOLEAN,
    player_id BIGINT,
    player_name TEXT,
    player_number DOUBLE PRECISION,
    player_position TEXT,
    grid TEXT
);
DROP TABLE IF EXISTS prod_match_player_stats;
CREATE TABLE IF NOT EXISTS prod_match_player_stats (
    games_minutes DOUBLE PRECISION,
    games_number BIGINT,
    games_position TEXT,
    games_rating TEXT,
    games_captain BOOLEAN,
    games_substitute BOOLEAN,
    offsides DOUBLE PRECISION,
    shots_total DOUBLE PRECISION,
    shots_on DOUBLE PRECISION,
    goals_total DOUBLE PRECISION,
    goals_conceded BIGINT,
    goals_assists DOUBLE PRECISION,
    goals_saves DOUBLE PRECISION,
    passes_total DOUBLE PRECISION,
    passes_key DOUBLE PRECISION,
    passes_accuracy TEXT,
    tackles_total DOUBLE PRECISION,
    tackles_blocks DOUBLE PRECISION,
    tackles_interceptions DOUBLE PRECISION,
    duels_total DOUBLE PRECISION,
    duels_won DOUBLE PRECISION,
    dribbles_attempts DOUBLE PRECISION,
    dribbles_success DOUBLE PRECISION,
    dribbles_past DOUBLE PRECISION,
    fouls_drawn DOUBLE PRECISION,
    fouls_committed DOUBLE PRECISION,
    cards_yellow BIGINT,
    cards_red BIGINT,
    penalty_won DOUBLE PRECISION,
    penalty_commited DOUBLE PRECISION,
    penalty_scored BIGINT,
    penalty_missed BIGINT,
    penalty_saved DOUBLE PRECISION,
    fixture_id BIGINT,
    team_id BIGINT,
    team_name TEXT,
    player_id BIGINT,
    player_name TEXT
);
DROP TABLE IF EXISTS prod_match_players;
CREATE TABLE IF NOT EXISTS prod_match_players (
    fixture_id BIGINT,
    team_id BIGINT,
    team_name TEXT,
    player_id BIGINT,
    player_name TEXT,
    player_photo TEXT
);
DROP TABLE IF EXISTS prod_match_scores;
CREATE TABLE IF NOT EXISTS prod_match_scores (
    fixture_id BIGINT,
    score_type TEXT,
    home DOUBLE PRECISION,
    away DOUBLE PRECISION
);
DROP TABLE IF EXISTS prod_match_summary;
CREATE TABLE IF NOT EXISTS prod_match_summary (
    fixture_id BIGINT,
    referee TEXT,
    timezone TEXT,
    date TIMESTAMPTZ,
    timestamp BIGINT,
    venue_id DOUBLE PRECISION,
    venue_name TEXT,
    venue_city TEXT,
    status_long TEXT,
    status_short TEXT,
    status_elapsed BIGINT,
    status_extra DOUBLE PRECISION,
    league_id BIGINT,
    league_name TEXT,
    league_country TEXT,
    league_season BIGINT,
    league_round TEXT,
    home_team_id BIGINT,
    away_team_id BIGINT,
    home_goals BIGINT,
    away_goals BIGINT,
    home_winner BOOLEAN,
    away_winner BOOLEAN
);
DROP TABLE IF EXISTS prod_match_team_stats;
CREATE TABLE IF NOT EXISTS prod_match_team_stats (
    fixture_id BIGINT,
    team_id BIGINT,
    team_name TEXT,
    stat_type TEXT,
    stat_value DOUBLE PRECISION
);
DROP TABLE IF EXISTS prod_match_teams;
CREATE TABLE IF NOT EXISTS prod_match_teams (
    fixture_id BIGINT,
    side TEXT,
    team_id BIGINT,
    team_name TEXT,
    team_logo TEXT,
    winner BOOLEAN
);
DROP TABLE IF EXISTS prod_teams_coaches;
CREATE TABLE IF NOT EXISTS prod_teams_coaches (
    fixture_id BIGINT,
    team_id BIGINT,
    team_name TEXT,
    coach_id DOUBLE PRECISION,
    coach_name TEXT
);

-- Fixture lookup indexes. PostgreSQL propagates indexes declared on a
-- partitioned parent to its existing and future partitions.
CREATE INDEX IF NOT EXISTS match_events_fixture_id_idx
    ON prod_match_events (fixture_id);

CREATE INDEX IF NOT EXISTS match_lineups_fixture_id_idx
    ON prod_match_lineups (fixture_id);

CREATE INDEX IF NOT EXISTS match_player_stats_fixture_id_idx
    ON prod_match_player_stats (fixture_id);

CREATE INDEX IF NOT EXISTS match_players_fixture_id_idx
    ON prod_match_players (fixture_id);

CREATE INDEX IF NOT EXISTS match_scores_fixture_id_idx
    ON prod_match_scores (fixture_id);

CREATE INDEX IF NOT EXISTS match_summary_fixture_id_idx
    ON prod_match_summary (fixture_id);

CREATE INDEX IF NOT EXISTS match_team_stats_fixture_id_idx
    ON prod_match_team_stats (fixture_id);

CREATE INDEX IF NOT EXISTS match_teams_fixture_id_idx
    ON prod_match_teams (fixture_id);

CREATE INDEX IF NOT EXISTS teams_coaches_fixture_id_idx
    ON prod_teams_coaches (fixture_id);

-- Composite indexes support fixture-scoped player and team joins.
CREATE INDEX IF NOT EXISTS match_players_fixture_player_idx
    ON prod_match_players (fixture_id, player_id);

CREATE INDEX IF NOT EXISTS match_teams_fixture_team_idx
    ON prod_match_teams (fixture_id, team_id);

CREATE INDEX IF NOT EXISTS match_team_stats_fixture_team_idx
    ON prod_match_team_stats (fixture_id, team_id);

-- Match-summary indexes support daily, seasonal, and league-specific ML jobs.
CREATE INDEX IF NOT EXISTS match_summary_date_idx
    ON prod_match_summary (date);

CREATE INDEX IF NOT EXISTS match_summary_league_season_idx
    ON prod_match_summary (league_id, league_season);

CREATE INDEX IF NOT EXISTS match_summary_season_date_idx
    ON prod_match_summary (league_season, date);
