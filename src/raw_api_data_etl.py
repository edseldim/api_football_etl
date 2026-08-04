import requests
import json
import os
import dotenv
import pandas as pd
from pathlib import Path
from typing import Any, Callable, Optional

dotenv.load_dotenv()


EXPECTED_PARQUET_SCHEMAS = {
    "argentina_available_leagues.parquet": {
        "league_id": {"kind": "int"},
        "league_name": {"kind": "string"},
        "league_type": {"kind": "string"},
        "league_logo": {"kind": "string"},
        "country_name": {"kind": "string"},
        "country_code": {"kind": "string"},
        "country_flag": {"kind": "string"},
        "season_year": {"kind": "int"},
        "season_start": {"kind": "string"},
        "season_end": {"kind": "string"},
        "season_current": {"kind": "bool"},
        "coverage_fixtures_events": {"kind": "bool"},
        "coverage_fixtures_lineups": {"kind": "bool"},
        "coverage_fixtures_statistics_fixtures": {"kind": "bool"},
        "coverage_fixtures_statistics_players": {"kind": "bool"},
        "coverage_standings": {"kind": "bool"},
        "coverage_players": {"kind": "bool"},
        "coverage_top_scorers": {"kind": "bool"},
        "coverage_top_assists": {"kind": "bool"},
        "coverage_top_cards": {"kind": "bool"},
        "coverage_injuries": {"kind": "bool"},
        "coverage_predictions": {"kind": "bool"},
        "coverage_odds": {"kind": "bool"},
    },
    "consolidated_fixtures_match_events.parquet": {
        "fixture_id": {"kind": "int"},
        "minute": {"kind": "int"},
        "extra": {"kind": "float"},
        "team_id": {"kind": "int"},
        "team_name": {"kind": "string"},
        "player_id": {"kind": "float"},
        "player_name": {"kind": "string"},
        "assist_id": {"kind": "float"},
        "assist_name": {"kind": "string"},
        "event_type": {"kind": "string"},
        "detail": {"kind": "string"},
        "comments": {"kind": "string"},
    },
    "consolidated_fixtures_match_lineups.parquet": {
        "fixture_id": {"kind": "int"},
        "team_id": {"kind": "int"},
        "team_name": {"kind": "string"},
        "formation": {"kind": "string"},
        "is_starting": {"kind": "bool"},
        "player_id": {"kind": "int"},
        "player_name": {"kind": "string"},
        "player_number": {"kind": "int"},
        "player_position": {"kind": "string"},
        "grid": {"kind": "string"},
    },
    "consolidated_fixtures_match_player_stats.parquet": {
        "games_minutes": {"kind": "int"},
        "games_number": {"kind": "int"},
        "games_position": {"kind": "string"},
        "games_rating": {"kind": "string"},
        "games_captain": {"kind": "bool"},
        "games_substitute": {"kind": "bool"},
        "offsides": {"kind": "float"},
        "shots_total": {"kind": "int"},
        "shots_on": {"kind": "int"},
        "goals_total": {"kind": "float"},
        "goals_conceded": {"kind": "float"},
        "goals_assists": {"kind": "float"},
        "goals_saves": {"kind": "float"},
        "passes_total": {"kind": "int"},
        "passes_key": {"kind": "int"},
        "passes_accuracy": {"kind": "string"},
        "tackles_total": {"kind": "float"},
        "tackles_blocks": {"kind": "float"},
        "tackles_interceptions": {"kind": "int"},
        "duels_total": {"kind": "float"},
        "duels_won": {"kind": "float"},
        "dribbles_attempts": {"kind": "int"},
        "dribbles_success": {"kind": "int"},
        "dribbles_past": {"kind": "float"},
        "fouls_drawn": {"kind": "float"},
        "fouls_committed": {"kind": "float"},
        "cards_yellow": {"kind": "int"},
        "cards_red": {"kind": "int"},
        "penalty_won": {"kind": "float"},
        "penalty_commited": {"kind": "float"},
        "penalty_scored": {"kind": "int"},
        "penalty_missed": {"kind": "int"},
        "penalty_saved": {"kind": "float"},
        "fixture_id": {"kind": "int"},
        "team_id": {"kind": "int"},
        "team_name": {"kind": "string"},
        "player_id": {"kind": "int"},
        "player_name": {"kind": "string"},
    },
    "consolidated_fixtures_match_players.parquet": {
        "fixture_id": {"kind": "int"},
        "team_id": {"kind": "int"},
        "team_name": {"kind": "string"},
        "player_id": {"kind": "int"},
        "player_name": {"kind": "string"},
        "player_photo": {"kind": "string"},
    },
    "consolidated_fixtures_match_scores.parquet": {
        "fixture_id": {"kind": "int"},
        "score_type": {"kind": "string"},
        "home": {"kind": "float"},
        "away": {"kind": "float"},
    },
    "consolidated_fixtures_match_summary.parquet": {
        "fixture_id": {"kind": "int"},
        "referee": {"kind": "string"},
        "timezone": {"kind": "string"},
        "date": {"kind": "string"},
        "timestamp": {"kind": "int"},
        "venue_id": {"kind": "float"},
        "venue_name": {"kind": "string"},
        "venue_city": {"kind": "string"},
        "status_long": {"kind": "string"},
        "status_short": {"kind": "string"},
        "status_elapsed": {"kind": "int"},
        "status_extra": {"kind": "int"},
        "league_id": {"kind": "int"},
        "league_name": {"kind": "string"},
        "league_country": {"kind": "string"},
        "league_season": {"kind": "int"},
        "league_round": {"kind": "string"},
        "home_team_id": {"kind": "int"},
        "away_team_id": {"kind": "int"},
        "home_goals": {"kind": "int"},
        "away_goals": {"kind": "int"},
        "home_winner": {"kind": "bool"},
        "away_winner": {"kind": "bool"},
    },
    "consolidated_fixtures_match_team_stats.parquet": {
        "fixture_id": {"kind": "int"},
        "team_id": {"kind": "int"},
        "team_name": {"kind": "string"},
        "stat_type": {"kind": "string"},
        "stat_value": {"kind": "float"},
    },
    "consolidated_fixtures_match_teams.parquet": {
        "fixture_id": {"kind": "int"},
        "side": {"kind": "string"},
        "team_id": {"kind": "int"},
        "team_name": {"kind": "string"},
        "team_logo": {"kind": "string"},
        "winner": {"kind": "bool"},
    },
    "consolidated_fixtures_teams_coaches.parquet": {
        "fixture_id": {"kind": "int"},
        "team_id": {"kind": "int"},
        "team_name": {"kind": "string"},
        "coach_id": {"kind": "int"},
        "coach_name": {"kind": "string"},
    },
}


TABLE_NAME_TO_PARQUET_SCHEMA = {
    "available_leagues": "argentina_available_leagues.parquet",
    "match_summary": "consolidated_fixtures_match_summary.parquet",
    "match_teams": "consolidated_fixtures_match_teams.parquet",
    "match_scores": "consolidated_fixtures_match_scores.parquet",
    "match_events": "consolidated_fixtures_match_events.parquet",
    "match_lineups": "consolidated_fixtures_match_lineups.parquet",
    "teams_coaches": "consolidated_fixtures_teams_coaches.parquet",
    "match_team_stats": "consolidated_fixtures_match_team_stats.parquet",
    "match_players": "consolidated_fixtures_match_players.parquet",
    "match_player_stats": "consolidated_fixtures_match_player_stats.parquet",
}


class FootballAPI:

    def __init__(self, api_key,
                 other_headers = None,
                 general_params = None,
                 debug = False,
                 log_event: Optional[Callable[[str, Any], None]] = None):
        """Initialize the FootballAPI client.

        Parameters:
            api_key (str): API key for football.api-sports.io.
            other_headers (dict): Optional HTTP headers to include in requests.
            general_params (dict): Default query parameters such as league, season, and country.
            debug (bool): Enable verbose request and response logging.
            log_event (callable, optional): Shared ``(level, message)`` logger.

        Returns:
            None
        """

        self.url = "https://v3.football.api-sports.io"
        self.headers = {
            'x-apisports-key': api_key
        }

        self.headers.update(other_headers or {})

        self.general_params = {
            "league": 128,
            "season": 2026,
            "country": "argentina"
        }

        self.general_params.update(general_params or {})
        self.debug = debug
        self._log_event = log_event or self._default_log_event

    @staticmethod
    def _default_log_event(level, message):
        """Provide safe terminal logging when the API is used independently."""
        print(f"[{str(level).upper()}] {message}")

    def _dtype_matches(self, series, expected_kind):
        """Return True when a DataFrame Series is compatible with the expected parquet kind.

        Parameters:
            series (pandas.Series): The series to validate.
            expected_kind (str): Expected schema kind (int, float, bool, string, datetime).

        Returns:
            bool: True when the series matches the expected kind.
        """
        if series.isna().all():
            if expected_kind in {"int", "float", "bool", "string"}:
                return True

        if expected_kind == "int":
            return pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series) and series.dropna().apply(lambda value: float(value).is_integer()).all()
        if expected_kind == "float":
            return pd.api.types.is_float_dtype(series) or pd.api.types.is_integer_dtype(series) or pd.api.types.is_object_dtype(series) and series.dropna().apply(lambda value: isinstance(value, (int, float)) or str(value).replace('.', '', 1).lstrip('-').isdigit()).all()
        if expected_kind == "bool":
            return pd.api.types.is_bool_dtype(series) or pd.api.types.is_object_dtype(series) and series.dropna().isin([True, False]).all()
        if expected_kind == "string":
            return pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)
        if expected_kind == "datetime":
            return pd.api.types.is_datetime64_any_dtype(series)
        return str(series.dtype) == expected_kind

    def validate_parquet_schema(self, data, expected_schema=None, source_name=None):
        """Validate a pandas DataFrame against the expected schema at runtime.

        Parameters:
            data (pandas.DataFrame): Data to validate.
            expected_schema (dict, optional): Schema definition mapping column names to kinds.
            source_name (str, optional): Table name lookup key for schema resolution.

        Returns:
            tuple: (is_valid, errors, warnings)
                is_valid (bool): True when no validation errors were found.
                errors (list[str]): Validation error messages.
                warnings (list[str]): Validation warning messages.
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError("data must be a pandas DataFrame")

        df = data
        label = source_name or "dataframe"
        if expected_schema is None:
            expected_schema = EXPECTED_PARQUET_SCHEMAS.get(label)
            if expected_schema is None and source_name is not None:
                schema_name = TABLE_NAME_TO_PARQUET_SCHEMA.get(source_name)
                if schema_name is not None:
                    expected_schema = EXPECTED_PARQUET_SCHEMAS.get(schema_name)
                    if expected_schema is not None:
                        label = schema_name
        if expected_schema is None:
            message = f"no schema definition found for {label}"
            self._log_event("ERROR", message)
            return False, [message], []

        errors = []
        warnings = []
        expected_columns = set(expected_schema)
        actual_columns = set(df.columns)

        if df.empty:
            warnings.append(f"{label} is empty")
            return True, errors, warnings

        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns

        if len(actual_columns) == 0:
            warnings.append(f"{label} has no columns")
            return True, errors, warnings
        if missing_columns:
            errors.append(f"{label} is missing columns: {sorted(missing_columns)}")
        if extra_columns:
            errors.append(f"{label} has unexpected columns: {sorted(extra_columns)}")

        for column_name, spec in expected_schema.items():
            if column_name not in df.columns:
                continue
            expected_kind = spec.get("kind")
            if expected_kind and not self._dtype_matches(df[column_name], expected_kind):
                errors.append(
                    f"{label}.{column_name} expects {expected_kind} but found {df[column_name].dtype}"
                )

        if errors:
            self._log_event("ERROR", "; ".join(errors))
        else:
            self._log_event("INFO", f"validated {label} successfully")

        return len(errors) == 0, errors, warnings

    def _format_filename(self):
        """Generate a filename for saving JSON output based on current API params.

        Returns:
            str: Generated JSON filename.
        """
        filename = f"country_id_{self.current_params.get('country','argentina')}" + \
                    f"_endpoint_id_{self.current_endpoint.strip('/').replace('/','_')}" + \
                    f"_league_id_{self.current_params.get('league',128)}" + \
                    f"_season_id_{self.current_params.get('season',2026)}" + \
                    f"_fixture_id_{self.current_params.get('fixture','NA')}"
        filename += ".json"

        return filename

    def _flatten_json(self, data, parent_key = "", sep = "_"):
        """Recursively flatten nested JSON objects into a single dict with joined keys.

        Parameters:
            data (Any): JSON data to flatten.
            parent_key (str): Parent key prefix used for nested keys.
            sep (str): Separator between nested keys.

        Returns:
            dict: Flattened JSON dictionary.
        """
        flattened = {}

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
                if isinstance(value, (dict, list)):
                    flattened.update(self._flatten_json(value, new_key, sep))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for index, value in enumerate(data):
                new_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
                if isinstance(value, (dict, list)):
                    flattened.update(self._flatten_json(value, new_key, sep))
                else:
                    flattened[new_key] = value
        else:
            flattened[parent_key] = data

        return flattened

    def _normalize_params_not_to_include(self, default_keys, params_not_to_include):
        """Build a normalized list of query parameters that should be excluded from the request.

        Parameters:
            default_keys (list|tuple|set): Default parameter names to exclude.
            params_not_to_include (list|tuple|set|str|None): Additional parameters to exclude.

        Returns:
            list: Final list of excluded parameter keys.
        """
        excluded_keys = set(default_keys or [])

        if params_not_to_include is None:
            return list(excluded_keys)

        if isinstance(params_not_to_include, (list, tuple, set)):
            excluded_keys.update(params_not_to_include)
        else:
            excluded_keys.add(params_not_to_include)

        return list(excluded_keys)

    def _normalize_numeric_value(self, value):
        """Convert a scalar value into a normalized float for numeric statistics.

        Parameters:
            value (Any): Raw value to normalize.

        Returns:
            float: Normalized float value or NaN.
        """
        if value is None:
            return float("nan")

        if isinstance(value, bool):
            return float(value)

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            text = value.strip()
            if not text:
                return float("nan")
            if text.endswith("%"):
                text = text[:-1].strip()
            try:
                return float(text)
            except ValueError:
                return float("nan")

        return float("nan")

    def _split_fixture_payload_to_tables(self, payload):
        """
        Convert a single fixture JSON payload into smaller dictionaries that map
        naturally to SQL tables.
        """
        if isinstance(payload, str):
            payload = json.loads(payload)

        if isinstance(payload, list):
            if len(payload) != 1:
                raise ValueError("Expected a single fixture payload object")
            payload = payload[0]

        if isinstance(payload, dict) and "response" in payload:
            response = payload.get("response")
            if isinstance(response, list):
                if len(response) != 1:
                    raise ValueError("Expected a single fixture payload object")
                payload = response[0]
            elif isinstance(response, dict):
                payload = response

        if not isinstance(payload, dict):
            raise TypeError("Payload must be a dict or a list containing a single dict")

        fixture = payload.get("fixture", {})
        league = payload.get("league", {})
        teams = payload.get("teams", {})
        goals = payload.get("goals", {})
        score = payload.get("score", {})
        events = payload.get("events", [])
        lineups = payload.get("lineups", [])
        statistics = payload.get("statistics", [])
        players = payload.get("players", [])

        match_summary = {
            "fixture_id": fixture.get("id"),
            "referee": fixture.get("referee"),
            "timezone": fixture.get("timezone"),
            "date": fixture.get("date"),
            "timestamp": fixture.get("timestamp"),
            "venue_id": fixture.get("venue", {}).get("id"),
            "venue_name": fixture.get("venue", {}).get("name"),
            "venue_city": fixture.get("venue", {}).get("city"),
            "status_long": fixture.get("status", {}).get("long"),
            "status_short": fixture.get("status", {}).get("short"),
            "status_elapsed": fixture.get("status", {}).get("elapsed"),
            "status_extra": fixture.get("status", {}).get("extra"),
            "league_id": league.get("id"),
            "league_name": league.get("name"),
            "league_country": league.get("country"),
            "league_season": league.get("season"),
            "league_round": league.get("round"),
            "home_team_id": teams.get("home", {}).get("id"),
            "away_team_id": teams.get("away", {}).get("id"),
            "home_goals": goals.get("home"),
            "away_goals": goals.get("away"),
            "home_winner": teams.get("home", {}).get("winner"),
            "away_winner": teams.get("away", {}).get("winner"),
        }

        match_teams = []
        for side, team in teams.items():
            if not isinstance(team, dict):
                continue
            match_teams.append({
                "fixture_id": fixture.get("id"),
                "side": side,
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "team_logo": team.get("logo"),
                "winner": team.get("winner")
            })

        match_scores = []
        for score_name in ("halftime", "fulltime", "extratime", "penalty"):
            score_block = score.get(score_name, {})
            if not isinstance(score_block, dict):
                continue
            match_scores.append({
                "fixture_id": fixture.get("id"),
                "score_type": score_name,
                "home": score_block.get("home"),
                "away": score_block.get("away")
            })

        match_events = []
        for event in events or []:
            match_events.append({
                "fixture_id": fixture.get("id"),
                "minute": event.get("time", {}).get("elapsed"),
                "extra": event.get("time", {}).get("extra"),
                "team_id": event.get("team", {}).get("id"),
                "team_name": event.get("team", {}).get("name"),
                "player_id": event.get("player", {}).get("id"),
                "player_name": event.get("player", {}).get("name"),
                "assist_id": event.get("assist", {}).get("id"),
                "assist_name": event.get("assist", {}).get("name"),
                "event_type": event.get("type"),
                "detail": event.get("detail"),
                "comments": event.get("comments")
            })

        match_lineups = []
        for lineup in lineups or []:
            team = lineup.get("team", {})
            formation = lineup.get("formation")
            for section_name in ("startXI", "substitutes"):
                for item in lineup.get(section_name, []) or []:
                    player = item.get("player", {})
                    match_lineups.append({
                        "fixture_id": fixture.get("id"),
                        "team_id": team.get("id"),
                        "team_name": team.get("name"),
                        "formation": formation,
                        "is_starting": section_name == "startXI",
                        "player_id": player.get("id"),
                        "player_name": player.get("name"),
                        "player_number": player.get("number"),
                        "player_position": player.get("pos"),
                        "grid": player.get("grid")
                    })

        teams_coaches = []
        for lineup in lineups or []:
            team = lineup.get("team", {})
            coach = lineup.get("coach",{})
            teams_coaches.append({
                "fixture_id": fixture.get("id"),
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "coach_id": coach.get("id"),
                "coach_name": coach.get("name")
            })

        match_team_stats = []
        for stat_group in statistics or []:
            team = stat_group.get("team", {})
            for stat in stat_group.get("statistics", []) or []:
                match_team_stats.append({
                    "fixture_id": fixture.get("id"),
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "stat_type": stat.get("type"),
                    "stat_value": self._normalize_numeric_value(stat.get("value"))
                })

        match_players = []
        match_player_stats = []
        for team_entry in players or []:
            team = team_entry.get("team", {})
            team_id = team.get("id")
            for player_entry in team_entry.get("players", []) or []:
                player = player_entry.get("player", {})
                match_players.append({
                    "fixture_id": fixture.get("id"),
                    "team_id": team_id,
                    "team_name": team.get("name"),
                    "player_id": player.get("id"),
                    "player_name": player.get("name"),
                    "player_photo": player.get("photo")
                })

                stats_list = player_entry.get("statistics", []) or []
                if stats_list:
                    stat_block = stats_list[0]
                    flat_stats = {}
                    for key, value in stat_block.items():
                        if isinstance(value, dict):
                            for inner_key, inner_value in value.items():
                                flat_stats[f"{key}_{inner_key}"] = inner_value
                        else:
                            flat_stats[key] = value
                    flat_stats.update({
                        "fixture_id": fixture.get("id"),
                        "team_id": team_id,
                        "team_name": team.get("name"),
                        "player_id": player.get("id"),
                        "player_name": player.get("name")
                    })
                    match_player_stats.append(flat_stats)

        resulting_tables = {
            "match_summary": match_summary,
            "match_teams": match_teams,
            "match_scores": match_scores,
            "match_events": match_events,
            "match_lineups": match_lineups,
            "teams_coaches": teams_coaches,
            "match_team_stats": match_team_stats,
            "match_players": match_players,
            "match_player_stats": match_player_stats,
        }

        self._log_event("INFO", f"Validating schema for fixture_id: {match_summary.get('fixture_id','NA')}")
        for table_name, table_data in resulting_tables.items():

            if isinstance(table_data, dict):
                frame = pd.DataFrame([table_data])
            elif isinstance(table_data, list):
                frame = pd.DataFrame(table_data)
            else:
                frame = pd.DataFrame([{"value": table_data}])

            is_valid, validation_errors, validation_warnings = self.validate_parquet_schema(
                frame,
                source_name=table_name
            )

            for warning_msg in validation_warnings:
                self._log_event("WARNING", warning_msg)

            if not is_valid:
                raise ValueError(
                    f"Fixture table '{table_name}' failed schema validation: "
                    + "; ".join(validation_errors)
                )

        return resulting_tables

    def split_league_payload_to_table(self, payload):
        """Convert a leagues payload into a validated, one-row-per-season table."""
        if isinstance(payload, str):
            payload = json.loads(payload)

        if isinstance(payload, dict) and "response" in payload:
            payload = payload.get("response")

        if isinstance(payload, dict):
            payload = [payload]

        if not isinstance(payload, list):
            raise TypeError("Payload must be a dict, a list of dicts, or a JSON string")

        rows = []
        for league_entry in payload:
            if not isinstance(league_entry, dict):
                raise TypeError("Each league payload item must be a dict")

            league = league_entry.get("league", {}) or {}
            country = league_entry.get("country", {}) or {}
            seasons = league_entry.get("seasons", []) or []

            for season in seasons:
                if not isinstance(season, dict):
                    continue

                coverage = season.get("coverage", {}) or {}
                fixtures_coverage = coverage.get("fixtures", {}) or {}
                rows.append({
                    "league_id": league.get("id"),
                    "league_name": league.get("name"),
                    "league_type": league.get("type"),
                    "league_logo": league.get("logo"),
                    "country_name": country.get("name"),
                    "country_code": country.get("code"),
                    "country_flag": country.get("flag"),
                    "season_year": season.get("year"),
                    "season_start": season.get("start"),
                    "season_end": season.get("end"),
                    "season_current": season.get("current"),
                    "coverage_fixtures_events": fixtures_coverage.get("events"),
                    "coverage_fixtures_lineups": fixtures_coverage.get("lineups"),
                    "coverage_fixtures_statistics_fixtures": fixtures_coverage.get("statistics_fixtures"),
                    "coverage_fixtures_statistics_players": fixtures_coverage.get("statistics_players"),
                    "coverage_standings": coverage.get("standings"),
                    "coverage_players": coverage.get("players"),
                    "coverage_top_scorers": coverage.get("top_scorers"),
                    "coverage_top_assists": coverage.get("top_assists"),
                    "coverage_top_cards": coverage.get("top_cards"),
                    "coverage_injuries": coverage.get("injuries"),
                    "coverage_predictions": coverage.get("predictions"),
                    "coverage_odds": coverage.get("odds"),
                })

        table_name = "available_leagues"
        frame = pd.DataFrame(rows)
        is_valid, validation_errors, validation_warnings = self.validate_parquet_schema(
            frame,
            source_name=table_name
        )

        for warning_msg in validation_warnings:
            self._log_event("WARNING", warning_msg)

        if not is_valid:
            raise ValueError(
                f"League table '{table_name}' failed schema validation: "
                + "; ".join(validation_errors)
            )

        return {table_name: rows}

    def _iterate_over_pages(self, res):
        """Retrieve all paginated response pages and combine them into a single list.

        Parameters:
            res (dict): Initial API response containing paging metadata.

        Returns:
            list: Combined responses from all pages.
        """

        paging_data = res.get("paging",{ "current": 1, "total": 1 })
        max_pages = paging_data["total"]
        current = paging_data["current"]

        self._log_event("INFO",f"detected pages {max_pages}")
        total_data = res["response"]
        for page_number in range(current,max_pages):
            self._log_event("INFO",f"retrieving page {page_number}")
            self.current_params["page"] = page_number+1
            iter_res = self._iterate_response(self.current_endpoint, res["parameters"])
            total_data.append(iter_res["response"])
        
        return total_data

    def _iterate_response(self,
                        endpoint,
                        params,
                        save_file = False,
                        iterate_pages = False,
                        flatten_result = False):
        """Send a request to the API endpoint and optionally save or paginate the response.

        Parameters:
            endpoint (str): API endpoint path.
            params (dict): Query parameters for the request.
            save_file (bool): Save the JSON response to disk when True.
            iterate_pages (bool): Follow paging and retrieve all pages when True.
            flatten_result (bool): Save a flattened version of the response when True.

        Returns:
            dict|list|tuple: API response data, optionally flattened if requested.
        """

        if self.debug:
            self._log_event("INFO",f"endpoint: {self.url+endpoint}")
            self._log_event("INFO",f"passing params to request {params}")

        res = requests.get(self.url+endpoint, headers = self.headers,params = params).json()

        if self.debug:
            self._log_event("INFO",res)

        if iterate_pages:
            full_res = self._iterate_over_pages(res)
            res = full_res

        if save_file:
            with open(self._format_filename(),"w") as fp:
                json.dump(res, fp, ensure_ascii = False, indent = 2)

            if flatten_result:

                flat = self._flatten_json(res)

                with open("flattened_"+self._format_filename(),"w") as fp:
                    json.dump(flat, fp, ensure_ascii = False, indent = 2)

                return res, flat

        return res

    def get_argentina_league_teams(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request the Argentina league teams endpoint and return the API response.

        Parameters:
            params (dict): Query parameters to include in the request.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is None:
            pass
        else:
            current_params.update(params)

        self.params_not_to_include = self._normalize_params_not_to_include([], params_not_to_include)

        self.current_params = current_params
        self.current_endpoint = "/teams"

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}

        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def get_argentina_league_players(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request the Argentina league players endpoint and return the API response.

        Parameters:
            params (dict): Query parameters to include in the request.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is None:
            pass
        else:
            current_params.update(params)

        self.params_not_to_include = self._normalize_params_not_to_include(["country"], params_not_to_include)

        self.current_params = current_params
        self.current_endpoint = "/players"

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}
        
        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def get_argentina_leagues(self, params = None, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request the Argentina leagues endpoint and return the API response.

        Parameters:
            params (dict|None): Query parameters to include in the request.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is not None:
            current_params.update(params)

        self.params_not_to_include = self._normalize_params_not_to_include(["league"],params_not_to_include)

        self.current_params = current_params
        self.current_endpoint = "/leagues"

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}

        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def get_argentina_league_fixture_data(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request fixture data for the Argentina league, optionally by fixture ID.

        Parameters:
            params (dict): Query parameters to include in the request. Common
                params passed to this method are ``{'id': fixture_id}`` when
                retrieving match_summary, match_teams, and match_scores.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is None:
            pass
        else:
            current_params.update(params)

        if params.get("id",None):
            params_not_to_include = [k for k, v in current_params.items() if k not in ["id"]]
        self.params_not_to_include = self._normalize_params_not_to_include(["country"], params_not_to_include)

        self.current_params = current_params
        self.current_endpoint = "/fixtures"

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}
        
        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def get_argentina_league_fixture_data_players_statistics(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request fixture player statistics for the Argentina league.

        Parameters:
            params (dict): Query parameters to include in the request. Common
                params passed to this method are ``{'fixture': fixture_id}`` when
                retrieving match_players and match_player_stats.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is None:
            pass
        else:
            current_params.update(params)

        self.params_not_to_include = self._normalize_params_not_to_include(["country","league","season"], params_not_to_include)
        

        self.current_params = current_params
        self.current_endpoint = "/fixtures/players"

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}
        
        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def get_argentina_league_fixture_data_events(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request fixture events for the Argentina league.

        Parameters:
            params (dict): Query parameters to include in the request. Common
                params passed to this method are ``{'fixture': fixture_id}``.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """
        return self.get_custom_api_request(
            "/fixtures/events",
            params,
            params_not_to_include=self._normalize_params_not_to_include(
                ["country", "league", "season", "id"], params_not_to_include
            ),
            save_file=save_file,
            iterate_pages=iterate_pages,
            flatten_result=flatten_result
        )

    def get_argentina_league_fixture_data_lineups(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request fixture lineups for the Argentina league.

        Parameters:
            params (dict): Query parameters to include in the request. Common
                params passed to this method are ``{'fixture': fixture_id}`` when
                retrieving match_lineups and teams_coaches.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """
        return self.get_custom_api_request(
            "/fixtures/lineups",
            params,
            params_not_to_include=self._normalize_params_not_to_include(
                ["country", "league", "season", "id"], params_not_to_include
            ),
            save_file=save_file,
            iterate_pages=iterate_pages,
            flatten_result=flatten_result
        )

    def get_argentina_league_fixture_data_statistics(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request fixture statistics for the Argentina league.

        Parameters:
            params (dict): Query parameters to include in the request. Common
                params passed to this method are ``{'fixture': fixture_id}`` when
                retrieving match_team_stats.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is None:
            pass
        else:
            current_params.update(params)

        self.params_not_to_include = self._normalize_params_not_to_include(["country","league","season"], params_not_to_include)
        

        self.current_params = current_params
        self.current_endpoint = "/fixtures/statistics"

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}
        
        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def get_argentina_league_fixture_data_injuries(self, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Request fixture injury data for the Argentina league.

        Parameters:
            params (dict): Query parameters to include in the request.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is None:
            pass
        else:
            current_params.update(params)

        self.params_not_to_include = self._normalize_params_not_to_include(["country"], params_not_to_include)
        

        self.current_params = current_params
        self.current_endpoint = "/injuries"

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}
        
        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def get_custom_api_request(self, endpoint, params, params_not_to_include = None, save_file = False, iterate_pages = False, flatten_result = False):
        """Send a custom API request to the specified endpoint using current client defaults.

        Parameters:
            endpoint (str): API endpoint path to call.
            params (dict): Query parameters to include in the request.
            params_not_to_include (list|tuple|set|str|None): Parameters to exclude from the request.
            save_file (bool): Save the raw response to a JSON file when True.
            iterate_pages (bool): Retrieve all paginated pages when True.
            flatten_result (bool): Save a flattened JSON file when True.

        Returns:
            dict|list: Parsed API response.
        """

        current_params = self.general_params.copy()
        if params is None:
            pass
        else:
            current_params.update(params)

        self.params_not_to_include = self._normalize_params_not_to_include([], params_not_to_include)
        

        self.current_params = current_params
        self.current_endpoint = endpoint

        filtered_params = {k: v for k, v in self.current_params.items() if k not in self.params_not_to_include}
        
        return self._iterate_response(self.current_endpoint, filtered_params, save_file = save_file, iterate_pages = iterate_pages, flatten_result = flatten_result)

    def retrieve_full_fixture_data(self, params = None):
        """Retrieve and normalize a full fixture payload into pre-validated table groups.

        Parameters:
            params (dict|None): Parameters to use for fixture retrieval (e.g. {'id': 1234}).

        Returns:
            dict: Normalized fixture tables keyed by table name.
        """

        fixture_data = self.get_argentina_league_fixture_data(save_file = False, 
                                                                params = params or {},
                                                                params_not_to_include=["fixture"],
                                                                iterate_pages = True, 
                                                                flatten_result = False)

        if isinstance(fixture_data, dict) and "response" in fixture_data:
            fixture_data = fixture_data.get("response", [])

        if isinstance(fixture_data, list) and len(fixture_data) != 1:
            fixture_payloads = []
            pending_items = list(fixture_data)
            while pending_items:
                fixture_payload = pending_items.pop(0)
                if isinstance(fixture_payload, list):
                    pending_items[0:0] = fixture_payload
                elif isinstance(fixture_payload, dict):
                    fixture_payloads.append(fixture_payload)

            resulting_tables = {table_name: [] for table_name in TABLE_NAME_TO_PARQUET_SCHEMA}
            resulting_tables.pop("available_leagues", None)
            for fixture_payload in fixture_payloads:
                fixture_tables = self._split_fixture_payload_to_tables(fixture_payload)
                for table_name, table_data in fixture_tables.items():
                    resulting_tables[table_name].extend(
                        self._normalize_table_rows(table_data)
                    )
        else:
            resulting_tables = self._split_fixture_payload_to_tables(fixture_data)

        return resulting_tables

    def retrieve_full_league_data(self, params = None):
        """Retrieve and normalize league payload data into a validated league table.

        Parameters:
            params (dict|None): Parameters to use for league retrieval.

        Returns:
            dict: Normalized league table keyed by 'available_leagues'.
        """

        league_data = self.get_argentina_leagues(
            params=params or {},
            save_file=False,
            iterate_pages=True,
            flatten_result=False
        )

        resulting_tables = self.split_league_payload_to_table(league_data)

        return resulting_tables

    def _normalize_table_rows(self, table_data):
        """Ensure table data is returned as a list of row dictionaries."""
        if table_data is None:
            return []
        if isinstance(table_data, list):
            return table_data
        if isinstance(table_data, dict):
            return [table_data]
        return [{"value": table_data}]

    def _response_items(self, api_response):
        """Return a flat list of objects from a normal or paginated API response."""
        if isinstance(api_response, dict) and "response" in api_response:
            api_response = api_response.get("response")

        items = []
        pending = list(api_response) if isinstance(api_response, list) else [api_response]
        while pending:
            item = pending.pop(0)
            if isinstance(item, list):
                pending[0:0] = item
            elif isinstance(item, dict):
                items.append(item)
        return items

    def _fixture_table_is_empty(self, table_data):
        """Return True when a fixture table is absent or contains no rows."""
        if table_data is None:
            return True
        if isinstance(table_data, pd.DataFrame):
            return table_data.empty
        if isinstance(table_data, dict):
            return len(list(table_data.values())) == 0
        if isinstance(table_data, (list, tuple, set)):
            return len(table_data) == 0
        return False

    def _fixture_fallback_tables(self, fixture_id, resulting_tables):
        """Fill empty fixture tables from API-Football's dedicated endpoints."""
        self._log_event("INFO", f"checking fixture {fixture_id} for fallback data")
        table_names = (
            "match_summary", "match_teams", "match_scores", "match_events",
            "match_lineups", "teams_coaches", "match_team_stats",
            "match_players", "match_player_stats"
        )
        # Set of table names whose values are absent or empty. Keeping this as a
        # set makes the endpoint-group intersections below explicit during debug.
        missing = {
            name for name in table_names
            if self._fixture_table_is_empty(resulting_tables.get(name))
        }
        if not missing:
            self._log_event(
                "INFO",
                f"fixture {fixture_id} has all tables; no fallback requests needed"
            )
            return resulting_tables

        self._log_event(
            "INFO",
            f"fixture {fixture_id} fallback required for tables: {sorted(missing)}"
        )

        # Summary, teams, goals and score are fields of the fixture itself.  The
        # dedicated API query for those fields is GET /fixtures?id={fixture_id}.
        # Set of tables recovered together by the core /fixtures endpoint.
        core_tables = {"match_summary", "match_teams", "match_scores"}
        # The intersection contains only core tables that actually need fallback.
        if missing & core_tables:
            response = self.get_argentina_league_fixture_data(
                {"id": fixture_id}, save_file=False, iterate_pages=False
            )
            items = self._response_items(response)
            if items:
                fallback = self._split_fixture_payload_to_tables(items[0])
                for name in missing & core_tables:
                    resulting_tables[name] = fallback.get(name)
            recovered = sorted(
                name for name in missing & core_tables
                if not self._fixture_table_is_empty(resulting_tables.get(name))
            )
            still_empty = sorted((missing & core_tables) - set(recovered))
            self._log_event(
                "INFO",
                f"fixture {fixture_id} /fixtures fallback recovered {recovered}; "
                f"still empty {still_empty}"
            )

        # Each set groups tables produced by one individual API endpoint, so one
        # request can recover every empty table in that group.
        endpoint_groups = (
            (
                {"match_events"},
                self.get_argentina_league_fixture_data_events,
                "events"
            ),
            (
                {"match_lineups", "teams_coaches"},
                self.get_argentina_league_fixture_data_lineups,
                "lineups"
            ),
            (
                {"match_team_stats"},
                self.get_argentina_league_fixture_data_statistics,
                "statistics"
            ),
            (
                {"match_players", "match_player_stats"},
                self.get_argentina_league_fixture_data_players_statistics,
                "players"
            ),
        )
        for group, endpoint_method, payload_key in endpoint_groups:
            # Set intersection selects only empty tables served by this endpoint.
            requested_tables = missing & group
            if not requested_tables:
                continue
            response = endpoint_method(
                {"fixture": fixture_id}, save_file=False, iterate_pages=False
            )
            items = self._response_items(response)
            fallback = self._split_fixture_payload_to_tables({
                "fixture": {"id": fixture_id},
                payload_key: items
            })
            for name in requested_tables:
                resulting_tables[name] = fallback.get(name)

            recovered = sorted(
                name for name in requested_tables
                if not self._fixture_table_is_empty(resulting_tables.get(name))
            )
            still_empty = sorted(requested_tables - set(recovered))
            self._log_event(
                "INFO",
                f"fixture {fixture_id} /fixtures/{payload_key} fallback recovered "
                f"{recovered}; still empty {still_empty}"
            )

        remaining_empty = sorted(
            name for name in missing
            if self._fixture_table_is_empty(resulting_tables.get(name))
        )
        recovered_tables = sorted(missing - set(remaining_empty))
        self._log_event(
            "INFO",
            f"fixture {fixture_id} fallback completed; recovered {recovered_tables}; "
            f"no extra data found for {remaining_empty}"
        )

        return resulting_tables

    def _save_parquet(self, df_name, df_dict):
        """Save the provided table data to a parquet file using a normalized DataFrame."""

        if df_dict is None:
            return

        if isinstance(df_dict, dict):
            if all(isinstance(v, (list, tuple, set)) for v in df_dict.values()):
                df = pd.DataFrame(df_dict)
            else:
                df = pd.DataFrame([df_dict])
        elif isinstance(df_dict, list):
            normalized_rows = []
            for row in df_dict:
                if isinstance(row, dict):
                    normalized_rows.append(row)
                else:
                    normalized_rows.append({"value": row})
            df = pd.DataFrame(normalized_rows)
        else:
            df = pd.DataFrame([{"value": df_dict}])

        df.to_parquet(f"{df_name}.parquet")

    def run_leagues(self, params = None, consolidate_output = False, save_to_parquet = False):
        """Retrieve, validate, and optionally save the available leagues table.

        Parameters:
            params (dict|None): Parameters to use for league retrieval.
            consolidate_output (bool): When True, save output with a consolidated file prefix.
            save_to_parquet (bool): When False, do not write parquet files to disk.

        Returns:
            dict: Retrieved league tables keyed by table name.
        """
        resulting_tables = self.retrieve_full_league_data(params=params)

        if save_to_parquet:
            for table_name, table_data in resulting_tables.items():
                if consolidate_output:
                    full_table_name = f"consolidated_leagues_{table_name}"
                else:
                    schema_name = TABLE_NAME_TO_PARQUET_SCHEMA.get(
                        table_name,
                        f"{table_name}.parquet"
                    )
                    full_table_name = Path(schema_name).stem
                self._log_event("INFO", f"saving {full_table_name}")
                self._save_parquet(full_table_name, table_data)

        return resulting_tables

    def run_full_season_data(self, params, consolidate_output = True, save_to_parquet = False):
        """Retrieve, validate, and optionally save all fixtures for a league and season.

        Parameters:
            params (dict): Required keys are 'league' and 'season'.
            consolidate_output (bool): When True, save consolidated fixture tables.
            save_to_parquet (bool): When False, do not write parquet files to disk.

        Returns:
            dict: Fixture tables keyed by table name, either consolidated or per-fixture.
        """
        if not isinstance(params, dict):
            raise TypeError("params must be a dict")

        required_params = {"league", "season"}
        missing_params = required_params - set(params)
        if missing_params:
            raise ValueError(f"Missing required params: {sorted(missing_params)}")

        league_id = params.get("league")
        season_id = params.get("season")
        if isinstance(league_id, bool) or not isinstance(league_id, int):
            raise TypeError("params['league'] must be an integer")
        if league_id <= 0:
            raise ValueError("params['league'] must be greater than zero")
        if isinstance(season_id, bool) or not isinstance(season_id, int):
            raise TypeError("params['season'] must be an integer")
        if season_id <= 0:
            raise ValueError("params['season'] must be greater than zero")
        if not isinstance(consolidate_output, bool):
            raise TypeError("consolidate_output must be a bool")

        self._log_event("INFO", f"\n\n==========Extracting fixtures id in league {league_id} and season {season_id}\n\n")
        request_params = params.copy()
        fixture_tables = self.retrieve_full_fixture_data(
            params=request_params
        )

        fixture_ids = sorted({
            row.get("fixture_id")
            for row in self._normalize_table_rows(fixture_tables.get("match_summary"))
            if isinstance(row, dict) and row.get("fixture_id") is not None
        })

        self._log_event("INFO", f"\n\n==========Extracting fixtures data in league {league_id} and season {season_id}\n\n")

        return self.run_fixtures(
            fixture_ids,
            consolidate_output=consolidate_output,
            save_to_parquet=save_to_parquet
        )

    def run_fixtures(self, fixture_ids, consolidate_output = False, save_to_parquet = False):
        """Retrieve fixture data for each fixture ID and optionally save to parquet files.

        Parameters:
            fixture_ids (list[int]): Fixture IDs to retrieve.
            consolidate_output (bool): When True, merge all fixtures into consolidated tables.
            save_to_parquet (bool): When False, do not write parquet files to disk.

        Returns:
            dict: Fixture results keyed by table name or fixture ID.
        """

        if consolidate_output:
            match_summary = []
            match_teams = []
            match_scores = []
            match_events = []
            match_lineups = []
            teams_coaches = []
            match_team_stats = []
            match_players = []
            match_player_stats = []
        else:
            processed_fixtures = {}

        for fixture_id in fixture_ids:
            resulting_tables = self.retrieve_full_fixture_data(params={"id":fixture_id})
            resulting_tables = self._fixture_fallback_tables(fixture_id, resulting_tables)

            if consolidate_output:
                match_summary.extend(self._normalize_table_rows(resulting_tables.get("match_summary")))
                match_teams.extend(self._normalize_table_rows(resulting_tables.get("match_teams")))
                match_scores.extend(self._normalize_table_rows(resulting_tables.get("match_scores")))
                match_events.extend(self._normalize_table_rows(resulting_tables.get("match_events")))
                match_lineups.extend(self._normalize_table_rows(resulting_tables.get("match_lineups")))
                teams_coaches.extend(self._normalize_table_rows(resulting_tables.get("teams_coaches")))
                match_team_stats.extend(self._normalize_table_rows(resulting_tables.get("match_team_stats")))
                match_players.extend(self._normalize_table_rows(resulting_tables.get("match_players")))
                match_player_stats.extend(self._normalize_table_rows(resulting_tables.get("match_player_stats")))

            else:

                processed_fixtures[fixture_id] = resulting_tables

                if save_to_parquet:
                    for tbl_name, tbl_dict in resulting_tables.items():
                        full_table_name = f"fixture_{fixture_id}_{tbl_name}"
                        self._log_event("INFO",f"saving {full_table_name}")
                        self._save_parquet(full_table_name, tbl_dict)
            
        if consolidate_output:

            total_output = {
                    "match_summary": match_summary,
                    "match_teams": match_teams,
                    "match_scores": match_scores,
                    "match_events": match_events,
                    "match_lineups": match_lineups,
                    "teams_coaches": teams_coaches,
                    "match_team_stats": match_team_stats,
                    "match_players": match_players,
                    "match_player_stats": match_player_stats,
                }
            
            if save_to_parquet:
                for tbl_name, tbl_dict in total_output.items():
                    full_table_name = f"consolidated_fixtures_{tbl_name}"
                    self._log_event("INFO",f"saving {full_table_name}")
                    self._save_parquet(full_table_name, tbl_dict)

            return total_output

        return processed_fixtures


if __name__ == "__main__":

    api = FootballAPI(api_key = os.environ["API_KEY"], general_params = {"season":2026}, debug = True)
    # self._log_event("INFO",api.general_params)
    # api.get_argentina_league_teams(save_file = True, iterate_pages = True)
    # api.get_argentina_league_players(save_file = True, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2015}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2016}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2017}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2018}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2019}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2020}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2021}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2022}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2023}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2024}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2025}, iterate_pages = True)
    # api.get_argentina_league_fixture_data(save_file = True, params = {"season":2026}, iterate_pages = True)
    # api.get_argentina_league_fixture_data_players_statistics(params={"fixture":1492037,"season":2026},save_file = True, iterate_pages = True)
    # api.get_argentina_league_fixture_data_statistics(params={"fixture":1544177,"season":2026},save_file = True, iterate_pages = True, flatten_result = True)
    # api.get_argentina_league_fixture_data_injuries(params={"fixture":1492038},save_file = True, iterate_pages = True)
    # api.get_custom_api_request("/players",
    #                            params={},
    #                            params_not_to_include=["country"],
    #                            save_file = True,
    #                            iterate_pages = True)
    # api.get_custom_api_request("/fixtures/players",
    #                            params={"fixture":1159026},
    #                            params_not_to_include=["league","season","country"],
    #                            save_file = True,
    #                            iterate_pages = True)

    # api.retrieve_full_fixture_data(params={"id":1159026})
    # api.run_leagues({"season":2026,"country":"argentina"})
    api.run_full_season_data({"league":128,"season":2026, "date":"2026-07-29"}, consolidate_output=True)
    # api.get_argentina_league_fixture_data(params={"league":130,"season":2025},save_file=True)
    # api.run_fixtures([1493018],consolidate_output=True)
    # api.run_fixtures([
    # 831664,
    # 831665,
    # 831666,
    # 831667,
    # 831668,
    # 831669,
    # 831670,
    # 831671,
    # 831672,
    # 831673,
    # 831674,
    # 831675,
    # 831676,
    # 831677,
    # 831678,
    # 831679,
    # 831680,
    # 831682,
    # 831681,
    # 831683,
    # 831684,
    # 831685,
    # 831686,
    # 831687,
    # 831689,
    # 831688,
    # 831690,
    # 831691,
    # 831692,
    # 831693,
    # 831694,
    # 831695,
    # 831696,
    # 831697,
    # 831698,
    # 831700,
    # 831699,
    # 831701,
    # 831703,
    # 831702,
    # 831704,
    # 831705,
    # 831718,
    # 831712,
    # 831706,
    # 831716,
    # 831714,
    # 831715,
    # 831717,
    # 831719,
    # 831707,
    # 831710,
    # 831711,
    # 831709,
    # 831713,
    # 831708,
    # 831731,
    # 831726,
    # 831721,
    # 831732,
    # 831722,
    # 831725,
    # 831723,
    # 831720,
    # 831724,
    # 831729,
    # 831728,
    # 831730,
    # 831727,
    # 831733,
    # 831747,
    # 831735,
    # 831745,
    # 831742,
    # 831743,
    # 831736,
    # 831738,
    # 831734,
    # 831741,
    # 831739,
    # 831746,
    # 831737,
    # 831744,
    # 831740,
    # 831758,
    # 831760,
    # 831761,
    # 831755,
    # 831748,
    # 831759,
    # 831756,
    # 831757,
    # 831749,
    # 831752,
    # 831750,
    # 831751,
    # 831754,
    # 831753,
    # 831775,
    # 831764,
    # 831768,
    # 831772,
    # 831762,
    # 831773,
    # 831766,
    # 831769,
    # 831771,
    # 831763,
    # 831770,
    # 831765,
    # 831767,
    # 831774,
    # 831780,
    # 831788,
    # 831784,
    # 831779,
    # 831777,
    # 831781,
    # 831785,
    # 831789,
    # 831776,
    # 831787,
    # 831778,
    # 831782,
    # 831786,
    # 831783,
    # 831793,
    # 831801,
    # 831803,
    # 831799,
    # 831796,
    # 831794,
    # 831802,
    # 831800,
    # 831795,
    # 831798,
    # 831792,
    # 831791,
    # 831797,
    # 831790,
    # 831815,
    # 831806,
    # 831813,
    # 831814,
    # 831812,
    # 831807,
    # 831804,
    # 831816,
    # 831805,
    # 831811,
    # 831810,
    # 831817,
    # 831808,
    # 831809,
    # 831829,
    # 831822,
    # 831823,
    # 831831,
    # 831826,
    # 831818,
    # 831827,
    # 831824,
    # 831819,
    # 831825,
    # 831821,
    # 831828,
    # 831820,
    # 831830,
    # 831833,
    # 831841,
    # 831834,
    # 831838,
    # 831832,
    # 831843,
    # 831844,
    # 831842,
    # 831839,
    # 831837,
    # 831845,
    # 831835,
    # 831840,
    # 831836,
    # 831852,
    # 831854,
    # 831855,
    # 831859,
    # 831857,
    # 831858,
    # 831851,
    # 831850,
    # 831847,
    # 831849,
    # 831848,
    # 831856,
    # 831853,
    # 831846,
    # 861539,
    # 861540,
    # 861541,
    # 861542,
    # 861839,
    # 861840,
    # 862248,
    # 863169,
    # 863171,
    # 863170,
    # 863172,
    # 863173,
    # 863174,
    # 863175,
    # 863176,
    # 863177,
    # 863178,
    # 863179,
    # 863180,
    # 863181,
    # 863182,
    # 863193,
    # 863189,
    # 863185,
    # 863190,
    # 863186,
    # 863192,
    # 863196,
    # 863194,
    # 863191,
    # 863184,
    # 863183,
    # 863187,
    # 863188,
    # 863195,
    # 863200,
    # 863204,
    # 863201,
    # 863208,
    # 863203,
    # 863197,
    # 863199,
    # 863209,
    # 863210,
    # 863198,
    # 863207,
    # 863202,
    # 863206,
    # 863205,
    # 863221,
    # 863218,
    # 863223,
    # 863213,
    # 863212,
    # 863211,
    # 863224,
    # 863214,
    # 863216,
    # 863215,
    # 863219,
    # 863220,
    # 863217,
    # 863222,
    # 863234,
    # 863226,
    # 863225,
    # 863235,
    # 863228,
    # 863237,
    # 863230,
    # 863238,
    # 863227,
    # 863232,
    # 863233,
    # 863229,
    # 863231,
    # 863236,
    # 863249,
    # 863252,
    # 863250,
    # 863242,
    # 863241,
    # 863243,
    # 863239,
    # 863251,
    # 863245,
    # 863240,
    # 863248,
    # 863247,
    # 863244,
    # 863246,
    # 863261,
    # 863258,
    # 863254,
    # 863253,
    # 863255,
    # 863259,
    # 863257,
    # 863260,
    # 863266,
    # 863263,
    # 863265,
    # 863262,
    # 863264,
    # 863256,
    # 863276,
    # 863272,
    # 863275,
    # 863271,
    # 863278,
    # 863270,
    # 863273,
    # 863274,
    # 863279,
    # 863268,
    # 863267,
    # 863280,
    # 863277,
    # 863269,
    # 863291,
    # 863286,
    # 863287,
    # 863290,
    # 863283,
    # 863281,
    # 863285,
    # 863289,
    # 863284,
    # 863294,
    # 863293,
    # 863288,
    # 863282,
    # 863292,
    # 863299,
    # 863303,
    # 863300,
    # 863304,
    # 863298,
    # 863297,
    # 863306,
    # 863308,
    # 863307,
    # 863301,
    # 863296,
    # 863295,
    # 863302,
    # 863305,
    # 863322,
    # 863310,
    # 863311,
    # 863321,
    # 863317,
    # 863320,
    # 863313,
    # 863314,
    # 863312,
    # 863319,
    # 863315,
    # 863316,
    # 863318,
    # 863324,
    # 863334,
    # 863326,
    # 863329,
    # 863332,
    # 863323,
    # 863336,
    # 863333,
    # 863325,
    # 863335,
    # 863327,
    # 863328,
    # 863330,
    # 863331,
    # 863349,
    # 863345,
    # 863344,
    # 863348,
    # 863340,
    # 863339,
    # 863342,
    # 863343,
    # 863347,
    # 863346,
    # 863350,
    # 863338,
    # 863337,
    # 863341,
    # 863353,
    # 863357,
    # 863362,
    # 863354,
    # 863358,
    # 863355,
    # 863356,
    # 863360,
    # 863359,
    # 863352,
    # 863351,
    # 863363,
    # 863361,
    # 863364,
    # 863372,
    # 863375,
    # 863374,
    # 863376,
    # 863373,
    # 863370,
    # 863369,
    # 863365,
    # 863368,
    # 863377,
    # 863367,
    # 863366,
    # 863378,
    # 863371,
    # 863384,
    # 863383,
    # 863389,
    # 863388,
    # 863382,
    # 863390,
    # 863391,
    # 863385,
    # 863380,
    # 863386,
    # 863381,
    # 863387,
    # 863379,
    # 863392,
    # 863402,
    # 863401,
    # 863396,
    # 863404,
    # 863406,
    # 863395,
    # 863405,
    # 863393,
    # 863394,
    # 863399,
    # 863400,
    # 863403,
    # 863397,
    # 863398,
    # 863408,
    # 863407,
    # 863418,
    # 863413,
    # 863420,
    # 863419,
    # 863412,
    # 863411,
    # 863416,
    # 863415,
    # 863409,
    # 863410,
    # 863414,
    # 863417,
    # 863434,
    # 863430,
    # 863429,
    # 863422,
    # 863421,
    # 863433,
    # 863426,
    # 863427,
    # 863428,
    # 863423,
    # 863424,
    # 863432,
    # 863425,
    # 863440,
    # 863436,
    # 863439,
    # 863443,
    # 863442,
    # 863444,
    # 863448,
    # 863437,
    # 863435,
    # 863441,
    # 863446,
    # 863445,
    # 863447,
    # 863438,
    # 863457,
    # 863456,
    # 863451,
    # 863461,
    # 863455,
    # 863454,
    # 863462,
    # 863459,
    # 863450,
    # 863449,
    # 863458,
    # 863452,
    # 863453,
    # 863460,
    # 863467,
    # 863473,
    # 863466,
    # 863476,
    # 863463,
    # 863475,
    # 863465,
    # 863470,
    # 863469,
    # 863464,
    # 863474,
    # 863468,
    # 863471,
    # 863472,
    # 863477,
    # 863487,
    # 863484,
    # 863490,
    # 863488,
    # 863485,
    # 863480,
    # 863478,
    # 863479,
    # 863481,
    # 863489,
    # 863483,
    # 863482,
    # 863494,
    # 863497,
    # 863491,
    # 863502,
    # 863493,
    # 863496,
    # 863498,
    # 863495,
    # 863501,
    # 863504,
    # 863499,
    # 863503,
    # 863500,
    # 863492,
    # 863514,
    # 863513,
    # 863515,
    # 863516,
    # 863508,
    # 863512,
    # 863511,
    # 863509,
    # 863506,
    # 863517,
    # 863507,
    # 863505,
    # 863510,
    # 863518,
    # 863527,
    # 863530,
    # 863524,
    # 863529,
    # 863521,
    # 863531,
    # 863526,
    # 863525,
    # 863519,
    # 863528,
    # 863520,
    # 863522,
    # 863523,
    # 863532,
    # 863309,
    # 863486,
    # 863533,
    # 863546,
    # 863541,
    # 863538,
    # 863540,
    # 863537,
    # 863539,
    # 863544,
    # 863542,
    # 863536,
    # 863535,
    # 863545,
    # 863534,
    # 863431,
    # 863543,
    # ], consolidate_output=True)
