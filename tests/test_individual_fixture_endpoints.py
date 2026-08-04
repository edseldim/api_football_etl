import unittest

import pandas as pd

from src.raw_api_data_etl import (
    EXPECTED_PARQUET_SCHEMAS,
    TABLE_NAME_TO_PARQUET_SCHEMA,
    FootballAPI,
)


class RecordingFootballAPI(FootballAPI):
    """FootballAPI test double that records requests instead of using HTTP."""

    def __init__(self, responses):
        super().__init__(
            "dummy",
            general_params={"league": 128, "season": 2026, "country": "argentina"},
            log_event=lambda *_: None,
        )
        self.responses = responses
        self.requests = []

    def _iterate_response(
        self,
        endpoint,
        params,
        save_file=False,
        iterate_pages=False,
        flatten_result=False,
    ):
        self.requests.append({
            "endpoint": endpoint,
            "params": params,
            "save_file": save_file,
            "iterate_pages": iterate_pages,
            "flatten_result": flatten_result,
        })
        return self.responses[endpoint]


class IndividualFixtureEndpointTests(unittest.TestCase):
    """Protect endpoint routing and raw API output contracts used by the pipeline."""

    def setUp(self):
        self.fixture_id = 123
        self.responses = {
            "/fixtures": {
                "response": [{
                    "fixture": {"id": self.fixture_id},
                    "teams": {"home": {"id": 1}, "away": {"id": 2}},
                    "goals": {"home": 2, "away": 1},
                    "score": {"fulltime": {"home": 2, "away": 1}},
                }]
            },
            "/fixtures/events": {
                "response": [{
                    "time": {"elapsed": 15, "extra": None},
                    "team": {"id": 1, "name": "Home"},
                    "player": {"id": 10, "name": "Scorer"},
                    "type": "Goal",
                    "detail": "Normal Goal",
                }]
            },
            "/fixtures/lineups": {
                "response": [{
                    "team": {"id": 1, "name": "Home"},
                    "formation": "4-4-2",
                    "coach": {"id": 7, "name": "Coach"},
                    "startXI": [{"player": {
                        "id": 10,
                        "name": "Player",
                        "number": 9,
                        "pos": "F",
                        "grid": "4:2",
                    }}],
                    "substitutes": [],
                }]
            },
            "/fixtures/statistics": {
                "response": [{
                    "team": {"id": 1, "name": "Home"},
                    "statistics": [{"type": "Shots on Goal", "value": 3}],
                }]
            },
            "/fixtures/players": {
                "response": [{
                    "team": {"id": 1, "name": "Home"},
                    "players": [{
                        "player": {"id": 10, "name": "Player"},
                        "statistics": [{
                            "games": {"minutes": 90, "rating": "7.5"},
                            "shots": {"total": 2, "on": 1},
                        }],
                    }],
                }]
            },
        }
        self.api = RecordingFootballAPI(self.responses)

    def _assert_endpoint_contract(self, method, endpoint, params):
        expected_output = self.responses[endpoint]

        actual_output = method(
            params,
            save_file=False,
            iterate_pages=True,
            flatten_result=False,
        )

        self.assertEqual(actual_output, expected_output)
        self.assertEqual(len(self.api.requests), 1)
        self.assertEqual(self.api.requests[0], {
            "endpoint": endpoint,
            "params": params,
            "save_file": False,
            "iterate_pages": True,
            "flatten_result": False,
        })
        self.api.requests.clear()

    def test_fixture_summary_teams_and_scores_endpoint_contract(self):
        self._assert_endpoint_contract(
            self.api.get_argentina_league_fixture_data,
            "/fixtures",
            {"id": self.fixture_id},
        )

    def test_fixture_events_endpoint_contract(self):
        self._assert_endpoint_contract(
            self.api.get_argentina_league_fixture_data_events,
            "/fixtures/events",
            {"fixture": self.fixture_id},
        )

    def test_fixture_lineups_and_coaches_endpoint_contract(self):
        self._assert_endpoint_contract(
            self.api.get_argentina_league_fixture_data_lineups,
            "/fixtures/lineups",
            {"fixture": self.fixture_id},
        )

    def test_fixture_team_statistics_endpoint_contract(self):
        self._assert_endpoint_contract(
            self.api.get_argentina_league_fixture_data_statistics,
            "/fixtures/statistics",
            {"fixture": self.fixture_id},
        )

    def test_fixture_players_and_player_statistics_endpoint_contract(self):
        self._assert_endpoint_contract(
            self.api.get_argentina_league_fixture_data_players_statistics,
            "/fixtures/players",
            {"fixture": self.fixture_id},
        )

    def test_all_nine_normalized_tables_pass_expected_schema_checks(self):
        payload = {
            "fixture": {
                "id": self.fixture_id,
                "referee": "Referee",
                "timezone": "UTC",
                "date": "2026-08-03T20:00:00+00:00",
                "timestamp": 1785787200,
                "venue": {"id": 100, "name": "Stadium", "city": "City"},
                "status": {"long": "Match Finished", "short": "FT", "elapsed": 90, "extra": 3},
            },
            "league": {
                "id": 128,
                "name": "Liga Profesional Argentina",
                "country": "Argentina",
                "season": 2026,
                "round": "Regular Season - 1",
            },
            "teams": {
                "home": {"id": 1, "name": "Home", "logo": "home.png", "winner": True},
                "away": {"id": 2, "name": "Away", "logo": "away.png", "winner": False},
            },
            "goals": {"home": 2, "away": 1},
            "score": {
                "halftime": {"home": 1, "away": 0},
                "fulltime": {"home": 2, "away": 1},
                "extratime": {"home": None, "away": None},
                "penalty": {"home": None, "away": None},
            },
            "events": [{
                "time": {"elapsed": 15, "extra": None},
                "team": {"id": 1, "name": "Home"},
                "player": {"id": 10, "name": "Player"},
                "assist": {"id": 11, "name": "Assistant"},
                "type": "Goal",
                "detail": "Normal Goal",
                "comments": None,
            }],
            "lineups": [{
                "team": {"id": 1, "name": "Home"},
                "formation": "4-4-2",
                "coach": {"id": 7, "name": "Coach"},
                "startXI": [{"player": {
                    "id": 10, "name": "Player", "number": 9, "pos": "F", "grid": "4:2"
                }}],
                "substitutes": [{"player": {
                    "id": 12, "name": "Substitute", "number": 18, "pos": "F", "grid": None
                }}],
            }],
            "statistics": [{
                "team": {"id": 1, "name": "Home"},
                "statistics": [{"type": "Shots on Goal", "value": 3}],
            }],
            "players": [{
                "team": {"id": 1, "name": "Home"},
                "players": [{
                    "player": {"id": 10, "name": "Player", "photo": "player.png"},
                    "statistics": [{
                        "games": {"minutes": 90, "number": 9, "position": "F",
                                  "rating": "7.5", "captain": False, "substitute": False},
                        "offsides": 1,
                        "shots": {"total": 2, "on": 1},
                        "goals": {"total": 1, "conceded": 0, "assists": 1, "saves": 0},
                        "passes": {"total": 25, "key": 2, "accuracy": "84%"},
                        "tackles": {"total": 1, "blocks": 0, "interceptions": 1},
                        "duels": {"total": 5, "won": 3},
                        "dribbles": {"attempts": 2, "success": 1, "past": 0},
                        "fouls": {"drawn": 1, "committed": 0},
                        "cards": {"yellow": 0, "red": 0},
                        "penalty": {"won": 0, "commited": 0, "scored": 0,
                                    "missed": 0, "saved": 0},
                    }],
                }],
            }],
        }

        tables = self.api._split_fixture_payload_to_tables(payload)

        self.assertEqual(set(tables), set(TABLE_NAME_TO_PARQUET_SCHEMA) - {"available_leagues"})
        for table_name, table_data in tables.items():
            with self.subTest(table_name=table_name):
                rows = self.api._normalize_table_rows(table_data)
                self.assertTrue(rows, f"{table_name} should contain normalized rows")

                frame = pd.DataFrame(rows)
                schema_name = TABLE_NAME_TO_PARQUET_SCHEMA[table_name]
                expected_schema = EXPECTED_PARQUET_SCHEMAS[schema_name]
                self.assertEqual(set(frame.columns), set(expected_schema))

                is_valid, errors, _warnings = self.api.validate_parquet_schema(
                    frame,
                    expected_schema=expected_schema,
                    source_name=table_name,
                )
                self.assertTrue(is_valid, errors)
                self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
