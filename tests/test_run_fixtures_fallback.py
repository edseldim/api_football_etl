"""Test fixture fallback orchestration using deterministic endpoint responses."""

import unittest

from src.api_football_etl.raw_api_data_etl import FootballAPI


class FallbackFootballAPI(FootballAPI):
    """Return empty core data and record each dedicated fallback endpoint call."""
    def __init__(self):
        super().__init__("dummy", log_event=lambda *_: None)
        self.calls = []

    def retrieve_full_fixture_data(self, params=None):
        # Simulate a /fixtures response in which every requested table is absent.
        return {}

    def get_argentina_league_fixture_data(self, params, **kwargs):
        self.calls.append("fixture")
        return {"response": [{
            "fixture": {"id": params["id"], "venue": {}, "status": {}},
            "league": {},
            "teams": {
                "home": {"id": 1, "name": "Home", "winner": True},
                "away": {"id": 2, "name": "Away", "winner": False},
            },
            "goals": {"home": 2, "away": 1},
            "score": {"fulltime": {"home": 2, "away": 1}},
        }]}

    def get_argentina_league_fixture_data_events(self, params, **kwargs):
        self.calls.append("events")
        return {"response": [{"time": {"elapsed": 10}, "team": {"id": 1}}]}

    def get_argentina_league_fixture_data_lineups(self, params, **kwargs):
        self.calls.append("lineups")
        return {"response": [{
            "team": {"id": 1, "name": "Home"},
            "formation": "4-4-2",
            "coach": {"id": 7, "name": "Coach"},
            "startXI": [{"player": {
                "id": 10, "name": "Player", "number": 9, "pos": "F", "grid": "4:2"
            }}],
            "substitutes": [],
        }]}

    def get_argentina_league_fixture_data_statistics(self, params, **kwargs):
        self.calls.append("statistics")
        return {"response": [{
            "team": {"id": 1, "name": "Home"},
            "statistics": [{"type": "Shots on Goal", "value": 3}],
        }]}

    def get_argentina_league_fixture_data_players_statistics(self, params, **kwargs):
        self.calls.append("players")
        return {"response": [{
            "team": {"id": 1, "name": "Home"},
            "players": [{
                "player": {"id": 10, "name": "Player"},
                "statistics": [{
                    "games": {"minutes": 90, "number": 9, "position": "F",
                              "rating": "7.0", "captain": False, "substitute": False},
                    "offsides": None, "shots": {"total": 1, "on": 1},
                    "goals": {"total": 1, "conceded": 0, "assists": 0, "saves": 0},
                    "passes": {"total": 10, "key": 1, "accuracy": "80%"},
                    "tackles": {"total": 0, "blocks": 0, "interceptions": 0},
                    "duels": {"total": 1, "won": 1},
                    "dribbles": {"attempts": 0, "success": 0, "past": 0},
                    "fouls": {"drawn": 0, "committed": 0},
                    "cards": {"yellow": 0, "red": 0},
                    "penalty": {"won": 0, "commited": 0, "scored": 0,
                                "missed": 0, "saved": 0},
                }],
            }],
        }]}


class RunFixturesFallbackTests(unittest.TestCase):
    """Verify absent fixture tables are recovered through grouped endpoints."""

    def test_missing_tables_are_retrieved_from_individual_endpoints(self):
        """Return no initial tables and assert all endpoint groups populate rows."""
        api = FallbackFootballAPI()

        result = api.run_fixtures([123], consolidate_output=False)

        self.assertEqual(
            api.calls, ["fixture", "events", "lineups", "statistics", "players"]
        )
        for table_name, rows in result[123].items():
            self.assertTrue(rows, table_name)

    def test_present_but_empty_tables_use_fallbacks(self):
        """Return every table as an empty list and assert the same full recovery."""
        api = FallbackFootballAPI()
        table_names = (
            "match_summary", "match_teams", "match_scores", "match_events",
            "match_lineups", "teams_coaches", "match_team_stats",
            "match_players", "match_player_stats",
        )
        api.retrieve_full_fixture_data = lambda params=None: {
            name: [] for name in table_names
        }

        result = api.run_fixtures([456], consolidate_output=False)

        self.assertEqual(
            api.calls, ["fixture", "events", "lineups", "statistics", "players"]
        )
        for table_name, rows in result[456].items():
            self.assertTrue(rows, table_name)


if __name__ == "__main__":
    unittest.main()
