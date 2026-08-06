"""Test selective fixture fallbacks, preservation, empty responses, and logging."""

import copy
import unittest

from tests.test_run_fixtures_fallback import FallbackFootballAPI


TABLE_NAMES = (
    "match_summary",
    "match_teams",
    "match_scores",
    "match_events",
    "match_lineups",
    "teams_coaches",
    "match_team_stats",
    "match_players",
    "match_player_stats",
)


def populated_tables():
    """Return non-empty sentinels suitable for testing fallback selection."""
    return {
        table_name: [{"source": "original", "table": table_name}]
        for table_name in TABLE_NAMES
    }


class SelectiveFallbackAPI(FallbackFootballAPI):
    """Configurable fallback API that can return empty individual responses."""

    def __init__(self, initial_tables, empty_endpoints=None):
        super().__init__()
        self.initial_tables = initial_tables
        self.empty_endpoints = set(empty_endpoints or [])

    def retrieve_full_fixture_data(self, params=None):
        return copy.deepcopy(self.initial_tables)

    def get_argentina_league_fixture_data(self, params, **kwargs):
        if "fixture" in self.empty_endpoints:
            self.calls.append("fixture")
            return {"response": []}
        return super().get_argentina_league_fixture_data(params, **kwargs)

    def get_argentina_league_fixture_data_events(self, params, **kwargs):
        if "events" in self.empty_endpoints:
            self.calls.append("events")
            return {"response": []}
        return super().get_argentina_league_fixture_data_events(params, **kwargs)

    def get_argentina_league_fixture_data_lineups(self, params, **kwargs):
        if "lineups" in self.empty_endpoints:
            self.calls.append("lineups")
            return {"response": []}
        return super().get_argentina_league_fixture_data_lineups(params, **kwargs)

    def get_argentina_league_fixture_data_statistics(self, params, **kwargs):
        if "statistics" in self.empty_endpoints:
            self.calls.append("statistics")
            return {"response": []}
        return super().get_argentina_league_fixture_data_statistics(params, **kwargs)

    def get_argentina_league_fixture_data_players_statistics(self, params, **kwargs):
        if "players" in self.empty_endpoints:
            self.calls.append("players")
            return {"response": []}
        return super().get_argentina_league_fixture_data_players_statistics(params, **kwargs)


class FixtureFallbackEdgeCaseTests(unittest.TestCase):
    """Exercise fallback decisions with configurable initial and endpoint data."""

    def test_existing_non_empty_tables_are_preserved(self):
        """Empty one table and assert only its endpoint runs while peers stay intact."""
        tables = populated_tables()
        original_events = copy.deepcopy(tables["match_events"])
        tables["match_lineups"] = []

        api = SelectiveFallbackAPI(tables)
        result = api.run_fixtures([101], consolidate_output=False)[101]

        self.assertEqual(api.calls, ["lineups"])
        self.assertEqual(result["match_events"], original_events)
        self.assertEqual(result["match_lineups"][0]["fixture_id"], 101)

    def test_only_the_endpoint_for_each_empty_table_group_is_called(self):
        """Subtest every table and compare recorded calls with its endpoint group."""
        cases = (
            (("match_summary",), ["fixture"]),
            (("match_teams",), ["fixture"]),
            (("match_scores",), ["fixture"]),
            (("match_events",), ["events"]),
            (("match_lineups",), ["lineups"]),
            (("teams_coaches",), ["lineups"]),
            (("match_team_stats",), ["statistics"]),
            (("match_players",), ["players"]),
            (("match_player_stats",), ["players"]),
        )

        for empty_tables, expected_calls in cases:
            with self.subTest(empty_tables=empty_tables):
                tables = populated_tables()
                preserved = {
                    name: copy.deepcopy(value)
                    for name, value in tables.items()
                    if name not in empty_tables
                }
                for table_name in empty_tables:
                    tables[table_name] = []

                api = SelectiveFallbackAPI(tables)
                result = api.run_fixtures([202], consolidate_output=False)[202]

                self.assertEqual(api.calls, expected_calls)
                for table_name, original_value in preserved.items():
                    self.assertEqual(result[table_name], original_value, table_name)
                for table_name in empty_tables:
                    self.assertTrue(result[table_name], table_name)

    def test_no_endpoint_is_called_when_all_tables_are_populated(self):
        """Provide all tables and assert zero fallback calls and unchanged output."""
        tables = populated_tables()
        api = SelectiveFallbackAPI(tables)

        result = api.run_fixtures([303], consolidate_output=False)[303]

        self.assertEqual(api.calls, [])
        self.assertEqual(result, tables)

    def test_empty_individual_responses_leave_tables_empty_without_stopping_other_fallbacks(self):
        """Return empty endpoint payloads and assert later fallback groups still run."""
        tables = populated_tables()
        tables["match_events"] = []
        tables["match_lineups"] = []
        tables["teams_coaches"] = []
        tables["match_team_stats"] = []
        tables["match_players"] = []
        tables["match_player_stats"] = []
        api = SelectiveFallbackAPI(
            tables,
            empty_endpoints={"events", "lineups", "statistics"},
        )

        result = api.run_fixtures([404], consolidate_output=False)[404]

        self.assertEqual(api.calls, ["events", "lineups", "statistics", "players"])
        self.assertEqual(result["match_events"], [])
        self.assertEqual(result["match_lineups"], [])
        self.assertEqual(result["teams_coaches"], [])
        self.assertEqual(result["match_team_stats"], [])
        self.assertTrue(result["match_players"])
        self.assertTrue(result["match_player_stats"])

    def test_fallback_logs_recovered_and_unavailable_data(self):
        """Capture injected log events and assert recovery and absence summaries."""
        tables = populated_tables()
        tables["match_events"] = []
        tables["match_players"] = []
        api = SelectiveFallbackAPI(tables, empty_endpoints={"events"})
        log_messages = []
        api._log_event = lambda level, message: log_messages.append((level, message))

        api.run_fixtures([505], consolidate_output=False)

        info_messages = [message for level, message in log_messages if level == "INFO"]
        self.assertTrue(any("checking fixture 505" in message for message in info_messages))
        self.assertTrue(any("fallback required" in message for message in info_messages))
        self.assertTrue(any("recovered ['match_players']" in message for message in info_messages))
        self.assertTrue(any("no extra data found for ['match_events']" in message for message in info_messages))


if __name__ == "__main__":
    unittest.main()
