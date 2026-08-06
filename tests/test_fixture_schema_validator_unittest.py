"""Test empty-table schema validation through the unittest interface."""

import unittest

import pandas as pd

from src.raw_api_data_etl import FootballAPI


class FixtureSchemaValidatorTests(unittest.TestCase):
    """Verify schema-validator edge cases with an injected event collector."""

    def test_empty_dataframe_emits_warning_with_injected_logger(self):
        """Validate an empty shaped DataFrame and assert validity plus warnings."""
        events = []
        api = FootballAPI("dummy", log_event=lambda level, message: events.append((level, message)))
        empty_df = pd.DataFrame(columns=["fixture_id", "side", "team_id", "team_name", "team_logo", "winner"])
        is_valid, errors, warnings = api.validate_parquet_schema(empty_df, source_name="match_teams")

        self.assertTrue(is_valid)
        self.assertEqual(errors, [])
        self.assertTrue(warnings)


if __name__ == "__main__":
    unittest.main()
