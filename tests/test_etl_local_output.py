import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from execute import FootballETL


class LocalOutputAPI:
    def __init__(self, **kwargs):
        pass

    def run_full_season_data(self, params, **kwargs):
        return {
            "match_summary": [{"fixture_id": 1}],
            "match_events": [{"fixture_id": 1, "event_type": "Goal"}],
        }


class ForbiddenDatabase:
    def __init__(self, **kwargs):
        raise AssertionError("Local-output mode must not initialize the database")


class RecordingDatabase:
    uploaded_names = []

    def __init__(self, **kwargs):
        type(self).uploaded_names = []

    def upload_dataframe(self, dataframe, table_name, **kwargs):
        type(self).uploaded_names.append(table_name)


class ETLLocalOutputTests(unittest.TestCase):
    def test_run_can_save_parquet_results_without_database_upload(self):
        saved_paths = []

        def record_parquet(dataframe, path, *args, **kwargs):
            self.assertIsInstance(dataframe, pd.DataFrame)
            saved_paths.append(Path(path))

        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ), patch("execute.PostgresConnector", ForbiddenDatabase), patch.object(
            pd.DataFrame, "to_parquet", record_parquet
        ):
            output_folder = Path(temp_folder) / "api-results"
            etl = FootballETL(
                "dummy", log_folder=Path(temp_folder) / "logs", prefix="test"
            )

            result = etl.run(
                {"league": 128, "season": 2026},
                save_locally=True,
                local_output_folder=output_folder,
            )

        self.assertEqual(set(result), {"match_summary", "match_events"})
        self.assertEqual(set(saved_paths), {
            output_folder / "test_match_summary.parquet",
            output_folder / "test_match_events.parquet",
        })
        self.assertIsNone(etl.database_connector)

    def test_default_prefix_uses_instantiation_timestamp(self):
        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ):
            etl = FootballETL("dummy", log_folder=Path(temp_folder) / "logs")

        self.assertRegex(etl.prefix, r"^\d{8}_\d{6}$")

    def test_invalid_prefix_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "prefix"):
            FootballETL("dummy", prefix="../other-folder")

    def test_sql_table_names_are_not_prefixed_by_default(self):
        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ), patch("execute.PostgresConnector", RecordingDatabase):
            etl = FootballETL(
                "dummy", log_folder=Path(temp_folder) / "logs", prefix="daily"
            )
            etl.run({"league": 128, "season": 2026})

        self.assertEqual(
            RecordingDatabase.uploaded_names,
            ["match_summary", "match_events"],
        )

    def test_sql_table_prefix_can_be_enabled_explicitly(self):
        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ), patch("execute.PostgresConnector", RecordingDatabase):
            etl = FootballETL(
                "dummy",
                log_folder=Path(temp_folder) / "logs",
                prefix="daily",
                prefix_sql_tables=True,
            )
            etl.run({"league": 128, "season": 2026})

        self.assertEqual(
            RecordingDatabase.uploaded_names,
            ["daily_match_summary", "daily_match_events"],
        )


if __name__ == "__main__":
    unittest.main()
