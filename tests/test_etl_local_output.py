"""Test ETL destination naming and routing with patched API and database clients."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from src.api_football_etl.execute import FootballETL


class LocalOutputAPI:
    """Return two deterministic logical tables without making HTTP requests."""
    def __init__(self, **kwargs):
        pass

    def run_full_season_data(self, params, **kwargs):
        return {
            "match_summary": [{"fixture_id": 1}],
            "match_events": [{"fixture_id": 1, "event_type": "Goal"}],
        }


class ForbiddenDatabase:
    """Fail immediately if local-output mode attempts database initialization."""
    def __init__(self, **kwargs):
        raise AssertionError("Local-output mode must not initialize the database")


class RecordingDatabase:
    """Record uploaded table names without connecting to a database."""
    uploaded_names = []
    sql_runs = []

    def __init__(self, **kwargs):
        type(self).uploaded_names = []
        type(self).sql_runs = []

    def upload_dataframe(self, dataframe, table_name, **kwargs):
        type(self).uploaded_names.append(table_name)

    def run_sql_file(self, file_path, params=None):
        type(self).sql_runs.append((Path(file_path), params))


class ETLLocalOutputTests(unittest.TestCase):
    """Verify local Parquet output and optional SQL-prefix behavior."""

    def test_run_can_save_parquet_results_without_database_upload(self):
        """Patch Parquet writes and assert prefixed paths with no database client."""
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
        """Instantiate without a prefix and match its value to the timestamp format."""
        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ):
            etl = FootballETL("dummy", log_folder=Path(temp_folder) / "logs")

        self.assertRegex(etl.prefix, r"^\d{8}_\d{6}$")

    def test_invalid_prefix_is_rejected(self):
        """Pass a path-like prefix and assert constructor validation raises ValueError."""
        with self.assertRaisesRegex(ValueError, "prefix"):
            FootballETL("dummy", prefix="../other-folder")

    def test_sql_table_names_are_not_prefixed_by_default(self):
        """Record uploads and assert default SQL names remain logical table names."""
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
        """Enable SQL prefixes and assert the recording connector receives them."""
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

    def test_insert_prod_runs_sql_file_after_database_uploads(self):
        """Opt in to production insertion and assert the configured script path."""
        run_params = {"league": 128, "season": 2026}
        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ), patch("execute.PostgresConnector", RecordingDatabase):
            etl = FootballETL("dummy", log_folder=Path(temp_folder) / "logs")
            etl.run(run_params, insert_prod=True)

        self.assertEqual(len(RecordingDatabase.sql_runs), 1)
        sql_path, sql_params = RecordingDatabase.sql_runs[0]
        self.assertEqual(sql_path.name, "insert_prod.sql")
        self.assertEqual(
            sql_path.parts[-4:],
            ("src", "static", "sql", "insert_prod.sql"),
        )
        self.assertIsNone(sql_params)

    def test_insert_prod_rejects_local_output_mode(self):
        """Request production insertion with local output and assert early rejection."""
        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ):
            etl = FootballETL("dummy", log_folder=Path(temp_folder) / "logs")
            with self.assertRaisesRegex(ValueError, "insert_prod"):
                etl.run(
                    {"league": 128, "season": 2026},
                    save_locally=True,
                    insert_prod=True,
                )

    def test_insert_prod_rejects_prefixed_sql_staging_tables(self):
        """Reject production promotion when staging names cannot match the SQL file."""
        with TemporaryDirectory() as temp_folder, patch(
            "execute.FootballAPI", LocalOutputAPI
        ):
            etl = FootballETL(
                "dummy",
                log_folder=Path(temp_folder) / "logs",
                prefix_sql_tables=True,
            )
            with self.assertRaisesRegex(ValueError, "prefix_sql_tables=False"):
                etl.run({"league": 128, "season": 2026}, insert_prod=True)


if __name__ == "__main__":
    unittest.main()
