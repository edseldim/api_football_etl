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
            etl = FootballETL("dummy", log_folder=Path(temp_folder) / "logs")

            result = etl.run(
                {"league": 128, "season": 2026},
                save_locally=True,
                local_output_folder=output_folder,
            )

        self.assertEqual(set(result), {"match_summary", "match_events"})
        self.assertEqual(set(saved_paths), {
            output_folder / "match_summary.parquet",
            output_folder / "match_events.parquet",
        })
        self.assertIsNone(etl.database_connector)


if __name__ == "__main__":
    unittest.main()
