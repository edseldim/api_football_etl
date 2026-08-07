"""Test safe, parameterized SQL-file execution without a PostgreSQL server."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import text

from src.database_conn import PostgresConnector


class SQLFileExecutionTests(unittest.TestCase):
    """Exercise SQL-file lookup and bindings against an in-memory SQLite engine."""

    def setUp(self):
        """Create a connector and temporary directory for explicit SQL file paths."""
        self.temp_directory = TemporaryDirectory()
        self.sql_directory = Path(self.temp_directory.name)
        self.events = []
        self.connector = PostgresConnector(
            "sqlite://",
            log_event=lambda level, message: self.events.append((level, str(message))),
        )

    def tearDown(self):
        """Remove temporary SQL files after each test."""
        self.temp_directory.cleanup()

    def test_run_sql_file_binds_params_and_commits_without_returning_rows(self):
        """Execute commands with shared params and inspect effects through a separate query."""
        (self.sql_directory / "select_value.sql").write_text(
            """
            CREATE TABLE values_table (value INTEGER);
            INSERT INTO values_table (value) VALUES (:value);
            """,
            encoding="utf-8",
        )

        result = self.connector.run_sql_file(
            self.sql_directory / "select_value.sql", {"value": 42}
        )

        self.assertIsNone(result)
        with self.connector.engine.connect() as connection:
            stored_value = connection.execute(
                text("SELECT value FROM values_table")
            ).scalar_one()
        self.assertEqual(stored_value, 42)
        messages = [message for _, message in self.events]
        self.assertIn("Loaded 2 querie(s) from file select_value.sql", messages)
        self.assertEqual(sum(message.startswith("RUNNING ") for message in messages), 2)
        self.assertEqual(messages.count("RAN SUCCESSFULLY"), 2)

    def test_run_sql_file_logs_failing_query_and_error(self):
        """Run invalid SQL and assert the error log includes its exception and query."""
        (self.sql_directory / "invalid.sql").write_text(
            "SELECT * FROM table_that_does_not_exist;",
            encoding="utf-8",
        )

        with self.assertRaises(Exception):
            self.connector.run_sql_file(self.sql_directory / "invalid.sql")

        error_messages = [message for level, message in self.events if level == "ERROR"]
        self.assertEqual(len(error_messages), 1)
        self.assertTrue(error_messages[0].startswith("ERROR "))
        self.assertIn("SELECT * FROM table_that_does_not_exist;", error_messages[0])

    def test_run_sql_file_rejects_non_sql_file_path(self):
        """Pass a non-SQL path and assert extension validation rejects it."""
        with self.assertRaisesRegex(ValueError, ".sql extension"):
            self.connector.run_sql_file(self.sql_directory / "query.txt")

    def test_run_sql_file_reports_missing_file(self):
        """Request an absent SQL filename and assert a clear FileNotFoundError."""
        with self.assertRaisesRegex(FileNotFoundError, "missing.sql"):
            self.connector.run_sql_file(self.sql_directory / "missing.sql")


if __name__ == "__main__":
    unittest.main()
