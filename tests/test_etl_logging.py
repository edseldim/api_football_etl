"""Test shared ETL logging with fake clients and captured failures or output."""

from unittest.mock import patch

import pandas as pd
import pytest

from src.api_football_etl.execute import FootballETL
from src.api_football_etl.database_conn import PostgresConnector
from src.api_football_etl.raw_api_data_etl import FootballAPI


class FakeAPI:
    """Emit initialization and extraction events through the injected logger."""
    def __init__(self, **kwargs):
        self.log_event = kwargs["log_event"]
        self.log_event("INFO", "API client initialized")

    def run_full_season_data(self, params, **kwargs):
        self.log_event("INFO", "API extraction completed")
        return {"match_summary": [{"fixture_id": 1}]}


class FakeDatabase:
    """Emit initialization and upload events without performing database I/O."""
    def __init__(self, **kwargs):
        self.log_event = kwargs["log_event"]
        self.log_event("INFO", "Database client initialized")

    def upload_dataframe(self, dataframe, table_name, **kwargs):
        self.log_event("INFO", f"Database uploaded {table_name}")


def test_etl_uses_one_shared_log_file(tmp_path):
    """Patch both clients and assert their lifecycle events share one ETL log file."""
    log_folder = tmp_path / "nested" / "logs"
    with patch("execute.FootballAPI", FakeAPI), patch(
        "execute.PostgresConnector", FakeDatabase
    ):
        etl = FootballETL(
            "dummy", log_folder=log_folder, debug=True, prefix="test"
        )
        result = etl.run({"league": 128, "season": 2026})

    log_files = list(log_folder.glob("api-etl-*.log"))
    assert len(log_files) == 1
    assert isinstance(result["match_summary"], pd.DataFrame)
    contents = log_files[0].read_text(encoding="utf-8")
    assert "API client initialized" in contents
    assert "API extraction completed" in contents
    assert "Database client initialized" in contents
    assert "Database uploaded match_summary" in contents
    assert "Full-season ETL completed" in contents


def test_etl_logs_and_reraises_extraction_failure(tmp_path):
    """Raise from a fake API and assert the same error is logged and propagated."""
    class FailingAPI(FakeAPI):
        def run_full_season_data(self, params, **kwargs):
            raise RuntimeError("API unavailable")

    with patch("execute.FootballAPI", FailingAPI), patch(
        "execute.PostgresConnector", FakeDatabase
    ):
        etl = FootballETL("dummy", log_folder=tmp_path)
        with pytest.raises(RuntimeError, match="API unavailable"):
            etl.run({"league": 128, "season": 2026})

    contents = next(tmp_path.glob("api-etl-*.log")).read_text(encoding="utf-8")
    assert "Full-season ETL failed: API unavailable" in contents


def test_unwritable_log_path_falls_back_without_recursion(tmp_path, capsys):
    """Use a file as the log directory and assert safe terminal fallback via capsys."""
    invalid_folder = tmp_path / "not-a-folder"
    invalid_folder.write_text("occupied", encoding="utf-8")

    with patch("execute.FootballAPI", FakeAPI), patch(
        "execute.PostgresConnector", FakeDatabase
    ):
        etl = FootballETL("dummy", log_folder=invalid_folder)
        etl.run({"league": 128, "season": 2026})

    output = capsys.readouterr().out
    assert "falling back to terminal" in output
    assert "Full-season ETL completed" in output


def test_direct_clients_accept_optional_log_callback(monkeypatch):
    """Inject one callback into both clients and assert expected lifecycle messages."""
    events = []
    callback = lambda level, message: events.append((level, str(message)))
    FootballAPI("dummy", log_event=callback)
    connector = PostgresConnector("sqlite://", log_event=callback)
    monkeypatch.setattr(pd.DataFrame, "to_sql", lambda self, **kwargs: None)

    connector.upload_dataframe(pd.DataFrame([{"id": 1}]), "sample")

    messages = [message for _, message in events]
    assert "PostgreSQL connector initialized" in messages
    assert "Uploading 1 rows to table sample" in messages
    assert "Uploaded 1 rows to table sample" in messages


def test_database_upload_failure_is_logged_and_reraised(monkeypatch):
    """Patch DataFrame.to_sql to fail and assert connector logging and propagation."""
    events = []
    connector = PostgresConnector(
        "sqlite://", log_event=lambda level, message: events.append((level, str(message)))
    )

    def fail_upload(self, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(pd.DataFrame, "to_sql", fail_upload)
    with pytest.raises(RuntimeError, match="write failed"):
        connector.upload_dataframe(pd.DataFrame([{"id": 1}]), "sample")

    assert any(
        level == "ERROR" and "write failed" in message
        for level, message in events
    )
