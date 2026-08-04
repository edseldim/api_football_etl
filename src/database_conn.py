import os
from pathlib import Path
from typing import Any, Callable, Optional, Union

import dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


dotenv.load_dotenv()


class PostgresConnector:
    """Connect to a PostgreSQL database and upload pandas DataFrames."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        env_path: Optional[Union[str, Path]] = None,
        echo: bool = False,
        log_event: Optional[Callable[[str, Any], None]] = None,
    ):
        """Create a database connector using the .env connection string.

        Parameters:
            database_url (str | None): Optional database connection URL. If not provided,
                the connector will read DATABASE_URL from the environment.
            env_path (str | Path | None): Optional path to a .env file to load.
            echo (bool): When True, SQLAlchemy will log SQL statements.
        """
        self._log_event = log_event or self._default_log_event

        try:
            if env_path is not None:
                dotenv.load_dotenv(dotenv_path=str(env_path))

            self.database_url = database_url or os.getenv("DATABASE_URL")
            if not self.database_url:
                raise ValueError("DATABASE_URL must be set either via argument or in the environment")

            self.engine: Engine = create_engine(self.database_url, echo=echo)
            self._log_event("INFO", "PostgreSQL connector initialized")
        except Exception as exc:
            self._log_event("ERROR", f"PostgreSQL connector initialization failed: {exc}")
            raise

    @staticmethod
    def _default_log_event(level, message):
        """Provide safe terminal logging when the connector is used independently."""
        print(f"[{str(level).upper()}] {message}")

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        schema: Optional[str] = None,
        dtype: Optional[dict] = None,
        method: Optional[str] = "multi",
    ) -> None:
        """Upload a pandas DataFrame to a PostgreSQL table.

        Parameters:
            df (pandas.DataFrame): DataFrame to upload.
            table_name (str): Destination table name.
            if_exists (str): Behavior if the table already exists: 'fail', 'replace', or 'append'.
            index (bool): Whether to write DataFrame index as a column.
            schema (str | None): Optional database schema to use.
            dtype (dict | None): Optional column dtype mapping for SQLAlchemy.
            method (str | None): Method used by pandas to_sql. Use 'multi' for batch inserts.

        Raises:
            TypeError: If df is not a pandas DataFrame.
            ValueError: If table_name is empty or invalid.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df must be a pandas DataFrame")
            if not table_name or not isinstance(table_name, str):
                raise ValueError("table_name must be a non-empty string")

            self._log_event(
                "INFO", f"Uploading {len(df)} rows to table {table_name}"
            )
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=index,
                schema=schema,
                dtype=dtype,
                method=method,
            )
            self._log_event(
                "INFO", f"Uploaded {len(df)} rows to table {table_name}"
            )
        except Exception as exc:
            self._log_event("ERROR", f"Upload to table {table_name!r} failed: {exc}")
            raise
