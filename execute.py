"""Orchestrate extraction of full-season football data and database uploads."""
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from src.database_conn import PostgresConnector
from src.raw_api_data_etl import FootballAPI


class FootballETL:
    """Extract a full season from the API and upload each resulting table."""

    def __init__(
        self,
        api_key: str,
        database_url: Optional[str] = None,
        log_folder: Union[str, Path] = ".logs",
        debug: bool = False,
        api_other_headers: Optional[dict] = None,
        api_general_params: Optional[dict] = None,
        database_env_path: Optional[Union[str, Path]] = None,
        database_echo: bool = False,
        prefix: Optional[str] = None,
        prefix_sql_tables: bool = False,
    ) -> None:
        """Initialize an API-to-database or API-to-Parquet ETL instance.

        Parameters:
            api_key (str): API-Football authentication key.
            database_url (str | None): SQLAlchemy database URL. When omitted,
                the connector reads ``DATABASE_URL`` from the environment.
            log_folder (str | Path): Directory for the instance log file.
            debug (bool): Enable verbose API-client logging.
            api_other_headers (dict | None): Additional API request headers.
            api_general_params (dict | None): Default parameters included in API
                requests, such as league, season, or country.
            database_env_path (str | Path | None): Optional ``.env`` file from
                which the database connector loads ``DATABASE_URL``.
            database_echo (bool): Enable SQLAlchemy statement logging.
            prefix (str | None): Prefix for local Parquet filenames and,
                optionally, SQL tables. Defaults to ``YYYYmmDD_HHMMSS``.
            prefix_sql_tables (bool): Apply ``prefix`` to SQL table names when
                True. SQL names remain unprefixed by default.
        """
        if prefix is None:
            prefix = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string or None")
        if not prefix or not re.fullmatch(r"[A-Za-z0-9_-]+", prefix):
            raise ValueError(
                "prefix must contain only letters, numbers, underscores, or hyphens"
            )
        if not isinstance(prefix_sql_tables, bool):
            raise TypeError("prefix_sql_tables must be a bool")
        self.prefix = prefix
        self.prefix_sql_tables = prefix_sql_tables
        self.log_folder = Path(log_folder).expanduser()
        self.log_file_path: Optional[Path] = None
        self._create_log_file()
        self._log_event("INFO", "Football ETL initialization started")

        try:
            self.api_client = FootballAPI(
                api_key=api_key,
                other_headers=api_other_headers,
                general_params=api_general_params,
                debug=debug,
                log_event=self._log_event,
            )
            # Database creation is deferred until run() actually uploads data.
            # This lets local-output runs work without DATABASE_URL configured.
            self.database_connector = None
            self._database_config = {
                "database_url": database_url,
                "env_path": database_env_path,
                "echo": database_echo,
            }
        except Exception as exc:
            self._log_event("ERROR", f"Football ETL initialization failed: {exc}")
            raise

        self._log_event("INFO", "Football ETL initialized")

    def _prefixed_table_name(self, table_name: str) -> str:
        """Return the prefixed persistence name for a logical table.

        Parameters:
            table_name (str): Logical table name returned by the API client.

        Returns:
            str: Name with the instance prefix prepended.
        """
        return f"{self.prefix}_{table_name}"

    def _get_database_connector(self) -> PostgresConnector:
        """Create and cache the database connector only when an upload is requested."""
        if self.database_connector is None:
            self.database_connector = PostgresConnector(
                **self._database_config,
                log_event=self._log_event,
            )
        return self.database_connector

    def _create_log_file(self) -> Optional[Path]:
        """Create the single log file used by the entire ETL instance."""
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        log_path = self.log_folder / f"api-etl-{timestamp}.log"

        try:
            self.log_folder.mkdir(parents=True, exist_ok=True)
            log_path.touch(exist_ok=True)
            self.log_file_path = log_path
            return log_path
        except OSError as exc:
            self.log_file_path = None
            print(
                "[WARNING] Unable to create ETL log file; "
                f"falling back to terminal: {exc}"
            )
            return None

    def _log_event(self, level: str, message: Any) -> None:
        """Write a shared ETL event to its file or safely fall back to stdout.

        Parameters:
            level (str): Log severity such as ``INFO`` or ``ERROR``.
            message (Any): Event message or value to record.
        """
        timestamp = datetime.now().strftime("%Y%m%d-%H:%M:%S")
        line = f"api-etl-{timestamp} [{str(level).upper()}]: {message}"

        if self.log_file_path is None:
            print(line)
            return

        try:
            with self.log_file_path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        except OSError as exc:
            self.log_file_path = None
            print(f"[WARNING] Unable to write ETL log file: {exc}")
            print(line)

    @staticmethod
    def _to_dataframe(table_data: Any) -> pd.DataFrame:
        """Normalize one table returned by the API into a DataFrame.

        Parameters:
            table_data (Any): DataFrame or table-shaped value to normalize.

        Returns:
            pandas.DataFrame: Normalized table data.
        """
        if isinstance(table_data, pd.DataFrame):
            return table_data
        if table_data is None:
            return pd.DataFrame()
        if isinstance(table_data, dict):
            if table_data and all(
                isinstance(value, (list, tuple, set))
                for value in table_data.values()
            ):
                return pd.DataFrame(table_data)
            return pd.DataFrame([table_data])
        if isinstance(table_data, list):
            rows = [row if isinstance(row, dict) else {"value": row} for row in table_data]
            return pd.DataFrame(rows)
        return pd.DataFrame([{"value": table_data}])

    def run(
        self,
        params: dict,
        *,
        if_exists: str = "replace",
        schema: Optional[str] = None,
        save_locally: bool = False,
        local_output_folder: Union[str, Path] = "data",
    ) -> Dict[str, pd.DataFrame]:
        """Run the full-season ETL and persist or upload every returned table.

        Table names are taken from the keys returned by ``run_full_season_data``.
        Local Parquet filenames use this instance's ``prefix``. SQL table names
        remain stable unless ``prefix_sql_tables=True`` was set at instantiation.
        By default, tables are uploaded to the configured database. When
        ``save_locally`` is True, database upload is skipped and each table is
        written to ``local_output_folder/<table_name>.parquet`` instead. The
        normalized DataFrames are returned for inspection or downstream use.

        Parameters:
            params (dict): Full-season extraction parameters, including
                ``league`` and ``season``.
            if_exists (str): SQL behavior when a destination table exists.
            schema (str | None): Optional destination database schema.
            save_locally (bool): Save Parquet files and skip SQL uploads when
                True.
            local_output_folder (str | Path): Directory for local Parquet files.

        Returns:
            dict[str, pandas.DataFrame]: Normalized tables keyed by logical name.
        """
        if not isinstance(save_locally, bool):
            raise TypeError("save_locally must be a bool")

        self._log_event("INFO", f"Full-season ETL started with params {params}")
        try:
            resulting_tables = self.api_client.run_full_season_data(
                params,
                consolidate_output=True,
                save_to_parquet=False,
            )
            if not isinstance(resulting_tables, dict):
                raise TypeError("run_full_season_data must return a dict of tables")

            output_folder = Path(local_output_folder).expanduser()
            if save_locally:
                output_folder.mkdir(parents=True, exist_ok=True)
                self._log_event("INFO", f"Saving API results locally to {output_folder}")
            else:
                database_connector = self._get_database_connector()

            processed_tables: Dict[str, pd.DataFrame] = {}
            for table_name, table_data in resulting_tables.items():
                dataframe = self._to_dataframe(table_data)
                prefixed_table_name = self._prefixed_table_name(table_name)
                self._log_event(
                    "INFO",
                    f"Prepared {len(dataframe)} rows for table {table_name}",
                )
                if save_locally:
                    output_path = output_folder / f"{prefixed_table_name}.parquet"
                    dataframe.to_parquet(output_path)
                    self._log_event("INFO", f"Saved {table_name} to {output_path}")
                else:
                    database_connector.upload_dataframe(
                        dataframe,
                        table_name=(
                            prefixed_table_name
                            if self.prefix_sql_tables
                            else table_name
                        ),
                        if_exists=if_exists,
                        schema=schema,
                    )
                processed_tables[table_name] = dataframe

            self._log_event(
                "INFO", f"Full-season ETL completed: {len(processed_tables)} tables"
            )
            return processed_tables
        except Exception as exc:
            self._log_event("ERROR", f"Full-season ETL failed: {exc}")
            raise


def main() -> None:

    api_key = os.environ.get("API_KEY")
    if not api_key:
        raise ValueError("API_KEY must be set in the environment or .env file")

    etl = FootballETL(api_key=api_key, debug=True)

    uploaded_tables = etl.run(
        params={"league":128,"season":2023}, save_locally=False
    )

    print(f"ETL completed: {len(uploaded_tables)} tables uploaded")


if __name__ == "__main__":
    main()
