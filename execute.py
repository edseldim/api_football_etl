"""Orchestrate extraction of full-season football data and database uploads."""
import pandas as pd
from typing import Any, Dict, Optional, Protocol
from src.database_conn import PostgresConnector
from src.raw_api_data_etl import FootballAPI


class SeasonDataAPI(Protocol):
    def run_full_season_data(self, params: dict, **kwargs: Any) -> dict: ...


class DataFrameUploader(Protocol):
    def upload_dataframe(
        self, df: pd.DataFrame, table_name: str, **kwargs: Any
    ) -> None: ...


class FullSeasonDataUploader:
    """Extract a full season from the API and upload each resulting table."""

    def __init__(
        self,
        api_client: SeasonDataAPI,
        database_connector: DataFrameUploader,
    ) -> None:
        self.api_client = api_client
        self.database_connector = database_connector

    @staticmethod
    def _to_dataframe(table_data: Any) -> pd.DataFrame:
        """Normalize one table returned by the API into a DataFrame."""
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
    ) -> Dict[str, pd.DataFrame]:
        """Run the full-season ETL and upload every returned table.

        Table names are taken from the keys returned by ``run_full_season_data``.
        The normalized DataFrames are returned for inspection or downstream use.
        """
        resulting_tables = self.api_client.run_full_season_data(
            params,
            consolidate_output=True,
            save_to_parquet=False,
        )
        if not isinstance(resulting_tables, dict):
            raise TypeError("run_full_season_data must return a dict of tables")

        uploaded_tables: Dict[str, pd.DataFrame] = {}
        for table_name, table_data in resulting_tables.items():
            dataframe = self._to_dataframe(table_data)
            self.database_connector.upload_dataframe(
                dataframe,
                table_name=table_name,
                if_exists=if_exists,
                schema=schema,
            )
            uploaded_tables[table_name] = dataframe

        return uploaded_tables


def main() -> None:
    """Example: extract one full season and upload its tables to PostgreSQL."""
    import os

    api_key = os.environ.get("API_KEY")
    if not api_key:
        raise ValueError("API_KEY must be set in the environment or .env file")

    api_client = FootballAPI(api_key=api_key)
    database_connector = PostgresConnector()
    etl = FullSeasonDataUploader(api_client, database_connector)

    uploaded_tables = etl.run(
        params={"league": 128, "season": 2026, "date":"2026-07-28"}
    )

    for table_name, dataframe in uploaded_tables.items():
        print(f"Uploaded {len(dataframe)} rows to {table_name}")


if __name__ == "__main__":
    main()
