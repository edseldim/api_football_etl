import os
from pathlib import Path

import dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


dotenv.load_dotenv()


class PostgresConnector:
    """Connect to a PostgreSQL database and upload pandas DataFrames."""

    def __init__(self, database_url: str | None = None, env_path: str | Path | None = None, echo: bool = False):
        """Create a database connector using the .env connection string.

        Parameters:
            database_url (str | None): Optional database connection URL. If not provided,
                the connector will read DATABASE_URL from the environment.
            env_path (str | Path | None): Optional path to a .env file to load.
            echo (bool): When True, SQLAlchemy will log SQL statements.
        """
        if env_path is not None:
            dotenv.load_dotenv(dotenv_path=str(env_path))

        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL must be set either via argument or in the environment")

        self.engine: Engine = create_engine(self.database_url, echo=echo)

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        schema: str | None = None,
        dtype: dict | None = None,
        method: str | None = "multi",
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
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")

        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists=if_exists,
            index=index,
            schema=schema,
            dtype=dtype,
            method=method,
        )
