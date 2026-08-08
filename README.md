# API Football ETL

An ETL pipeline that extracts a complete league season from
[API-Football](https://www.api-football.com/), normalizes each fixture into
relational tables, validates the resulting schemas, and writes the data either
to Parquet files or PostgreSQL.

## Main features

- Retrieves every fixture for a given API-Football league and season.
- Splits nested fixture responses into nine normalized datasets:
  `match_summary`, `match_teams`, `match_scores`, `match_events`,
  `match_lineups`, `teams_coaches`, `match_team_stats`, `match_players`, and
  `match_player_stats`.
- Uses dedicated API endpoints as fallbacks when events, lineups, coaches, team
  statistics, or player statistics are absent from the main fixture response.
- Coerces and validates DataFrame columns against declared, Parquet-compatible
  schemas.
- Saves timestamp-prefixed Parquet files for local analysis.
- Uploads DataFrames to PostgreSQL through SQLAlchemy, with configurable schema
  and `if_exists` behavior.
- Produces one log file per ETL instance under `.logs/`, with terminal logging
  as a fallback.
- Returns all processed tables as pandas DataFrames for further processing.

## Requirements

- Python 3.9 or newer.
- An active API-Football account and API key.
- Network access to `https://v3.football.api-sports.io`.
- Enough API quota for the selected season. The pipeline first discovers the
  fixtures and then makes per-fixture requests, including fallback requests for
  missing data.
- PostgreSQL and a valid SQLAlchemy connection URL only when using database
  output. Local Parquet output does not require a database.

The Python dependencies are installed by `setup.py`: pandas, PyArrow,
Requests, python-dotenv, SQLAlchemy, sqlparse, and the PostgreSQL driver.

## Installation

Create and activate a virtual environment, then install the project in editable
mode from the repository root.

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

On macOS or Linux, activate the environment with:

```bash
source .venv/bin/activate
```

## Configuration

Create a `.env` file in the repository root. It is ignored by Git.

```dotenv
API_KEY=your_api_football_key
DATABASE_URL=postgresql_connection_string
```

`API_KEY` is required for every real extraction. `DATABASE_URL` is required
only for PostgreSQL output. Both values may instead be supplied directly to
`FootballETL`.

Never commit API keys or database credentials.

## Run locally with Parquet output

Run the following from the repository root, changing the league and season IDs
to the desired API-Football values:

```python
import os

from src.api_football_etl.execute import FootballETL


etl = FootballETL(api_key=os.environ["API_KEY"])
tables = etl.run(
    params={"league": 128, "season": 2026},
    save_locally=True,
    local_output_folder="data",
)

print({name: len(frame) for name, frame in tables.items()})
```

Each output filename is prefixed with the ETL creation timestamp, for example
`data/20260807_173000_match_summary.parquet`. Pass `prefix="my_run"` to
`FootballETL` when a stable custom prefix is preferred.

## Run with PostgreSQL output

```python
import os

from src.api_football_etl.execute import FootballETL


etl = FootballETL(
    api_key=os.environ["API_KEY"],
    database_url=os.environ["DATABASE_URL"],
)
tables = etl.run(
    params={"league": 128, "season": 2026},
    save_locally=False,
    if_exists="replace",
    schema="public",
)
```

Database table names are unprefixed by default. Set
`prefix_sql_tables=True` on `FootballETL` to retain separate, prefixed tables
for each run. Valid `if_exists` values are `fail`, `replace`, and `append`.

## Pipeline flow

1. Validate the required integer `league` and `season` parameters.
2. Request the season fixture list and collect fixture IDs.
3. Retrieve and normalize every fixture.
4. Fetch missing fixture sections from their dedicated API endpoints.
5. Consolidate rows across the season and enforce the expected schemas.
6. Write each table to Parquet or upload it to PostgreSQL.
7. Return a dictionary of the resulting pandas DataFrames.

## Tests

Run the automated test suite from the repository root:

```powershell
python -m pytest
```

Tests that connect to a real API or local PostgreSQL instance additionally need
the corresponding credentials and services configured.
