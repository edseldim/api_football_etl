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

## Production tables

The production schema is defined in
[`create_prod_tables.sql`](src/api_football_etl/static/sql/create_prod_tables.sql).
It creates the following PostgreSQL tables:

| Table | Contents |
| --- | --- |
| `prod_argentina_available_leagues` | League, country, season, and API coverage metadata. |
| `prod_match_summary` | One match-level record with fixture status, venue, league, teams, and final goals. |
| `prod_match_teams` | Home and away team details for each fixture. |
| `prod_match_scores` | Half-time, full-time, extra-time, and penalty score values. |
| `prod_match_events` | Goals, cards, substitutions, and other timestamped match events. |
| `prod_match_lineups` | Formations, starters, substitutes, positions, and formation-grid locations. |
| `prod_teams_coaches` | Coaches associated with each fixture and team. |
| `prod_match_team_stats` | Long-form, fixture-level team statistics such as possession and shots. |
| `prod_match_players` | Player identity and team membership within each fixture. |
| `prod_match_player_stats` | Per-fixture player performance, including shots, goals, passes, tackles, cards, and penalties. |

Most match tables can be joined through `fixture_id`. Player-level joins use `fixture_id` and `player_id`, while team-level joins use `fixture_id` and `team_id`. The DDL provides indexes for these common joins as well as match summary queries by date, league and season. It intentionally defines no primary or foreign keys because the extracted source data does not establish reliable row uniqueness and some identifiers are nullable.

The ETL first writes unprefixed staging tables such as `match_summary` and `match_events`. Setting `insert_prod=True` copies those staging rows into their corresponding `prod_*` tables with [`insert_prod.sql`](src/api_football_etl/static/sql/insert_prod.sql), then runs `ANALYZE` so PostgreSQL refreshes its query-planning statistics. Production promotion requires `save_locally=False` and `prefix_sql_tables=False`.

Run the production DDL once before the first promotion:

```powershell
psql "$env:DATABASE_URL" -f src/api_football_etl/static/sql/create_prod_tables.sql
```

Be careful when rerunning this command: the DDL starts by dropping each existing `prod_*` table, so all data currently stored in those tables is deleted. The promotion script uses plain `INSERT` statements and does not deduplicate rows; avoid promoting the same staging dataset more than once unless duplicates are intended.

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
