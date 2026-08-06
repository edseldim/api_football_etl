"""Exercise a live PostgreSQL connection with create, insert, and select SQL.

This integration script loads ``DATABASE_URL``, creates ``measurements`` when
needed, inserts one temperature row, and prints the ten newest rows. It verifies
connectivity and basic write/read behavior by executing real SQL at import/run
time; unlike the other test modules, it contains no isolated test function and
requires an explicitly configured, reachable database.
"""

import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

database_url = os.environ["DATABASE_URL"]

with psycopg.connect(database_url) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS measurements (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                sensor_name TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL
            )
            """
        )

        cursor.execute(
            """
            INSERT INTO measurements (sensor_name, value)
            VALUES (%s, %s)
            """,
            ("temperature", 24.7),
        )

        cursor.execute(
            """
            SELECT id, recorded_at, sensor_name, value
            FROM measurements
            ORDER BY recorded_at DESC
            LIMIT 10
            """
        )

        for row in cursor.fetchall():
            print(row)
