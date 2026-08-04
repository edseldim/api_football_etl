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