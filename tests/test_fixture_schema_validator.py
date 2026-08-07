"""Test schema validation against a representative nullable fixture-team row."""

import pandas as pd

from src.raw_api_data_etl import FootballAPI, coerce_dataframe_to_schema


def test_validate_dataframe_schema_accepts_matching_columns_with_missing_values():
    """Build a matching DataFrame with a null winner and assert clean validation."""
    api = FootballAPI("dummy")
    df = pd.DataFrame(
        {
            "fixture_id": [1],
            "side": ["home"],
            "team_id": [10],
            "team_name": ["A"],
            "team_logo": ["x"],
            "winner": [None],
        }
    )

    is_valid, errors, warnings = api.validate_parquet_schema(
        df, source_name="match_teams"
    )

    assert is_valid, errors
    assert errors == []
    assert warnings == []


def test_schema_coercion_converts_all_null_float_and_nullable_integer_columns():
    """Coerce object columns and assert concrete float and nullable integer dtypes."""
    frame = pd.DataFrame(
        {
            "fixture_id": ["123"],
            "minute": ["45"],
            "extra": [None],
            "team_id": ["10"],
            "team_name": ["Team"],
            "player_id": [None],
            "player_name": [None],
            "assist_id": [None],
            "assist_name": [None],
            "event_type": ["Goal"],
            "detail": ["Normal Goal"],
            "comments": [None],
        },
        dtype=object,
    )

    converted = coerce_dataframe_to_schema(frame, source_name="match_events")

    assert str(converted["fixture_id"].dtype) == "Int64"
    assert str(converted["minute"].dtype) == "Int64"
    assert str(converted["extra"].dtype) == "float64"
    assert pd.isna(converted.loc[0, "extra"])
    assert str(converted["team_name"].dtype) == "string"


def test_schema_coercion_adds_typed_columns_to_empty_table():
    """Coerce a zero-column table and assert its complete typed schema is retained."""
    converted = coerce_dataframe_to_schema(
        pd.DataFrame(), source_name="match_player_stats"
    )

    assert converted.empty
    assert "games_minutes" in converted.columns
    assert "fixture_id" in converted.columns
    assert str(converted["games_minutes"].dtype) == "Int64"
    assert str(converted["fixture_id"].dtype) == "Int64"
    assert str(converted["games_position"].dtype) == "string"
