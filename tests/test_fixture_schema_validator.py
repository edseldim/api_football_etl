import pandas as pd

from src.raw_api_data_etl import FootballAPI


def test_validate_dataframe_schema_accepts_matching_columns_with_missing_values():
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
