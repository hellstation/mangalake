import pandas as pd

from etl.load.snowflake_load import _prepare_df


def test_prepare_df_sets_load_date_and_normalizes_types():
    df = pd.DataFrame(
        [
            {
                "MANGA_ID": "1",
                "YEAR": "2020",
                "UPDATED_AT": "2024-01-01T12:00:00Z",
            },
            {
                "MANGA_ID": "2",
                "YEAR": "invalid",
                "UPDATED_AT": "bad-date",
            },
        ]
    )

    result = _prepare_df(df, "2025-02-10")

    assert str(result.loc[0, "LOAD_DATE"]) == "2025-02-10"
    assert str(result.loc[0, "UPDATED_AT"]) == "2024-01-01 12:00:00+00:00"
    assert result.loc[0, "YEAR"] == 2020
    assert pd.isna(result.loc[1, "YEAR"])
    assert pd.isna(result.loc[1, "UPDATED_AT"])
