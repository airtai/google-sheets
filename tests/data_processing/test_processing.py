from typing import List

import pandas as pd
import pytest

from google_sheets.data_processing.processing import process_data_f, validate_data


@pytest.mark.parametrize(
    ("df", "expected"),
    [
        (
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                    "Station To": ["B", "A"],
                }
            ),
            "",
        ),
        (
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                }
            ),
            """Mandatory columns missing in the name data.
Please provide the following columns: ['Country', 'Station From', 'Station To']
""",
        ),
        (
            pd.DataFrame(
                [["USA", "A", "B", "B"], ["USA", "B", "A", "C"]],
                columns=["Country", "Station From", "Station To", "Station To"],
            ),
            """Duplicate columns found in the name data.
Please provide unique column names.
""",
        ),
    ],
)
def test_validate_data(df: pd.DataFrame, expected: str) -> None:
    mandatory_columns = ["Country", "Station From", "Station To"]
    assert validate_data(df, mandatory_columns, "name") == expected


@pytest.mark.parametrize(
    ("template_df", "new_campaign_df", "expected"),
    [
        (
            pd.DataFrame(
                {
                    "Campaign": ["", ""],
                    "Ad Group": ["", ""],
                    "Keyword": ["k1", "k2"],
                    "Max CPC": ["", ""],
                }
            ),
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                    "Station To": ["C", "D"],
                }
            ),
            [
                ["Campaign", "Ad Group", "Keyword", "Max CPC"],
                ["USA - A - C", "A - C", "k1", ""],
                ["USA - A - C", "C - A", "k1", ""],
                ["USA - B - D", "B - D", "k1", ""],
                ["USA - B - D", "D - B", "k1", ""],
                ["USA - A - C", "A - C", "k2", ""],
                ["USA - A - C", "C - A", "k2", ""],
                ["USA - B - D", "B - D", "k2", ""],
                ["USA - B - D", "D - B", "k2", ""],
            ],
        ),
        (
            pd.DataFrame(
                {
                    "Campaign": ["", ""],
                    "Ad Group": ["", ""],
                    "Keyword": ["k1 INSERT_STATION_FROM", "k2"],
                    "Max CPC": ["", ""],
                }
            ),
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                    "Station To": ["C", "D"],
                }
            ),
            [
                ["Campaign", "Ad Group", "Keyword", "Max CPC"],
                ["USA - A - C", "A - C", "k1 A", ""],
                ["USA - A - C", "C - A", "k1 C", ""],
                ["USA - B - D", "B - D", "k1 B", ""],
                ["USA - B - D", "D - B", "k1 D", ""],
                ["USA - A - C", "A - C", "k2", ""],
                ["USA - A - C", "C - A", "k2", ""],
                ["USA - B - D", "B - D", "k2", ""],
                ["USA - B - D", "D - B", "k2", ""],
            ],
        ),
    ],
)
def test_process_data_f(
    template_df: pd.DataFrame, new_campaign_df: pd.DataFrame, expected: List[List[str]]
) -> None:
    assert process_data_f(template_df, new_campaign_df).values == expected
