from typing import List

import pandas as pd
import pytest

from google_sheets.data_processing.processing import process_data_f, validate_input_data


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
def test_validate_input_data(df: pd.DataFrame, expected: str) -> None:
    mandatory_columns = ["Country", "Station From", "Station To"]
    assert validate_input_data(df, mandatory_columns, "name") == expected


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
            pd.DataFrame(
                {
                    "Campaign": [
                        "USA - A - C",
                        "USA - A - C",
                        "USA - B - D",
                        "USA - B - D",
                        "USA - A - C",
                        "USA - A - C",
                        "USA - B - D",
                        "USA - B - D",
                    ],
                    "Ad Group": [
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                    ],
                    "Keyword": ["k1", "k1", "k1", "k1", "k2", "k2", "k2", "k2"],
                    "Max CPC": ["", "", "", "", "", "", "", ""],
                }
            ),
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
            pd.DataFrame(
                {
                    "Campaign": [
                        "USA - A - C",
                        "USA - A - C",
                        "USA - B - D",
                        "USA - B - D",
                        "USA - A - C",
                        "USA - A - C",
                        "USA - B - D",
                        "USA - B - D",
                    ],
                    "Ad Group": [
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                    ],
                    "Keyword": ["k1 A", "k1 C", "k1 B", "k1 D", "k2", "k2", "k2", "k2"],
                    "Max CPC": ["", "", "", "", "", "", "", ""],
                }
            ),
        ),
    ],
)
def test_process_data_f(
    template_df: pd.DataFrame, new_campaign_df: pd.DataFrame, expected: List[List[str]]
) -> None:
    process_data_f(template_df, new_campaign_df).equals(expected)
