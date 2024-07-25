from typing import List, Optional

import pandas as pd
import pytest

from google_sheets.data_processing.processing import (
    process_data_f,
    validate_input_data,
    validate_output_data,
)


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
    ("merged_campaigns_ad_groups_df", "template_df", "new_campaign_df", "expected"),
    [
        (
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "INSERT_COUNTRY - INSERT_STATION_FROM - INSERT_STATION_TO"
                    ],
                    "Ad Group Name": ["INSERT_STATION_FROM - INSERT_STATION_TO"],
                    "Match Type": ["Exact"],
                }
            ),
            pd.DataFrame(
                {
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
                    "Campaign Name": [
                        "INSERT_COUNTRY - INSERT_STATION_FROM - INSERT_STATION_TO"
                    ],
                    "Ad Group Name": ["INSERT_STATION_FROM - INSERT_STATION_TO"],
                    "Match Type": ["Exact"],
                }
            ),
            pd.DataFrame(
                {
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
    merged_campaigns_ad_groups_df: pd.DataFrame,
    template_df: pd.DataFrame,
    new_campaign_df: pd.DataFrame,
    expected: List[List[str]],
) -> None:
    process_data_f(merged_campaigns_ad_groups_df, template_df, new_campaign_df).equals(
        expected
    )


@pytest.mark.parametrize(
    ("df", "issues_column"),
    [
        (
            pd.DataFrame(
                {
                    "Headline 1": ["H1", "H1", "H1", "H1"],
                    "Headline 2": ["H1", "H2", "H2", ("H" * 31)],
                    "Headline 3": ["H3", "H3", "H3", ""],
                    "Description 1": ["D1", "D1", "D2", "D3"],
                    "Description 2": ["D1", "D1", "D3", ""],
                    "Path 1": ["P1", "P1", "P1", "P1"],
                    "Path 2": ["P2", "P2", "P2", "P2"],
                    "Final URL": ["URL", "URL", "URL", "URL"],
                }
            ),
            [
                "Duplicate headlines found.\nDuplicate descriptions found.\n",
                "Duplicate descriptions found.\n",
                "",
                "Minimum 3 headlines are required, found 2.\nMinimum 2 descriptions are required, found 1.\nHeadline length should be less than 30 characters, found 31 in column Headline 2.\n",
            ],
        ),
        (
            pd.DataFrame(
                {
                    "Headline 1": ["H1", "H1", "H1", "H1"],
                    "Headline 2": ["H2", "H2", "H2", "H2"],
                    "Headline 3": ["H3", "H3", "H3", "H3"],
                    "Description 1": ["D1", "D1", "D1", "D1"],
                    "Description 2": ["D2", "D2", "D2", "D2"],
                    "Path 1": ["P1", "P1", "P1", "P1"],
                    "Path 2": ["P2", "P2", "P2", "P2"],
                    "Final URL": ["URL", "URL", "URL", "URL"],
                }
            ),
            None,
        ),
    ],
)
def test_validate_output_data(
    df: pd.DataFrame, issues_column: Optional[List[str]]
) -> None:
    expected = df.copy()
    result = validate_output_data(df, "ad")

    if issues_column:
        expected.insert(0, "Issues", "")
        expected["Issues"] = issues_column

    assert result.equals(expected)
