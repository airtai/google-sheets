from typing import List, Optional

import pandas as pd
import pytest

from google_sheets.data_processing.processing import (
    _update_campaign_name,
    _validate_language_codes,
    process_campaign_data_f,
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
                        "{INSERT_COUNTRY} - {INSERT_STATION_FROM} - {INSERT_STATION_TO} - {INSERT_LANGUAGE_CODE}"
                    ],
                    "Language Code": ["EN"],
                    "Ad Group Name": ["{INSERT_STATION_FROM} - {INSERT_STATION_TO}"],
                    "Match Type": ["Exact"],
                }
            ),
            pd.DataFrame(
                {
                    "Keyword": ["k1", "k2"],
                    "Max CPC": ["", ""],
                    "Language Code": ["EN", "EN"],
                    "Negative": ["FALSE", "FALSE"],
                    "Level": [None, None],
                    "Keyword Match Type": ["Exact", "Exact"],
                }
            ),
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                    "Station To": ["C", "D"],
                    "Language Code": ["EN", "EN"],
                }
            ),
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "USA - A - C - EN",
                        "USA - A - C - EN",
                        "USA - B - D - EN",
                        "USA - B - D - EN",
                        "USA - A - C - EN",
                        "USA - A - C - EN",
                        "USA - B - D - EN",
                        "USA - B - D - EN",
                    ],
                    "Ad Group Name": [
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                    ],
                    "Match Type": [
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                    ],
                    "Keyword": ["k1", "k1", "k1", "k1", "k2", "k2", "k2", "k2"],
                    "Max CPC": ["", "", "", "", "", "", "", ""],
                    "Negative": [
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                    ],
                    "Level": [None, None, None, None, None, None, None, None],
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "{INSERT_COUNTRY} - {INSERT_STATION_FROM} - {INSERT_STATION_TO} - {INSERT_LANGUAGE_CODE}"
                    ],
                    "Language Code": ["EN"],
                    "Ad Group Name": ["{INSERT_STATION_FROM} - {INSERT_STATION_TO}"],
                    "Match Type": ["Exact"],
                }
            ),
            pd.DataFrame(
                {
                    "Keyword": ["k1 {INSERT_STATION_FROM}", "k2"],
                    "Max CPC": ["", ""],
                    "Language Code": ["EN", "EN"],
                    "Negative": ["FALSE", "FALSE"],
                    "Level": [None, None],
                    "Keyword Match Type": ["Exact", "Exact"],
                }
            ),
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                    "Station To": ["C", "D"],
                    "Language Code": ["EN", "EN"],
                }
            ),
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "USA - A - C - EN",
                        "USA - A - C - EN",
                        "USA - B - D - EN",
                        "USA - B - D - EN",
                        "USA - A - C - EN",
                        "USA - A - C - EN",
                        "USA - B - D - EN",
                        "USA - B - D - EN",
                    ],
                    "Ad Group Name": [
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                    ],
                    "Match Type": [
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                        "Exact",
                    ],
                    "Keyword": ["k1 A", "k1 C", "k1 B", "k1 D", "k2", "k2", "k2", "k2"],
                    "Max CPC": ["", "", "", "", "", "", "", ""],
                    "Negative": [
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                        "FALSE",
                    ],
                    "Level": [None, None, None, None, None, None, None, None],
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "{INSERT_COUNTRY} - {INSERT_STATION_FROM} - {INSERT_STATION_TO} - {INSERT_LANGUAGE_CODE}",
                        "{INSERT_COUNTRY} - {INSERT_STATION_FROM} - {INSERT_STATION_TO} - {INSERT_LANGUAGE_CODE}",
                    ],
                    "Language Code": ["EN", "DE"],
                    "Ad Group Name": [
                        "{INSERT_STATION_FROM} - {INSERT_STATION_TO}",
                        "{INSERT_STATION_FROM} - {INSERT_STATION_TO}",
                    ],
                    "Match Type": ["Exact", "Exact"],
                }
            ),
            pd.DataFrame(
                {
                    "Keyword": ["k1", "k2"],
                    "Max CPC": ["", ""],
                    "Language Code": ["EN", "DE"],
                    "Negative": ["FALSE", "FALSE"],
                    "Level": [None, None],
                    "Keyword Match Type": ["Exact", "Exact"],
                }
            ),
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                    "Station To": ["C", "D"],
                    "Language Code": ["EN", "DE"],
                }
            ),
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "USA - A - C - EN",
                        "USA - A - C - EN",
                        "USA - B - D - DE",
                        "USA - B - D - DE",
                    ],
                    "Ad Group Name": [
                        "A - C",
                        "C - A",
                        "B - D",
                        "D - B",
                    ],
                    "Match Type": ["Exact", "Exact", "Exact", "Exact"],
                    "Keyword": ["k1", "k1", "k2", "k2"],
                    "Max CPC": ["", "", "", ""],
                    "Negative": ["FALSE", "FALSE", "FALSE", "FALSE"],
                    "Level": [None, None, None, None],
                }
            ),
        ),
    ],
)
def test_process_data_f(
    merged_campaigns_ad_groups_df: pd.DataFrame,
    template_df: pd.DataFrame,
    new_campaign_df: pd.DataFrame,
    expected: pd.DataFrame,
) -> None:
    processed_data = process_data_f(
        merged_campaigns_ad_groups_df, template_df, new_campaign_df, "keyword"
    )
    assert all(processed_data.columns == expected.columns)

    processed_data = processed_data.astype(str).reset_index(drop=True)
    expected = expected.astype(str).reset_index(drop=True)
    expected = expected.sort_values(
        by=["Campaign Name", "Ad Group Name"], ignore_index=True
    )
    assert processed_data.equals(expected)


@pytest.mark.parametrize(
    ("campaigns_template_df", "new_campaign_df", "expected"),
    [
        (
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "{INSERT_COUNTRY} - {INSERT_STATION_FROM} - {INSERT_STATION_TO} - {INSERT_LANGUAGE_CODE}"
                    ],
                    "Language Code": ["EN"],
                    "Campaign Budget": ["100"],
                    "Search Network": [True],
                    "Google Search Network": [False],
                    "Default max. CPC": [0.3],
                }
            ),
            pd.DataFrame(
                {
                    "Country": ["USA", "USA"],
                    "Station From": ["A", "B"],
                    "Station To": ["C", "D"],
                    "Language Code": ["EN", "EN"],
                }
            ),
            pd.DataFrame(
                {
                    "Campaign Name": [
                        "USA - A - C - EN",
                        "USA - B - D - EN",
                    ],
                    "Language Code": ["EN", "EN"],
                    "Campaign Budget": ["100", "100"],
                    "Search Network": [True, True],
                    "Google Search Network": [False, False],
                    "Default max. CPC": [0.3, 0.3],
                },
            ),
        ),
    ],
)
def test_process_campaign_data_f(
    campaigns_template_df: pd.DataFrame,
    new_campaign_df: pd.DataFrame,
    expected: pd.DataFrame,
) -> None:
    processed_data = process_campaign_data_f(campaigns_template_df, new_campaign_df)
    assert processed_data.equals(expected)


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


@pytest.mark.parametrize(
    ("new_camaign_row", "campaign_name", "language_code", "expected"),
    [
        (
            pd.Series(
                {
                    "Country": "USA",
                    "Station From": "A",
                    "Station To": "B",
                }
            ),
            "{INSERT_COUNTRY} - {INSERT_STATION_FROM} - {INSERT_STATION_TO}",
            "EN",
            "USA - A - B",
        ),
    ],
)
def test_update_campaign_name(
    new_camaign_row: pd.Series, campaign_name: str, language_code: str, expected: str
) -> None:
    assert (
        _update_campaign_name(new_camaign_row, campaign_name, language_code) == expected
    )


@pytest.mark.parametrize(
    ("new_campaign_df", "valid_language_codes", "raises_error"),
    [
        (
            pd.DataFrame(
                {
                    "Language Code": ["EN", "DE", "FR"],
                }
            ),
            ["EN", "DE", "FR"],
            False,
        ),
        (
            pd.DataFrame(
                {
                    "Language Code": ["EN", "DE", "FR"],
                }
            ),
            ["EN", "DE"],
            True,
        ),
    ],
)
def test_validate_language_codes(
    new_campaign_df: pd.DataFrame, valid_language_codes: List[str], raises_error: bool
) -> None:
    if raises_error:
        with pytest.raises(ValueError, match="FR"):
            _validate_language_codes(new_campaign_df, valid_language_codes, "table")
    else:
        _validate_language_codes(new_campaign_df, valid_language_codes, "table")
