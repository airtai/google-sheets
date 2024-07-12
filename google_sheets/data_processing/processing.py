from typing import List, Literal, Optional

import pandas as pd

__all__ = ["process_data_f", "validate_input_data", "validate_output_data"]


def validate_input_data(
    df: pd.DataFrame, mandatory_columns: List[str], name: str
) -> str:
    error_msg = ""
    if len(df.columns) != len(set(df.columns)):
        error_msg = f"""Duplicate columns found in the {name} data.
Please provide unique column names.
"""
    if not all(col in df.columns for col in mandatory_columns):
        error_msg += f"""Mandatory columns missing in the {name} data.
Please provide the following columns: {mandatory_columns}
"""
    if error_msg:
        return error_msg
    return ""


INSERT_STATION_FROM = "INSERT_STATION_FROM"
INSERT_STATION_TO = "INSERT_STATION_TO"
INSERT_COUNTRY = "INSERT_COUNTRY"
INSERT_CRITERION_TYPE = "INSERT_CRITERION_TYPE"


def process_data_f(
    merged_campaigns_ad_groups_df: pd.DataFrame,
    template_df: pd.DataFrame,
    new_campaign_df: pd.DataFrame,
    target_resource: Optional[str] = None,
) -> pd.DataFrame:
    template_df = pd.merge(merged_campaigns_ad_groups_df, template_df, how="cross")
    final_df = pd.DataFrame(columns=template_df.columns)
    for _, template_row in template_df.iterrows():
        for _, new_campaign_row in new_campaign_df.iterrows():
            stations = [
                {
                    "Station From": new_campaign_row["Station From"],
                    "Station To": new_campaign_row["Station To"],
                },
                # Reverse the order of the stations
                {
                    "Station From": new_campaign_row["Station To"],
                    "Station To": new_campaign_row["Station From"],
                },
            ]
            if target_resource == "ad":
                stations[0]["Final Url"] = new_campaign_row["Final Url From"]
                stations[1]["Final Url"] = new_campaign_row["Final Url To"]

            for station in stations:
                new_row = template_row.copy()
                new_row["Campaign Name"] = new_row["Campaign Name"].replace(
                    INSERT_COUNTRY, new_campaign_row["Country"]
                )
                new_row["Campaign Name"] = new_row["Campaign Name"].replace(
                    INSERT_STATION_FROM, new_campaign_row["Station From"]
                )
                new_row["Campaign Name"] = new_row["Campaign Name"].replace(
                    INSERT_STATION_TO, new_campaign_row["Station To"]
                )

                new_row["Ad Group Name"] = new_row["Ad Group Name"].replace(
                    INSERT_CRITERION_TYPE, new_row["Match Type"]
                )

                new_row = new_row.str.replace(
                    INSERT_COUNTRY, new_campaign_row["Country"]
                )
                new_row = new_row.str.replace(
                    INSERT_STATION_FROM, station["Station From"]
                )
                new_row = new_row.str.replace(INSERT_STATION_TO, station["Station To"])

                if target_resource == "ad":
                    new_row["Final URL"] = station["Final Url"]

                final_df = pd.concat(
                    [final_df, pd.DataFrame([new_row])], ignore_index=True
                )

    final_df = final_df.sort_values(
        by=["Campaign Name", "Ad Group Name"], ignore_index=True
    )

    return final_df


MIN_HEADLINES = 3
MAX_HEADLINES = 15
MIN_DESCRIPTIONS = 2
MAX_DESCRIPTIONS = 4

MAX_HEADLINE_LENGTH = 30
MAX_DESCRIPTION_LENGTH = 90


def _validate_output_data_ad(df: pd.DataFrame) -> pd.DataFrame:  # noqa: C901
    df["Issues"] = ""
    headline_columns = [col for col in df.columns if "Headline" in col]
    description_columns = [col for col in df.columns if "Description" in col]

    for index, row in df.iterrows():
        # Check for duplicate headlines and descriptions
        if len(set(row[headline_columns])) != len(row[headline_columns]):
            df.loc[index, "Issues"] += "Duplicate headlines found.\n"
        if len(set(row[description_columns])) != len(row[description_columns]):
            df.loc[index, "Issues"] += "Duplicate descriptions found.\n"

        # Check for the number of headlines and descriptions
        headline_count = len(
            [headline for headline in row[headline_columns] if headline]
        )
        if headline_count < MIN_HEADLINES:
            df.loc[index, "Issues"] += (
                f"Minimum {MIN_HEADLINES} headlines are required, found {headline_count}.\n"
            )
        elif headline_count > MAX_HEADLINES:
            df.loc[index, "Issues"] += (
                f"Maximum {MAX_HEADLINES} headlines are allowed, found {headline_count}.\n"
            )

        description_count = len(
            [description for description in row[description_columns] if description]
        )
        if description_count < MIN_DESCRIPTIONS:
            df.loc[index, "Issues"] += (
                f"Minimum {MIN_DESCRIPTIONS} descriptions are required, found {description_count}.\n"
            )
        elif description_count > MAX_DESCRIPTIONS:
            df.loc[index, "Issues"] += (
                f"Maximum {MAX_DESCRIPTIONS} descriptions are allowed, found {description_count}.\n"
            )

        # Check for the length of headlines and descriptions
        for headline_column in headline_columns:
            headline = row[headline_column]
            if len(headline) > MAX_HEADLINE_LENGTH:
                df.loc[index, "Issues"] += (
                    f"Headline length should be less than {MAX_HEADLINE_LENGTH} characters, found {len(headline)} in column {headline_column}.\n"
                )

        for description_column in description_columns:
            description = row[description_column]
            if len(description) > MAX_DESCRIPTION_LENGTH:
                df.loc[index, "Issues"] += (
                    f"Description length should be less than {MAX_DESCRIPTION_LENGTH} characters, found {len(description)} in column {description_column}.\n"
                )

        # TODO: Check for the final URL
        # if not row["Final URL"]:
        #     df.loc[index, "Issues"] += "Final URL is missing.\n"

    if not df["Issues"].any():
        df = df.drop(columns=["Issues"])

    return df


def validate_output_data(
    df: pd.DataFrame, target_resource: Literal["ad", "keyword"]
) -> pd.DataFrame:
    if target_resource == "keyword":
        # No validation required for keyword data currently
        return df

    return _validate_output_data_ad(df)
