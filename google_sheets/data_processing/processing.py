from typing import List

import pandas as pd

__all__ = ["process_data_f", "validate_input_data"]


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


def process_data_f(
    template_df: pd.DataFrame, new_campaign_df: pd.DataFrame
) -> pd.DataFrame:
    final_df = pd.DataFrame(columns=template_df.columns)
    for _, template_row in template_df.iterrows():
        for _, new_campaign_row in new_campaign_df.iterrows():
            campaign = f"{new_campaign_row['Country']} - {new_campaign_row['Station From']} - {new_campaign_row['Station To']}"
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
            for station in stations:
                new_row = template_row.copy()
                new_row["Campaign"] = campaign
                new_row["Ad Group"] = (
                    f"{station['Station From']} - {station['Station To']}"
                )

                # Replace the placeholders in all columns with the actual station names INSERT_STATION_FROM
                new_row = new_row.str.replace(
                    INSERT_STATION_FROM, station["Station From"]
                )
                new_row = new_row.str.replace(INSERT_STATION_TO, station["Station To"])

                final_df = pd.concat(
                    [final_df, pd.DataFrame([new_row])], ignore_index=True
                )

    return final_df
