from typing import List

import pandas as pd

from ..model import GoogleSheetValues

__all__ = ["process_data_f", "validate_data"]


def validate_data(df: pd.DataFrame, mandatory_columns: List[str], name: str) -> str:
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


def process_data_f(
    template_df: pd.DataFrame, new_campaign_df: pd.DataFrame
) -> GoogleSheetValues:
    final_df = pd.DataFrame(columns=template_df.columns)
    for _, template_row in template_df.iterrows():
        for _, new_campaign_row in new_campaign_df.iterrows():
            campaign = f"{new_campaign_row['Country']} - {new_campaign_row['Station From']} - {new_campaign_row['Station To']}"
            for ad_group in [
                f"{new_campaign_row['Station From']} - {new_campaign_row['Station To']}",
                f"{new_campaign_row['Station To']} - {new_campaign_row['Station From']}",
            ]:
                new_row = template_row.copy()
                new_row["Campaign"] = campaign
                new_row["Ad Group"] = ad_group
                final_df = pd.concat(
                    [final_df, pd.DataFrame([new_row])], ignore_index=True
                )

    values = [final_df.columns.tolist(), *final_df.values.tolist()]
    return GoogleSheetValues(values=values)
