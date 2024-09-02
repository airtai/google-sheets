from typing import List, Literal

import pandas as pd

__all__ = [
    "process_campaign_data_f",
    "process_data_f",
    "validate_input_data",
    "validate_output_data",
]


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


INSERT_STATION_FROM = "{INSERT_STATION_FROM}"
INSERT_STATION_TO = "{INSERT_STATION_TO}"
INSERT_COUNTRY = "{INSERT_COUNTRY}"
INSERT_CRITERION_TYPE = "{INSERT_CRITERION_TYPE}"
INSERT_LANGUAGE_CODE = "{INSERT_LANGUAGE_CODE}"


def _update_campaign_name(
    new_campaign_row: pd.Series,
    campaign_name: str,
    language_code: str,
    include_locations: str,
) -> str:
    campaign_name = campaign_name.format(
        INSERT_COUNTRY=new_campaign_row["Country"],
        INSERT_STATION_FROM=new_campaign_row["Station From"],
        INSERT_STATION_TO=new_campaign_row["Station To"],
        INSERT_LANGUAGE_CODE=language_code,
        INSERT_TARGET_LOCATION=include_locations,
    )
    return campaign_name


def _validate_language_codes(
    new_campaign_df: pd.DataFrame, valid_language_codes: List[str], table_name: str
) -> None:
    invalid_language_codes = new_campaign_df[
        ~new_campaign_df["Language Code"].isin(valid_language_codes)
    ]["Language Code"].unique()
    if invalid_language_codes.size:
        raise ValueError(
            f"""Table: '{table_name}' currently does NOT have any data for the following language codes:
    {invalid_language_codes}.

    Please provide data for the above language codes or choose a different language code.
    """
        )


COPY_ALL_WITH_PREFIX = [
    "Exclude Location",
    "Include Location",
    "Include Language",
    "Exclude Language",
    "Sitelink",
]


def _copy_all_with_prefixes(
    new_campaign_row: pd.Series,
    new_row: pd.Series,
    prefixes: List[str] = COPY_ALL_WITH_PREFIX,
) -> pd.Series:
    for prefix in prefixes:
        columns = [col for col in new_campaign_row.index if col.startswith(prefix)]
        for col in columns:
            new_row[col] = new_campaign_row[col]
    return new_row


def _get_target_location(new_campaign_row: pd.Series) -> str:
    include_locations_columns = [
        col for col in new_campaign_row.index if col.startswith("Include Location")
    ]
    include_locations_values = [
        new_campaign_row[col]
        for col in include_locations_columns
        if new_campaign_row[col]
    ]
    if include_locations_values:
        include_locations = "-".join(include_locations_values)
    else:
        include_locations = "Worldwide"
    return include_locations


def process_campaign_data_f(
    campaigns_template_df: pd.DataFrame, new_campaign_df: pd.DataFrame
) -> pd.DataFrame:
    new_campaign_df["Language Code"] = new_campaign_df["Language Code"].str.upper()
    campaigns_template_df["Language Code"] = campaigns_template_df[
        "Language Code"
    ].str.upper()

    _validate_language_codes(
        new_campaign_df,
        valid_language_codes=campaigns_template_df["Language Code"].unique(),
        table_name="Campaigns",
    )

    final_df = None
    # columns are campaign_template_df.columns + columns which start with COPY_ALL_WITH_PREFIX
    columns = list(campaigns_template_df.columns) + [
        col
        for col in new_campaign_df.columns
        if any(col.startswith(prefix) for prefix in COPY_ALL_WITH_PREFIX)
    ]
    for _, new_campaign_row in new_campaign_df.iterrows():
        for _, template_row in campaigns_template_df[
            campaigns_template_df["Language Code"] == new_campaign_row["Language Code"]
        ].iterrows():
            new_row = template_row.copy()
            new_row = _copy_all_with_prefixes(new_campaign_row, new_row)
            include_locations = _get_target_location(new_campaign_row)

            new_row["Campaign Name"] = _update_campaign_name(
                new_campaign_row,
                campaign_name=new_row["Campaign Name"],
                language_code=new_row["Language Code"],
                include_locations=include_locations,
            )

            if final_df is None:
                final_df = pd.DataFrame([new_row], columns=columns)
            else:
                final_df = pd.concat(
                    [final_df, pd.DataFrame([new_row], columns=columns)],
                    ignore_index=True,
                )

            final_df["Search Network"] = final_df["Search Network"].astype(bool)
            final_df["Google Search Network"] = final_df[
                "Google Search Network"
            ].astype(bool)
            final_df["Default max. CPC"] = final_df["Default max. CPC"].astype(float)

    return final_df


def _use_template_row(new_campaign_row: pd.Series, template_row: pd.Series) -> bool:
    if template_row["Category"] is None:
        return True

    category_columns = [
        col for col in new_campaign_row.index if col.startswith("Category")
    ]
    category_values = [
        new_campaign_row[col] for col in category_columns if new_campaign_row[col]
    ]

    return not (category_values and template_row["Category"] not in category_values)


def process_data_f(
    merged_campaigns_ad_groups_df: pd.DataFrame,
    template_df: pd.DataFrame,
    new_campaign_df: pd.DataFrame,
    target_resource: str,
) -> pd.DataFrame:
    merged_campaigns_ad_groups_df["Language Code"] = merged_campaigns_ad_groups_df[
        "Language Code"
    ].str.upper()
    template_df["Language Code"] = template_df["Language Code"].str.upper()
    new_campaign_df["Language Code"] = new_campaign_df["Language Code"].str.upper()
    template_df = pd.merge(
        merged_campaigns_ad_groups_df,
        template_df,
        how="inner",
        on="Language Code",
    )

    _validate_language_codes(
        new_campaign_df,
        valid_language_codes=template_df["Language Code"].unique(),
        table_name=target_resource,
    )

    final_df = pd.DataFrame(columns=template_df.columns)
    for _, new_campaign_row in new_campaign_df.iterrows():
        for _, template_row in template_df[
            template_df["Language Code"] == new_campaign_row["Language Code"]
        ].iterrows():
            if not _use_template_row(new_campaign_row, template_row):
                continue
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
                include_locations = _get_target_location(new_campaign_row)
                new_row["Campaign Name"] = _update_campaign_name(
                    new_campaign_row,
                    campaign_name=new_row["Campaign Name"],
                    language_code=new_row["Language Code"],
                    include_locations=include_locations,
                )

                new_row = new_row.str.replace(
                    INSERT_COUNTRY, new_campaign_row["Country"]
                )
                new_row = new_row.str.replace(
                    INSERT_STATION_FROM, station["Station From"]
                )
                new_row = new_row.str.replace(INSERT_STATION_TO, station["Station To"])
                new_row = new_row.str.replace(
                    INSERT_CRITERION_TYPE, new_row["Match Type"]
                )

                if target_resource == "ad":
                    new_row["Final URL"] = station["Final Url"]
                elif (
                    target_resource == "keyword"
                    and new_row["Negative"]
                    and new_row["Negative"].lower() == "true"
                ):
                    new_row["Match Type"] = new_row["Keyword Match Type"]

                    if "Campaign" in new_row["Level"]:
                        new_row["Ad Group Name"] = None

                final_df = pd.concat(
                    [final_df, pd.DataFrame([new_row])], ignore_index=True
                )

    final_df = final_df.drop(columns=["Language Code", "Category"])
    if target_resource == "keyword":
        final_df = final_df.drop(columns=["Keyword Match Type"])
    final_df = final_df.drop_duplicates(ignore_index=True)

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

MAX_PATH_LENGTH = 15


def _validate_output_data_ad(df: pd.DataFrame) -> pd.DataFrame:  # noqa: C901
    df.insert(0, "Issues", "")

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

        for path in ["Path 1", "Path 2"]:
            if row[path] and len(row[path]) > MAX_PATH_LENGTH:
                df.loc[index, "Issues"] += (
                    f"{path} length should be less than {MAX_PATH_LENGTH} characters, found {len(row[path])}.\n"
                )

        if not row["Final URL"]:
            df.loc[index, "Issues"] += "Final URL is missing.\n"

    if not df["Issues"].any():
        df = df.drop(columns=["Issues"])

    return df


def validate_output_data(
    df: pd.DataFrame, target_resource: Literal["ad", "campaign" "keyword"]
) -> pd.DataFrame:
    if target_resource == "ad":
        return _validate_output_data_ad(df)
    # No validation required for campaign and keyword data currently
    return df
