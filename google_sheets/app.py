import json
import logging
from os import environ
from typing import Annotated, Dict, List, Literal, Union

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import RedirectResponse
from googleapiclient.errors import HttpError

from . import __version__
from .db_helpers import get_db_connection
from .google_api import (
    build_service,
    create_sheet_f,
    get_all_sheet_titles_f,
    get_files_f,
    get_google_oauth_url,
    get_sheet_f,
    get_token_request_data,
    oauth2_settings,
    update_sheet_f,
)
from .model import GoogleSheetValues

__all__ = ["app"]

logging.basicConfig(level=logging.INFO)

host = environ.get("DOMAIN", "localhost")
port = 8000
protocol = "http" if host == "localhost" else "https"
base_url = (
    f"{protocol}://{host}:{port}" if host == "localhost" else f"{protocol}://{host}"
)

app = FastAPI(
    servers=[{"url": base_url, "description": "Google Sheets app server"}],
    version=__version__,
    title="google-sheets",
)


async def is_authenticated_for_ads(user_id: int) -> bool:
    async with get_db_connection() as db:
        data = await db.gauth.find_unique(where={"user_id": user_id})

    if not data:
        return False
    return True


# Route 1: Redirect to Google OAuth
@app.get("/login")
async def get_login_url(
    request: Request,
    user_id: int = Query(title="User ID"),
    force_new_login: bool = Query(title="Force new login", default=False),
) -> Dict[str, str]:
    if not force_new_login:
        is_authenticated = await is_authenticated_for_ads(user_id=user_id)
        if is_authenticated:
            return {"login_url": "User is already authenticated"}

    google_oauth_url = get_google_oauth_url(user_id)
    markdown_url = f"To navigate Google Ads waters, I require access to your account. Please [click here]({google_oauth_url}) to grant permission."
    return {"login_url": markdown_url}


@app.get("/login/success")
async def get_login_success() -> Dict[str, str]:
    return {"login_success": "You have successfully logged in"}


# Route 2: Save user credentials/token to a JSON file
@app.get("/login/callback")
async def login_callback(
    code: str = Query(title="Authorization Code"), state: str = Query(title="State")
) -> RedirectResponse:
    if not state.isdigit():
        raise HTTPException(status_code=400, detail="User ID must be an integer")
    user_id = int(state)

    token_request_data = get_token_request_data(code)

    async with httpx.AsyncClient() as client:
        response = await client.post(
            oauth2_settings["tokenUrl"], data=token_request_data
        )

    if response.status_code == 200:
        token_data = response.json()

    async with httpx.AsyncClient() as client:
        userinfo_response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {token_data['access_token']}"},
        )

    if userinfo_response.status_code == 200:
        user_info = userinfo_response.json()
    async with get_db_connection() as db:
        await db.gauth.upsert(
            where={"user_id": user_id},
            data={
                "create": {
                    "user_id": user_id,
                    "creds": json.dumps(token_data),
                    "info": json.dumps(user_info),
                },
                "update": {
                    "creds": json.dumps(token_data),
                    "info": json.dumps(user_info),
                },
            },
        )

    # redirect_domain = environ.get("REDIRECT_DOMAIN", "https://captn.ai")
    # logged_in_message = "I have successfully logged in"
    # redirect_uri = f"{redirect_domain}/chat/{chat_uuid}?msg={logged_in_message}"
    # return RedirectResponse(redirect_uri)
    # redirect to success page
    return RedirectResponse(url=f"{base_url}/login/success")


@app.get("/get-sheet", description="Get data from a Google Sheet")
async def get_sheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    spreadsheet_id: Annotated[
        str, Query(description="ID of the Google Sheet to fetch data from")
    ],
    title: Annotated[
        str,
        Query(description="The title of the sheet to fetch data from"),
    ],
) -> Union[str, List[List[str]]]:
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")
    values = await get_sheet_f(
        service=service, spreadsheet_id=spreadsheet_id, range=title
    )

    if not values:
        return "No data found."

    return values  # type: ignore[no-any-return]


@app.post(
    "/update-sheet",
    description="Update data in a Google Sheet within the existing spreadsheet",
)
async def update_sheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    spreadsheet_id: Annotated[
        str, Query(description="ID of the Google Sheet to fetch data from")
    ],
    title: Annotated[
        str,
        Query(description="The title of the sheet to update"),
    ],
    sheet_values: GoogleSheetValues,
) -> Response:
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")

    try:
        await update_sheet_f(
            service=service,
            spreadsheet_id=spreadsheet_id,
            range=title,
            sheet_values=sheet_values,
        )
    except HttpError as e:
        raise HTTPException(status_code=e.status_code, detail=e._get_reason()) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e

    return Response(
        status_code=status.HTTP_200_OK,
        content=f"Sheet with the name '{title}' has been updated successfully.",
    )


@app.post(
    "/create-sheet",
    description="Create a new Google Sheet within the existing spreadsheet",
)
async def create_sheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    spreadsheet_id: Annotated[
        str, Query(description="ID of the Google Sheet to fetch data from")
    ],
    title: Annotated[
        str,
        Query(description="The title of the new sheet"),
    ],
) -> Response:
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")
    try:
        await create_sheet_f(
            service=service, spreadsheet_id=spreadsheet_id, title=title
        )
    except HttpError as e:
        if (
            e.status_code == status.HTTP_400_BAD_REQUEST
            and f'A sheet with the name "{title}" already exists' in e._get_reason()
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'A sheet with the name "{title}" already exists. Please enter another name.',
            ) from e
        raise HTTPException(status_code=e.status_code, detail=e._get_reason()) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e

    return Response(
        status_code=status.HTTP_201_CREATED,
        content=f"Sheet with the name '{title}' has been created successfully.",
    )


@app.get("/get-all-file-names", description="Get all sheets associated with the user")
async def get_all_file_names(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
) -> Dict[str, str]:
    service = await build_service(user_id=user_id, service_name="drive", version="v3")
    files: List[Dict[str, str]] = await get_files_f(service=service)
    # create dict where key is id and value is name
    files_dict = {file["id"]: file["name"] for file in files}
    return files_dict


@app.get(
    "/get-all-sheet-titles",
    description="Get all sheet titles within a Google Spreadsheet",
)
async def get_all_sheet_titles(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    spreadsheet_id: Annotated[
        str, Query(description="ID of the Google Sheet to fetch data from")
    ],
) -> List[str]:
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")
    try:
        sheets = await get_all_sheet_titles_f(
            service=service, spreadsheet_id=spreadsheet_id
        )
    except HttpError as e:
        raise HTTPException(status_code=e.status_code, detail=e._get_reason()) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e
    return sheets


NEW_CAMPAIGN_MANDATORY_COLUMNS = ["Country", "Station From", "Station To"]
MANDATORY_TEMPLATE_COLUMNS = [
    "Campaign",
    "Ad Group",
    "Headline 1",
    "Headline 2",
    "Headline 3",
    "Description Line 1",
    "Description Line 2",
    "Final Url",
]


def process_ad_data(
    template_df: pd.DataFrame, new_campaign_df: pd.DataFrame
) -> GoogleSheetValues:
    mandatory_columns_error_message = ""
    if not all(
        col in new_campaign_df.columns for col in NEW_CAMPAIGN_MANDATORY_COLUMNS
    ):
        mandatory_columns_error_message = f"""Mandatory columns missing in the new campaign data.
Please provide the following columns: {NEW_CAMPAIGN_MANDATORY_COLUMNS}"""

    if not all(col in template_df.columns for col in MANDATORY_TEMPLATE_COLUMNS):
        mandatory_columns_error_message = f"""Mandatory columns missing in the template data.
Please provide the following columns: {MANDATORY_TEMPLATE_COLUMNS}"""
    if mandatory_columns_error_message:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=mandatory_columns_error_message,
        )

    # create new dataframe with columns from the template data
    # For each row in the new campaign data frame, create two rows in the final data frame
    # Both rows will have the same data except for the Ad Group column
    # One will have "Station From - Station To" and the other will have "Station To - Station From"
    # Campaign value will be the same for both rows - "Country - Station From - Station To"
    final_df = pd.DataFrame(columns=template_df.columns)
    for _, row in new_campaign_df.iterrows():
        campaign = f"{row['Country']} - {row['Station From']} - {row['Station To']}"
        for ad_group in [
            f"{row['Station From']} - {row['Station To']}",
            f"{row['Station To']} - {row['Station From']}",
        ]:
            new_row = row.copy()
            new_row["Campaign"] = campaign
            new_row["Ad Group"] = ad_group
            final_df = final_df.append(new_row, ignore_index=True)
            # AttributeError: 'DataFrame' object has no attribute 'append'

    return GoogleSheetValues(values=final_df.values.tolist())


@app.post("/process-data")
async def process_data(
    template_sheet_values: GoogleSheetValues,
    new_campaign_sheet_values: GoogleSheetValues,
    target_resource: Annotated[
        Literal["ad", "keyword"], Query(description="The target resource to be updated")
    ],
) -> GoogleSheetValues:
    if (
        len(template_sheet_values.values) < 2
        or len(new_campaign_sheet_values.values) < 2
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both template and new campaign data should have at least two rows (header and data).",
        )
    try:
        template_df = pd.DataFrame(
            template_sheet_values.values[1:], columns=template_sheet_values.values[0]
        )
        new_campaign_df = pd.DataFrame(
            new_campaign_sheet_values.values[1:],
            columns=new_campaign_sheet_values.values[0],
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid data format. Please provide data in the correct format: {e}",
        ) from e

    if target_resource == "ad":
        return process_ad_data(template_df, new_campaign_df)

    raise NotImplementedError("Processing for keyword data is not implemented yet.")
