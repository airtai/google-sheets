import json
import logging
from datetime import datetime
from os import environ
from typing import Annotated, Dict, List, Literal, Union

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import RedirectResponse
from googleapiclient.errors import HttpError

from . import __version__
from .data_processing import process_data_f, validate_data
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
) -> Union[str, GoogleSheetValues]:
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")
    values = await get_sheet_f(
        service=service, spreadsheet_id=spreadsheet_id, range=title
    )

    if not values:
        return "No data found."

    return GoogleSheetValues(values=values)


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
MANDATORY_AD_TEMPLATE_COLUMNS = [
    "Campaign",
    "Ad Group",
    "Headline 1",
    "Headline 2",
    "Headline 3",
    "Description Line 1",
    "Description Line 2",
    "Final Url",
]

MANDATORY_KEYWORD_TEMPLATE_COLUMNS = [
    "Campaign",
    "Ad Group",
    "Keyword",
    "Criterion Type",
    "Max CPC",
]


@app.post(
    "/process-data",
    description="Process data to generate new ads or keywords based on the template",
)
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

    validation_error_msg = validate_data(
        df=new_campaign_df,
        mandatory_columns=NEW_CAMPAIGN_MANDATORY_COLUMNS,
        name="new campaign",
    )

    if target_resource == "ad":
        validation_error_msg += validate_data(
            df=template_df,
            mandatory_columns=MANDATORY_AD_TEMPLATE_COLUMNS,
            name="ads template",
        )
    else:
        validation_error_msg += validate_data(
            df=template_df,
            mandatory_columns=MANDATORY_KEYWORD_TEMPLATE_COLUMNS,
            name="keyword template",
        )
    if validation_error_msg:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=validation_error_msg
        )

    return process_data_f(template_df, new_campaign_df)


@app.post(
    "/process-spreadsheet",
    description="Process data to generate new ads or keywords based on the template",
)
async def process_spreadsheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    template_spreadsheet_id: Annotated[
        str, Query(description="ID of the Google Sheet with the template data")
    ],
    template_sheet_title: Annotated[
        str,
        Query(description="The title of the sheet with the template data"),
    ],
    new_campaign_spreadsheet_id: Annotated[
        str, Query(description="ID of the Google Sheet with the new campaign data")
    ],
    new_campaign_sheet_title: Annotated[
        str,
        Query(description="The title of the sheet with the new campaign data"),
    ],
    target_resource: Annotated[
        Literal["ad", "keyword"], Query(description="The target resource to be updated")
    ],
) -> Response:
    template_values = await get_sheet(
        user_id=user_id,
        spreadsheet_id=template_spreadsheet_id,
        title=template_sheet_title,
    )
    new_campaign_values = await get_sheet(
        user_id=user_id,
        spreadsheet_id=new_campaign_spreadsheet_id,
        title=new_campaign_sheet_title,
    )

    if not isinstance(template_values, GoogleSheetValues) or not isinstance(
        new_campaign_values, GoogleSheetValues
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"""Invalid data format.
template_values: {template_values}

new_campaign_values: {new_campaign_values}

Please provide data in the correct format.""",
        )

    processed_values = await process_data(
        template_sheet_values=template_values,
        new_campaign_sheet_values=new_campaign_values,
        target_resource=target_resource,
    )

    title = (
        f"Captn - {target_resource.capitalize()}s {datetime.now():%Y-%m-%d %H:%M:%S}"
    )
    await create_sheet(
        user_id=user_id,
        spreadsheet_id=new_campaign_spreadsheet_id,
        title=title,
    )
    await update_sheet(
        user_id=user_id,
        spreadsheet_id=new_campaign_spreadsheet_id,
        title=title,
        sheet_values=processed_values,
    )

    return Response(
        status_code=status.HTTP_201_CREATED,
        content=f"Sheet with the name 'Captn - {target_resource.capitalize()}s' has been created successfully.",
    )
