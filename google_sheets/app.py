import json
import logging
from datetime import datetime
from os import environ
from typing import Annotated, Any, Dict, List, Optional, Union

import httpx
import pandas as pd
from fastapi import Body, FastAPI, HTTPException, Query, Response, status
from fastapi.responses import RedirectResponse
from googleapiclient.errors import HttpError

from . import __version__
from .data_processing import process_data_f, validate_input_data, validate_output_data
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

    return bool(data)


# Route 1: Redirect to Google OAuth
@app.get("/login", description="Get the URL to log in with Google")
async def get_login_url(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    conv_uuid: Annotated[
        Optional[str], Query(description="The conversation UUID")
    ] = None,
    force_new_login: Annotated[bool, Query(description="Force new login")] = False,
) -> Dict[str, str]:
    _check_parameters_are_not_none({"conv_uuid": conv_uuid})
    if not force_new_login:
        is_authenticated = await is_authenticated_for_ads(user_id=user_id)
        if is_authenticated:
            return {"login_url": "User is already authenticated"}

    google_oauth_url = get_google_oauth_url(user_id, conv_uuid)  # type: ignore
    markdown_url = f"To navigate Google Ads waters, I require access to your account. Please [click here]({google_oauth_url}) to grant permission."
    return {"login_url": markdown_url}


def _check_parameters_are_not_none(kwargs: Dict[str, Any]) -> None:
    error_message = "The following parameters are required: "
    missing_parameters = [key for key, value in kwargs.items() if value is None]
    if missing_parameters:
        error_message += ", ".join(missing_parameters)
        raise HTTPException(status_code=400, detail=error_message)


REDIRECT_DOMAIN = environ.get("REDIRECT_DOMAIN", "http://localhost:3000")


# Route 2: Save user credentials/token to a JSON file
@app.get("/login/callback")
async def login_callback(
    code: Annotated[
        str,
        Query(description="The authorization code received after successful login"),
    ],
    state: Annotated[Optional[str], Query(description="State")] = None,
) -> RedirectResponse:
    _check_parameters_are_not_none({"state": state})
    user_id_and_chat_uuid = state.split(":")  # type: ignore
    if not user_id_and_chat_uuid[0].isdigit():  # type: ignore
        raise HTTPException(status_code=400, detail="User ID must be an integer")
    user_id = int(user_id_and_chat_uuid[0])
    chat_uuid = user_id_and_chat_uuid[1]

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

    logged_in_message = "I have successfully logged in"
    redirect_uri = f"{REDIRECT_DOMAIN}/chat/{chat_uuid}?msg={logged_in_message}"
    return RedirectResponse(redirect_uri)


@app.get("/get-sheet", description="Get data from a Google Sheet")
async def get_sheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    spreadsheet_id: Annotated[
        Optional[str], Query(description="ID of the Google Sheet to fetch data from")
    ] = None,
    title: Annotated[
        Optional[str],
        Query(description="The title of the sheet to fetch data from"),
    ] = None,
) -> Union[str, GoogleSheetValues]:
    _check_parameters_are_not_none({"spreadsheet_id": spreadsheet_id, "title": title})
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")
    values = await get_sheet_f(
        service=service,
        spreadsheet_id=spreadsheet_id,  # type: ignore
        range=title,  # type: ignore
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
        Optional[str], Query(description="ID of the Google Sheet to fetch data from")
    ] = None,
    title: Annotated[
        Optional[str],
        Query(description="The title of the sheet to update"),
    ] = None,
    sheet_values: Annotated[
        Optional[GoogleSheetValues],
        Body(embed=True, description="Values to be written to the Google Sheet"),
    ] = None,
) -> Response:
    _check_parameters_are_not_none(
        {"spreadsheet_id": spreadsheet_id, "title": title, "sheet_values": sheet_values}
    )
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")

    try:
        await update_sheet_f(
            service=service,
            spreadsheet_id=spreadsheet_id,  # type: ignore
            range=title,  # type: ignore
            sheet_values=sheet_values,  # type: ignore
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
        Optional[str], Query(description="ID of the Google Sheet to fetch data from")
    ] = None,
    title: Annotated[
        Optional[str],
        Query(description="The title of the new sheet"),
    ] = None,
) -> Response:
    _check_parameters_are_not_none({"spreadsheet_id": spreadsheet_id, "title": title})
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")
    try:
        await create_sheet_f(
            service=service,
            spreadsheet_id=spreadsheet_id,  # type: ignore
            title=title,  # type: ignore
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
        Optional[str], Query(description="ID of the Google Sheet to fetch data from")
    ] = None,
) -> List[str]:
    _check_parameters_are_not_none({"spreadsheet_id": spreadsheet_id})
    service = await build_service(user_id=user_id, service_name="sheets", version="v4")
    try:
        sheets = await get_all_sheet_titles_f(
            service=service,
            spreadsheet_id=spreadsheet_id,  # type: ignore
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


def _validate_target_resource(target_resource: Optional[str]) -> None:
    if target_resource not in ["ad", "keyword"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The target resource should be either 'ad' or 'keyword'.",
        )


@app.post(
    "/process-data",
    description="Process data to generate new ads or keywords based on the template",
)
async def process_data(
    template_sheet_values: Annotated[
        Optional[GoogleSheetValues],
        Body(
            embed=True,
            description="Template values to be used for generating new ads or keywords",
        ),
    ] = None,
    new_campaign_sheet_values: Annotated[
        Optional[GoogleSheetValues],
        Body(
            embed=True,
            description="New campaign values to be used for generating new ads or keywords",
        ),
    ] = None,
    target_resource: Annotated[
        Optional[str],
        Query(
            description="The target resource to be updated. This can be 'ad' or 'keyword'"
        ),
    ] = None,
) -> GoogleSheetValues:
    _check_parameters_are_not_none(
        {
            "template_sheet_values": template_sheet_values,
            "new_campaign_sheet_values": new_campaign_sheet_values,
            "target_resource": target_resource,
        }
    )
    _validate_target_resource(target_resource)
    if (
        len(template_sheet_values.values) < 2  # type: ignore
        or len(new_campaign_sheet_values.values) < 2  # type: ignore
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both template and new campaign data should have at least two rows (header and data).",
        )
    try:
        template_df = pd.DataFrame(
            template_sheet_values.values[1:],  # type: ignore
            columns=template_sheet_values.values[0],  # type: ignore
        )
        new_campaign_df = pd.DataFrame(
            new_campaign_sheet_values.values[1:],  # type: ignore
            columns=new_campaign_sheet_values.values[0],  # type: ignore
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid data format. Please provide data in the correct format: {e}",
        ) from e

    validation_error_msg = validate_input_data(
        df=new_campaign_df,
        mandatory_columns=NEW_CAMPAIGN_MANDATORY_COLUMNS,
        name="new campaign",
    )

    if target_resource == "ad":
        validation_error_msg += validate_input_data(
            df=template_df,
            mandatory_columns=MANDATORY_AD_TEMPLATE_COLUMNS,
            name="ads template",
        )
    else:
        validation_error_msg += validate_input_data(
            df=template_df,
            mandatory_columns=MANDATORY_KEYWORD_TEMPLATE_COLUMNS,
            name="keyword template",
        )
    if validation_error_msg:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=validation_error_msg
        )

    processed_df = process_data_f(template_df, new_campaign_df)

    validated_df = validate_output_data(
        processed_df,
        target_resource,  # type: ignore
    )

    values = [validated_df.columns.tolist(), *validated_df.values.tolist()]

    return GoogleSheetValues(values=values)


@app.post(
    "/process-spreadsheet",
    description="Process data to generate new ads or keywords based on the template",
)
async def process_spreadsheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    template_spreadsheet_id: Annotated[
        Optional[str],
        Query(description="ID of the Google Sheet with the template data"),
    ] = None,
    template_sheet_title: Annotated[
        Optional[str],
        Query(description="The title of the sheet with the template data"),
    ] = None,
    new_campaign_spreadsheet_id: Annotated[
        Optional[str],
        Query(description="ID of the Google Sheet with the new campaign data"),
    ] = None,
    new_campaign_sheet_title: Annotated[
        Optional[str],
        Query(description="The title of the sheet with the new campaign data"),
    ] = None,
    target_resource: Annotated[
        Optional[str],
        Query(
            description="The target resource to be updated, options: 'ad' or 'keyword'"
        ),
    ] = None,
) -> str:
    _check_parameters_are_not_none(
        {
            "template_spreadsheet_id": template_spreadsheet_id,
            "template_sheet_title": template_sheet_title,
            "new_campaign_spreadsheet_id": new_campaign_spreadsheet_id,
            "new_campaign_sheet_title": new_campaign_sheet_title,
            "target_resource": target_resource,
        }
    )
    _validate_target_resource(target_resource)
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
        f"Captn - {target_resource.capitalize()}s {datetime.now():%Y-%m-%d %H:%M:%S}"  # type: ignore
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

    return f"Sheet with the name 'Captn - {target_resource.capitalize()}s' has been created successfully."  # type: ignore
