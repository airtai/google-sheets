import json
import logging
import urllib.parse
from os import environ
from pathlib import Path
from typing import Annotated, Any, Dict, List, Union

import httpx
from asyncify import asyncify
from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import RedirectResponse
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from prisma.errors import RecordNotFoundError
from pydantic import Json

from . import __version__
from .db_helpers import get_db_connection

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

# Load client secret data from the JSON file
with Path("client_secret.json").open() as secret_file:
    client_secret_data = json.load(secret_file)

# OAuth2 configuration
oauth2_settings = {
    "auth_uri": client_secret_data["web"]["auth_uri"],
    "tokenUrl": client_secret_data["web"]["token_uri"],
    "clientId": client_secret_data["web"]["client_id"],
    "clientSecret": client_secret_data["web"]["client_secret"],
    "redirectUri": client_secret_data["web"]["redirect_uris"][0],
}


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

    google_oauth_url = (
        f"{oauth2_settings['auth_uri']}?client_id={oauth2_settings['clientId']}"
        f"&redirect_uri={oauth2_settings['redirectUri']}&response_type=code"
        f"&scope={urllib.parse.quote_plus('email https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive.metadata.readonly')}"
        f"&access_type=offline&prompt=consent&state={user_id}"
    )
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

    token_request_data = {
        "code": code,
        "client_id": oauth2_settings["clientId"],
        "client_secret": oauth2_settings["clientSecret"],
        "redirect_uri": oauth2_settings["redirectUri"],
        "grant_type": "authorization_code",
    }

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


async def load_user_credentials(user_id: Union[int, str]) -> Any:
    async with get_db_connection() as db:
        try:
            data = await db.gauth.find_unique_or_raise(where={"user_id": user_id})  # type: ignore[typeddict-item]
        except RecordNotFoundError as e:
            raise HTTPException(
                status_code=404, detail="User hasn't grant access yet!"
            ) from e

    return data.creds


async def _build_service(user_id: int, service_name: str, version: str) -> Any:
    user_credentials = await load_user_credentials(user_id)
    sheets_credentials: Dict[str, str] = {
        "refresh_token": user_credentials["refresh_token"],
        "client_id": oauth2_settings["clientId"],
        "client_secret": oauth2_settings["clientSecret"],
    }

    creds = Credentials.from_authorized_user_info(  # type: ignore[no-untyped-call]
        info=sheets_credentials,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.metadata.readonly",
        ],
    )
    service = build(serviceName=service_name, version=version, credentials=creds)
    return service


@asyncify  # type: ignore[misc]
def _get_sheet(service: Any, spreadsheet_id: str, range: str) -> Any:
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range).execute()
    values = result.get("values", [])

    return values


@app.get("/get-sheet", description="Get data from a Google Sheet")
async def get_sheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    spreadsheet_id: Annotated[
        str, Query(description="ID of the Google Sheet to fetch data from")
    ],
    range: Annotated[
        str,
        Query(description="The range of cells to fetch data from. E.g. 'Sheet1!A1:B2'"),
    ],
) -> Union[str, List[List[str]]]:
    service = await _build_service(user_id=user_id, service_name="sheets", version="v4")
    values = await _get_sheet(
        service=service, spreadsheet_id=spreadsheet_id, range=range
    )

    if not values:
        return "No data found."

    return values  # type: ignore[no-any-return]


@asyncify  # type: ignore[misc]
def _create_sheet(service: Any, spreadsheet_id: str, title: str) -> None:
    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": title,
                    }
                }
            }
        ]
    }
    request = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    )
    request.execute()


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
    sheet_data: Annotated[
        Json[Any],
        Query(description="The data to update in the sheet. Must be a valid JSON data"),
    ],
) -> Response:
    service = await _build_service(user_id=user_id, service_name="sheets", version="v4")

    try:
        # Values are intended to be a 2d array.
        # They should be in the form of [[ 'a', 'b', 'c'], [ 1, 2, 3 ]]
        values = [list(sheet_data.keys()), list(sheet_data.values())]
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            valueInputOption="RAW",
            range=title,
            body={"majorDimension": "ROWS", "values": values},
        ).execute()
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
    service = await _build_service(user_id=user_id, service_name="sheets", version="v4")
    try:
        await _create_sheet(service=service, spreadsheet_id=spreadsheet_id, title=title)
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


@asyncify  # type: ignore[misc]
def _get_files(service: Any) -> List[Dict[str, str]]:
    # Call the Drive v3 API
    results = (
        service.files()
        .list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            pageSize=100,  # The default value is 100
            fields="nextPageToken, files(id, name)",
        )
        .execute()
    )
    items = results.get("files", [])
    return items  # type: ignore[no-any-return]


@app.get("/get-all-file-names", description="Get all sheets associated with the user")
async def get_all_file_names(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
) -> Dict[str, str]:
    service = await _build_service(user_id=user_id, service_name="drive", version="v3")
    files: List[Dict[str, str]] = await _get_files(service=service)
    # create dict where key is id and value is name
    files_dict = {file["id"]: file["name"] for file in files}
    return files_dict
