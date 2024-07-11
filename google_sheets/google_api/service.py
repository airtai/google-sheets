from typing import Any, Dict, List, Union

from asyncify import asyncify
from fastapi import HTTPException
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from prisma.errors import RecordNotFoundError

from ..db_helpers import get_db_connection
from ..model import GoogleSheetValues
from .oauth_settings import oauth2_settings

__all__ = [
    "build_service",
    "create_sheet_f",
    "get_all_sheet_titles_f",
    "get_files_f",
    "get_sheet_f",
    "update_sheet_f",
]


async def _load_user_credentials(user_id: Union[int, str]) -> Any:
    async with get_db_connection() as db:
        try:
            data = await db.gauth.find_unique_or_raise(where={"user_id": user_id})  # type: ignore[typeddict-item]
        except RecordNotFoundError as e:
            raise HTTPException(
                status_code=404, detail="User hasn't grant access yet!"
            ) from e

    return data.creds


async def build_service(user_id: int, service_name: str, version: str) -> Any:
    user_credentials = await _load_user_credentials(user_id)
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
def get_files_f(service: Any) -> List[Dict[str, str]]:
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


@asyncify  # type: ignore[misc]
def get_sheet_f(service: Any, spreadsheet_id: str, range: str) -> Any:
    # Call the Sheets API
    sheet = service.spreadsheets()
    try:
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range).execute()
        values = result.get("values", [])
    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Unable to read from spreadsheet with id '{spreadsheet_id}', and range '{range}'",
        ) from e

    return values


@asyncify  # type: ignore[misc]
def update_sheet_f(
    service: Any, spreadsheet_id: str, range: str, sheet_values: GoogleSheetValues
) -> None:
    # Values are intended to be a 2d array.
    # They should be in the form of [[ 'a', 'b', 'c'], [ 1, 2, 3 ]]
    request = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            valueInputOption="RAW",
            range=range,
            body={"majorDimension": "ROWS", "values": sheet_values.values},
        )
    )
    request.execute()


@asyncify  # type: ignore[misc]
def create_sheet_f(service: Any, spreadsheet_id: str, title: str) -> None:
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


@asyncify  # type: ignore[misc]
def get_all_sheet_titles_f(service: Any, spreadsheet_id: str) -> List[str]:
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get("sheets", "")
    return [sheet["properties"]["title"] for sheet in sheets]
