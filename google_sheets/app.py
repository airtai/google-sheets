import datetime
import json
import logging
from os import environ
from pathlib import Path
from typing import Annotated, Any, List, Union

import python_weather
from fastapi import FastAPI, HTTPException, Query
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from pydantic import BaseModel

from . import __version__
from .db_helpers import get_db_connection, get_wasp_db_url

__all__ = ["app"]

logging.basicConfig(level=logging.INFO)

host = environ.get("DOMAIN", "localhost")
port = 8000
protocol = "http" if host == "localhost" else "https"
base_url = (
    f"{protocol}://{host}:{port}" if host == "localhost" else f"{protocol}://{host}"
)

app = FastAPI(
    servers=[{"url": base_url, "description": "Weather app server"}],
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


class HourlyForecast(BaseModel):
    forecast_time: datetime.time
    temperature: int
    description: str


class DailyForecast(BaseModel):
    forecast_date: datetime.date
    temperature: int
    hourly_forecasts: List[HourlyForecast]


class Weather(BaseModel):
    city: str
    temperature: int
    daily_forecasts: List[DailyForecast]


@app.get("/", description="Get weather forecast for a given city")
async def get_weather(
    city: Annotated[str, Query(description="city for which forecast is requested")],
) -> Weather:
    async with python_weather.Client(unit=python_weather.METRIC) as client:
        # fetch a weather forecast from a city
        weather = await client.get(city)

        daily_forecasts = []
        # get the weather forecast for a few days
        for daily in weather.daily_forecasts:
            hourly_forecasts = [
                HourlyForecast(
                    forecast_time=hourly.time,
                    temperature=hourly.temperature,
                    description=hourly.description,
                )
                for hourly in daily.hourly_forecasts
            ]
            daily_forecasts.append(
                DailyForecast(
                    forecast_date=daily.date,
                    temperature=daily.temperature,
                    hourly_forecasts=hourly_forecasts,
                )
            )

        weather_response = Weather(
            city=city,
            temperature=weather.temperature,
            daily_forecasts=daily_forecasts,
            hourly_forecasts=hourly_forecasts,
        )
    return weather_response


async def get_user(user_id: Union[int, str]) -> Any:
    wasp_db_url = await get_wasp_db_url()
    async with get_db_connection(db_url=wasp_db_url) as db:
        user = await db.query_first(
            f'SELECT * from "User" where id={user_id}'  # nosec: [B608]
        )
    if not user:
        raise HTTPException(status_code=404, detail=f"user_id {user_id} not found")
    return user


async def load_user_credentials(user_id: Union[int, str]) -> Any:
    await get_user(user_id=user_id)
    async with get_db_connection() as db:
        data = await db.gauth.find_unique_or_raise(where={"user_id": user_id})

    return data.creds


def _get_sheet(user_credentials: Any, spreadshit_id: str, range: str) -> Any:
    sheets_credentials = {
        "refresh_token": user_credentials["refresh_token"],
        "client_id": oauth2_settings["clientId"],
        "client_secret": oauth2_settings["clientSecret"],
    }

    creds = Credentials.from_authorized_user_info(
        info=sheets_credentials, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadshit_id, range=range).execute()
    values = result.get("values", [])

    return values


@app.get("/sheet", description="Get data from a Google Sheet")
async def get_sheet(
    user_id: Annotated[
        int, Query(description="The user ID for which the data is requested")
    ],
    spreadshit_id: Annotated[
        str, Query(description="ID of the Google Sheet to fetch data from")
    ],
    range: Annotated[
        str,
        Query(description="The range of cells to fetch data from. E.g. 'Sheet1!A1:B2'"),
    ],
) -> Union[str, List[List[str]]]:
    user_credentials = await load_user_credentials(user_id)
    values = _get_sheet(user_credentials, spreadshit_id, range)

    if not values:
        return "No data found."

    return values  # type: ignore[no-any-return]
