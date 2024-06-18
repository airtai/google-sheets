import datetime
from unittest.mock import patch

from fastapi.testclient import TestClient

from google_sheets import __version__ as version
from google_sheets.app import app

client = TestClient(app)


class TestRoutes:
    def test_get_sheet(self) -> None:
        with patch(
            "google_sheets.app.load_user_credentials",
            return_value={"refresh_token": "abcdf"},
        ) as mock_load_user_credentials:
            excepted = [
                ["Campaign", "Ad Group", "Keyword"],
                ["Campaign A", "Ad group A", "Keyword A"],
                ["Campaign A", "Ad group A", "Keyword B"],
                ["Campaign A", "Ad group A", "Keyword C"],
            ]
            with patch(
                "google_sheets.app._get_sheet", return_value=excepted
            ) as mock_get_sheet:
                response = client.get(
                    "/sheet?user_id=123&spreadsheet_id=abc&range=Sheet1"
                )
                mock_load_user_credentials.assert_called_once()
                mock_get_sheet.assert_called_once()
                assert response.status_code == 200
                assert response.json() == excepted

    def test_weather_route(self) -> None:
        response = client.get("/?city=Chennai")
        assert response.status_code == 200
        resp_json = response.json()
        assert resp_json.get("city") == "Chennai"
        assert resp_json.get("temperature") > 0

        assert len(resp_json.get("daily_forecasts")) > 0
        daily_forecasts = resp_json.get("daily_forecasts")
        assert isinstance(daily_forecasts, list)

        first_daily_forecast = daily_forecasts[0]
        assert (
            first_daily_forecast.get("forecast_date")
            == datetime.date.today().isoformat()
        )
        assert first_daily_forecast.get("temperature") > 0
        assert len(first_daily_forecast.get("hourly_forecasts")) > 0

        first_hourly_forecast = first_daily_forecast.get("hourly_forecasts")[0]
        assert isinstance(first_hourly_forecast, dict)
        assert first_hourly_forecast.get("forecast_time") is not None
        assert first_hourly_forecast.get("temperature") > 0  # type: ignore
        assert first_hourly_forecast.get("description") is not None

    def test_openapi(self) -> None:
        expected = {
            "openapi": "3.1.0",
            "info": {"title": "google-sheets", "version": version},
            "servers": [
                {"url": "http://localhost:8000", "description": "Weather app server"}
            ],
            "paths": {
                "/": {
                    "get": {
                        "summary": "Get Weather",
                        "operationId": "get_weather__get",
                        "description": "Get weather forecast for a given city",
                        "parameters": [
                            {
                                "name": "city",
                                "in": "query",
                                "description": "city for which forecast is requested",
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "title": "City",
                                    "description": "city for which forecast is requested",
                                },
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": "#/components/schemas/Weather"
                                        }
                                    }
                                },
                            },
                            "422": {
                                "description": "Validation Error",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": "#/components/schemas/HTTPValidationError"
                                        }
                                    }
                                },
                            },
                        },
                    }
                }
            },
            "components": {
                "schemas": {
                    "DailyForecast": {
                        "properties": {
                            "forecast_date": {
                                "type": "string",
                                "format": "date",
                                "title": "Forecast Date",
                            },
                            "temperature": {"type": "integer", "title": "Temperature"},
                            "hourly_forecasts": {
                                "items": {
                                    "$ref": "#/components/schemas/HourlyForecast"
                                },
                                "type": "array",
                                "title": "Hourly Forecasts",
                            },
                        },
                        "type": "object",
                        "required": [
                            "forecast_date",
                            "temperature",
                            "hourly_forecasts",
                        ],
                        "title": "DailyForecast",
                    },
                    "HTTPValidationError": {
                        "properties": {
                            "detail": {
                                "items": {
                                    "$ref": "#/components/schemas/ValidationError"
                                },
                                "type": "array",
                                "title": "Detail",
                            }
                        },
                        "type": "object",
                        "title": "HTTPValidationError",
                    },
                    "HourlyForecast": {
                        "properties": {
                            "forecast_time": {
                                "type": "string",
                                "format": "time",
                                "title": "Forecast Time",
                            },
                            "temperature": {"type": "integer", "title": "Temperature"},
                            "description": {"type": "string", "title": "Description"},
                        },
                        "type": "object",
                        "required": ["forecast_time", "temperature", "description"],
                        "title": "HourlyForecast",
                    },
                    "ValidationError": {
                        "properties": {
                            "loc": {
                                "items": {
                                    "anyOf": [{"type": "string"}, {"type": "integer"}]
                                },
                                "type": "array",
                                "title": "Location",
                            },
                            "msg": {"type": "string", "title": "Message"},
                            "type": {"type": "string", "title": "Error Type"},
                        },
                        "type": "object",
                        "required": ["loc", "msg", "type"],
                        "title": "ValidationError",
                    },
                    "Weather": {
                        "properties": {
                            "city": {"type": "string", "title": "City"},
                            "temperature": {"type": "integer", "title": "Temperature"},
                            "daily_forecasts": {
                                "items": {"$ref": "#/components/schemas/DailyForecast"},
                                "type": "array",
                                "title": "Daily Forecasts",
                            },
                        },
                        "type": "object",
                        "required": ["city", "temperature", "daily_forecasts"],
                        "title": "Weather",
                    },
                }
            },
        }
        response = client.get("/openapi.json")
        assert response.status_code == 200
        resp_json = response.json()

        assert resp_json == expected
