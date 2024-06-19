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

    def test_openapi(self) -> None:
        expected = {
            "openapi": "3.1.0",
            "info": {"title": "google-sheets", "version": version},
            "servers": [
                {
                    "url": "http://localhost:8000",
                    "description": "Google Sheets app server",
                }
            ],
            "paths": {
                "/sheet": {
                    "get": {
                        "summary": "Get Sheet",
                        "description": "Get data from a Google Sheet",
                        "operationId": "get_sheet_sheet_get",
                        "parameters": [
                            {
                                "name": "user_id",
                                "in": "query",
                                "required": True,
                                "schema": {
                                    "type": "integer",
                                    "description": "The user ID for which the data is requested",
                                    "title": "User Id",
                                },
                                "description": "The user ID for which the data is requested",
                            },
                            {
                                "name": "spreadsheet_id",
                                "in": "query",
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "description": "ID of the Google Sheet to fetch data from",
                                    "title": "Spreadsheet Id",
                                },
                                "description": "ID of the Google Sheet to fetch data from",
                            },
                            {
                                "name": "range",
                                "in": "query",
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "description": "The range of cells to fetch data from. E.g. 'Sheet1!A1:B2'",
                                    "title": "Range",
                                },
                                "description": "The range of cells to fetch data from. E.g. 'Sheet1!A1:B2'",
                            },
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "anyOf": [
                                                {"type": "string"},
                                                {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "array",
                                                        "items": {"type": "string"},
                                                    },
                                                },
                                            ],
                                            "title": "Response Get Sheet Sheet Get",
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
                }
            },
        }
        response = client.get("/openapi.json")
        assert response.status_code == 200
        resp_json = response.json()

        assert resp_json == expected
