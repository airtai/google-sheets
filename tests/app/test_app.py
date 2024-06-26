from typing import Optional, Union
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from googleapiclient.errors import HttpError

from google_sheets import __version__ as version
from google_sheets.app import app

client = TestClient(app)


class TestGetSheet:
    def test_get_sheet(self) -> None:
        with patch(
            "google_sheets.google_api.service._load_user_credentials",
            return_value={"refresh_token": "abcdf"},
        ) as mock_load_user_credentials:
            excepted = [
                ["Campaign", "Ad Group", "Keyword"],
                ["Campaign A", "Ad group A", "Keyword A"],
                ["Campaign A", "Ad group A", "Keyword B"],
                ["Campaign A", "Ad group A", "Keyword C"],
            ]
            with patch(
                "google_sheets.app.get_sheet_f", return_value=excepted
            ) as mock_get_sheet:
                response = client.get(
                    "/get-sheet?user_id=123&spreadsheet_id=abc&title=Sheet1"
                )
                mock_load_user_credentials.assert_called_once()
                mock_get_sheet.assert_called_once()
                assert response.status_code == 200
                assert response.json() == excepted


def _create_http_error_mock(reason: str, status: int) -> HttpError:
    resp = MagicMock()
    resp.reason = reason
    resp.status = status

    return HttpError(resp=resp, content=b"")


class TestCreateSheet:
    @pytest.mark.parametrize(
        ("side_effect", "expected_status_code"),
        [
            (None, 201),
            (
                _create_http_error_mock(
                    'A sheet with the name "Sheet2" already exists', 400
                ),
                400,
            ),
            (_create_http_error_mock("Bad Request", 400), 400),
            (Exception("Some error"), 500),
        ],
    )
    def test_create_sheet(
        self,
        side_effect: Optional[Union[HttpError, Exception]],
        expected_status_code: int,
    ) -> None:
        with (
            patch(
                "google_sheets.google_api.service._load_user_credentials",
                return_value={"refresh_token": "abcdf"},
            ) as mock_load_user_credentials,
            patch(
                "google_sheets.app.create_sheet_f", side_effect=[side_effect]
            ) as mock_create_sheet,
        ):
            response = client.post(
                "/create-sheet?user_id=123&spreadsheet_id=abc&title=Sheet2"
            )
            mock_load_user_credentials.assert_called_once()
            mock_create_sheet.assert_called_once()
            assert response.status_code == expected_status_code


class TestGetAllSheetTitles:
    def test_get_all_sheet_titles(self) -> None:
        with (
            patch(
                "google_sheets.google_api.service._load_user_credentials",
                return_value={"refresh_token": "abcdf"},
            ) as mock_load_user_credentials,
            patch(
                "google_sheets.app.get_all_sheet_titles_f",
                return_value=["Sheet1", "Sheet2"],
            ) as mock_get_all_sheet_titles,
        ):
            expected = ["Sheet1", "Sheet2"]
            response = client.get(
                "/get-all-sheet-titles?user_id=123&spreadsheet_id=abc"
            )
            mock_load_user_credentials.assert_called_once()
            mock_get_all_sheet_titles.assert_called_once()
            assert response.status_code == 200
            assert response.json() == expected


class TestUpdateSheet:
    @pytest.mark.parametrize(
        ("side_effect", "expected_status_code"),
        [
            (None, 200),
            (_create_http_error_mock("Bad Request", 400), 400),
            (Exception("Some error"), 500),
        ],
    )
    def test_update_sheet(
        self,
        side_effect: Optional[Union[HttpError, Exception]],
        expected_status_code: int,
    ) -> None:
        with (
            patch(
                "google_sheets.google_api.service._load_user_credentials",
                return_value={"refresh_token": "abcdf"},
            ) as mock_load_user_credentials,
            patch(
                "google_sheets.app.update_sheet_f", side_effect=[side_effect]
            ) as mock_update_sheet,
        ):
            json_data = {
                "values": [["Campaign", "Ad Group"], ["Campaign A", "Ad group A"]]
            }
            response = client.post(
                "/update-sheet?user_id=123&spreadsheet_id=abc&title=Sheet1",
                json=json_data,
            )
            mock_load_user_credentials.assert_called_once()
            mock_update_sheet.assert_called_once()
            assert response.status_code == expected_status_code


class TestGetAllFileNames:
    def test_get_all_file_names(self) -> None:
        with (
            patch(
                "google_sheets.google_api.service._load_user_credentials",
                return_value={"refresh_token": "abcdf"},
            ) as mock_load_user_credentials,
            patch(
                "google_sheets.app.get_files_f",
                return_value=[
                    {"id": "abc", "name": "file1"},
                    {"id": "def", "name": "file2"},
                ],
            ) as mock_get_files,
        ):
            expected = {"abc": "file1", "def": "file2"}
            response = client.get("/get-all-file-names?user_id=123")
            mock_load_user_credentials.assert_called_once()
            mock_get_files.assert_called_once()
            assert response.status_code == 200
            assert response.json() == expected


class TestOpenAPIJSON:
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
                "/login": {
                    "get": {
                        "summary": "Get Login Url",
                        "operationId": "get_login_url_login_get",
                        "parameters": [
                            {
                                "name": "user_id",
                                "in": "query",
                                "required": True,
                                "schema": {"type": "integer", "title": "User ID"},
                            },
                            {
                                "name": "force_new_login",
                                "in": "query",
                                "required": False,
                                "schema": {
                                    "type": "boolean",
                                    "title": "Force new login",
                                    "default": False,
                                },
                            },
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "additionalProperties": {"type": "string"},
                                            "title": "Response Get Login Url Login Get",
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
                },
                "/login/success": {
                    "get": {
                        "summary": "Get Login Success",
                        "operationId": "get_login_success_login_success_get",
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "additionalProperties": {"type": "string"},
                                            "type": "object",
                                            "title": "Response Get Login Success Login Success Get",
                                        }
                                    }
                                },
                            }
                        },
                    }
                },
                "/login/callback": {
                    "get": {
                        "summary": "Login Callback",
                        "operationId": "login_callback_login_callback_get",
                        "parameters": [
                            {
                                "name": "code",
                                "in": "query",
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "title": "Authorization Code",
                                },
                            },
                            {
                                "name": "state",
                                "in": "query",
                                "required": True,
                                "schema": {"type": "string", "title": "State"},
                            },
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {"application/json": {"schema": {}}},
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
                },
                "/get-sheet": {
                    "get": {
                        "summary": "Get Sheet",
                        "description": "Get data from a Google Sheet",
                        "operationId": "get_sheet_get_sheet_get",
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
                                "name": "title",
                                "in": "query",
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "description": "The title of the sheet to fetch data from",
                                    "title": "Title",
                                },
                                "description": "The title of the sheet to fetch data from",
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
                                            "title": "Response Get Sheet Get Sheet Get",
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
                },
                "/update-sheet": {
                    "post": {
                        "summary": "Update Sheet",
                        "description": "Update data in a Google Sheet within the existing spreadsheet",
                        "operationId": "update_sheet_update_sheet_post",
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
                                "name": "title",
                                "in": "query",
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "description": "The title of the sheet to update",
                                    "title": "Title",
                                },
                                "description": "The title of the sheet to update",
                            },
                        ],
                        "requestBody": {
                            "required": True,
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/GoogleSheetValues"
                                    }
                                }
                            },
                        },
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {"application/json": {"schema": {}}},
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
                },
                "/create-sheet": {
                    "post": {
                        "summary": "Create Sheet",
                        "description": "Create a new Google Sheet within the existing spreadsheet",
                        "operationId": "create_sheet_create_sheet_post",
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
                                "name": "title",
                                "in": "query",
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "description": "The title of the new sheet",
                                    "title": "Title",
                                },
                                "description": "The title of the new sheet",
                            },
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {"application/json": {"schema": {}}},
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
                },
                "/get-all-file-names": {
                    "get": {
                        "summary": "Get All File Names",
                        "description": "Get all sheets associated with the user",
                        "operationId": "get_all_file_names_get_all_file_names_get",
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
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "additionalProperties": {"type": "string"},
                                            "title": "Response Get All File Names Get All File Names Get",
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
                },
                "/get-all-sheet-titles": {
                    "get": {
                        "summary": "Get All Sheet Titles",
                        "description": "Get all sheet titles within a Google Spreadsheet",
                        "operationId": "get_all_sheet_titles_get_all_sheet_titles_get",
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
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "title": "Response Get All Sheet Titles Get All Sheet Titles Get",
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
                },
            },
            "components": {
                "schemas": {
                    "GoogleSheetValues": {
                        "properties": {
                            "values": {
                                "items": {"items": {}, "type": "array"},
                                "type": "array",
                                "title": "Values",
                                "description": "Values to be written to the Google Sheet.",
                            }
                        },
                        "type": "object",
                        "required": ["values"],
                        "title": "GoogleSheetValues",
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
