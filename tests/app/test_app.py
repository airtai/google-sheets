from typing import Optional, Union
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from googleapiclient.errors import HttpError

from google_sheets.app import app
from google_sheets.model import GoogleSheetValues

client = TestClient(app)


class TestGetSheet:
    def test_get_sheet(self) -> None:
        with patch(
            "google_sheets.google_api.service._load_user_credentials",
            return_value={"refresh_token": "abcdf"},
        ) as mock_load_user_credentials:
            values = [
                ["Campaign", "Ad Group", "Keyword"],
                ["Campaign A", "Ad group A", "Keyword A"],
                ["Campaign A", "Ad group A", "Keyword B"],
                ["Campaign A", "Ad group A", "Keyword C"],
            ]
            with patch(
                "google_sheets.app.get_sheet_f", return_value=values
            ) as mock_get_sheet:
                response = client.get(
                    "/get-sheet?user_id=123&spreadsheet_id=abc&title=Sheet1"
                )
                mock_load_user_credentials.assert_called_once()
                mock_get_sheet.assert_called_once()
                assert response.status_code == 200

                excepted = GoogleSheetValues(values=values).model_dump()
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
                "sheet_values": {
                    "values": [["Campaign", "Ad Group"], ["Campaign A", "Ad group A"]]
                }
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


class TestProcessData:
    @pytest.mark.parametrize(
        ("template_sheet_values", "new_campaign_sheet_values", "status_code", "detail"),
        [
            (
                GoogleSheetValues(
                    values=[
                        ["Campaign", "Ad Group", "Keyword"],
                    ]
                ),
                GoogleSheetValues(
                    values=[
                        ["Country", "Station From", "Station To"],
                        ["India", "Delhi", "Mumbai"],
                    ]
                ),
                400,
                "Both template and new campaign data should have at least two rows",
            ),
            (
                GoogleSheetValues(
                    values=[
                        ["Campaign", "Ad Group", "Keyword"],
                        ["Campaign A", "Ad group A", "Keyword A"],
                    ]
                ),
                GoogleSheetValues(
                    values=[
                        ["Country", "Station From", "Station To"],
                        ["India", "Delhi", "Mumbai"],
                    ]
                ),
                400,
                "Mandatory columns missing in the keyword template data.",
            ),
            (
                GoogleSheetValues(
                    values=[
                        [
                            "Campaign",
                            "Ad Group",
                            "Keyword",
                            "Criterion Type",
                            "Max CPC",
                        ],
                        ["Campaign A", "Ad group A", "Keyword A", "Exact", "1"],
                    ]
                ),
                GoogleSheetValues(
                    values=[
                        ["Country", "Station From", "Station To"],
                        ["India", "Delhi", "Mumbai"],
                    ]
                ),
                200,
                GoogleSheetValues(
                    values=[
                        [
                            "Campaign",
                            "Ad Group",
                            "Keyword",
                            "Criterion Type",
                            "Max CPC",
                        ],
                        [
                            "India - Delhi - Mumbai",
                            "Delhi - Mumbai",
                            "Keyword A",
                            "Exact",
                            "1",
                        ],
                        [
                            "India - Delhi - Mumbai",
                            "Mumbai - Delhi",
                            "Keyword A",
                            "Exact",
                            "1",
                        ],
                    ],
                ),
            ),
        ],
    )
    def test_process_data(
        self,
        template_sheet_values: GoogleSheetValues,
        new_campaign_sheet_values: GoogleSheetValues,
        status_code: int,
        detail: Union[str, GoogleSheetValues],
    ) -> None:
        response = client.post(
            "/process-data?target_resource=keyword",
            json={
                "template_sheet_values": template_sheet_values.model_dump(),
                "new_campaign_sheet_values": new_campaign_sheet_values.model_dump(),
            },
        )

        assert response.status_code == status_code
        if isinstance(detail, GoogleSheetValues):
            assert response.json() == detail.model_dump()
        else:
            assert detail in response.json()["detail"]


class TestOpenAPIJSON:
    def test_openapi(self) -> None:
        response = client.get("/openapi.json")
        assert response.status_code == 200

        paths = response.json()["paths"]
        expected_path_keys = [
            "/login",
            "/login/success",
            "/login/callback",
            "/get-sheet",
            "/update-sheet",
            "/create-sheet",
            "/get-all-file-names",
            "/get-all-sheet-titles",
            "/process-data",
            "/process-spreadsheet",
        ]

        for key in expected_path_keys:
            assert key in paths
