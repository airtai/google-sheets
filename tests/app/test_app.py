from typing import Any, Dict, Optional, Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from googleapiclient.errors import HttpError

from google_sheets.app import _check_parameters_are_not_none, app, process_data
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
        ("template_sheet_values", "new_campaign_sheet_values", "detail"),
        [
            (
                GoogleSheetValues(
                    values=[
                        ["Keyword"],
                    ]
                ),
                GoogleSheetValues(
                    values=[
                        [
                            "Country",
                            "Station From",
                            "Station To",
                            "Final Url From",
                            "Final Url To",
                        ],
                        [
                            "India",
                            "Delhi",
                            "Mumbai",
                            "https://www.example.com/from",
                            "https://www.example.com/to",
                        ],
                    ]
                ),
                "Both template and new campaign data should have at least two rows",
            ),
            (
                GoogleSheetValues(
                    values=[
                        ["Fake column"],
                        ["fake"],
                    ]
                ),
                GoogleSheetValues(
                    values=[
                        [
                            "Country",
                            "Station From",
                            "Station To",
                            "Final Url From",
                            "Final Url To",
                        ],
                        [
                            "India",
                            "Delhi",
                            "Mumbai",
                            "https://www.example.com/from",
                            "https://www.example.com/to",
                        ],
                    ]
                ),
                "Mandatory columns missing in the keyword template data.",
            ),
            (
                GoogleSheetValues(
                    values=[
                        [
                            "Keyword",
                        ],
                        ["Keyword A"],
                    ]
                ),
                GoogleSheetValues(
                    values=[
                        [
                            "Country",
                            "Station From",
                            "Station To",
                            "Final Url From",
                            "Final Url To",
                        ],
                        [
                            "India",
                            "Delhi",
                            "Mumbai",
                            "https://www.example.com/from",
                            "https://www.example.com/to",
                        ],
                    ]
                ),
                GoogleSheetValues(
                    values=[
                        [
                            "Campaign Name",
                            "Ad Group Name",
                            "Match Type",
                            "Keyword",
                        ],
                        [
                            "India - Delhi - Mumbai",
                            "Delhi - Mumbai",
                            "Exact",
                            "Keyword A",
                        ],
                        [
                            "India - Delhi - Mumbai",
                            "Mumbai - Delhi",
                            "Exact",
                            "Keyword A",
                        ],
                    ],
                ),
            ),
        ],
    )
    @pytest.mark.asyncio()
    async def test_process_data(
        self,
        template_sheet_values: GoogleSheetValues,
        new_campaign_sheet_values: GoogleSheetValues,
        detail: Union[str, GoogleSheetValues],
    ) -> None:
        merged_campaigns_ad_groups_df = pd.DataFrame(
            {
                "Campaign Name": [
                    "INSERT_COUNTRY - INSERT_STATION_FROM - INSERT_STATION_TO"
                ],
                "Ad Group Name": ["INSERT_STATION_FROM - INSERT_STATION_TO"],
                "Match Type": ["Exact"],
            }
        )
        if isinstance(detail, GoogleSheetValues):
            processed_data = await process_data(
                template_sheet_values=template_sheet_values,
                new_campaign_sheet_values=new_campaign_sheet_values,
                merged_campaigns_ad_groups_df=merged_campaigns_ad_groups_df,
                target_resource="keyword",
            )
            assert processed_data.model_dump() == detail.model_dump()

        else:
            with pytest.raises(HTTPException) as exc:
                await process_data(
                    template_sheet_values=template_sheet_values,
                    new_campaign_sheet_values=new_campaign_sheet_values,
                    merged_campaigns_ad_groups_df=merged_campaigns_ad_groups_df,
                    target_resource="keyword",
                )
            assert detail in exc.value.detail


class TestOpenAPIJSON:
    def test_openapi(self) -> None:
        response = client.get("/openapi.json")
        assert response.status_code == 200

        paths = response.json()["paths"]
        expected_path_keys = [
            "/login",
            "/login/callback",
            "/get-sheet",
            "/update-sheet",
            "/create-sheet",
            "/get-all-file-names",
            "/get-all-sheet-titles",
            "/process-spreadsheet",
        ]

        for key in expected_path_keys:
            assert key in paths


class TestHelperFunctions:
    @pytest.mark.parametrize(
        ("endpoint_params", "raises_exception"),
        [
            ({"user_id": "123", "spreadsheet_id": "abc", "title": "Sheet1"}, False),
            ({"user_id": "123", "spreadsheet_id": "abc", "title": None}, True),
        ],
    )
    def test_check_parameters_are_not_none(
        self, endpoint_params: Dict[str, Any], raises_exception: bool
    ) -> None:
        if raises_exception:
            with pytest.raises(HTTPException):
                _check_parameters_are_not_none(endpoint_params)
        else:
            _check_parameters_are_not_none(endpoint_params)
