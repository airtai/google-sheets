from typing import Any, List

from pydantic import BaseModel, Field


class GoogleSheetValues(BaseModel):
    values: List[List[Any]] = Field(
        ..., title="Values", description="Values to be written to the Google Sheet."
    )
    issues_present: bool = Field(
        default=False,
        title="Issues Present",
        description="Whether any issues are present.",
    )
