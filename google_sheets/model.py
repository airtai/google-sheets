from typing import Any, List

from pydantic import BaseModel, Field


class GoogleSheetValues(BaseModel):
    values: List[List[Any]] = Field(
        ..., title="Values", description="Values to be written to the Google Sheet."
    )
