
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ProductionEvent(BaseModel):
    """A single semiconductor production measurement."""

    event_id: str = Field(
        ...,
        description="Unique identifier for this production measurement",
    )
    event_timestamp: datetime = Field(
        ...,
        description="Original timestamp from the SECOM dataset",
    )
    unit_id: int = Field(
        ...,
        ge=0,
        description="Sequential production-unit identifier",
    )

    sensor_060: float | None = None
    sensor_104: float | None = None
    sensor_130: float | None = None
    sensor_029: float | None = None
    sensor_511: float | None = None
    sensor_317: float | None = None
    sensor_125: float | None = None
    sensor_126: float | None = None

    quality_label: Literal[-1, 1] = Field(
        ...,
        description="-1 means pass; 1 means fail",
    )

    class Config:
        extra = "forbid"
