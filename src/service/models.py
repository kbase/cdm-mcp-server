"""
Pydantic models for the Spark Manager API.
"""

from typing import Annotated

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    """Standard error response model."""

    error: Annotated[int | None, Field(description="Error code")] = None
    error_type: Annotated[str | None, Field(description="Error type")] = None
    message: Annotated[str | None, Field(description="Error message")] = None


class HealthResponse(BaseModel):
    """Health check response model."""

    status: Annotated[str, Field(description="Health status")]
