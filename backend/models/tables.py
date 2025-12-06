"""
Data models for table operations.

This module defines Pydantic models for table queries and responses.
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, field_validator


class TableQueryParams(BaseModel):
    """Query parameters for table data retrieval."""

    catalog: str = Field(..., description="The catalog name")
    schema_name: str = Field(..., description="The schema name", alias="schema")
    table: str = Field(..., description="The table name")
    limit: int = Field(100, description="Maximum number of records to return")
    offset: int = Field(0, description="Number of records to skip")
    columns: str = Field("*", description="Comma-separated list of columns to retrieve")
    filters: Optional[List[Dict[str, Any]]] = Field(
        None, description="Structured filters: [{column, op, value}]"
    )

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v):
        """Validate that limit is a positive integer and not too large."""
        if v <= 0:
            raise ValueError("Limit must be greater than 0")
        if v > 1000:
            raise ValueError("Limit cannot exceed 1000")
        return v

    @field_validator("offset")
    @classmethod
    def validate_offset(cls, v):
        """Validate that offset is a non-negative integer."""
        if v < 0:
            raise ValueError("Offset must be non-negative")
        return v


class TableResponse(BaseModel):
    """Response model for table data."""

    data: List[Dict] = Field(..., description="The table records")
    count: int = Field(..., description="The number of records returned")
    total: Optional[int] = Field(
        None, description="The total number of records (if available)"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "data": [
                    {"id": 1, "name": "Example"},
                    {"id": 2, "name": "Another Example"},
                ],
                "count": 2,
                "total": 100,
            }
        }
    }


class TableInsertRequest(BaseModel):
    """Request model for inserting data into a table."""

    catalog: str = Field(..., description="The catalog name")
    schema_name: str = Field(..., description="The schema name", alias="schema")
    table: str = Field(..., description="The table name")
    data: List[Dict] = Field(..., description="The records to insert")

    model_config = {
        "json_schema_extra": {
            "example": {
                "catalog": "my_catalog",
                "schema": "my_schema",
                "table": "my_table",
                "data": [
                    {"id": 1, "name": "Example"},
                    {"id": 2, "name": "Another Example"},
                ],
            }
        }
    }
    model_config.setdefault("populate_by_name", True)

class TableUpdateRequest(BaseModel):
    """Request model for updating data in a table."""
    
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    table: str
    key_column: str
    key_value: Any
    updates: Dict[str, Any]

class TableDeleteRequest(BaseModel):
    """Request model for deleting data from a table."""

    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    table: str
    key_column: str
    key_value: Any
    soft: Optional[bool] = True
