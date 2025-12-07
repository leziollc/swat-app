"""
Data models for table operations.

This module defines Pydantic models for table queries and responses.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

# Supported SQL data types for Unity Catalog
SQLDataType = Literal[
    "BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT",
    "DOUBLE", "FLOAT", "DECIMAL",
    "STRING", "VARCHAR", "CHAR",
    "BOOLEAN",
    "DATE", "TIMESTAMP",
    "BINARY",
    "ARRAY", "MAP", "STRUCT"
]


class ColumnDefinition(BaseModel):
    """Definition of a table column with name and data type."""

    name: str = Field(..., description="Column name")
    data_type: SQLDataType = Field(..., description="SQL data type")
    nullable: bool = Field(True, description="Whether the column can be NULL")

    @field_validator("name")
    @classmethod
    def validate_column_name(cls, v):
        """Validate column name is a valid identifier."""
        if not v or not isinstance(v, str):
            raise ValueError("Column name must be a non-empty string")
        if not v[0].isalpha() and v[0] != "_":
            raise ValueError(f"Column name '{v}' must start with a letter or underscore")
        if not all(c.isalnum() or c == "_" for c in v):
            raise ValueError(f"Column name '{v}' contains invalid characters")
        return v

    @field_validator("data_type")
    @classmethod
    def validate_data_type(cls, v):
        """Normalize data type to uppercase."""
        return v.upper()


class TableQueryParams(BaseModel):
    """Query parameters for table data retrieval."""

    catalog: str = Field(..., description="The catalog name")
    schema_name: str = Field(..., description="The schema name", alias="schema")
    table: str = Field(..., description="The table name")
    limit: int = Field(100, description="Maximum number of records to return")
    offset: int = Field(0, description="Number of records to skip")
    columns: str = Field("*", description="Comma-separated list of columns to retrieve")
    filters: list[dict[str, Any]] | None = Field(
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

    data: list[dict] = Field(..., description="The table records")
    count: int = Field(..., description="The number of records returned")
    total: int | None = Field(
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
    data: list[dict] = Field(..., description="The records to insert")
    auto_create: bool = Field(
        False,
        description="If true, automatically create catalog, schema, and table if they don't exist"
    )
    schema_definition: list[ColumnDefinition] | None = Field(
        None,
        description="Table schema definition (required if auto_create=true and table doesn't exist)"
    )

    @field_validator("schema_definition")
    @classmethod
    def validate_schema_with_auto_create(cls, v, info):
        """Validate that schema_definition is provided when auto_create is true."""
        # Note: info.data contains other fields that have been validated
        if info.data.get("auto_create") and v is None:
            raise ValueError("schema_definition is required when auto_create=true")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "catalog": "my_catalog",
                "schema": "my_schema",
                "table": "my_table",
                "auto_create": False,
                "data": [
                    {"id": 1, "name": "Example"},
                    {"id": 2, "name": "Another Example"},
                ],
            }
        },
        "populate_by_name": True
    }

class TableUpdateRequest(BaseModel):
    """Request model for updating data in a table."""

    catalog: str | None = None
    schema_name: str | None = None
    table: str
    key_column: str
    key_value: Any
    updates: dict[str, Any]

class TableDeleteRequest(BaseModel):
    """Request model for deleting data from a table."""

    catalog: str | None = None
    schema_name: str | None = None
    table: str
    key_column: str
    key_value: Any
    soft: bool | None = True
