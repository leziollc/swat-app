"""
Data models for table operations.

This module defines Pydantic models for table queries and responses.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

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
                    {"api10": 4201234567, "api14": 42012345671234, "wellname": "SMITH 1-15H", "spuddate": "01/15/2024", "firstproddate": "03/20/2024", "compldate": "03/10/2024", "oileur": 125000.50, "gaseur": 450000.75, "wellspacing": 640.0},
                    {"api10": 4201234568, "api14": 42012345681234, "wellname": "JONES 2-22H", "spuddate": "02/01/2024", "firstproddate": "04/15/2024", "compldate": "04/05/2024", "oileur": 98500.25, "gaseur": 380000.00, "wellspacing": 640.0}
                ],
                "count": 2,
                "total": 100
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
        if info.data.get("auto_create") and v is None:
            raise ValueError("schema_definition is required when auto_create=true")
        return v

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "auto_create": False,
                    "data": [
                        {"api10": 4201234567, "api14": 42012345671234, "wellname": "SMITH 1-15H", "spuddate": "01/15/2024", "firstproddate": "03/20/2024", "compldate": "03/10/2024", "oileur": 125000.50, "gaseur": 450000.75, "wellspacing": 640.0},
                        {"api10": 4201234568, "api14": 42012345681234, "wellname": "JONES 2-22H", "spuddate": "02/01/2024", "firstproddate": "04/15/2024", "compldate": "04/05/2024", "oileur": 98500.25, "gaseur": 380000.00, "wellspacing": 640.0}
                    ]
                },
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "auto_create": True,
                    "schema_definition": [
                        {"name": "api10", "data_type": "BIGINT", "nullable": False},
                        {"name": "api14", "data_type": "BIGINT", "nullable": False},
                        {"name": "wellname", "data_type": "STRING", "nullable": False},
                        {"name": "spuddate", "data_type": "STRING", "nullable": True},
                        {"name": "firstproddate", "data_type": "STRING", "nullable": True},
                        {"name": "compldate", "data_type": "STRING", "nullable": True},
                        {"name": "oileur", "data_type": "DOUBLE", "nullable": True},
                        {"name": "gaseur", "data_type": "DOUBLE", "nullable": True},
                        {"name": "wellspacing", "data_type": "DOUBLE", "nullable": True}
                    ],
                    "data": [
                        {"api10": 4201234567, "api14": 42012345671234, "wellname": "SMITH 1-15H", "spuddate": "01/15/2024", "firstproddate": "03/20/2024", "compldate": "03/10/2024", "oileur": 125000.50, "gaseur": 450000.75, "wellspacing": 640.0},
                        {"api10": 4201234568, "api14": 42012345681234, "wellname": "JONES 2-22H", "spuddate": "02/01/2024", "firstproddate": "04/15/2024", "compldate": "04/05/2024", "oileur": 98500.25, "gaseur": 380000.00, "wellspacing": 640.0}
                    ]
                }
            ]
        },
        "populate_by_name": True
    }

class BulkUpdateItem(BaseModel):
    """Single update item for bulk updates with different values per record."""

    key_value: Any = Field(..., description="The value of the key column for this record")
    updates: dict[str, Any] = Field(..., description="Column updates for this specific record")


class TableUpdateRequest(BaseModel):
    """Request model for updating data in a table.

    Supports 4 update scenarios:
    1. Single record: key_value + updates
    2. Multiple records (same updates): key_values + updates
    3. Multiple records (different updates): key_column + bulk_updates
    3. Filter-based: filters + soft
    """

    catalog: str | None = None
    schema_name: str | None = Field(None, alias="schema")
    table: str
    key_column: str | None = None
    key_value: Any | None = None
    key_values: list[Any] | None = None
    bulk_updates: list[BulkUpdateItem] | None = None
    filters: list[dict[str, Any]] | None = None
    updates: dict[str, Any] | None = None

    @field_validator("key_column")
    @classmethod
    def validate_key_column_when_needed(cls, v, info):
        """Validate key_column is provided when using key-based updates."""
        data = info.data
        if (data.get("key_value") is not None or data.get("key_values") or data.get("bulk_updates")) and not v:
            raise ValueError("key_column is required when using key_value, key_values, or bulk_updates")
        return v

    @field_validator("updates")
    @classmethod
    def validate_updates_structure(cls, v, info):
        """Validate updates is provided for non-bulk scenarios."""
        data = info.data
        if not v and not data.get("bulk_updates"):
            raise ValueError("updates is required unless using bulk_updates")
        if v and data.get("bulk_updates"):
            raise ValueError("Cannot specify both 'updates' and 'bulk_updates'")
        return v

    def model_post_init(self, __context):
        """Validate mutually exclusive update methods."""
        update_methods = [
            self.key_value is not None,
            self.key_values is not None,
            self.bulk_updates is not None,
            self.filters is not None
        ]
        if sum(update_methods) != 1:
            raise ValueError(
                "Must specify exactly one update method: key_value, key_values, bulk_updates, or filters"
            )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "key_column": "api10",
                    "key_value": 4201234567,
                    "updates": {
                        "oileur": 130000.00,
                        "gaseur": 475000.00
                    }
                },
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "key_column": "api10",
                    "key_values": [4201234567, 4201234568, 4201234569],
                    "updates": {
                        "wellspacing": 640.0
                    }
                },
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "key_column": "api10",
                    "bulk_updates": [
                        {"key_value": 4201234567, "updates": {"oileur": 130000.00, "gaseur": 475000.00}},
                        {"key_value": 4201234568, "updates": {"wellspacing": 320.0}},
                        {"key_value": 4201234569, "updates": {"compldate": "05/15/2024"}}
                    ]
                },
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "filters": [
                        {"column": "oileur", "op": "<", "value": 100000}
                    ],
                    "updates": {
                        "wellspacing": 640.0
                    }
                }
            ]
        },
        "populate_by_name": True
    }

class TableDeleteRequest(BaseModel):
    """Request model for deleting data from a table.

    Supports 3 deletion scenarios:
    1. Single record: key_column + key_value + soft
    2. Multiple records: key_column + key_values + soft
    3. Filter-based: filters + soft
    """

    catalog: str | None = None
    schema_name: str | None = Field(None, alias="schema")
    table: str
    key_column: str | None = None
    key_value: Any | None = None
    key_values: list[Any] | None = None
    filters: list[dict[str, Any]] | None = None
    soft: bool | None = True

    @field_validator("key_column")
    @classmethod
    def validate_key_column_when_needed(cls, v, info):
        """Validate key_column is provided when using key-based deletion."""
        data = info.data
        if (data.get("key_value") is not None or data.get("key_values")) and not v:
            raise ValueError("key_column is required when using key_value or key_values")
        return v

    def model_post_init(self, __context):
        """Validate mutually exclusive deletion methods."""
        delete_methods = [
            self.key_value is not None,
            self.key_values is not None,
            self.filters is not None
        ]
        if sum(delete_methods) != 1:
            raise ValueError(
                "Must specify exactly one deletion method: key_value, key_values, or filters"
            )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "key_column": "api10",
                    "key_value": 4201234567,
                    "soft": True
                },
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "key_column": "api10",
                    "key_values": [4201234567, 4201234568, 4201234569],
                    "soft": True
                },
                {
                    "catalog": "oil_gas_catalog",
                    "schema": "production_schema",
                    "table": "wells",
                    "filters": [
                        {"column": "oileur", "op": "=", "value": 0}
                    ],
                    "soft": True
                }
            ],
        },
        "populate_by_name": True
    }

