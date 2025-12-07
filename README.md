# FastAPI + Databricks Unity Catalog

A production-ready FastAPI application for performing CRUD operations on Databricks Unity Catalog tables via SQL warehouses.

## Features

✨ **Auto-Create Tables** - Automatically create tables with schema validation  
🔒 **Security** - SQL injection prevention via parameterized queries and identifier validation  
📊 **Schema Validation** - 18 SQL data types with Pydantic validation and detailed error responses  
🚀 **Async Support** - Full async/await with FastAPI  
🧪 **Comprehensive Tests** - 100 tests passing (99 passed + 1 skipped) with full coverage  
🔧 **GitHub Actions** - Automated CI/CD with pytest (80% coverage), mypy, and ruff  
📝 **Auto Audit** - 8 automatic audit columns: `record_uuid`, `is_deleted`, `inserted_at`, `inserted_by`, `updated_at`, `updated_by`, `deleted_at`, `deleted_by`  
🔄 **Multi-Record Operations** - Update and delete single, multiple, or filtered records in one request  
💡 **Enhanced Error Handling** - Schema validation errors include expected schema for faster debugging  
📋 **Database Logging** - Automatic error logging to Databricks `api_log` table for monitoring and debugging

## Quick Start

### 1. Setup Environment

**Create and activate virtual environment (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**Install dependencies:**
```powershell
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file:
```env
DATABRICKS_CONFIG_PROFILE="Your Profile Name"
DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
```

**Note:** The app uses Databricks SDK Config from `~/.databrickscfg` for authentication. Ensure you have a valid profile configured.

### 3. Run the Application

**Development mode with auto-reload:**
```powershell
python -m uvicorn backend.app:app --reload
```

**Production mode:**
```powershell
python -m uvicorn backend.app:app --host 0.0.0.0 --port 8000
```

Access the API at: `http://localhost:8000`  
Interactive docs: `http://localhost:8000/docs`

## Testing

**Run all tests:**
```powershell
pytest -q
```

**Run with verbose output:**
```powershell
pytest -v
```

**Run specific test suite:**
```powershell
pytest backend/tests/test_security.py -v
pytest backend/tests/test_pagination.py -v
pytest backend/tests/test_concurrency.py -v
```

**Run with coverage:**
```powershell
pytest --cov=backend --cov-report=html
```

**Linting and Type Checking:**
```powershell
ruff check backend/
mypy backend/ --ignore-missing-imports --no-strict-optional
```

## CI/CD

GitHub Actions workflows automatically run on push to `dev`/`main` and pull requests to `main`:

- **`pytest.yml`** - Runs all tests with 80% coverage requirement
- **`ruff.yml`** - Lints code for style and errors (fails on any issues)
- **`mypy.yml`** - Type checks all Python code (fails on type errors)
- **`deploy.yml`** - Creates deployment artifact (triggered on `main` branch)

Pull requests to `main` must pass all three checks (pytest, ruff, mypy) before merging. Tests run in CI using mocked database connections, so no Databricks credentials are required.

## Configuration

The app uses environment variables for configuration:

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID for connections | Yes |
| `DATABRICKS_CONFIG_PROFILE` | Username for audit trail (inserted_by, updated_by, deleted_by) | Yes |
| `DATABRICKS_LOGGING_ENABLED` | Enable database logging for API errors (default: `true`) | No |
| `DATABRICKS_LOG_CATALOG` | Catalog for api_log table (defaults to `DATABRICKS_CATALOG`) | No |
| `DATABRICKS_LOG_SCHEMA` | Schema for api_log table (defaults to `DATABRICKS_SCHEMA`) | No |

Additional configuration is loaded from `~/.databrickscfg` via Databricks SDK.

## Database Logging

The application automatically logs all API errors and exceptions to a Databricks table called `api_log` for persistent audit trails and troubleshooting. Logging is enabled by default and can be configured via environment variables.

### Features

- **Automatic Error Tracking** - All exceptions are automatically logged with full context
- **Non-Blocking** - Logging failures don't break the application
- **Auto Table Creation** - The `api_log` table is created automatically if it doesn't exist
- **Rich Context** - Logs include request details, stack traces, error types, and audit information
- **Configurable** - Enable/disable logging and configure catalog/schema locations

### Log Table Schema

The `api_log` table contains 15 columns:

| Column | Type | Description |
|--------|------|-------------|
| `log_id` | STRING | Unique UUID for each log entry |
| `timestamp` | TIMESTAMP | When the error occurred (UTC) |
| `level` | STRING | Log level: ERROR, WARNING, or INFO |
| `endpoint` | STRING | API endpoint that generated the error |
| `method` | STRING | HTTP method (GET, POST, etc.) |
| `status_code` | INT | HTTP status code returned |
| `error_type` | STRING | Exception class name |
| `error_message` | STRING | Error message text |
| `stack_trace` | STRING | Full stack trace for debugging |
| `request_body` | STRING | Request body (truncated at 5000 chars) |
| `user` | STRING | User who made the request |
| `catalog` | STRING | Databricks catalog being accessed |
| `schema` | STRING | Databricks schema being accessed |
| `table_name` | STRING | Databricks table being accessed |
| `execution_time_ms` | DOUBLE | Request execution time in milliseconds |

### Configuration

**Enable/Disable Logging:**
```env
DATABRICKS_LOGGING_ENABLED=true  # Set to 'false' to disable logging
```

**Configure Log Location:**
```env
DATABRICKS_LOG_CATALOG=my_catalog  # Optional, defaults to DATABRICKS_CATALOG
DATABRICKS_LOG_SCHEMA=my_schema    # Optional, defaults to DATABRICKS_SCHEMA
```

The full table path will be: `{DATABRICKS_LOG_CATALOG}.{DATABRICKS_LOG_SCHEMA}.api_log`

### Querying Logs

**Recent errors:**
```sql
SELECT timestamp, level, endpoint, method, status_code, error_type, error_message
FROM my_catalog.my_schema.api_log
WHERE level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 100;
```

**Errors by endpoint:**
```sql
SELECT endpoint, COUNT(*) as error_count, MAX(timestamp) as last_error
FROM my_catalog.my_schema.api_log
WHERE level = 'ERROR'
GROUP BY endpoint
ORDER BY error_count DESC;
```

**Errors with stack traces for debugging:**
```sql
SELECT timestamp, endpoint, error_type, error_message, stack_trace
FROM my_catalog.my_schema.api_log
WHERE error_type = 'DatabaseError'
ORDER BY timestamp DESC
LIMIT 10;
```

## Audit Columns

All tables created by this API automatically include 8 audit columns to track record lifecycle and provide comprehensive audit trails:

| Column | Type | Description | When Set |
|--------|------|-------------|----------|
| `record_uuid` | STRING | Unique identifier (UUID v4) for the record | On insert (immutable) |
| `is_deleted` | BOOLEAN | Soft delete flag | `false` on insert, `true` on soft delete |
| `inserted_at` | TIMESTAMP | When the record was created (UTC) | On insert |
| `inserted_by` | STRING | Who created the record | On insert |
| `updated_at` | TIMESTAMP | When the record was last updated (UTC) | On update |
| `updated_by` | STRING | Who last updated the record | On update |
| `deleted_at` | TIMESTAMP | When the record was soft deleted (UTC) | On soft delete |
| `deleted_by` | STRING | Who soft deleted the record | On soft delete |

### Benefits

- **Unique Identifiers** - Every record has a guaranteed unique UUID across all tables
- **Soft Deletes** - Records are marked as deleted rather than physically removed
- **Full History** - Track who created, updated, and deleted each record with timestamps
- **Distributed Systems** - UUIDs work better than auto-increment IDs in distributed environments
- **Audit Compliance** - Complete audit trail for compliance and debugging

### Querying with Audit Columns

**Get active records only:**
```sql
SELECT * FROM oil_gas_catalog.production_schema.wells
WHERE is_deleted = false;
```

**Track record by UUID:**
```sql
SELECT record_uuid, api10, api14, wellname, updated_at, updated_by
FROM oil_gas_catalog.production_schema.wells
WHERE record_uuid = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';
```

**Audit trail for recently modified records:**
```sql
SELECT record_uuid, api10, wellname, inserted_by, inserted_at, updated_by, updated_at
FROM oil_gas_catalog.production_schema.wells
WHERE updated_at > current_timestamp() - INTERVAL 24 HOURS
ORDER BY updated_at DESC;
```

**Soft deleted records:**
```sql
SELECT record_uuid, api10, wellname, deleted_by, deleted_at
FROM oil_gas_catalog.production_schema.wells
WHERE is_deleted = true
ORDER BY deleted_at DESC;
```

## API Endpoints

### Health Check

**`GET /api/v1/healthcheck`**
- Returns API status, component health, and timestamp
- Tests Databricks database connectivity
- No authentication required
- Used for monitoring and load balancer health checks

**Response (Healthy):**
```json
{
  "status": "healthy",
  "components": {
    "api": "up",
    "database": "connected"
  },
  "timestamp": "2025-12-06T10:30:00.000000+00:00"
}
```

**Response (Degraded):**
```json
{
  "status": "degraded",
  "components": {
    "api": "up",
    "database": "error: Connection timeout"
  },
  "timestamp": "2025-12-06T10:30:00.000000+00:00"
}
```

### Records Endpoints

All records endpoints operate on Unity Catalog tables with the format `catalog.schema.table`.

---

#### **POST `/api/v1/records/write` - Insert Records**

Insert data into a table with optional auto-creation of table.

**Request Body:**
```json
{
  "catalog": "oil_gas_catalog",
  "schema": "production_schema",
  "table": "wells",
  "auto_create": true,
  "schema_definition": [
    {"name": "api10", "data_type": "BIGINT", "nullable": false},
    {"name": "api14", "data_type": "BIGINT", "nullable": false},
    {"name": "wellname", "data_type": "STRING", "nullable": false},
    {"name": "spuddate", "data_type": "STRING", "nullable": true},
    {"name": "firstproddate", "data_type": "STRING", "nullable": true},
    {"name": "compldate", "data_type": "STRING", "nullable": true},
    {"name": "oileur", "data_type": "DOUBLE", "nullable": true},
    {"name": "gaseur", "data_type": "DOUBLE", "nullable": true},
    {"name": "wellspacing", "data_type": "DOUBLE", "nullable": true}
  ],
  "data": [
    {
      "api10": 4201234567,
      "api14": 42012345671234,
      "wellname": "SMITH 1-15H",
      "spuddate": "01/15/2024",
      "firstproddate": "03/20/2024",
      "compldate": "03/10/2024",
      "oileur": 125000.50,
      "gaseur": 450000.75,
      "wellspacing": 640.0
    }
  ]
}
```

**Auto-Create Behavior:**

**When `auto_create: true`:**
- **Catalog and schema must already exist in Databricks** (not auto-created)
- Automatically creates table with `schema_definition` if it doesn't exist
- Adds 8 audit columns to new tables: `record_uuid` (unique identifier), `is_deleted`, `inserted_at`, `inserted_by`, `updated_at`, `updated_by`, `deleted_at`, `deleted_by`
- `schema_definition` is **required** when `auto_create: true`

**When `auto_create: false` (default):**
- Assumes catalog, schema, and table already exist
- Directly inserts data into existing table
- Fails with error if table doesn't exist
- Still adds audit field values to inserted records

**Schema Validation:**
- If `schema_definition` is provided (regardless of `auto_create`), data is validated against it before inserting
- Validates data types, nullable constraints, and column names
- Highly recommended to provide `schema_definition` for data integrity
- **Error responses include the expected schema**: When validation fails, the error response contains the `expected_schema` in the `details` field to help with debugging

**Example Schema Validation Error Response:**
```json
{
  "error": true,
  "message": "Schema validation failed: Record 0: Column 'order_id' expects integer, got str",
  "details": {
    "expected_schema": [
      {"name": "order_id", "type": "BIGINT", "nullable": false},
      {"name": "amount", "type": "DOUBLE", "nullable": true}
    ]
  }
}
```

**Supported Data Types:**
```
BIGINT, INT, INTEGER, SMALLINT, TINYINT    # Integer types
DOUBLE, FLOAT, DECIMAL                      # Numeric types
STRING, VARCHAR, CHAR                       # String types
BOOLEAN                                     # Boolean type
DATE, TIMESTAMP, TIMESTAMP_NTZ              # Date/time types
BINARY, ARRAY, MAP, STRUCT                  # Complex types
```

**Response:**
```json
{
  "message": "Inserted 1 record(s) into my_catalog.my_schema.customers",
  "count": 1
}
```

---

#### **GET `/api/v1/records/read` - Query Records**

Read records from a table with filtering and pagination.

**Query Parameters:**
- `catalog` (required) - Catalog name
- `schema` (required) - Schema name
- `table` (required) - Table name
- `limit` (optional, default: 100, max: 1000) - Number of records to return
- `offset` (optional, default: 0) - Number of records to skip
- `columns` (optional, default: "*") - Comma-separated column names
- `filters` (optional) - JSON string of filter array, e.g., `[{"column":"status","op":"=","value":"active"}]`

**Example Request:**
```
GET /api/v1/records/read?catalog=oil_gas_catalog&schema=production_schema&table=wells&limit=10&offset=0&columns=api10,api14,wellname,oileur,gaseur&filters=[{"column":"oileur","op":">","value":100000},{"column":"spuddate","op":"like","value":"%2024"}]
```

**Response:**
```json
{
  "data": [
    {
      "api10": 4201234567,
      "api14": 42012345671234,
      "wellname": "SMITH 1-15H",
      "oileur": 125000.50,
      "gaseur": 450000.75
    }
  ],
  "count": 1,
  "total": 150
}
```

---

#### **PUT `/api/v1/records/update` - Update Records**

Update one or more records in a table. Supports 4 update scenarios.

**Scenario 1: Single Record Update**
```json
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
}
```

**Scenario 2: Multiple Records (Same Updates)**
```json
{
  "catalog": "oil_gas_catalog",
  "schema": "production_schema",
  "table": "wells",
  "key_column": "api10",
  "key_values": [4201234567, 4201234568, 4201234569],
  "updates": {
    "wellspacing": 640.0
  }
}
```

**Scenario 3: Multiple Records (Different Updates)**
```json
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
}
```

**Scenario 4: Filter-Based Update**
```json
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
```

**Features:**
- Automatically updates `updated_at` (timestamp) and `updated_by` (username from DATABRICKS_CONFIG_PROFILE) audit fields
- Verifies record existence before updating - returns accurate count of actually updated records
- Returns `not_found` array showing which records don't exist in the table
- Parameterized queries prevent SQL injection
- Must specify exactly one update method (key_value, key_values, bulk_updates, or filters)

**Response (all records found):**
```json
{
  "data": [{"updated_at": "2025-12-07T01:00:00.000000+00:00", "updated_by": "user"}],
  "count": 3,
  "total": null
}
```

**Response (some records not found):**
```json
{
  "data": [{
    "updated_at": "2025-12-07T01:00:00.000000+00:00",
    "updated_by": "user",
    "not_found": [4201234569]
  }],
  "count": 2,
  "total": null
}
```

---

#### **DELETE `/api/v1/records/delete` - Delete Records**

Delete one or more records (soft or hard delete).

Supports 3 deletion scenarios:
1. **Single record**: Provide `key_column`, `key_value`, and `soft`
2. **Multiple records**: Provide `key_column`, `key_values` (array), and `soft`
3. **Filter-based**: Provide `filters` and `soft`

**Scenario 1: Single Record**
```json
{
  "catalog": "oil_gas_catalog",
  "schema": "production_schema",
  "table": "wells",
  "key_column": "api10",
  "key_value": 4201234567,
  "soft": true
}
```

**Scenario 2: Multiple Records**
```json
{
  "catalog": "oil_gas_catalog",
  "schema": "production_schema",
  "table": "wells",
  "key_column": "api10",
  "key_values": [4201234567, 4201234568, 4201234569],
  "soft": true
}
```

**Scenario 3: Filter-Based Deletion**
```json
{
  "catalog": "oil_gas_catalog",
  "schema": "production_schema",
  "table": "wells",
  "filters": [
    {"column": "oileur", "op": "=", "value": 0}
  ],
  "soft": true
}
```

**Soft Delete (`soft: true`):**
- Sets `is_deleted=true`, `deleted_at` timestamp, `deleted_by` username, `updated_at`, and `updated_by`
- Record(s) remain in table but marked as deleted
- Requires table to have `is_deleted` column (automatically added with `auto_create`)

**Hard Delete (`soft: false`):**
- Permanently removes record(s) from table

**Features:**
- Automatic `deleted_at`, `deleted_by`, `updated_at`, `updated_by` audit fields for soft deletes
- Verifies record existence before deleting - returns accurate count of actually deleted records
- Returns `not_found` array showing which records don't exist in the table
- Parameterized queries prevent SQL injection
- Must specify exactly one deletion method (key_value, key_values, or filters)

**Response (all records found):**
```json
{
  "data": [{"is_deleted": true}],
  "count": 3,
  "total": null
}
```

**Response (some records not found):**
```json
{
  "data": [
    {"is_deleted": true},
    {"not_found": [4201234569]}
  ],
  "count": 2,
  "total": null
}
```

## Project Structure

```
da-app-fastapi/
├── backend/
│   ├── app.py                 # FastAPI application entry point
│   ├── config/
│   │   └── settings.py        # Application settings
│   ├── errors/
│   │   ├── exceptions.py      # Custom exception classes
│   │   └── handlers.py        # Global exception handlers
│   ├── models/
│   │   └── tables.py          # Pydantic models for requests/responses
│   ├── routes/
│   │   └── v1/
│   │       ├── healthcheck.py # Health check endpoint
│   │       └── records.py     # CRUD endpoints
│   ├── services/
│   │   └── db/
│   │       ├── connector.py   # Databricks SQL connector
│   │       └── sql_helpers.py # SQL query builders
│   └── tests/
│       ├── conftest.py        # pytest fixtures
│       ├── test_app.py        # Application tests
│       ├── test_security.py   # SQL injection tests
│       ├── test_pagination.py # Pagination tests
│       ├── test_concurrency.py # Concurrency tests
│       └── test_connection_failures.py # Error handling tests
├── .github/
│   └── workflows/
│       ├── pytest.yml         # Test workflow
│       ├── ruff.yml           # Linting workflow
│       ├── mypy.yml           # Type checking workflow
│       └── deploy.yml         # Deployment workflow
├── .env.example               # Environment template
├── requirements.txt           # Python dependencies
├── pyproject.toml             # Project config (pytest, ruff, mypy)
└── README.md                  # This file
```

## Error Handling

The application provides consistent error responses:

**Validation Error (400):**
```json
{
  "error": true,
  "message": "Validation error",
  "details": {
    "errors": [
      {
        "type": "missing",
        "loc": ["body", "catalog"],
        "msg": "Field required"
      }
    ]
  }
}
```

**Invalid Identifier Error (400):**
```json
{
  "error": true,
  "message": "Invalid identifier: test'; DROP TABLE users; --",
  "details": {}
}
```

**Schema Validation Error (400):**
```json
{
  "error": true,
  "message": "Schema validation failed: Record 0: Column 'customer_id' expects integer, got str",
  "details": {
    "expected_schema": [
      {"name": "customer_id", "type": "BIGINT", "nullable": false},
      {"name": "name", "type": "STRING", "nullable": false},
      {"name": "balance", "type": "DOUBLE", "nullable": true}
    ]
  }
}
```

**Database Error (500):**
```json
{
  "error": true,
  "message": "Database operation failed",
  "details": {
    "type": "DatabaseError",
    "info": "Connection timeout"
  }
}
```

**Configuration Error (500):**
```json
{
  "error": true,
  "message": "SQL warehouse ID not configured",
  "details": {}
}
```

## Security

- **SQL Injection Prevention** - All queries use parameterized statements
- **Identifier Validation** - Catalog/schema/table names validated against SQL injection
- **Type Safety** - Pydantic models enforce type checking
- **Error Sanitization** - Stack traces hidden in production
