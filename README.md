# FastAPI + Databricks Unity Catalog

[![Tests](https://github.com/leziollc/da-app-fastapi/actions/workflows/pytest.yml/badge.svg)](https://github.com/leziollc/da-app-fastapi/actions/workflows/pytest.yml)
[![Lint](https://github.com/leziollc/da-app-fastapi/actions/workflows/ruff.yml/badge.svg)](https://github.com/leziollc/da-app-fastapi/actions/workflows/ruff.yml)
[![Type Check](https://github.com/leziollc/da-app-fastapi/actions/workflows/mypy.yml/badge.svg)](https://github.com/leziollc/da-app-fastapi/actions/workflows/mypy.yml)

A production-ready FastAPI application for performing CRUD operations on Databricks Unity Catalog tables via SQL warehouses.

## Features

✨ **Auto-Create Resources** - Automatically create catalogs, schemas, and tables with schema validation  
🔒 **Security** - SQL injection prevention via parameterized queries and identifier validation  
📊 **Schema Validation** - 18 SQL data types with Pydantic validation  
🚀 **Async Support** - Full async/await with FastAPI  
🧪 **Comprehensive Tests** - 82 tests with 92.7% pass rate  
🔧 **GitHub Actions** - Automated CI/CD with linting, type checking, and testing  
📝 **Auto Audit** - Automatic `created_at`, `created_by`, `updated_at`, `updated_by` fields

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
ruff check backend/          # Linting
mypy backend/ --ignore-missing-imports --no-strict-optional  # Type checking
```

## CI/CD

GitHub Actions workflows automatically run on push to `dev` or `main` branches:

- **`pytest.yml`** - Runs all 82 tests
- **`ruff.yml`** - Lints code for style and errors
- **`mypy.yml`** - Type checks all Python code
- **`deploy.yml`** - Creates deployment artifact (triggered on `main` branch)

Tests run in CI using mocked database connections, so no Databricks credentials are required.

## Configuration

The app uses environment variables for configuration:

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID for connections | Yes |
| `DATABRICKS_CONFIG_PROFILE` | Profile name for audit fields | Yes |

Additional configuration is loaded from `~/.databrickscfg` via Databricks SDK.

## API Endpoints

### Health Check

**`GET /api/v1/healthcheck`**
- Returns API status and timestamp
- No authentication required
- Used for monitoring and load balancer health checks

**Response:**
```json
{
  "status": "OK",
  "timestamp": "2025-12-06T10:30:00.000000+00:00"
}
```

### Records Endpoints

All records endpoints operate on Unity Catalog tables with the format `catalog.schema.table`.

---

#### **POST `/api/v1/records/write` - Insert Records**

Insert data into a table with optional auto-creation of catalog/schema/table.

**Request Body:**
```json
{
  "catalog": "my_catalog",
  "schema": "my_schema",
  "table": "customers",
  "auto_create": true,
  "schema_definition": [
    {"name": "customer_id", "data_type": "BIGINT", "nullable": false},
    {"name": "name", "data_type": "STRING", "nullable": false},
    {"name": "email", "data_type": "STRING", "nullable": true},
    {"name": "balance", "data_type": "DOUBLE", "nullable": true},
    {"name": "is_active", "data_type": "BOOLEAN", "nullable": false}
  ],
  "data": [
    {
      "customer_id": 1,
      "name": "John Doe",
      "email": "john@example.com",
      "balance": 1500.50,
      "is_active": true
    }
  ]
}
```

**Auto-Create Features:**
- Set `auto_create: true` to automatically create missing catalog/schema/table
- `schema_definition` is required when `auto_create: true`
- Automatic audit fields: `created_at`, `created_by` (added if not present)

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

#### **POST `/api/v1/records/read` - Query Records**

Read records from a table with filtering and pagination.

**Query Parameters:**
- `catalog` (required) - Catalog name
- `schema` (required) - Schema name
- `table` (required) - Table name
- `limit` (optional, default: 100, max: 1000) - Number of records to return
- `offset` (optional, default: 0) - Number of records to skip
- `columns` (optional, default: "*") - Comma-separated column names
- `filters` (optional) - JSON array of filter objects

**Example Request:**
```
POST /api/v1/records/read
```

**Request Body:**
```json
{
  "catalog": "my_catalog",
  "schema": "my_schema",
  "table": "customers",
  "limit": 10,
  "offset": 0,
  "columns": "customer_id,name,email",
  "filters": [
    {"column": "is_active", "op": "=", "value": true},
    {"column": "balance", "op": ">", "value": 1000}
  ]
}
```

**Response:**
```json
{
  "data": [
    {
      "customer_id": 1,
      "name": "John Doe",
      "email": "john@example.com"
    }
  ],
  "count": 1,
  "total": 150
}
```

---

#### **POST `/api/v1/records/update` - Update Record**

Update a single record identified by a key column.

**Request Body:**
```json
{
  "catalog": "my_catalog",
  "schema": "my_schema",
  "table": "customers",
  "key_column": "customer_id",
  "key_value": 1,
  "updates": {
    "email": "newemail@example.com",
    "balance": 2000.00
  }
}
```

**Features:**
- Automatic `updated_at` and `updated_by` audit fields
- Parameterized queries prevent SQL injection

**Response:**
```json
{
  "message": "Updated 1 record(s) in my_catalog.my_schema.customers"
}
```

---

#### **POST `/api/v1/records/delete` - Delete Record**

Delete a record (soft or hard delete).

**Request Body:**
```json
{
  "catalog": "my_catalog",
  "schema": "my_schema",
  "table": "customers",
  "key_column": "customer_id",
  "key_value": 1,
  "soft": true
}
```

**Soft Delete (`soft: true`):**
- Sets `deleted_at` timestamp
- Record remains in table but marked as deleted

**Hard Delete (`soft: false`):**
- Permanently removes record from table

**Response:**
```json
{
  "message": "Soft deleted record in my_catalog.my_schema.customers"
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
├── pytest.ini                 # pytest configuration
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

## Security

- **SQL Injection Prevention** - All queries use parameterized statements
- **Identifier Validation** - Catalog/schema/table names validated against SQL injection
- **Type Safety** - Pydantic models enforce type checking
- **Error Sanitization** - Stack traces hidden in production
