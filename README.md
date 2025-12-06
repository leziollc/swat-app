# da-app-fastapi

FastAPI example app that demonstrates querying Unity Catalog (Databricks SQL warehouse)
and performing CRUD-like operations via a SQL connector. The project includes
linting, type checking, and test workflows suitable for GitHub Actions.

Quick notes
- Uses Databricks SQL connector (Unity Catalog) for table queries.
- Orders endpoints were refactored to use a SQL connector (no Lakebase runtime
	required at import time).
- Tests mock the connector so CI can run without Databricks credentials.

Local development

1. Create and activate virtualenv (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Run the app locally:

```powershell
python -m uvicorn backend.app:app --reload
```

Tests and CI

- Run tests locally: `pytest -q`
- CI includes separate workflows for `ruff`, `mypy`, `pytest` and a deploy
	workflow that can target `dev`, `staging`, or `prod` via branch triggers
	and manual dispatch.

Contributing

- Follow ruff and mypy rules; run `ruff .` and `mypy .` before opening PRs.

Configuration

The app reads configuration from environment variables. Important ones used in
development and CI:

- `DATABRICKS_WAREHOUSE_ID` - Databricks SQL warehouse id used by connector
- `DATABRICKS_HOST` and `DATABRICKS_TOKEN` - optional; tests mock connector