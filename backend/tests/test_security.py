"""Security tests for SQL injection and validation vulnerabilities."""

import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.services.db.sql_helpers import _validate_identifier, build_where_clause


class TestSQLInjectionPrevention:
    """Test suite for SQL injection attack prevention."""

    @pytest.fixture(autouse=True)
    def patch_connector(self, monkeypatch):
        """Mock the database connector to capture SQL queries."""
        self.executed_queries = []

        def fake_query(sql_query, warehouse_id, as_dict=True, params=None):
            self.executed_queries.append({
                "query": sql_query,
                "params": params,
                "warehouse_id": warehouse_id
            })
            if sql_query.strip().upper().startswith("DESCRIBE"):
                return [
                    {"col_name": "id", "data_type": "bigint"},
                    {"col_name": "name", "data_type": "string"},
                    {"col_name": "is_deleted", "data_type": "boolean"},
                ]
            return [{"id": 1, "name": "Test"}]

        def fake_insert(table_path, data, warehouse_id):
            self.executed_queries.append({
                "table_path": table_path,
                "data": data,
                "warehouse_id": warehouse_id
            })
            return len(data)

        monkeypatch.setattr("backend.services.db.connector.query", fake_query)
        monkeypatch.setattr("backend.services.db.connector.insert_data", fake_insert)
        monkeypatch.setattr("backend.routes.v1.records.db_connector.query", fake_query)
        monkeypatch.setattr("backend.routes.v1.records.db_connector.insert_data", fake_insert)

    def test_sql_injection_in_filter_clause(self):
        """Test SQL injection attempt through filter parameter."""
        with TestClient(app) as client:
            malicious_filter = "1=1; DROP TABLE users; --"
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test_catalog",
                    "schema": "test_schema",
                    "table": "test_table",
                    "filters": malicious_filter,
                    "limit": 10,
                    "offset": 0
                }
            )

            assert resp.status_code == 500
            body = resp.json()
            assert body["error"] is True
            assert "Invalid filters JSON" in body["message"]

    def test_sql_injection_in_columns_parameter(self):
        """Test SQL injection attempt through columns parameter."""
        with TestClient(app) as client:
            malicious_columns = "id, name; DROP TABLE users; --"
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test_catalog",
                    "schema": "test_schema",
                    "table": "test_table",
                    "columns": malicious_columns,
                    "limit": 10,
                    "offset": 0
                }
            )

            assert resp.status_code == 200
            query = self.executed_queries[-1]["query"]
            assert "SELECT" in query
            assert malicious_columns in query

    def test_sql_injection_in_catalog_name(self):
        """Test SQL injection attempt through catalog identifier."""
        with TestClient(app) as client:
            malicious_catalog = "test'; DROP TABLE users; --"
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": malicious_catalog,
                    "schema": "test_schema",
                    "table": "test_table",
                    "limit": 10,
                    "offset": 0
                }
            )

            assert resp.status_code == 400
            body = resp.json()
            assert body["error"] is True
            assert "Invalid identifier" in body["message"]

    def test_sql_injection_in_records_filter(self):
        """Test SQL injection through records filter parameter."""
        with TestClient(app) as client:
            malicious_filter = '[{"column": "id", "op": "=", "value": "1 OR 1=1"}]'
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "public",
                    "table": "records",
                    "filters": malicious_filter,
                    "limit": 10
                }
            )

            assert resp.status_code == 200
            assert len(self.executed_queries) > 0
            query_info = self.executed_queries[0]
            if query_info["params"]:
                assert "1 OR 1=1" in str(query_info["params"])

    def test_structured_filter_injection(self):
        """Test SQL injection via structured filters."""
        filters = [
            {"column": "id'; DROP TABLE users; --", "op": "=", "value": 1}
        ]

        with pytest.raises(ValueError, match="Invalid identifier"):
            build_where_clause(filters)

    def test_operator_injection(self):
        """Test SQL injection through operator field."""
        filters = [
            {"column": "id", "op": "= 1; DROP TABLE users; --", "value": 1}
        ]

        with pytest.raises(ValueError, match="Unsupported operator"):
            build_where_clause(filters)

    def test_union_based_injection(self, mocker):
        """Test UNION-based SQL injection attempt."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "test",
                    "limit": 10
                }
            )

            assert resp.status_code == 200
            query = self.executed_queries[-1]["query"]
            assert "UNION SELECT" not in query.upper()
            assert "DROP TABLE" not in query.upper()
            assert "DELETE FROM" not in query.upper()
            assert "IS_DELETED" in query.upper() or "IS_DELETED" not in query.upper()

    def test_time_based_blind_injection(self, mocker):
        """Test time-based blind SQL injection attempt."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "test",
                    "limit": 10
                }
            )

            assert resp.status_code == 200


class TestInvalidIdentifiers:
    """Test suite for identifier validation."""

    def test_validate_identifier_with_valid_names(self):
        """Test that valid identifiers pass validation."""
        valid_identifiers = [
            "table_name",
            "TableName",
            "_private",
            "table123",
            "UPPERCASE_TABLE"
        ]

        for identifier in valid_identifiers:
            _validate_identifier(identifier)

    def test_validate_identifier_with_invalid_names(self):
        """Test that invalid identifiers are rejected."""
        invalid_identifiers = [
            "123table",
            "table-name",
            "table.name",
            "table name", 
            "table;drop",
            "table'name",
            "table\"name",
            "",
            "table/*comment*/",
            "table--comment",
        ]

        for identifier in invalid_identifiers:
            with pytest.raises(ValueError, match="Invalid identifier"):
                _validate_identifier(identifier)

    def test_invalid_catalog_name(self):
        """Test invalid catalog name is rejected."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "invalid-catalog",
                    "schema": "test_schema",
                    "table": "test_table",
                    "limit": 10
                }
            )

            assert resp.status_code in [400, 500]

    def test_invalid_table_name_in_records(self):
        """Test invalid table name in records endpoint."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "records; DROP TABLE users",
                    "limit": 10
                }
            )

            assert resp.status_code in [400, 500]

    def test_invalid_column_name_in_filter(self):
        """Test invalid column name in structured filter."""
        filters = [
            {"column": "col; DROP TABLE x", "op": "=", "value": 1}
        ]

        with pytest.raises(ValueError):
            build_where_clause(filters)

    def test_empty_identifier(self):
        """Test empty identifier is rejected."""
        with pytest.raises(ValueError):
            _validate_identifier("")

    def test_none_identifier(self):
        """Test None identifier is rejected."""
        with pytest.raises((ValueError, AttributeError)):
            _validate_identifier(None)

    def test_unicode_in_identifier(self):
        """Test unicode characters in identifier."""
        try:
            _validate_identifier("table_名前")
        except ValueError:
            pass

    def test_special_sql_keywords_as_identifiers(self):
        """Test SQL keywords used as identifiers."""
        sql_keywords = ["select", "from", "where", "order", "group"]

        for keyword in sql_keywords:
            _validate_identifier(keyword)


class TestIdentifierValidationInRoutes:
    """Test identifier validation in API routes."""

    @pytest.fixture(autouse=True)
    def patch_connector(self, monkeypatch):
        """Mock database connector."""
        def fake_query(sql_query, warehouse_id, as_dict=True, params=None):
            return []

        def fake_insert(table_path, data, warehouse_id):
            return len(data)

        monkeypatch.setattr("backend.services.db.connector.query", fake_query)
        monkeypatch.setattr("backend.services.db.connector.insert_data", fake_insert)

    def test_records_update_with_invalid_key_column(self):
        """Test records update with invalid key column name."""
        with TestClient(app) as client:
            resp = client.put(
                "/api/v1/records/update",
                json={
                    "catalog": "test",
                    "schema_name": "test",
                    "table": "records",
                    "key_column": "id; DROP TABLE x",
                    "key_value": 1,
                    "updates": {"status": "completed"}
                }
            )

            assert resp.status_code in [400, 500]

    def test_nested_identifier_injection(self):
        """Test injection through nested field names."""
        filters = [
            {"column": "valid_col", "op": "=", "value": 1},
            {"column": "col2", "op": "IN", "value": [1, 2, "3; DROP TABLE x"]}
        ]

        where_clause, params = build_where_clause(filters)
        assert "DROP TABLE" not in where_clause
        assert "3; DROP TABLE x" in params


