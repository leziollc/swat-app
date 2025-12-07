"""Tests for pagination with large datasets."""

import pytest
from fastapi.testclient import TestClient

from backend.app import app


class TestLargeDatasetPagination:
    """Test suite for pagination with large datasets."""

    @pytest.fixture(autouse=True)
    def patch_connector(self, monkeypatch):
        """Mock database connector to simulate large datasets."""
        self.query_calls = []

        def fake_query(sql_query, warehouse_id, as_dict=True, params=None):
            self.query_calls.append({
                "query": sql_query,
                "params": params
            })

            # Extract LIMIT and OFFSET from query
            import re
            limit_match = re.search(r'LIMIT (\d+)', sql_query)
            offset_match = re.search(r'OFFSET (\d+)', sql_query)

            limit = int(limit_match.group(1)) if limit_match else 100
            offset = int(offset_match.group(1)) if offset_match else 0

            # Simulate large dataset (10000 records)
            total_records = 10000

            # Generate fake records for this page
            records = []
            for i in range(offset, min(offset + limit, total_records)):
                records.append({
                    "id": i,
                    "name": f"Record_{i}",
                    "value": i * 10.5,
                    "is_deleted": False
                })

            return records

        def fake_insert(table_path, data, warehouse_id):
            return len(data)

        monkeypatch.setattr("backend.services.db.connector.query", fake_query)
        monkeypatch.setattr("backend.services.db.connector.insert_data", fake_insert)

    def test_first_page_pagination(self):
        """Test retrieving the first page of results."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 100,
                    "offset": 0
                }
            )

            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 100
            assert len(data["data"]) == 100
            assert data["data"][0]["id"] == 0
            assert data["data"][99]["id"] == 99

    def test_middle_page_pagination(self):
        """Test retrieving a middle page of results."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 50,
                    "offset": 500
                }
            )

            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 50
            assert data["data"][0]["id"] == 500
            assert data["data"][49]["id"] == 549

    def test_last_page_pagination(self):
        """Test retrieving the last page with partial results."""
        with TestClient(app) as client:
            # Request last 100 records (9900-9999)
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 100,
                    "offset": 9900
                }
            )

            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 100
            assert data["data"][0]["id"] == 9900
            assert data["data"][99]["id"] == 9999

    def test_beyond_last_page(self):
        """Test requesting data beyond the last page."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 100,
                    "offset": 10000
                }
            )

            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 0
            assert len(data["data"]) == 0

    def test_maximum_limit_boundary(self):
        """Test pagination with maximum allowed limit."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 1000,
                    "offset": 0
                }
            )

            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 1000

    def test_limit_exceeds_maximum(self):
        """Test that limit exceeding maximum is rejected."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 2000,  # Exceeds max of 1000
                    "offset": 0
                }
            )

            # Should return validation error
            assert resp.status_code == 400  # Bad Request (Pydantic validation)

    def test_negative_offset(self):
        """Test that negative offset is rejected."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 100,
                    "offset": -10
                }
            )

            # Should return validation error
            assert resp.status_code == 400  # Bad Request (Pydantic validation)

    def test_zero_limit(self):
        """Test that zero limit is rejected."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 0,
                    "offset": 0
                }
            )

            # Should return validation error
            assert resp.status_code == 400  # Bad Request (Pydantic validation)

    def test_large_offset_performance(self):
        """Test that large offsets are handled (though may be slow)."""
        with TestClient(app) as client:
            # Very large offset (this would be slow in real database)
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 10,
                    "offset": 9990
                }
            )

            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 10
            assert data["data"][0]["id"] == 9990

    def test_records_pagination(self):
        """Test pagination in records endpoint."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "records",
                    "limit": 50,
                    "offset": 100
                }
            )

            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 50
            # Verify correct offset applied
            assert len(self.query_calls) > 0
            assert "OFFSET 100" in self.query_calls[-1]["query"]

    def test_pagination_with_filters(self):
        """Test pagination combined with filters."""
        with TestClient(app) as client:
            filters = '[{"column": "value", "op": ">", "value": 50}]'
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": 20,
                    "offset": 10,
                    "filter_expr": filters
                }
            )

            assert resp.status_code == 200
            # Both pagination and filter should be applied
            query = self.query_calls[-1]["query"]
            assert "LIMIT 20" in query
            assert "OFFSET 10" in query

    def test_multiple_page_iteration(self):
        """Test iterating through multiple pages."""
        with TestClient(app) as client:
            page_size = 100
            total_fetched = 0
            all_ids = set()

            # Fetch first 5 pages
            for page in range(5):
                resp = client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": "large_table",
                        "limit": page_size,
                        "offset": page * page_size
                    }
                )

                assert resp.status_code == 200
                data = resp.json()
                total_fetched += data["count"]

                # Collect IDs to check for duplicates
                for record in data["data"]:
                    all_ids.add(record["id"])

            # Should have fetched 500 unique records
            assert total_fetched == 500
            assert len(all_ids) == 500  # No duplicates

    def test_string_limit_parameter(self):
        """Test that string limit parameter is handled correctly."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": "50",  # String instead of int
                    "offset": "100"  # String instead of int
                }
            )

            # FastAPI should coerce to int
            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 50

    def test_float_limit_parameter(self):
        """Test that float limit parameter causes validation error."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table",
                    "limit": "50.5",
                    "offset": 0
                }
            )

            # Should cause validation error
            assert resp.status_code == 422

    def test_default_pagination_values(self):
        """Test default pagination values when not specified."""
        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "large_table"
                    # No limit or offset specified
                }
            )

            assert resp.status_code == 200
            # Should use default limit (100) and offset (0)
            query = self.query_calls[-1]["query"]
            assert "LIMIT 100" in query
            assert "OFFSET 0" in query


