"""Tests for concurrent request handling."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from fastapi.testclient import TestClient

from backend.app import app


class TestConcurrentRequests:
    """Test suite for concurrent request handling."""

    @pytest.fixture(autouse=True)
    def patch_connector(self, monkeypatch):
        """Mock database connector with simulated delays."""
        self.concurrent_calls = []
        self.lock = threading.Lock()

        def fake_query(sql_query, warehouse_id, as_dict=True, params=None):
            sql_upper = sql_query.strip().upper()

            if sql_upper.startswith("DESCRIBE"):
                return [
                    {"col_name": "id", "data_type": "bigint"},
                    {"col_name": "name", "data_type": "string"},
                    {"col_name": "value", "data_type": "double"},
                    {"col_name": "is_deleted", "data_type": "boolean"},
                ]

            if "COUNT(*)" in sql_upper or "SELECT COUNT" in sql_upper:
                return [{"cnt": 1}]

            with self.lock:
                call_time = time.time()
                self.concurrent_calls.append({
                    "query": sql_query,
                    "time": call_time,
                    "thread": threading.current_thread().name
                })

            time.sleep(0.1)

            return [{"id": 1, "name": "Test", "value": 100.5}]

        def fake_insert(table_path, data, warehouse_id):
            with self.lock:
                self.concurrent_calls.append({
                    "operation": "insert",
                    "time": time.time(),
                    "thread": threading.current_thread().name
                })
            time.sleep(0.1)
            return len(data)

        monkeypatch.setattr("backend.services.db.connector.query", fake_query)
        monkeypatch.setattr("backend.services.db.connector.insert_data", fake_insert)

    def test_concurrent_read_requests(self):
        """Test multiple concurrent read requests."""
        with TestClient(app) as client:
            def make_request(request_id):
                resp = client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": "test_table",
                        "limit": 10,
                        "offset": request_id * 10
                    }
                )
                return resp.status_code, request_id

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(make_request, i) for i in range(10)]
                results = [future.result() for future in as_completed(futures)]

            assert len(results) == 10
            for status_code, _request_id in results:
                assert status_code == 200

            assert len(self.concurrent_calls) == 10

    def test_concurrent_write_requests(self):
        """Test multiple concurrent write requests."""
        with TestClient(app) as client:
            def make_insert(request_id):
                resp = client.post(
                    "/api/v1/records/write",
                    json={
                        "catalog": "test",
                        "schema": "test",
                        "table": "test_table",
                        "data": [{"id": request_id, "name": f"Record_{request_id}"}]
                    }
                )
                return resp.status_code, request_id

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(make_insert, i) for i in range(5)]
                results = [future.result() for future in as_completed(futures)]

            assert len(results) == 5
            for status_code, _ in results:
                assert status_code == 201

    def test_mixed_concurrent_operations(self):
        """Test concurrent mix of read and write operations."""
        with TestClient(app) as client:
            def make_read():
                return client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": "test_table",
                        "limit": 10
                    }
                ).status_code

            def make_write(i):
                return client.post(
                    "/api/v1/records/write",
                    json={
                        "catalog": "test",
                        "schema": "test",
                        "table": "test_table",
                        "data": [{"id": i, "name": f"Record_{i}"}]
                    }
                ).status_code

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = []
                for i in range(4):
                    futures.append(executor.submit(make_read))
                    futures.append(executor.submit(make_write, i))

                results = [future.result() for future in as_completed(futures)]

            assert len(results) == 8
            for status_code in results:
                assert status_code in [200, 201]

    def test_concurrent_records_operations(self):
        """Test concurrent operations on records endpoint."""
        with TestClient(app) as client:
            def read_records(offset):
                return client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": "records",
                        "limit": 10,
                        "offset": offset
                    }
                ).status_code

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(read_records, i * 10) for i in range(5)]
                results = [future.result() for future in as_completed(futures)]

            assert len(results) == 5
            for status_code in results:
                assert status_code == 200

    def test_concurrent_healthcheck_requests(self, mocker):
        """Test that healthcheck can handle many concurrent requests."""
        mock_query = mocker.patch("backend.services.db.connector.query")
        mock_query.return_value = [[1]]

        with TestClient(app) as client:
            def check_health():
                return client.get("/api/v1/healthcheck").status_code

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(check_health) for _ in range(20)]
                results = [future.result() for future in as_completed(futures)]

            assert len(results) == 20
            for status_code in results:
                assert status_code == 200

    def test_concurrent_requests_different_warehouses(self):
        """Test concurrent requests to different warehouses."""
        with TestClient(app) as client:
            def make_request(warehouse_id):
                return client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": f"table_{warehouse_id}",
                        "limit": 10
                    }
                ).status_code

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(make_request, i) for i in range(3)]
                results = [future.result() for future in as_completed(futures)]

            assert all(status == 200 for status in results)

    def test_concurrent_update_operations(self):
        """Test concurrent update operations."""
        with TestClient(app) as client:
            def update_order(order_id):
                return client.put(
                    "/api/v1/records/update",
                    json={
                        "catalog": "test",
                        "schema_name": "test",
                        "table": "records",
                        "key_column": "id",
                        "key_value": order_id,
                        "updates": {"status": "updated"}
                    }
                ).status_code

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(update_order, i) for i in range(1, 6)]
                results = [future.result() for future in as_completed(futures)]

            assert all(status == 200 for status in results)

    def test_concurrent_delete_operations(self):
        """Test concurrent delete operations."""
        with TestClient(app) as client:
            def delete_order(order_id):
                return client.request(
                    "DELETE",
                    "/api/v1/records/delete",
                    json={
                        "catalog": "test",
                        "schema_name": "test",
                        "table": "records",
                        "key_column": "id",
                        "key_value": order_id,
                        "soft": True
                    }
                ).status_code

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(delete_order, i) for i in range(1, 4)]
                results = [future.result() for future in as_completed(futures)]

            assert all(status == 200 for status in results)

    def test_race_condition_on_same_resource(self):
        """Test race condition when multiple requests modify same resource."""
        with TestClient(app) as client:
            def update_same_order(new_value):
                return client.put(
                    "/api/v1/records/update",
                    json={
                        "catalog": "test",
                        "schema_name": "test",
                        "table": "records",
                        "key_column": "id",
                        "key_value": 1,
                        "updates": {"status": f"status_{new_value}"}
                    }
                ).status_code

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(update_same_order, i) for i in range(5)]
                results = [future.result() for future in as_completed(futures)]

            assert all(status == 200 for status in results)

    def test_sequential_vs_concurrent_performance(self):
        """Compare sequential vs concurrent request performance."""
        with TestClient(app) as client:
            def make_request():
                return client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": "test_table",
                        "limit": 10
                    }
                )

            num_requests = 5

            start = time.time()
            for _ in range(num_requests):
                make_request()
            sequential_time = time.time() - start

            start = time.time()
            with ThreadPoolExecutor(max_workers=num_requests) as executor:
                futures = [executor.submit(make_request) for _ in range(num_requests)]
                [future.result() for future in as_completed(futures)]
            concurrent_time = time.time() - start

            assert concurrent_time < sequential_time * 1.5
            assert sequential_time > 0
            assert concurrent_time > 0

    def test_concurrent_requests_thread_safety(self):
        """Verify thread safety of concurrent operations."""
        with TestClient(app) as client:
            errors = []

            def make_request_safe(request_id):
                try:
                    resp = client.get(
                        "/api/v1/records/read",
                        params={
                            "catalog": "test",
                            "schema": "test",
                            "table": "test_table",
                            "limit": 10,
                            "offset": request_id * 10
                        }
                    )
                    return resp.status_code
                except Exception as e:
                    errors.append(str(e))
                    return None

            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [executor.submit(make_request_safe, i) for i in range(15)]
                results = [future.result() for future in as_completed(futures)]

            assert len(errors) == 0
            assert len(errors) == 0
            assert all(status == 200 for status in results if status is not None)



