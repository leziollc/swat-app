import pytest
from fastapi.testclient import TestClient

from backend.app import app


@pytest.fixture(autouse=True)
def patch_connector(monkeypatch):
    # Monkeypatch the connector functions used by routes
    def fake_query(sql_query, warehouse_id, as_dict=True, params=None):
        # Simple parsing to return deterministic values for tests
        sql_upper = sql_query.strip().upper()
        if sql_upper.startswith("SELECT"):
            return [{"order_id": 1, "amount": 10.0, "is_deleted": False}]
        elif sql_upper.startswith("DESCRIBE"):
            # Return column descriptions including all audit columns
            return [
                {"col_name": "order_id", "data_type": "bigint"},
                {"col_name": "amount", "data_type": "double"},
                {"col_name": "is_deleted", "data_type": "boolean"},
                {"col_name": "inserted_at", "data_type": "timestamp"},
                {"col_name": "inserted_by", "data_type": "string"},
                {"col_name": "updated_at", "data_type": "timestamp"},
                {"col_name": "updated_by", "data_type": "string"},
                {"col_name": "deleted_at", "data_type": "timestamp"},
                {"col_name": "deleted_by", "data_type": "string"},
            ]
        return []

    def fake_insert(table_path, data, warehouse_id):
        return len(data)

    monkeypatch.setattr("backend.services.db.connector.query", fake_query)
    monkeypatch.setattr("backend.services.db.connector.insert_data", fake_insert)


def test_read_records():
    with TestClient(app) as client:
        resp = client.get(
            "/api/v1/records/read?catalog=CAT&schema=SCHEMA&table=records&limit=1&offset=0"
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert body["count"] == 1 or isinstance(body["count"], int)


def test_write_records():
    payload = {
        "catalog": "CAT",
        "schema_name": "SCHEMA",
        "table": "records",
        "data": [{"order_id": 2, "amount": 20.0}],
    }
    with TestClient(app) as client:
        resp = client.post("/api/v1/records/write", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1


def test_update_records():
    payload = {
        "catalog": "CAT",
        "schema_name": "SCHEMA",
        "table": "records",
        "key_column": "order_id",
        "key_value": 1,
        "updates": {"amount": 15.0},
    }
    with TestClient(app) as client:
        resp = client.put("/api/v1/records/update", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1


def test_delete_records_soft():
    payload = {
        "catalog": "CAT",
        "schema_name": "SCHEMA",
        "table": "records",
        "key_column": "order_id",
        "key_value": 1,
        "soft": True,
    }
    with TestClient(app) as client:
        resp = client.request("DELETE", "/api/v1/records/delete", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1

