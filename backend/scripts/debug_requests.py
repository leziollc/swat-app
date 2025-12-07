from fastapi.testclient import TestClient

from backend.app import app

client = TestClient(app)

# Example with auto-create enabled
auto_create_example = {
    "catalog": "my_catalog",
    "schema_name": "my_schema",
    "table": "my_table",
    "auto_create": True,
    "schema_definition": [
        {"name": "id", "data_type": "BIGINT", "nullable": False},
        {"name": "name", "data_type": "STRING", "nullable": False},
        {"name": "amount", "data_type": "DOUBLE", "nullable": True},
        {"name": "created_at", "data_type": "TIMESTAMP", "nullable": True},
        {"name": "created_by", "data_type": "STRING", "nullable": True}
    ],
    "data": [
        {"id": 1, "name": "Example 1", "amount": 100.50},
        {"id": 2, "name": "Example 2", "amount": 200.75}
    ]
}

endpoints = [
    ("GET", "/api/v1/records/read?catalog=CAT&schema=SCHEMA&table=records&limit=1&offset=0", None),
    ("POST", "/api/v1/records/write", {"catalog":"CAT","schema_name":"SCHEMA","table":"records","data":[{"order_id":2,"amount":20.0}]}),
    # Example with auto-create:
    # ("POST", "/api/v1/records/write", auto_create_example),
    ("PUT", "/api/v1/records/update", {"catalog":"CAT","schema_name":"SCHEMA","table":"records","key_column":"order_id","key_value":1,"updates":{"amount":15.0}}),
    ("DELETE", "/api/v1/records/delete", {"catalog":"CAT","schema_name":"SCHEMA","table":"records","key_column":"order_id","key_value":1,"soft":True}),
]

for method, path, body in endpoints:
    print('---', method, path)
    if method == 'GET':
        r = client.get(path)
    elif method == 'POST':
        r = client.post(path, json=body)
    elif method == 'PUT':
        r = client.put(path, json=body)
    elif method == 'DELETE':
        r = client.request('DELETE', path, json=body)
    else:
        continue
    print('status', r.status_code)
    try:
        print('json', r.json())
    except Exception:
        print('text', r.text)

print('done')

