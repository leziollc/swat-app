from fastapi.testclient import TestClient
from backend.app import app

client = TestClient(app)

endpoints = [
    ("GET", "/api/v1/orders/read?catalog=CAT&schema=SCHEMA&table=orders&limit=1&offset=0", None),
    ("POST", "/api/v1/orders/write", {"catalog":"CAT","schema_name":"SCHEMA","table":"orders","data":[{"order_id":2,"amount":20.0}]}),
    ("PUT", "/api/v1/orders/update", {"catalog":"CAT","schema_name":"SCHEMA","table":"orders","key_column":"order_id","key_value":1,"updates":{"amount":15.0}}),
    ("DELETE", "/api/v1/orders/delete", {"catalog":"CAT","schema_name":"SCHEMA","table":"orders","key_column":"order_id","key_value":1,"soft":True}),
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
