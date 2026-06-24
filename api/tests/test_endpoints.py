import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)
HEADERS = {"X-API-Key": "healthlakehouse2024"}


def test_health_check():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_auth_required():
    resp = client.get("/api/v1/diseases/trend")
    assert resp.status_code == 401


def test_disease_trend():
    resp = client.get(
        "/api/v1/diseases/trend?indicator_code=WHS4_100&limit=5", headers=HEADERS
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["count"] > 0


def test_vaccination_coverage():
    resp = client.get("/api/v1/vaccination/coverage?limit=10", headers=HEADERS)
    assert resp.status_code == 200
    assert resp.json()["count"] > 0


def test_outbreak_alerts():
    resp = client.get("/api/v1/outbreaks/alerts?limit=10", headers=HEADERS)
    assert resp.status_code == 200


def test_time_travel_invalid_table():
    resp = client.get(
        "/api/v1/query/time-travel?table=bronze.secret_table", headers=HEADERS
    )
    assert resp.status_code == 400
