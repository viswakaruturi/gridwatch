
import pytest
from fastapi.testclient import TestClient

from api.main import app


client = TestClient(app)


@pytest.mark.parametrize(
    "path",
    [
        "/health",
        "/api/respondents",
        "/api/demand/TEPC?limit=5",
        "/api/forecast-accuracy/TEPC",
        "/api/grid-operations/TEPC?limit=5",
        "/api/data-quality/TEPC",
    ],
)
def test_endpoints_return_success(path):
    response = client.get(path)

    assert response.status_code == 200


def test_health_response():
    response = client.get("/health")
    data = response.json()

    assert data["status"] == "healthy"
    assert data["database"] == "connected"


def test_demand_pagination():
    response = client.get("/api/demand/TEPC?limit=5&offset=0")
    data = response.json()

    assert response.status_code == 200
    assert data["count"] == 5
    assert len(data["data"]) == 5


def test_unknown_respondent():
    response = client.get("/api/demand/INVALID")

    assert response.status_code == 404


def test_invalid_limit():
    response = client.get("/api/demand/TEPC?limit=2000")

    assert response.status_code == 422


def test_invalid_date_range():
    response = client.get(
        "/api/demand/TEPC"
        "?start_date=2026-11-22"
        "&end_date=2026-11-20"
    )

    assert response.status_code == 400
