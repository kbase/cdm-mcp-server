"""Tests for the main application."""


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/apis/mcp/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


