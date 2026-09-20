from __future__ import annotations

from fastapi.testclient import TestClient

from integration_hub.main import app


def test_providers_and_connection_lifecycle():
    with TestClient(app) as client:
        providers = client.get("/v1/providers").json()
        assert any(p["provider"] == "zoho_crm" and p["oauth"] for p in providers)

        streams = client.get("/v1/providers/zoho_crm/streams").json()
        assert {s["name"] for s in streams} == {"contacts", "accounts", "leads", "deals"}

        created = client.post(
            "/v1/connections",
            json={
                "tenant_id": "acme",
                "provider": "zoho_crm",
                "name": "Acme Zoho Prod",
                "config": {"dc": "com"},
            },
        )
        assert created.status_code == 201
        connection_id = created.json()["id"]
        assert created.json()["status"] == "pending_auth"

        configured = client.put(
            f"/v1/connections/{connection_id}/streams",
            json=[{"stream": "contacts", "schedule_seconds": 300}],
        )
        assert configured.status_code == 200
        assert configured.json()[0]["stream"] == "contacts"

        authorize = client.get(f"/v1/oauth/zoho_crm/authorize?connection_id={connection_id}").json()
        assert authorize["authorize_url"].startswith("https://accounts.zoho.com/oauth/v2/auth?")
        assert "access_type=offline" in authorize["authorize_url"]

        assert client.delete(f"/v1/connections/{connection_id}").status_code == 204
