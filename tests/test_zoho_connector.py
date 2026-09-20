from __future__ import annotations

import httpx
import pytest
import respx

from integration_hub.core.base import Cursor, WriteMode
from integration_hub.providers.zoho_crm.connector import ZohoCrmConnector

API = "https://www.zohoapis.com/crm/v8"


@pytest.fixture
def connector(ctx):
    return ZohoCrmConnector(ctx)


@respx.mock
async def test_read_paginates_and_advances_cursor(connector):
    respx.get(f"{API}/Contacts").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [{"id": "1", "First_Name": "A", "Modified_Time": "2026-02-01T10:00:00+00:00"}],
                    "info": {"more_records": True, "next_page_token": "tok-2"},
                },
            ),
            httpx.Response(
                200,
                json={
                    "data": [{"id": "2", "First_Name": "B", "Modified_Time": "2026-02-02T10:00:00+00:00"}],
                    "info": {"more_records": False},
                },
            ),
        ]
    )
    respx.get(f"{API}/Contacts/deleted").mock(return_value=httpx.Response(200, json={"data": []}))

    pages = [page async for page in connector.read("contacts", Cursor(), page_size=200)]

    assert [r.external_id for p in pages for r in p.records] == ["1", "2"]
    assert pages[0].has_more is True
    assert pages[-1].cursor.since.isoformat() == "2026-02-02T10:00:00+00:00"


@respx.mock
async def test_upsert_maps_outcomes(connector):
    respx.post(f"{API}/Contacts/upsert").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {"code": "SUCCESS", "status": "success", "action": "insert", "details": {"id": "99"}},
                    {
                        "code": "DUPLICATE_DATA",
                        "status": "error",
                        "message": "duplicate data",
                        "details": {"api_name": "Email"},
                    },
                ]
            },
        )
    )

    result = await connector.write(
        "contacts",
        [{"first_name": "A", "email": "a@x.com"}, {"first_name": "B", "email": "a@x.com"}],
        mode=WriteMode.UPSERT,
    )

    assert result.succeeded == 1
    assert result.failed == 1
    assert result.outcomes[0].external_id == "99"
    assert "DUPLICATE_DATA" in result.outcomes[1].error


@respx.mock
async def test_read_by_ids_used_for_webhook_hydration(connector):
    route = respx.get(f"{API}/Contacts").mock(
        return_value=httpx.Response(200, json={"data": [{"id": "7", "First_Name": "W"}]})
    )

    pages = [
        page
        async for page in connector.read("contacts", Cursor(extra={"ids": ["7"]}), page_size=1)
    ]

    assert route.calls.last.request.url.params["ids"] == "7"
    assert pages[0].records[0].external_id == "7"


def test_parse_webhook_rejects_bad_token(connector):
    from integration_hub.core.errors import ValidationFailed

    with pytest.raises(ValidationFailed):
        connector.parse_webhook({}, {"token": "wrong", "module": "Contacts", "ids": ["1"]})


def test_parse_webhook_emits_events(connector):
    events = connector.parse_webhook(
        {},
        {
            "token": "conn-1",
            "channel_id": "123",
            "module": "Contacts",
            "operation": "edit",
            "ids": ["1", "2"],
            "server_time": "2026-02-01T10:00:00+00:00",
        },
    )

    assert [e.external_id for e in events] == ["1", "2"]
    assert {e.operation for e in events} == {"update"}
    assert len({e.dedupe_key for e in events}) == 2
