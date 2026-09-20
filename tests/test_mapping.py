from __future__ import annotations

from integration_hub.providers.zoho_crm.mappings import CONTACTS, DEALS


def test_contact_to_canonical_flattens_lookups_and_addresses():
    row = {
        "id": "554023000000123001",
        "First_Name": "Ada",
        "Last_Name": "Lovelace",
        "Email": "Ada@Example.COM",
        "Mailing_City": "London",
        "Account_Name": {"id": "554023000000999001", "name": "Analytical Engines Ltd"},
        "Modified_Time": "2026-02-01T10:15:00+00:00",
        "Custom_Field__c": "keep-me",
    }

    canonical = CONTACTS.to_canonical(row)

    assert canonical["external_id"] == "554023000000123001"
    assert canonical["email"] == "ada@example.com"
    assert canonical["address"]["city"] == "London"
    assert canonical["company_external_id"] == "554023000000999001"
    assert canonical["company_name"] == "Analytical Engines Ltd"
    assert canonical["extras"]["Custom_Field__c"] == "keep-me"
    assert canonical["updated_at"].year == 2026


def test_contact_to_remote_skips_read_only_fields():
    remote = CONTACTS.to_remote(
        {
            "first_name": "Ada",
            "last_name": "Lovelace",
            "email": "ada@example.com",
            "address": {"city": "London"},
            "company_name": "Analytical Engines Ltd",
            "extras": {"Custom_Field__c": "keep-me"},
        }
    )

    assert remote["First_Name"] == "Ada"
    assert remote["Mailing_City"] == "London"
    assert "Account_Name" not in remote  # lookup by name is not writable on Contacts
    assert "Modified_Time" not in remote
    assert remote["Custom_Field__c"] == "keep-me"


def test_deal_lookup_is_written_as_id_reference():
    remote = DEALS.to_remote({"name": "Big Deal", "company_external_id": "554023000000999001"})

    assert remote["Deal_Name"] == "Big Deal"
    assert remote["Account_Name"] == {"id": "554023000000999001"}
