from __future__ import annotations

from integration_hub.core.canonical import ObjectType
from integration_hub.core.mapping import FieldMap, ObjectMapping

_COMMON = [
    FieldMap("Modified_Time", "updated_at", to_canonical="datetime", writable=False),
    FieldMap("Created_Time", "created_at", to_canonical="datetime", writable=False),
    FieldMap("Owner", "owner_email", to_canonical="lookup_name", writable=False),
]

CONTACTS = ObjectMapping(
    remote_object="Contacts",
    id_field="id",
    updated_field="Modified_Time",
    fields=[
        FieldMap("First_Name", "first_name"),
        FieldMap("Last_Name", "last_name"),
        FieldMap("Email", "email", to_canonical="lower"),
        FieldMap("Phone", "phone"),
        FieldMap("Mobile", "mobile"),
        FieldMap("Title", "title"),
        FieldMap("Account_Name", "company_external_id", to_canonical="lookup_id", writable=False),
        FieldMap("Account_Name", "company_name", to_canonical="lookup_name", writable=False),
        FieldMap("Mailing_Street", "address.street"),
        FieldMap("Mailing_City", "address.city"),
        FieldMap("Mailing_State", "address.state"),
        FieldMap("Mailing_Zip", "address.postal_code"),
        FieldMap("Mailing_Country", "address.country"),
        FieldMap("Description", "description"),
        *_COMMON,
    ],
)

ACCOUNTS = ObjectMapping(
    remote_object="Accounts",
    id_field="id",
    updated_field="Modified_Time",
    fields=[
        FieldMap("Account_Name", "name"),
        FieldMap("Website", "domain"),
        FieldMap("Phone", "phone"),
        FieldMap("Industry", "industry"),
        FieldMap("Employees", "employee_count", to_canonical="int", to_remote="int"),
        FieldMap("Annual_Revenue", "annual_revenue", to_canonical="float", to_remote="float"),
        FieldMap("Billing_Street", "address.street"),
        FieldMap("Billing_City", "address.city"),
        FieldMap("Billing_State", "address.state"),
        FieldMap("Billing_Code", "address.postal_code"),
        FieldMap("Billing_Country", "address.country"),
        FieldMap("Description", "description"),
        *_COMMON,
    ],
)

LEADS = ObjectMapping(
    remote_object="Leads",
    id_field="id",
    updated_field="Modified_Time",
    fields=[
        FieldMap("First_Name", "first_name"),
        FieldMap("Last_Name", "last_name"),
        FieldMap("Email", "email", to_canonical="lower"),
        FieldMap("Phone", "phone"),
        FieldMap("Company", "company_name"),
        FieldMap("Lead_Status", "status"),
        FieldMap("Lead_Source", "source"),
        FieldMap("Description", "description"),
        *_COMMON,
    ],
)

DEALS = ObjectMapping(
    remote_object="Deals",
    id_field="id",
    updated_field="Modified_Time",
    fields=[
        FieldMap("Deal_Name", "name"),
        FieldMap("Amount", "amount", to_canonical="float", to_remote="float"),
        FieldMap("Currency", "currency"),
        FieldMap("Stage", "stage"),
        FieldMap("Pipeline", "pipeline", to_canonical="lookup_name"),
        FieldMap("Probability", "probability", to_canonical="float", to_remote="float"),
        FieldMap("Closing_Date", "close_date"),
        FieldMap("Account_Name", "company_external_id", to_canonical="lookup_id", to_remote="lookup_ref"),
        FieldMap("Contact_Name", "contact_external_id", to_canonical="lookup_id", to_remote="lookup_ref"),
        FieldMap("Description", "description"),
        *_COMMON,
    ],
)

MAPPINGS: dict[str, ObjectMapping] = {
    "contacts": CONTACTS,
    "accounts": ACCOUNTS,
    "leads": LEADS,
    "deals": DEALS,
}

OBJECT_TYPES: dict[str, ObjectType] = {
    "contacts": ObjectType.CONTACT,
    "accounts": ObjectType.COMPANY,
    "leads": ObjectType.LEAD,
    "deals": ObjectType.DEAL,
}

# Fields Zoho can use to resolve an existing record during /upsert.
DEDUPE_FIELDS: dict[str, list[str]] = {
    "contacts": ["Email"],
    "accounts": ["Account_Name"],
    "leads": ["Email"],
    "deals": ["Deal_Name"],
}
