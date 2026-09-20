from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ObjectType(StrEnum):
    """Canonical object types the hub understands. Providers map their modules onto these."""

    CONTACT = "contact"
    COMPANY = "company"
    LEAD = "lead"
    DEAL = "deal"
    PRODUCT = "product"
    NOTE = "note"
    TASK = "task"
    USER = "user"


class CanonicalBase(BaseModel):
    """Provider-neutral shape. `extras` keeps everything we do not model explicitly."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    external_id: str | None = None
    updated_at: datetime | None = None
    created_at: datetime | None = None
    owner_email: str | None = None
    extras: dict[str, Any] = Field(default_factory=dict)


class Address(BaseModel):
    model_config = ConfigDict(extra="forbid")

    street: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str | None = None


class Contact(CanonicalBase):
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    mobile: str | None = None
    title: str | None = None
    company_external_id: str | None = None
    company_name: str | None = None
    address: Address | None = None
    description: str | None = None


class Company(CanonicalBase):
    name: str | None = None
    domain: str | None = None
    phone: str | None = None
    industry: str | None = None
    employee_count: int | None = None
    annual_revenue: float | None = None
    address: Address | None = None
    description: str | None = None


class Lead(CanonicalBase):
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    company_name: str | None = None
    status: str | None = None
    source: str | None = None
    description: str | None = None


class Deal(CanonicalBase):
    name: str | None = None
    amount: float | None = None
    currency: str | None = None
    stage: str | None = None
    pipeline: str | None = None
    probability: float | None = None
    close_date: str | None = None
    company_external_id: str | None = None
    contact_external_id: str | None = None
    description: str | None = None


CANONICAL_MODELS: dict[ObjectType, type[CanonicalBase]] = {
    ObjectType.CONTACT: Contact,
    ObjectType.COMPANY: Company,
    ObjectType.LEAD: Lead,
    ObjectType.DEAL: Deal,
}
