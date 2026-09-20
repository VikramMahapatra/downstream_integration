from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from integration_hub.core.errors import ConfigurationError

Transform = Callable[[Any], Any]

_TRANSFORMS: dict[str, Transform] = {}


def transform(name: str) -> Callable[[Transform], Transform]:
    def deco(fn: Transform) -> Transform:
        _TRANSFORMS[name] = fn
        return fn

    return deco


def get_transform(name: str) -> Transform:
    try:
        return _TRANSFORMS[name]
    except KeyError as exc:
        raise ConfigurationError(f"Unknown transform '{name}'") from exc


# --- built-in transforms -------------------------------------------------


@transform("str")
def _to_str(v: Any) -> Any:
    return None if v is None else str(v)


@transform("float")
def _to_float(v: Any) -> Any:
    if v in (None, ""):
        return None
    return float(v)


@transform("int")
def _to_int(v: Any) -> Any:
    if v in (None, ""):
        return None
    return int(float(v))


@transform("lower")
def _lower(v: Any) -> Any:
    return v.lower() if isinstance(v, str) else v


@transform("strip")
def _strip(v: Any) -> Any:
    return v.strip() if isinstance(v, str) else v


@transform("datetime")
def _to_datetime(v: Any) -> Any:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    text = str(v).replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@transform("lookup_id")
def _lookup_id(v: Any) -> Any:
    """Zoho-style lookup fields arrive as {"id": "...", "name": "..."}."""
    if isinstance(v, dict):
        return v.get("id")
    return v


@transform("lookup_name")
def _lookup_name(v: Any) -> Any:
    if isinstance(v, dict):
        return v.get("name")
    return v


@transform("lookup_ref")
def _lookup_ref(v: Any) -> Any:
    """Outbound counterpart: a related-record id must be sent as {"id": ...}."""
    if v is None or isinstance(v, dict):
        return v
    return {"id": str(v)}


# --- path helpers --------------------------------------------------------


def get_path(data: Any, path: str) -> Any:
    cur = data
    for part in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            cur = getattr(cur, part, None)
    return cur


def set_path(data: dict, path: str, value: Any) -> None:
    parts = path.split(".")
    cur = data
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value


class FieldMap:
    """One declarative field rule, usable in both directions.

    `remote` is the provider field path, `canonical` the hub field path.
    """

    __slots__ = ("remote", "canonical", "to_canonical", "to_remote", "writable", "const")

    def __init__(
        self,
        remote: str,
        canonical: str,
        *,
        to_canonical: str | Transform | None = None,
        to_remote: str | Transform | None = None,
        writable: bool = True,
        const: Any = None,
    ):
        self.remote = remote
        self.canonical = canonical
        self.to_canonical = self._resolve(to_canonical)
        self.to_remote = self._resolve(to_remote)
        self.writable = writable
        self.const = const

    @staticmethod
    def _resolve(t: str | Transform | None) -> Transform | None:
        if t is None:
            return None
        return get_transform(t) if isinstance(t, str) else t


class ObjectMapping:
    """Bidirectional mapping between one provider module and one canonical object."""

    def __init__(
        self,
        *,
        remote_object: str,
        fields: list[FieldMap],
        id_field: str = "id",
        updated_field: str | None = None,
        passthrough_extras: bool = True,
    ):
        self.remote_object = remote_object
        self.fields = fields
        self.id_field = id_field
        self.updated_field = updated_field
        self.passthrough_extras = passthrough_extras

    def to_canonical(self, record: dict) -> dict:
        out: dict[str, Any] = {}
        mapped_remote = {f.remote.split(".")[0] for f in self.fields}
        for f in self.fields:
            value = get_path(record, f.remote)
            if f.to_canonical:
                value = f.to_canonical(value)
            if value is not None:
                set_path(out, f.canonical, value)
        if self.passthrough_extras:
            extras = {
                k: v
                for k, v in record.items()
                if k not in mapped_remote and k != self.id_field and not k.startswith("$")
            }
            if extras:
                out.setdefault("extras", {}).update(extras)
        out.setdefault("external_id", record.get(self.id_field))
        return out

    def to_remote(self, canonical: dict) -> dict:
        out: dict[str, Any] = {}
        for f in self.fields:
            if not f.writable:
                continue
            if f.const is not None:
                set_path(out, f.remote, f.const)
                continue
            value = get_path(canonical, f.canonical)
            if value is None:
                continue
            if f.to_remote:
                value = f.to_remote(value)
            set_path(out, f.remote, value)
        for k, v in (canonical.get("extras") or {}).items():
            out.setdefault(k, v)
        return out
