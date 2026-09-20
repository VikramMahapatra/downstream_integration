from __future__ import annotations

import importlib
import pkgutil
from typing import Type

from integration_hub.core.base import Connector
from integration_hub.core.errors import ConfigurationError

_REGISTRY: dict[str, Type[Connector]] = {}
_OAUTH_PROVIDERS: dict[str, object] = {}


def register_connector(cls: Type[Connector]) -> Type[Connector]:
    if not cls.provider_key:
        raise ConfigurationError(f"{cls.__name__} must define provider_key")
    _REGISTRY[cls.provider_key] = cls
    return cls


def register_oauth_provider(key: str, handler: object) -> None:
    _OAUTH_PROVIDERS[key] = handler


def get_connector_class(provider: str) -> Type[Connector]:
    load_providers()
    try:
        return _REGISTRY[provider]
    except KeyError as exc:
        raise ConfigurationError(
            f"Unknown provider '{provider}'. Available: {sorted(_REGISTRY)}"
        ) from exc


def get_oauth_provider(provider: str):
    load_providers()
    try:
        return _OAUTH_PROVIDERS[provider]
    except KeyError as exc:
        raise ConfigurationError(f"Provider '{provider}' does not support OAuth") from exc


def list_providers() -> list[dict]:
    load_providers()
    return [
        {
            "provider": key,
            "display_name": cls.display_name,
            "capabilities": sorted(cls.capabilities),
            "oauth": key in _OAUTH_PROVIDERS,
        }
        for key, cls in sorted(_REGISTRY.items())
    ]


_loaded = False


def load_providers() -> None:
    """Import every package under integration_hub.providers so decorators run."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    import integration_hub.providers as providers_pkg

    for mod in pkgutil.iter_modules(providers_pkg.__path__):
        importlib.import_module(f"{providers_pkg.__name__}.{mod.name}")
