from __future__ import annotations


class IntegrationError(Exception):
    """Base class for every error raised by the integration layer."""

    retryable = False

    def __init__(self, message: str, *, provider: str | None = None, details: object = None):
        super().__init__(message)
        self.message = message
        self.provider = provider
        self.details = details


class ConfigurationError(IntegrationError):
    """Connection is missing configuration or is wired incorrectly."""


class AuthError(IntegrationError):
    """Credentials are missing, expired beyond recovery, or revoked."""


class PermissionError_(IntegrationError):
    """Authenticated but the scope/profile does not allow the operation."""


class RateLimitError(IntegrationError):
    retryable = True

    def __init__(self, message: str, *, retry_after: float | None = None, **kw):
        super().__init__(message, **kw)
        self.retry_after = retry_after


class TransientError(IntegrationError):
    """Network blip or 5xx - safe to retry."""

    retryable = True


class ValidationFailed(IntegrationError):
    """Remote system rejected the payload; retrying will not help."""


class RecordError(IntegrationError):
    """Per-record failure captured during a batch write."""

    def __init__(self, message: str, *, record_id: str | None = None, code: str | None = None, **kw):
        super().__init__(message, **kw)
        self.record_id = record_id
        self.code = code
