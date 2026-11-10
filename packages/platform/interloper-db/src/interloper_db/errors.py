"""Errors the platform's persistence raises and its services map to responses.

Each still subclasses the framework's `InterloperError`, so one ``except
InterloperError`` catches them along with the framework's own.
"""

from __future__ import annotations

from interloper.errors import InterloperError


class NotFoundError(InterloperError, KeyError):
    """A database record was not found.

    Raised by the store layer; the API answers HTTP 404.
    """

    def __str__(self) -> str:
        """The message as raised, without the quotes ``KeyError`` adds.

        Returns:
            The first argument as text, or an empty string when there is none.
        """
        return str(self.args[0]) if self.args else ""


class ConflictError(InterloperError, ValueError):
    """A record's current state does not allow the requested operation.

    Raised by the store layer for a request that was well-formed but arrived
    at the wrong moment (a run already terminal, a stack whose latest attempt
    did not fail); the API answers HTTP 409.
    """


class InUseError(ConflictError):
    """A record cannot be deleted because other records still reference it.

    Raised by the store layer; the API answers HTTP 409 with the referrers.
    ``referrers`` carries the referencing records as ``{id, kind, key, name}``
    mappings so callers can build structured error payloads.
    """

    def __init__(self, message: str, referrers: list[dict[str, str | None]] | None = None) -> None:
        """Initialize with a user-facing message and the referencing records.

        Args:
            message: User-facing explanation of the conflict.
            referrers: The referencing records as ``{id, kind, key, name}``
                mappings; ``None`` is stored as an empty list.
        """
        super().__init__(message)
        self.referrers = referrers or []


class QuotaExceededError(InterloperError):
    """An organisation quota does not allow the requested operation.

    Raised by the store layer; API routes surface it as HTTP 429 with the
    structured fields so clients can show limit and usage.
    """

    def __init__(self, message: str, *, quota: str, limit: int, used: int) -> None:
        """Initialize with a user-facing message and the quota context.

        Args:
            message: User-facing explanation of the refusal.
            quota: Name of the exceeded quota (e.g. ``"max_sources"``).
            limit: The quota's limit.
            used: Usage already counted against the limit.
        """
        super().__init__(message)
        self.quota = quota
        self.limit = limit
        self.used = used


class HydrationError(InterloperError):
    """Failed to reconstruct a live object from a database record.

    Raised when a stored spec, config, or resource cannot be
    deserialized back into its framework class (e.g. missing fields,
    unknown import path, validation failure).
    """


class ComponentDriftError(InterloperError):
    """A persisted component references a catalog key that no longer resolves.

    Raised when hydrating a stored source or asset whose catalog key has
    *drifted*: the underlying class was renamed or removed from the code
    (``missing``), or is not exposed by this deployment's catalog
    (``disabled``). Distinct from `HydrationError` (which signals a
    reconstruction failure for a key that *does* resolve) so callers and
    the API layer can treat drift as a recoverable, user-resolvable state
    rather than a hard failure.
    """
