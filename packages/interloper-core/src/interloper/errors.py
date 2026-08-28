"""Custom exception hierarchy for the Interloper framework.

All exceptions inherit from :class:`InterloperError`, allowing users to catch
any framework error with a single ``except InterloperError`` clause, or target
specific domains (``DAGError``, ``ConfigError``, etc.) for finer control.

Each domain exception also inherits from the built-in exception it replaces
(e.g., ``DAGError(InterloperError, ValueError)``), preserving backward
compatibility with existing ``except ValueError:`` handlers.
"""

from __future__ import annotations

from pydantic import ValidationError


class InterloperError(Exception):
    """Base exception for all Interloper framework errors."""


# -- Configuration -------------------------------------------------------------


class ConfigError(InterloperError, ValueError):
    """A configuration value is missing, has the wrong type, or cannot be resolved."""


class SpecError(ConfigError):
    """A component spec document is invalid or cannot be loaded.

    Raised on YAML parse failures, unresolved ``${VAR}`` references, and
    malformed spec documents.
    """


# -- DAG -----------------------------------------------------------------------


class DAGError(InterloperError, ValueError):
    """An error in DAG construction or validation."""


class CircularDependencyError(DAGError):
    """A circular dependency was detected in the DAG."""


class DependencyNotFoundError(DAGError):
    """A referenced dependency is not present in the DAG."""


class AssetNotFoundError(DAGError, KeyError):
    """An asset key was not found in the DAG."""


# -- Asset ---------------------------------------------------------------------


class AssetError(InterloperError, ValueError):
    """An error in asset definition, configuration, or execution setup."""


class DependencyContractError(AssetError):
    """A wired dependency does not match the declared requires contract."""


# -- Source --------------------------------------------------------------------


class SourceError(InterloperError, ValueError):
    """An error in source definition or instantiation."""


# -- Connection ----------------------------------------------------------------


class ConnectionCheckError(InterloperError):
    """A connection check failed with a curated, user-facing message.

    Raised from ``Connection.check()`` implementations when the failure
    deserves a better message than the generic HTTP-error categorisation.
    """


# -- Partitioning --------------------------------------------------------------


class PartitionError(InterloperError, ValueError):
    """An error related to partitioning configuration or constraints."""


# -- Schema / Normalizer -------------------------------------------------------


class SchemaError(InterloperError, ValueError):
    """An error in schema validation, reconciliation, or inference."""


class NormalizerError(InterloperError, TypeError):
    """The normalizer received data it cannot coerce to ``list[dict]``."""


class DestinationError(InterloperError):
    """Base class for destination-related errors."""


class DataNotFoundError(DestinationError):
    """No data was found in the destination backend for the requested key."""


# -- Events --------------------------------------------------------------------


class EventError(InterloperError, ValueError):
    """An error in event deserialization or processing."""


# -- Runner --------------------------------------------------------------------


class RunnerError(InterloperError, RuntimeError):
    """An error in runner orchestration or scheduling."""


# -- REST / Authentication -----------------------------------------------------


class AuthenticationError(InterloperError, ValueError):
    """An authentication or token error in the REST client."""


# -- Lookup / Not Found --------------------------------------------------------


class NotFoundError(InterloperError, KeyError):
    """A database record was not found.

    Raised by the store layer; API routes catch it and return HTTP 404.
    """


class InUseError(InterloperError):
    """A record cannot be deleted because other records still reference it.

    Raised by the store layer; API routes catch it and return HTTP 409.
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


# -- Hydration / Catalog -------------------------------------------------------


class HydrationError(InterloperError):
    """Failed to reconstruct a live object from a database record.

    Raised when a stored spec, config, or resource cannot be
    deserialized back into its framework class (e.g. missing fields,
    unknown import path, validation failure).
    """


class CatalogKeyError(ConfigError):
    """A component key was not found in the catalog.

    Raised when a source, destination, or resource key referenced in
    the database does not match any registered component definition.
    """


class ComponentDriftError(InterloperError):
    """A persisted component references a catalog key that no longer resolves.

    Raised when hydrating a stored source or asset whose catalog key has
    *drifted* — the underlying class was renamed or removed from the code
    (``missing``), or is not exposed by this deployment's catalog
    (``disabled``). Distinct from :class:`HydrationError` (which signals a
    reconstruction failure for a key that *does* resolve) so callers and
    the API layer can treat drift as a recoverable, user-resolvable state
    rather than a hard failure.
    """


# -- Formatting ----------------------------------------------------------------


def format_exception(exception: BaseException) -> str:
    """Format an exception as a non-empty, single-line error string.

    ``str(exception)`` alone is empty for message-less exceptions (e.g.
    ``httpx.ReadTimeout``), which downstream consumers — event rows, run
    results, the UI — treat as "no error". Always lead with the type name so
    the error stays identifiable either way.

    Pydantic's ``ValidationError`` never formats via ``str()``: its string
    embeds each error's ``input_value``, and for a sensitive model (e.g. a
    connection's decrypted payload) that would leak secrets into whatever
    carries the message. It collapses to field-and-reason lines instead.

    Args:
        exception: The exception to format; any ``BaseException``.

    Returns:
        ``"TypeName: message"``, or just ``"TypeName"`` when the message is empty.
    """
    if isinstance(exception, ValidationError):
        details = "; ".join(
            f"{'.'.join(str(loc) for loc in error['loc']) or '(root)'}: {error['msg']}"
            for error in exception.errors(include_url=False, include_input=False)
        )
        return f"ValidationError: {exception.error_count()} validation error(s) for {exception.title}: {details}"
    message = str(exception)
    name = type(exception).__name__
    return f"{name}: {message}" if message else name
