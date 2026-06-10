"""Custom exception hierarchy for the Interloper framework.

All exceptions inherit from :class:`InterloperError`, allowing users to catch
any framework error with a single ``except InterloperError`` clause, or target
specific domains (``DAGError``, ``ConfigError``, etc.) for finer control.

Each domain exception also inherits from the built-in exception it replaces
(e.g., ``DAGError(InterloperError, ValueError)``), preserving backward
compatibility with existing ``except ValueError:`` handlers.
"""

from __future__ import annotations


class InterloperError(Exception):
    """Base exception for all Interloper framework errors."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class ConfigError(InterloperError, ValueError):
    """A configuration value is missing, has the wrong type, or cannot be resolved."""


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


class DAGError(InterloperError, ValueError):
    """An error in DAG construction or validation."""


class CircularDependencyError(DAGError):
    """A circular dependency was detected in the DAG."""


class DependencyNotFoundError(DAGError):
    """A referenced dependency is not present in the DAG."""


class AssetNotFoundError(DAGError, KeyError):
    """An asset key was not found in the DAG."""


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------


class AssetError(InterloperError, ValueError):
    """An error in asset definition, configuration, or execution setup."""


class DependencyContractError(AssetError):
    """A wired dependency does not match the declared requires contract."""


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class SourceError(InterloperError, ValueError):
    """An error in source definition or instantiation."""


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------


class PartitionError(InterloperError, ValueError):
    """An error related to partitioning configuration or constraints."""


# ---------------------------------------------------------------------------
# Schema / Normalizer
# ---------------------------------------------------------------------------


class SchemaError(InterloperError, ValueError):
    """An error in schema validation, reconciliation, or inference."""


class NormalizerError(InterloperError, TypeError):
    """The normalizer received data it cannot coerce to ``list[dict]``."""


class RepresentationError(InterloperError, TypeError):
    """No data representation is registered for the requested key."""


# ---------------------------------------------------------------------------
# Destination
# ---------------------------------------------------------------------------


class DestinationError(InterloperError):
    """Base class for destination-related errors."""


class DataNotFoundError(DestinationError):
    """No data was found in the destination backend for the requested key."""


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


class EventError(InterloperError, ValueError):
    """An error in event deserialization or processing."""


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class RunnerError(InterloperError, RuntimeError):
    """An error in runner orchestration or scheduling."""


# ---------------------------------------------------------------------------
# REST / Authentication
# ---------------------------------------------------------------------------


class AuthenticationError(InterloperError, ValueError):
    """An authentication or token error in the REST client."""


# ---------------------------------------------------------------------------
# Lookup / Not Found
# ---------------------------------------------------------------------------


class NotFoundError(InterloperError, KeyError):
    """A database record was not found.

    Base class for entity-specific not-found errors used by the store
    layer.  API routes catch these and return HTTP 404.
    """


class SourceNotFoundError(NotFoundError):
    """A source record was not found."""


class JobNotFoundError(NotFoundError):
    """A job record was not found."""


class RunNotFoundError(NotFoundError):
    """A run record was not found."""


class ResourceNotFoundError(NotFoundError):
    """A resource record was not found."""


# ---------------------------------------------------------------------------
# Hydration / Catalog
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------


class SchedulingError(RunnerError):
    """An error in cron evaluation, queue polling, or run dispatch."""


# ---------------------------------------------------------------------------
# External Providers
# ---------------------------------------------------------------------------


class ExternalProviderError(InterloperError):
    """An external API call failed (e.g. Amazon Ads, Google Ads)."""


class ProviderAuthError(ExternalProviderError, AuthenticationError):
    """Authentication with an external provider failed (invalid/expired token)."""


class ProviderRateLimitError(ExternalProviderError):
    """An external provider returned a rate-limit response (retriable)."""
