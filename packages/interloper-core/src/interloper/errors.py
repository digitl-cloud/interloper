"""Custom exception hierarchy for the Interloper framework.

All exceptions inherit from `InterloperError`, allowing users to catch
any framework error with a single ``except InterloperError`` clause, or target
specific domains (``DAGError``, ``ConfigError``, etc.) for finer control.

Most domain exceptions also inherit the built-in they refine (e.g.,
``DAGError(InterloperError, ValueError)``), so existing ``except ValueError:``
handlers keep catching them.

Some validation raises built-ins directly: unknown constructor keyword
arguments, a ``FetchField`` provider reference that does not resolve, an
``oauth=`` decorator option on a non-OAuth connection, and multiple
discriminator fields raise ``TypeError``; an invalid key or identifier, a
window ending before it starts, an unsupported granularity, and
``bounded_gather(limit=0)`` raise ``ValueError``; ``Registry[...]`` on a
missing name raises ``KeyError``. Binding a component a relation does not
accept raises ``ConfigError`` instead.
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


class AssetNotFoundError(DAGError, KeyError):
    """An asset key was not found in the DAG."""

    def __str__(self) -> str:
        """The message as raised, without the quotes ``KeyError`` adds.

        Returns:
            The first argument as text, or an empty string when there is none.
        """
        return str(self.args[0]) if self.args else ""


# -- Asset ---------------------------------------------------------------------


class AssetError(InterloperError, ValueError):
    """An error in asset definition, configuration, or execution setup."""


# -- Connection ----------------------------------------------------------------


class ConnectionCheckError(InterloperError):
    """A connection check failed with a curated, user-facing message.

    Raised from ``Connection.check()`` implementations when the failure
    deserves a better message than the generic HTTP-error categorisation.
    """


# -- Partitioning --------------------------------------------------------------


class PartitionError(ConfigError):
    """A partitioning declaration, a partition key or a run's window is invalid."""


# -- Schema / Normalizer -------------------------------------------------------


class SchemaError(InterloperError, ValueError):
    """An error in schema validation, reconciliation, or inference."""


class NormalizerError(InterloperError, TypeError):
    """The normalizer received data that is not ``list[dict]``."""


class RepresentationError(InterloperError, TypeError):
    """No registered representation matches the data's type."""


# -- Destination ---------------------------------------------------------------


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


# -- Catalog -------------------------------------------------------------------


class CatalogKeyError(ConfigError):
    """A component key was not found in the catalog.

    Raised when a source, destination, or resource key referenced in
    the database does not match any registered component definition.
    """


# -- Formatting ----------------------------------------------------------------


def format_exception(exception: BaseException) -> str:
    """Format an exception as a non-empty, single-line error string.

    ``str(exception)`` alone is empty for message-less exceptions (e.g.
    ``httpx2.ReadTimeout``), which downstream consumers (event rows, run
    results, the UI) treat as "no error". Always lead with the type name so
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
