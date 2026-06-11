"""Field helpers for annotating resource fields with UI widget hints.

Each helper is a thin wrapper around ``pydantic.Field`` that injects
``x-widget`` (and optionally other ``x-*`` keys) into ``json_schema_extra``.
All standard Pydantic ``Field`` kwargs (``title``, ``description``, etc.)
are forwarded transparently.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

# Internal fields that should be stripped from config schemas exposed
# to the UI.  These are framework plumbing, not user-configurable.
_INTERNAL_FIELDS = frozenset({"resources"})


def strip_internal_fields(schema: dict[str, Any]) -> dict[str, Any]:
    """Remove internal framework fields from a JSON Schema.

    Strips properties listed in ``_INTERNAL_FIELDS`` and adjusts the
    ``required`` list accordingly.  Returns a shallow copy; the original
    schema is not mutated.

    Args:
        schema: A JSON Schema dict (from ``model_json_schema()``).

    Returns:
        A copy of the schema without internal fields.
    """
    properties = schema.get("properties", {})
    if not _INTERNAL_FIELDS & properties.keys():
        return schema
    kept = {name: prop for name, prop in properties.items() if name not in _INTERNAL_FIELDS}
    filtered = {**schema, "properties": kept}
    if "required" in filtered:
        filtered["required"] = [r for r in filtered["required"] if r not in _INTERNAL_FIELDS]
        if not filtered["required"]:
            del filtered["required"]
    return filtered


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extra(kwargs: dict[str, Any], widget: str) -> dict[str, Any]:
    """Pop json_schema_extra from kwargs and inject the widget hint.

    Returns:
        The extra dict with ``x-widget`` set.
    """
    extra = kwargs.pop("json_schema_extra", {})
    extra["x-widget"] = widget
    return extra


# ---------------------------------------------------------------------------
# Field factories
# ---------------------------------------------------------------------------


def InputField(default: Any = ..., **kwargs: Any) -> Any:
    """String field rendered as a standard text input.

    Args:
        default: Default value (``...`` means required).
        **kwargs: Forwarded to ``pydantic.Field``.

    Returns:
        A Pydantic Field descriptor.

    Example::

        account_id: str = InputField(description="Your account ID")
    """
    return Field(default, json_schema_extra=_extra(kwargs, "text"), **kwargs)


def SecretField(default: Any = ..., **kwargs: Any) -> Any:
    """String field rendered as a password input.

    Args:
        default: Default value (``...`` means required).
        **kwargs: Forwarded to ``pydantic.Field``.

    Returns:
        A Pydantic Field descriptor.

    Example::

        api_key: str = SecretField(description="API key")
    """
    return Field(default, json_schema_extra=_extra(kwargs, "password"), **kwargs)


def TextField(default: Any = ..., **kwargs: Any) -> Any:
    """String field rendered as a multi-line textarea.

    Args:
        default: Default value (``...`` means required).
        **kwargs: Forwarded to ``pydantic.Field``.

    Returns:
        A Pydantic Field descriptor.

    Example::

        query: str = TextField(description="SQL query")
    """
    return Field(default, json_schema_extra=_extra(kwargs, "textarea"), **kwargs)


def JsonField(default: Any = ..., **kwargs: Any) -> Any:
    """Dict/object field rendered as a JSON code editor.

    Args:
        default: Default value (``...`` means required).
        **kwargs: Forwarded to ``pydantic.Field``.

    Returns:
        A Pydantic Field descriptor.

    Example::

        config: dict = JsonField(default_factory=dict)
    """
    return Field(default, json_schema_extra=_extra(kwargs, "json"), **kwargs)


def SelectField(
    default: Any = ...,
    *,
    options: list[dict[str, str]] | None = None,
    options_from: str | None = None,
    **kwargs: Any,
) -> Any:
    """String field rendered as a dropdown select.

    Options can be provided statically via ``options``, or resolved
    dynamically at render time via ``options_from``.

    Args:
        default: Default value (``...`` means required).
        options: Static list of ``{"label": "...", "value": "..."}`` dicts.
        options_from: Entity whose configured instances provide the options
            (e.g. ``"destinations"``). Serialized as ``x-options-from`` in
            the JSON Schema so the frontend can resolve options from context.
        **kwargs: Forwarded to ``pydantic.Field``.

    Returns:
        A Pydantic Field descriptor.

    Example::

        region: str = SelectField(
            options=[
                {"label": "US", "value": "us"},
                {"label": "EU", "value": "eu"},
            ],
        )
    """
    extra = _extra(kwargs, "select")
    if options is not None:
        extra["x-options"] = options
    if options_from is not None:
        extra["x-options-from"] = options_from
    return Field(default, json_schema_extra=extra, **kwargs)


def FetchField(
    default: Any = ...,
    *,
    endpoint: str,
    depends_on: str | list[str] = "connection",
    label_key: str = "name",
    value_key: str = "id",
    **kwargs: Any,
) -> Any:
    """Field whose options are fetched from an external API at runtime.

    The frontend reads ``x-fetch`` from the JSON Schema and renders a
    select/autocomplete that calls the daemon's ``/external/{endpoint}``
    route, passing credentials from the resource referenced by
    ``depends_on``.

    Args:
        default: Default value (``...`` means required).
        endpoint: Path under ``/external/`` (e.g. ``"amazon-ads/profiles"``).
        depends_on: Resource slot name(s) whose data is sent as the
            request body.  A string for a single dependency or a list
            for multiple.
        label_key: Key in each response item used as the display label.
        value_key: Key in each response item used as the stored value.
        **kwargs: Forwarded to ``pydantic.Field``.

    Returns:
        A Pydantic Field descriptor.

    Example::

        profile_id: str = FetchField(
            endpoint="amazon-ads/profiles",
            depends_on="connection",
            label_key="name",
            value_key="profile_id",
        )
    """
    extra = _extra(kwargs, "fetch")
    extra["x-fetch"] = {
        "endpoint": endpoint,
        "depends_on": [depends_on] if isinstance(depends_on, str) else depends_on,
        "label_key": label_key,
        "value_key": value_key,
    }
    return Field(default, json_schema_extra=extra, **kwargs)
