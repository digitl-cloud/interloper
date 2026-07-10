"""Field helpers for annotating resource fields with UI widget hints.

Each helper is a thin wrapper around ``pydantic.Field`` that injects
``x-widget`` (and optionally other ``x-*`` keys) into ``json_schema_extra``.
All standard Pydantic ``Field`` kwargs (``title``, ``description``, etc.)
are forwarded transparently.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, TypeVar

from pydantic import BaseModel, Field

F = TypeVar("F", bound=Callable[..., Any])


# -- Fetch providers -----------------------------------------------------------


#: Attribute stamped on a method by :func:`fetch_field_provider`.
FETCH_FIELD_PROVIDER_ATTR = "__is_fetch_field_provider__"


def fetch_field_provider(fn: F) -> F:
    """Mark a resource method as a :func:`FetchField` data source.

    A ``FetchField(provider="<slot>.<method>")`` resolves its options by
    instantiating the resource in slot ``<slot>`` (from the credentials the
    form already holds) and calling the method named ``<method>``. Only
    methods marked with this decorator may be invoked that way — it is the
    allowlist that stops the browser from calling arbitrary attributes.

    The method runs inside the API process, which installs the connection
    *classes* but not their heavy provider SDK extras, so a provider must
    use lightweight HTTP (``httpx``), never ``self.api`` (the SDK client).

    It returns a ``list[dict]``; the ``FetchField`` picks ``label_key`` /
    ``value_key`` out of each item::

        class FacebookAdsConnection(il.Connection):
            @il.fetch_field_provider
            async def accounts(self) -> list[dict]:
                ...

    Returns:
        The same callable, stamped as a fetch provider.
    """
    setattr(fn, FETCH_FIELD_PROVIDER_ATTR, True)
    return fn


def is_fetch_field_provider(obj: Any) -> bool:
    """Return whether *obj* (a function or bound method) is a fetch provider."""
    return bool(getattr(obj, FETCH_FIELD_PROVIDER_ATTR, False))


def validate_fetch_field_providers(cls: type[BaseModel], res_types: dict[str, Any]) -> None:
    """Check every ``FetchField(provider=...)`` on *cls* resolves to a provider.

    For each field carrying an ``x-fetch``, the provider ``"<slot>.<method>"``
    must name a declared resource slot whose resource class exposes
    ``<method>`` marked with :func:`fetch_field_provider`. Fails loudly at
    catalog-build time rather than letting the form silently fail at runtime.

    Args:
        cls: The component class being defined (source or destination).
        res_types: The component's ``resource_types`` (slot → resource class).

    Raises:
        TypeError: If a provider reference is malformed, names an unknown
            slot, or targets a method that is not a ``@fetch_field_provider``.
    """
    for field_name, field in cls.model_fields.items():
        extra = field.json_schema_extra
        if not isinstance(extra, dict):
            continue
        fetch = extra.get("x-fetch")
        if not isinstance(fetch, dict):
            continue
        provider = fetch.get("provider")
        slot, _, method = str(provider).partition(".")
        if not slot or not method:
            raise TypeError(
                f"{cls.__name__}.{field_name}: FetchField provider '{provider}' must be of the form '<slot>.<method>'"
            )
        res_cls = res_types.get(slot)
        if res_cls is None:
            raise TypeError(
                f"{cls.__name__}.{field_name}: FetchField provider '{provider}' "
                f"references resource slot '{slot}', which is not declared in resources={{}}"
            )
        target = getattr(res_cls, method, None)
        if not is_fetch_field_provider(target):
            raise TypeError(
                f"{cls.__name__}.{field_name}: FetchField provider '{provider}' targets "
                f"'{res_cls.__name__}.{method}', which is not a @fetch_field_provider method"
            )


# -- Schema stripping ----------------------------------------------------------


# Internal fields that should be stripped from config schemas exposed
# to the UI.  These are framework plumbing, not user-configurable.
_INTERNAL_FIELDS = frozenset({"id", "resources"})


def strip_internal_fields(schema: dict[str, Any], extra: Iterable[str] = ()) -> dict[str, Any]:
    """Remove internal framework fields from a JSON Schema.

    Strips properties listed in ``_INTERNAL_FIELDS`` (plus *extra*), adjusts
    the ``required`` list, and drops ``$defs`` entries no remaining property
    references.  Returns a shallow copy; the original schema is not mutated.

    Args:
        schema: A JSON Schema dict (from ``model_json_schema()``).
        extra: Additional class-declared internal field names to strip.

    Returns:
        A copy of the schema without internal fields.
    """
    internal = _INTERNAL_FIELDS | frozenset(extra)
    properties = schema.get("properties", {})
    if not internal & properties.keys():
        return schema
    kept = {name: prop for name, prop in properties.items() if name not in internal}
    filtered = {**schema, "properties": kept}
    if "required" in filtered:
        filtered["required"] = [r for r in filtered["required"] if r not in internal]
        if not filtered["required"]:
            del filtered["required"]
    if "$defs" in filtered:
        import json

        props_str = json.dumps(kept)
        filtered["$defs"] = {k: v for k, v in filtered["$defs"].items() if f'"#/$defs/{k}"' in props_str}
        if not filtered["$defs"]:
            del filtered["$defs"]
    return filtered


# -- Helpers -------------------------------------------------------------------


def _extra(kwargs: dict[str, Any], widget: str) -> dict[str, Any]:
    """Pop json_schema_extra from kwargs and inject the widget hint.

    Returns:
        The extra dict with ``x-widget`` set.
    """
    extra = kwargs.pop("json_schema_extra", {})
    extra["x-widget"] = widget
    return extra


# -- Field factories -----------------------------------------------------------


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
    provider: str,
    label_key: str = "name",
    value_key: str = "id",
    **kwargs: Any,
) -> Any:
    """Field whose options are fetched from a resource's provider method.

    The backend instantiates the resource in slot ``<slot>`` (from the
    credentials the form already holds) and calls the :func:`fetch_field_provider`
    method ``<method>`` on it. The lookup logic lives next to the credentials
    it uses — there is no per-provider API route to hand-write.

    The dependency is the provider's own slot (the ``<slot>`` part): the
    frontend waits for that resource to be selected, then resolves via
    ``/external/resolve``. It is implicit in ``provider``, so there is no
    separate ``depends_on``.

    Args:
        default: Default value (``...`` means required).
        provider: ``"<slot>.<method>"`` reference to a ``@fetch_field_provider``
            method on the resource in slot ``<slot>``.
        label_key: Key in each response item used as the display label.
        value_key: Key in each response item used as the stored value.
        **kwargs: Forwarded to ``pydantic.Field``.

    Returns:
        A Pydantic Field descriptor.

    Raises:
        ValueError: If ``provider`` is not of the form ``"<slot>.<method>"``.

    Example::

        account_id: str = FetchField(
            provider="connection.accounts",
            label_key="name",
            value_key="account_id",
        )
    """
    slot, _, method = provider.partition(".")
    if not slot or not method:
        raise ValueError(f"FetchField provider '{provider}' must be of the form '<slot>.<method>'")
    extra = _extra(kwargs, "fetch")
    extra["x-fetch"] = {
        "provider": provider,
        "label_key": label_key,
        "value_key": value_key,
    }
    return Field(default, json_schema_extra=extra, **kwargs)
