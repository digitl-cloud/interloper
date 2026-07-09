"""Decorator for creating Connection subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.connection.base import Connection, OAuthConnection
from interloper.oauth import OAuthConfig

# Bounded TypeVar so that classes already extending Connection preserve their
# specific type through the decorator.  Plain classes fall through to the
# fallback overload and get type[Connection].
ConnectionT = TypeVar("ConnectionT", bound=Connection)


@overload
def connection(cls: type[ConnectionT], /) -> type[ConnectionT]: ...
@overload
def connection(cls: type, /) -> type[Connection]: ...
@overload
def connection(
    *,
    key: str = ...,
    name: str = ...,
    icon: str = ...,
    tags: list[str] = ...,
    oauth: OAuthConfig = ...,
) -> Callable[[type[ConnectionT]], type[ConnectionT]]: ...
def connection(
    cls: type | None = None,
    /,
    *,
    key: str | None = None,
    name: str | None = None,
    icon: str | None = None,
    tags: list[str] | None = None,
    oauth: OAuthConfig | None = None,
) -> type[Connection] | Callable[[type], type[Connection]]:
    """Create a Connection subclass from a decorated class.

    Can be used bare or with arguments::

        @connection
        class MyConnection:
            host: str = "localhost"
            port: int = 5432
            username: str
            password: str

        @connection(key="custom", name="Custom Connection")
        class OtherConnection:
            url: str

    Class-level traits — identity (key, name, icon, tags) and behavior
    (oauth) — belong in the decorator; the class body declares fields::

        @connection(
            name="Amazon Ads",
            oauth=OAuthConfig("amazon", scope="advertising::campaign_management"),
        )
        class AmazonAdsConnection(OAuthConnection):
            location: str = SelectField(...)

    The decorated class's annotations and attributes become the Connection
    subclass body.  Since Connection extends ``BaseSettings``, fields can
    still be loaded from environment variables.

    Returns:
        A Connection subclass.  Building it fails with a TypeError if
        ``oauth.fields`` maps token response keys to model fields the
        class does not declare.
    """
    classvars: dict[str, Any] = {}
    if key is not None:
        classvars["key"] = key
    if name is not None:
        classvars["name"] = name
    if icon is not None:
        classvars["icon"] = icon
    if tags is not None:
        classvars["tags"] = tags
    if oauth is not None:
        classvars["oauth"] = oauth

    def build(cls: type) -> type[Connection]:
        result = Connection.build_class(cls, classvars=classvars)
        # OAuth lives on OAuthConnection — reject it on a plain Connection.
        if oauth is not None and not issubclass(result, OAuthConnection):
            raise TypeError(
                f"{result.__name__}: oauth=... requires subclassing OAuthConnection, not Connection."
            )
        return result

    if cls is not None:
        return build(cls)
    return build
