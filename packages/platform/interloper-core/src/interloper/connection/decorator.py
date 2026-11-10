"""The connection decorator: a Connection subclass from a decorated class."""

from __future__ import annotations

from interloper.component.decorator import Decorator
from interloper.connection.base import Connection


class ConnectionDecorator(Decorator[Connection]):
    """Create a Connection subclass from a decorated class.

    Can be used bare or with arguments:

    ```py
    @connection
    class MyConnection:
        host: str = "localhost"
        port: int = 5432
        username: str
        password: str

    @connection(key="custom", name="Custom Connection")
    class OtherConnection:
        url: str
    ```

    Class-level traits, identity (key, name, icon, tags) and behaviour (oauth),
    belong in the decorator; the class body declares fields:

    ```py
    @connection(
        name="Amazon Ads",
        oauth=OAuthConfig("amazon", scope="advertising::campaign_management"),
    )
    class AmazonAdsConnection(OAuthConnection):
        location: str = SelectField(...)
    ```

    The decorated class's annotations and attributes become the Connection subclass
    body. Since Connection extends ``BaseSettings``, fields can still be loaded from
    environment variables.

    ``relations`` maps a relation name to a `Relation`, a component class, or a list
    of component classes narrowing the relation the decorated class declares under
    that name. Every other keyword is one of the public ClassVars and field defaults
    the decorated class declares, or `Connection` itself for a plain class (``key``,
    ``name``, ``icon``, ``tags``, ``auto_renew``, and ``oauth`` on an
    `OAuthConnection`). An unknown name is a ``TypeError`` at decoration, naming the
    subclass that declares it when one does. Building the class fails with a
    ``TypeError`` if ``oauth.fields`` maps token response keys to model fields the
    class does not declare.
    """


connection = ConnectionDecorator(Connection)
