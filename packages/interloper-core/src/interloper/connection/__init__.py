"""Connection: a resource holding connection and credential values, OAuth variants included."""

from interloper.connection.base import (
    Connection,
    ConnectionState,
    OAuthConnection,
    RefreshTokenOAuthConnection,
    Renewal,
)
from interloper.connection.decorator import connection

__all__ = [
    "Connection",
    "ConnectionState",
    "OAuthConnection",
    "RefreshTokenOAuthConnection",
    "Renewal",
    "connection",
]
