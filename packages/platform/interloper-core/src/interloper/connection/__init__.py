"""Connection: a resource holding connection and credential values, OAuth variants included."""

from interloper.connection.base import (
    CheckResult,
    Connection,
    ConnectionState,
    OAuthConnection,
    RefreshTokenOAuthConnection,
    Renewal,
)
from interloper.connection.decorator import connection

__all__ = [
    "CheckResult",
    "Connection",
    "ConnectionState",
    "OAuthConnection",
    "RefreshTokenOAuthConnection",
    "Renewal",
    "connection",
]
