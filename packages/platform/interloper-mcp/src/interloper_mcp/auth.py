"""Bearer-token verification: the user's tokens as MCP access tokens.

The SDK treats the server as an OAuth resource server; a
:class:`~mcp.server.auth.provider.TokenVerifier` is its seam for validating
whatever bearer token arrives. The verifier resolves it for the MCP
audience, which takes personal access tokens and the access tokens issued
for MCP, and the verified token names only the user: each tool call names
the organisation it acts in.
"""

from __future__ import annotations

import anyio.to_thread
from interloper_db import Audience, Profile, Store
from mcp.server.auth.provider import AccessToken, TokenVerifier


class UserAccessToken(AccessToken):
    """An access token standing for a verified user."""

    profile: Profile


class PatVerifier(TokenVerifier):
    """Verify raw bearer tokens against the token store.

    Resolution enforces the token's kind and audience, revocation and expiry;
    the resulting token carries the user every tool call runs as.
    """

    def __init__(self, store: Store) -> None:
        """Bind the verifier to the store tokens resolve against.

        Args:
            store: The Store instance.
        """
        self._store = store

    async def verify_token(self, token: str) -> AccessToken | None:
        """Resolve a raw bearer token; ``None`` rejects the request with a 401.

        Args:
            token: The bearer token exactly as presented by the client.

        Returns:
            The access token carrying the user, or ``None`` when the token is
            unknown, revoked, expired, or not one MCP accepts.
        """
        # The store is synchronous — keep the event loop free.
        result = await anyio.to_thread.run_sync(self._store.tokens.resolve, token, Audience.MCP)
        if result is None:
            return None

        profile, row = result
        return UserAccessToken(
            token=token,
            client_id=str(profile.id),
            scopes=[],
            expires_at=int(row.expires_at.timestamp()) if row.expires_at else None,
            profile=profile,
        )
