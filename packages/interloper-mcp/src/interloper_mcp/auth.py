"""Bearer-token verification: personal access tokens as MCP access tokens.

The SDK treats the server as an OAuth resource server; a
:class:`~mcp.server.auth.provider.TokenVerifier` is its seam for validating
whatever bearer token arrives. Interloper PATs slot straight in — and an
OAuth authorization server can later mint short-lived tokens verified through
this same seam without touching the tools.
"""

from __future__ import annotations

import anyio.to_thread
from interloper_db import Store
from mcp.server.auth.provider import AccessToken, TokenVerifier


class PatAccessToken(AccessToken):
    """An access token backed by a verified personal access token."""

    org_id: str
    role: str
    profile_id: str


class PatVerifier(TokenVerifier):
    """Verify raw bearer tokens against the PAT store.

    Resolution enforces revocation, expiry, and live org membership; the
    resulting token carries the tenant scope every tool call runs under.
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    async def verify_token(self, token: str) -> AccessToken | None:
        """Resolve a raw bearer token; ``None`` rejects the request with a 401.

        Args:
            token: The bearer token exactly as presented by the client.
        """
        # The store is synchronous — keep the event loop free.
        result = await anyio.to_thread.run_sync(self._store.resolve_token, token)
        if result is None:
            return None

        profile, pat, role = result
        return PatAccessToken(
            token=token,
            client_id=str(profile.id),
            scopes=[f"role:{role}"],
            expires_at=int(pat.expires_at.timestamp()) if pat.expires_at else None,
            org_id=str(pat.organisation_id),
            role=role,
            profile_id=str(profile.id),
        )
