"""Token persistence: create, resolve, list, revoke.

Tokens are opaque bearer secrets whose SHA-256 hash is the only thing
stored. A token stands for its user and carries no organisation: resolving
one yields the holder, and the caller reads the holder's live role in the
organisation it acts in, so a role change or removal applies at once.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import Engine
from sqlmodel import col, select

from interloper_db.crypto import hash_token
from interloper_db.errors import NotFoundError
from interloper_db.models import Audience, Profile, Token, TokenKind
from interloper_db.session import save, session_scope
from interloper_db.store.page import Page, PageQuery

TOKEN_PREFIX = "ilp_"

TOKEN_PREFIX_LEN = 12

# last_used_at is informational; throttling the bump keeps hot MCP sessions
# from turning every tool call into a write.
LAST_USED_THROTTLE_SECONDS = 60


class TokenStore:
    """Store methods for tokens."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def create_personal(self, user_id: UUID, name: str, expires_at: datetime | None = None) -> tuple[Token, str]:
        """Create a personal access token.

        Args:
            user_id: Profile UUID of the token holder.
            name: User-facing label (e.g. "Claude Code laptop").
            expires_at: Optional expiry; ``None`` means the token never expires.

        Returns:
            ``(row, raw_token)`` — the raw token is never stored and cannot be
            recovered later.
        """
        raw = TOKEN_PREFIX + secrets.token_urlsafe(36)

        with session_scope(self._engine) as session:
            db_token = Token(
                user_id=user_id,
                kind=TokenKind.PERSONAL,
                name=name,
                token_prefix=raw[:TOKEN_PREFIX_LEN],
                token_hash=hash_token(raw),
                expires_at=expires_at,
            )
            save(session, db_token)
            return db_token, raw

    def resolve(self, raw: str, audience: Audience) -> tuple[Profile, Token] | None:
        """Resolve a raw secret presented to a surface to its holder and row.

        Args:
            raw: The raw secret as presented by the client.
            audience: The surface it was presented to; a token of a kind the
                surface does not accept, or an access token issued for
                another surface, does not resolve.

        Returns:
            ``(Profile, Token)`` when the token is valid for the surface, else
            ``None``: on no match, a kind or resource the surface refuses,
            revocation, expiry, or a deleted profile.
        """
        token_hash = hash_token(raw)
        now = datetime.now(timezone.utc)

        with session_scope(self._engine) as session:
            db_token = session.exec(select(Token).where(Token.token_hash == token_hash)).first()
            if not db_token:
                return None

            if db_token.kind not in audience.kinds:
                return None
            if db_token.kind == TokenKind.ACCESS and db_token.resource != audience.value:
                return None
            if db_token.revoked_at is not None:
                return None
            if db_token.expires_at is not None and db_token.expires_at < now:
                return None

            db_profile = session.get(Profile, db_token.user_id)
            if not db_profile:
                return None

            last_used = db_token.last_used_at
            if last_used is None or last_used < now - timedelta(seconds=LAST_USED_THROTTLE_SECONDS):
                db_token.last_used_at = now
                save(session, db_token)

            return db_profile, db_token

    def list(self, user_id: UUID, query: PageQuery) -> Page[Token]:
        """List a user's personal access tokens, newest first.

        Args:
            user_id: Profile UUID of the holder.
            query: The window to read.

        Returns:
            The page of tokens. Rows carry only the display prefix and the
            hash — exposing neither raw secrets nor anything recoverable.
        """
        statement = (
            select(Token)
            .where(Token.user_id == user_id, Token.kind == TokenKind.PERSONAL)
            .order_by(col(Token.created_at).desc(), col(Token.id))
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def get(self, token_id: UUID) -> Token:
        """Get a token row by ID.

        Args:
            token_id: Token UUID.

        Returns:
            The token row.

        Raises:
            NotFoundError: If no token carries that id.
        """
        with session_scope(self._engine) as session:
            db_token = session.get(Token, token_id)
            if not db_token:
                raise NotFoundError(f"Token {token_id} not found")
            return db_token

    def revoke(self, token_id: UUID) -> Token:
        """Revoke a token (soft delete — the row stays for audit).

        Args:
            token_id: Token UUID.

        Returns:
            The revoked row. Revoking an already-revoked token is a no-op.
        """
        with session_scope(self._engine) as session:
            db_token = self.get(token_id)
            if db_token.revoked_at is None:
                db_token.revoked_at = datetime.now(timezone.utc)
                save(session, db_token)
            return db_token
