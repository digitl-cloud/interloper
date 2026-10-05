"""Personal access token persistence: create, resolve, list, revoke.

Tokens are opaque bearer secrets (``ilp_`` prefix) whose SHA-256 hash is the
only thing stored — the same scheme as session tokens. Resolution returns the
holder's *live* role in the token's organisation, so a role change or removal
applies to existing tokens immediately.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlmodel import col, select

from interloper_db.crypto import hash_token
from interloper_db.models import PersonalAccessToken, Profile
from interloper_db.session import save, session_scope
from interloper_db.store.members import MemberStore
from interloper_db.store.page import Page, PageQuery

TOKEN_PREFIX = "ilp_"

TOKEN_PREFIX_LEN = 12

# last_used_at is informational; throttling the bump keeps hot MCP sessions
# from turning every tool call into a write.
LAST_USED_THROTTLE_SECONDS = 60


class TokenQuery(PageQuery):
    """Which of a user's tokens a listing reads.

    Attributes:
        org_id: Keep the tokens scoped to this organisation; ``None`` keeps
            every organisation's.
    """

    org_id: UUID | None = None


class TokenStore:
    """Store methods for personal access tokens."""

    def __init__(self, engine: Engine, members: MemberStore) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            members: Membership facet, for the live role a token's scope
                resolves against.
        """
        self._engine = engine
        self._members = members

    def create(
        self,
        user_id: UUID,
        org_id: UUID,
        *,
        name: str,
        expires_at: datetime | None = None,
    ) -> tuple[PersonalAccessToken, str]:
        """Create a personal access token.

        Args:
            user_id: Profile UUID of the token holder.
            org_id: Organisation the token is scoped to.
            name: User-facing label (e.g. "Claude Code laptop").
            expires_at: Optional expiry; ``None`` means the token never expires.

        Returns:
            ``(row, raw_token)`` — the raw token is never stored and cannot be
            recovered later.
        """
        raw = TOKEN_PREFIX + secrets.token_urlsafe(36)

        with session_scope(self._engine) as session:
            db_token = PersonalAccessToken(
                user_id=user_id,
                organisation_id=org_id,
                name=name,
                token_prefix=raw[:TOKEN_PREFIX_LEN],
                token_hash=hash_token(raw),
                expires_at=expires_at,
            )
            save(session, db_token)
            return db_token, raw

    def resolve(self, raw: str) -> tuple[Profile, PersonalAccessToken, str] | None:
        """Resolve a raw token to its holder, row, and live org role.

        Args:
            raw: The raw bearer token as presented by the client.

        Returns:
            ``(Profile, PersonalAccessToken, role)`` when the token is valid,
            else ``None`` — on no match, revocation, expiry, a deleted
            profile, or the holder no longer being a member of the token's
            organisation.
        """
        token_hash = hash_token(raw)
        now = datetime.now(timezone.utc)

        with session_scope(self._engine) as session:
            db_token = session.exec(
                select(PersonalAccessToken).where(PersonalAccessToken.token_hash == token_hash)
            ).first()
            if not db_token:
                return None

            if db_token.revoked_at is not None:
                return None
            if db_token.expires_at is not None and db_token.expires_at < now:
                return None

            db_profile = session.get(Profile, db_token.user_id)
            if not db_profile:
                return None

            role = self._members.role(db_token.organisation_id, db_token.user_id)
            if role is None:
                return None

            last_used = db_token.last_used_at
            if last_used is None or last_used < now - timedelta(seconds=LAST_USED_THROTTLE_SECONDS):
                db_token.last_used_at = now
                save(session, db_token)

            return db_profile, db_token, role

    def list(self, user_id: UUID, query: TokenQuery) -> Page[PersonalAccessToken]:
        """List a user's tokens, newest first.

        Args:
            user_id: Profile UUID of the holder.
            query: Which organisation's tokens, and the window to read.

        Returns:
            The page of tokens. Rows carry only the display prefix and the
            hash — exposing neither raw secrets nor anything recoverable.
        """
        statement = (
            select(PersonalAccessToken)
            .where(PersonalAccessToken.user_id == user_id)
            .order_by(col(PersonalAccessToken.created_at).desc(), col(PersonalAccessToken.id))
        )
        if query.org_id is not None:
            statement = statement.where(PersonalAccessToken.organisation_id == query.org_id)
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def get(self, token_id: UUID) -> PersonalAccessToken:
        """Get a token row by ID.

        Args:
            token_id: Token UUID.

        Returns:
            The token row.

        Raises:
            NotFoundError: If no token carries that id.
        """
        with session_scope(self._engine) as session:
            db_token = session.get(PersonalAccessToken, token_id)
            if not db_token:
                raise NotFoundError(f"Token {token_id} not found")
            return db_token

    def revoke(self, token_id: UUID) -> PersonalAccessToken:
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
