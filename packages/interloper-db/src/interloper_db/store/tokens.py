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
from sqlmodel import select

from interloper_db.models import PersonalAccessToken, Profile
from interloper_db.store.auth import _as_utc, _get_membership, _hash_token
from interloper_db.store.base import StoreBase

TOKEN_PREFIX = "ilp_"

TOKEN_PREFIX_LEN = 12

# last_used_at is informational; throttling the bump keeps hot MCP sessions
# from turning every tool call into a write.
LAST_USED_THROTTLE_SECONDS = 60


class TokenMixin(StoreBase):
    """Store methods for personal access tokens.

    Error contract (same as the other mixins): lookups return ``None`` when
    the row is absent, mutations raise :class:`NotFoundError` on a missing
    target.
    """

    def create_token(
        self,
        user_id: UUID,
        organisation_id: UUID,
        name: str,
        expires_at: datetime | None = None,
    ) -> tuple[PersonalAccessToken, str]:
        """Create a personal access token.

        Args:
            user_id: Profile UUID of the token holder.
            organisation_id: Organisation the token is scoped to.
            name: User-facing label (e.g. "Claude Code laptop").
            expires_at: Optional expiry; ``None`` means the token never expires.

        Returns:
            ``(row, raw_token)`` — the raw token is never stored and cannot be
            recovered later.
        """
        raw = TOKEN_PREFIX + secrets.token_urlsafe(36)

        with self._session() as session:
            db_token = PersonalAccessToken(
                user_id=user_id,
                organisation_id=organisation_id,
                name=name,
                token_prefix=raw[:TOKEN_PREFIX_LEN],
                token_hash=_hash_token(raw),
                expires_at=expires_at,
            )
            session.add(db_token)
            session.commit()
            session.refresh(db_token)
            return db_token, raw

    def resolve_token(self, raw: str) -> tuple[Profile, PersonalAccessToken, str] | None:
        """Resolve a raw token to its holder, row, and live org role.

        Args:
            raw: The raw bearer token as presented by the client.

        Returns:
            ``(Profile, PersonalAccessToken, role)`` when the token is valid,
            else ``None`` — on no match, revocation, expiry, a deleted
            profile, or the holder no longer being a member of the token's
            organisation.
        """
        token_hash = _hash_token(raw)
        now = datetime.now(timezone.utc)

        with self._session() as session:
            db_token = session.exec(
                select(PersonalAccessToken).where(PersonalAccessToken.token_hash == token_hash)
            ).first()
            if not db_token:
                return None

            if db_token.revoked_at is not None:
                return None
            if db_token.expires_at is not None and _as_utc(db_token.expires_at) < now:
                return None

            db_profile = session.get(Profile, db_token.user_id)
            if not db_profile:
                return None

            membership = _get_membership(session, db_token.user_id, db_token.organisation_id)
            if not membership:
                return None

            last_used = db_token.last_used_at
            if last_used is None or _as_utc(last_used) < now - timedelta(seconds=LAST_USED_THROTTLE_SECONDS):
                db_token.last_used_at = now
                session.add(db_token)
                session.commit()
                session.refresh(db_token)

            return db_profile, db_token, membership.role

    def list_tokens(self, user_id: UUID, organisation_id: UUID | None = None) -> list[PersonalAccessToken]:
        """List a user's tokens, optionally filtered to one organisation.

        Args:
            user_id: Profile UUID.
            organisation_id: Restrict to this organisation when provided.

        Returns:
            Token rows, newest first. Rows carry only the display prefix and
            the hash — exposing neither raw secrets nor anything recoverable.
        """
        with self._session() as session:
            statement = select(PersonalAccessToken).where(PersonalAccessToken.user_id == user_id)
            if organisation_id is not None:
                statement = statement.where(PersonalAccessToken.organisation_id == organisation_id)
            statement = statement.order_by(PersonalAccessToken.created_at.desc())  # ty: ignore[unresolved-attribute]
            return list(session.exec(statement).all())

    def get_token(self, token_id: UUID) -> PersonalAccessToken | None:
        """Get a token row by ID.

        Args:
            token_id: Token UUID.

        Returns:
            The row or ``None``.
        """
        with self._session() as session:
            return session.get(PersonalAccessToken, token_id)

    def revoke_token(self, token_id: UUID) -> PersonalAccessToken:
        """Revoke a token (soft delete — the row stays for audit).

        Args:
            token_id: Token UUID.

        Returns:
            The revoked row. Revoking an already-revoked token is a no-op.

        Raises:
            NotFoundError: If the token does not exist.
        """
        with self._session() as session:
            db_token = session.get(PersonalAccessToken, token_id)
            if not db_token:
                raise NotFoundError(f"Token {token_id} not found")
            if db_token.revoked_at is None:
                db_token.revoked_at = datetime.now(timezone.utc)
                session.add(db_token)
                session.commit()
                session.refresh(db_token)
            return db_token
