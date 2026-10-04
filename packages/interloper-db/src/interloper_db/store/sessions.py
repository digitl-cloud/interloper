"""Login session persistence: the proof a browser carries.

Only the token's SHA-256 hash is stored; the raw token lives in the cookie.
A session optionally carries the organisation the user is working in.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

from interloper.utils import assume_utc
from sqlalchemy import Engine, delete
from sqlmodel import col, select

from interloper_db.crypto import hash_token
from interloper_db.models import AuthSession, Profile
from interloper_db.session import commit, session_scope

SESSION_EXPIRY_DAYS = 30


class SessionStore:
    """Store methods for login sessions."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def create(self, user_id: UUID, org_id: UUID | None = None) -> str:
        """Open a session and return the raw (unhashed) token.

        Args:
            user_id: Profile UUID.
            org_id: Organisation the session starts in, or ``None`` for none yet.

        Returns:
            The raw session token, to be set as a cookie.
        """
        token = secrets.token_urlsafe(48)
        with session_scope(self._engine) as session:
            session.add(
                AuthSession(
                    user_id=user_id,
                    organisation_id=org_id,
                    token_hash=hash_token(token),
                    expires_at=datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRY_DAYS),
                )
            )
            commit(session)
        return token

    def resolve(self, token: str) -> tuple[Profile, AuthSession] | None:
        """Resolve a session token to its profile and session row.

        An expired session is deleted on sight. Absence is an ordinary answer
        here (a stale cookie), so it reads as ``None`` rather than raising.

        Args:
            token: The raw session token from the cookie.

        Returns:
            ``(Profile, AuthSession)`` when the session is live, else ``None``.
        """
        with session_scope(self._engine) as session:
            db_session = session.exec(select(AuthSession).where(AuthSession.token_hash == hash_token(token))).first()
            if not db_session:
                return None
            if assume_utc(db_session.expires_at) < datetime.now(timezone.utc):
                session.delete(db_session)
                commit(session)
                return None
            db_profile = session.get(Profile, db_session.user_id)
            if not db_profile:
                return None
            return db_profile, db_session

    def switch_org(self, token: str, org_id: UUID, user_id: UUID) -> None:
        """Make an organisation the session's active one, and remember it on the profile.

        Idempotent: a token that resolves to no session leaves only the
        profile's preference updated.

        Args:
            token: The raw session token.
            org_id: Organisation UUID to switch to.
            user_id: The profile whose ``last_organisation_id`` follows the switch.
        """
        with session_scope(self._engine) as session:
            db_session = session.exec(select(AuthSession).where(AuthSession.token_hash == hash_token(token))).first()
            if db_session:
                db_session.organisation_id = org_id
                session.add(db_session)
            db_profile = session.get(Profile, user_id)
            if db_profile:
                db_profile.last_organisation_id = org_id
                session.add(db_profile)
            commit(session)

    def delete_all(self, user_id: UUID) -> None:
        """End every session of a user (logout everywhere).

        Args:
            user_id: Profile UUID.
        """
        with session_scope(self._engine) as session:
            session.connection().execute(delete(AuthSession).where(col(AuthSession.user_id) == user_id))
            commit(session)
