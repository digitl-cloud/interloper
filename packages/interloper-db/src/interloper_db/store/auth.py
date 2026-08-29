"""Authentication persistence: who a person is, and the session proving it.

Profiles are identities (Google OAuth), sessions are the proof a browser
carries. What a profile may do, and in which organisation, is
:mod:`~interloper_db.store.organisations`.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

from interloper.errors import NotFoundError
from interloper.utils import assume_utc
from sqlalchemy import Engine
from sqlmodel import select

from interloper_db.crypto import hash_token
from interloper_db.models import (
    AuthSession,
    Invitation,
    Organisation,
    PersonalAccessToken,
    Profile,
    UserOrganisation,
)
from interloper_db.session import commit, session_scope

SESSION_EXPIRY_DAYS = 30


class AuthStore:
    """Store methods for profiles and the sessions authenticating them."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    # -- Profiles --------------------------------------------------------------

    def upsert_profile(
        self,
        *,
        google_id: str,
        email: str,
        name: str | None = None,
        avatar_url: str | None = None,
    ) -> Profile:
        """Create or update a profile by Google ID.

        Args:
            google_id: Google OAuth subject identifier.
            email: User email.
            name: Display name. Only fills an empty profile — a name the user
                set themselves (:meth:`update_profile`) survives logins.
            avatar_url: Avatar URL (``None`` leaves the stored value untouched).

        Returns:
            The upserted Profile row.
        """
        with session_scope(self._engine) as session:
            statement = select(Profile).where(Profile.google_id == google_id)
            db_profile = session.exec(statement).first()

            if db_profile:
                db_profile.email = email
                if name is not None and not db_profile.name:
                    db_profile.name = name
                if avatar_url is not None:
                    db_profile.avatar_url = avatar_url
                session.add(db_profile)
            else:
                db_profile = Profile(
                    email=email,
                    name=name,
                    google_id=google_id,
                    avatar_url=avatar_url,
                )
                session.add(db_profile)

            commit(session)
            session.refresh(db_profile)
            return db_profile

    def update_profile(self, user_id: UUID, *, name: str | None = None, timezone: str | None = None) -> Profile:
        """Update a profile's user-editable fields.

        Args:
            user_id: Profile UUID.
            name: New display name (``None`` leaves the stored value untouched).
            timezone: New IANA timezone name (``None`` leaves the stored value untouched).

        Returns:
            The updated Profile.

        Raises:
            NotFoundError: If the profile is not found.
        """
        with session_scope(self._engine) as session:
            db_profile = session.get(Profile, user_id)
            if not db_profile:
                raise NotFoundError(f"Profile {user_id} not found")
            if name is not None:
                db_profile.name = name
            if timezone is not None:
                db_profile.timezone = timezone
            session.add(db_profile)
            commit(session)
            session.refresh(db_profile)
            return db_profile

    def set_super_admin(self, user_id: UUID, is_super_admin: bool = True) -> Profile:
        """Set the platform-wide super-admin flag on a profile.

        Args:
            user_id: Profile UUID.
            is_super_admin: Flag value to set.

        Returns:
            The updated Profile.

        Raises:
            NotFoundError: If the profile is not found.
        """
        with session_scope(self._engine) as session:
            db_profile = session.get(Profile, user_id)
            if not db_profile:
                raise NotFoundError(f"Profile {user_id} not found")
            db_profile.is_super_admin = is_super_admin
            session.add(db_profile)
            commit(session)
            return db_profile

    def get_profile(self, user_id: UUID) -> Profile:
        """Get a profile by ID.

        Args:
            user_id: Profile UUID.

        Returns:
            The profile row.

        Raises:
            NotFoundError: If no profile carries that id.
        """
        with session_scope(self._engine) as session:
            db_profile = session.get(Profile, user_id)
            if not db_profile:
                raise NotFoundError(f"Profile {user_id} not found")
            return db_profile

    def list_all_profiles(self) -> list[tuple[Profile, list[Organisation]]]:
        """List every profile with the organisations it belongs to (super-admin only).

        Returns:
            List of ``(Profile, organisations)`` tuples.
        """
        with session_scope(self._engine) as session:
            profiles = session.exec(select(Profile)).all()
            memberships = session.exec(
                select(UserOrganisation.user_id, Organisation).join(
                    Organisation,
                    UserOrganisation.organisation_id == Organisation.id,  # ty: ignore[invalid-argument-type]
                )
            ).all()
            orgs_by_user: dict[UUID, list[Organisation]] = {}
            for user_id, org in memberships:
                orgs_by_user.setdefault(user_id, []).append(org)
            return [(profile, orgs_by_user.get(profile.id, [])) for profile in profiles]

    def delete_profile(self, user_id: UUID) -> None:
        """Delete a profile and everything anchored to it.

        Removes the user's sessions, personal access tokens, organisation
        memberships, and the invitations they sent, then the profile row.

        Args:
            user_id: Profile UUID.

        Raises:
            NotFoundError: If the profile is not found.
        """
        with session_scope(self._engine) as session:
            db_profile = session.get(Profile, user_id)
            if not db_profile:
                raise NotFoundError(f"Profile {user_id} not found")

            for db_session in session.exec(select(AuthSession).where(AuthSession.user_id == user_id)).all():
                session.delete(db_session)
            for token in session.exec(
                select(PersonalAccessToken).where(PersonalAccessToken.user_id == user_id)
            ).all():
                session.delete(token)
            for membership in session.exec(
                select(UserOrganisation).where(UserOrganisation.user_id == user_id)
            ).all():
                session.delete(membership)
            for invitation in session.exec(select(Invitation).where(Invitation.invited_by == user_id)).all():
                session.delete(invitation)

            session.delete(db_profile)
            commit(session)

    def get_profile_by_google_id(self, google_id: str) -> Profile | None:
        """Get a profile by Google OAuth subject identifier.

        Args:
            google_id: Google OAuth subject identifier.

        Returns:
            The Profile or None.
        """
        with session_scope(self._engine) as session:
            return session.exec(select(Profile).where(Profile.google_id == google_id)).first()

    # -- Sessions --------------------------------------------------------------

    def create_session(self, user_id: UUID, organisation_id: UUID | None = None) -> str:
        """Create a session and return the raw (unhashed) token.

        Args:
            user_id: Profile UUID.
            organisation_id: Optional org to bind to session.

        Returns:
            The raw session token (to be set as a cookie).
        """
        token = secrets.token_urlsafe(48)
        token_hash = hash_token(token)

        with session_scope(self._engine) as session:
            db_session = AuthSession(
                user_id=user_id,
                organisation_id=organisation_id,
                token_hash=token_hash,
                expires_at=datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRY_DAYS),
            )
            session.add(db_session)
            commit(session)

        return token

    def resolve_session(self, token: str) -> tuple[Profile, AuthSession] | None:
        """Resolve a session token to a profile and session row.

        Args:
            token: The raw session token from the cookie.

        Returns:
            ``(Profile, Session)`` if valid, else ``None``.
        """
        token_hash = hash_token(token)

        with session_scope(self._engine) as session:
            statement = select(AuthSession).where(AuthSession.token_hash == token_hash)
            db_session = session.exec(statement).first()
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

    def set_session_org(self, token: str, org_id: UUID, user_id: UUID | None = None) -> None:
        """Update the session's active organisation and persist preference on profile.

        Args:
            token: The raw session token.
            org_id: Organisation UUID to switch to.
            user_id: If provided, also update ``last_organisation_id`` on the profile.
        """
        token_hash = hash_token(token)

        with session_scope(self._engine) as session:
            db_session = session.exec(
                select(AuthSession).where(AuthSession.token_hash == token_hash)
            ).first()
            if db_session:
                db_session.organisation_id = org_id
                session.add(db_session)

            if user_id:
                db_profile = session.get(Profile, user_id)
                if db_profile:
                    db_profile.last_organisation_id = org_id
                    session.add(db_profile)

            commit(session)

    def delete_user_sessions(self, user_id: UUID) -> None:
        """Delete all sessions for a user.

        Args:
            user_id: Profile UUID.
        """
        with session_scope(self._engine) as session:
            db_sessions = session.exec(select(AuthSession).where(AuthSession.user_id == user_id)).all()
            for db_session in db_sessions:
                session.delete(db_session)
            commit(session)
