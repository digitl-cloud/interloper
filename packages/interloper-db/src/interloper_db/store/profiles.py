"""Profile persistence: who a person is.

A profile is an identity (Google OAuth). The sessions proving it are
:mod:`~interloper_db.store.sessions`; what it may do, and in which
organisation, is :mod:`~interloper_db.store.members`.
"""

from __future__ import annotations

from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import Engine, delete
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

from interloper_db.models import AuthSession, Invitation, PersonalAccessToken, Profile, UserOrganisation
from interloper_db.session import commit, session_scope
from interloper_db.store.page import Page, PageQuery


class ProfileStore:
    """Store methods for profiles."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def get(self, profile_id: UUID) -> Profile:
        """Get a profile by ID.

        Args:
            profile_id: Profile UUID.

        Returns:
            The profile row.
        """
        with session_scope(self._engine) as session:
            return self._get(session, profile_id)

    def get_by_google_id(self, google_id: str) -> Profile | None:
        """Get a profile by its Google OAuth subject identifier.

        Absence is an ordinary answer here (a first login), so it reads as
        ``None`` rather than raising.

        Args:
            google_id: Google OAuth subject identifier.

        Returns:
            The profile, or ``None`` when no profile carries that identifier.
        """
        with session_scope(self._engine) as session:
            return session.exec(select(Profile).where(Profile.google_id == google_id)).first()

    def list(self, query: PageQuery) -> Page[Profile]:
        """List every profile, oldest first, each with the organisations it belongs to.

        Args:
            query: The window to read.

        Returns:
            The page of profiles, their ``organisations`` loaded.
        """
        statement = (
            select(Profile)
            .options(selectinload(Profile.organisations))  # ty: ignore[invalid-argument-type]
            .order_by(col(Profile.created_at), col(Profile.id))
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def upsert(
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
                set themselves (:meth:`update`) survives logins.
            avatar_url: Avatar URL (``None`` leaves the stored value untouched).

        Returns:
            The upserted Profile row.
        """
        with session_scope(self._engine) as session:
            db_profile = session.exec(select(Profile).where(Profile.google_id == google_id)).first()
            if db_profile:
                db_profile.email = email
                if name is not None and not db_profile.name:
                    db_profile.name = name
                if avatar_url is not None:
                    db_profile.avatar_url = avatar_url
            else:
                db_profile = Profile(email=email, name=name, google_id=google_id, avatar_url=avatar_url)
            session.add(db_profile)
            commit(session)
            session.refresh(db_profile)
            return db_profile

    def update(self, profile_id: UUID, *, name: str | None = None, timezone: str | None = None) -> Profile:
        """Update a profile's user-editable fields.

        Args:
            profile_id: Profile UUID.
            name: New display name (``None`` leaves the stored value untouched).
            timezone: New IANA timezone name (``None`` leaves the stored value untouched).

        Returns:
            The updated Profile.
        """
        with session_scope(self._engine) as session:
            db_profile = self._get(session, profile_id)
            if name is not None:
                db_profile.name = name
            if timezone is not None:
                db_profile.timezone = timezone
            session.add(db_profile)
            commit(session)
            session.refresh(db_profile)
            return db_profile

    def set_super_admin(self, profile_id: UUID, *, value: bool) -> Profile:
        """Set the platform-wide super-admin flag on a profile.

        Args:
            profile_id: Profile UUID.
            value: Whether the profile is a super-admin.

        Returns:
            The updated Profile.
        """
        with session_scope(self._engine) as session:
            db_profile = self._get(session, profile_id)
            db_profile.is_super_admin = value
            session.add(db_profile)
            commit(session)
            session.refresh(db_profile)
            return db_profile

    def delete(self, profile_id: UUID) -> None:
        """Delete a profile and everything anchored to it.

        Removes the user's sessions, personal access tokens, organisation
        memberships, and the invitations they sent, then the profile row.

        Args:
            profile_id: Profile UUID.
        """
        with session_scope(self._engine) as session:
            db_profile = self._get(session, profile_id)
            for statement in (
                delete(AuthSession).where(col(AuthSession.user_id) == profile_id),
                delete(PersonalAccessToken).where(col(PersonalAccessToken.user_id) == profile_id),
                delete(UserOrganisation).where(col(UserOrganisation.user_id) == profile_id),
                delete(Invitation).where(col(Invitation.invited_by) == profile_id),
            ):
                session.connection().execute(statement)
            session.delete(db_profile)
            commit(session)

    @staticmethod
    def _get(session: Session, profile_id: UUID) -> Profile:
        """Fetch a profile row.

        Args:
            session: Open session to read through.
            profile_id: Profile UUID.

        Returns:
            The profile row.

        Raises:
            NotFoundError: If no profile carries that id.
        """
        db_profile = session.get(Profile, profile_id)
        if not db_profile:
            raise NotFoundError(f"Profile {profile_id} not found")
        return db_profile
