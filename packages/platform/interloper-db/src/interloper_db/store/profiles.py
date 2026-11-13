"""Profile persistence: who a person is, and the identities they sign in with.

A profile is found by an identity, the issuer and subject an identity provider
names the person by. The tokens proving it, login sessions among them, are
:mod:`~interloper_db.store.tokens`; what it may do, and in which organisation,
is :mod:`~interloper_db.store.members`.
"""

from __future__ import annotations

import builtins
from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import Engine, delete, func
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, select

from interloper_db.errors import NotFoundError
from interloper_db.models import AuthorizationCode, Identity, Invitation, Profile, Token, UserOrganisation
from interloper_db.session import commit, save, session_scope
from interloper_db.store.page import Page, PageQuery


class ProfileQuery(PageQuery):
    """Which profiles a listing reads.

    Attributes:
        super_admin: Keep only super-admins (``True``) or only everyone else
            (``False``); ``None`` keeps every profile.
    """

    super_admin: bool | None = None


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

        Raises:
            NotFoundError: If no profile carries that id.
        """
        with session_scope(self._engine) as session:
            db_profile = session.get(Profile, profile_id)
            if not db_profile:
                raise NotFoundError(f"Profile {profile_id} not found")
            return db_profile

    def get_by_identity(self, issuer: str, subject: str) -> Profile | None:
        """Get the profile an identity signs in to.

        Absence is an ordinary answer here (a first login), so it reads as
        ``None`` rather than raising.

        Args:
            issuer: The identity provider's issuer identifier.
            subject: The subject the provider names the person by.

        Returns:
            The profile, or ``None`` when no profile has that identity.
        """
        with session_scope(self._engine) as session:
            return self._by_identity(session, issuer, subject)

    def list(self, query: ProfileQuery) -> Page[Profile]:
        """List profiles, oldest first, each with the organisations it belongs to.

        Args:
            query: Whether to keep super-admins only, and the window to read.

        Returns:
            The page of profiles, their ``organisations`` loaded.
        """
        statement = (
            select(Profile)
            .options(selectinload(Profile.organisations))  # ty: ignore[invalid-argument-type]
            .order_by(col(Profile.created_at), col(Profile.id))
        )
        if query.super_admin is not None:
            statement = statement.where(col(Profile.is_super_admin).is_(query.super_admin))
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def upsert(
        self,
        *,
        issuer: str,
        subject: str,
        email: str,
        name: str | None = None,
        avatar_url: str | None = None,
    ) -> Profile:
        """Create or update the profile an identity signs in to.

        A new identity creates a new profile with it, in one commit.

        Args:
            issuer: The identity provider's issuer identifier.
            subject: The subject the provider names the person by.
            email: User email.
            name: Display name. Only fills an empty profile — a name the user
                set themselves (:meth:`update`) survives logins.
            avatar_url: Avatar URL (``None`` leaves the stored value untouched).

        Returns:
            The upserted Profile row.
        """
        with session_scope(self._engine) as session:
            db_profile = self._by_identity(session, issuer, subject)
            if db_profile:
                db_profile.email = email
                if name is not None and not db_profile.name:
                    db_profile.name = name
                if avatar_url is not None:
                    db_profile.avatar_url = avatar_url
            else:
                db_profile = Profile(email=email, name=name, avatar_url=avatar_url)
                session.add(db_profile)
                session.flush()
                session.add(Identity(issuer=issuer, subject=subject, profile_id=db_profile.id))
            save(session, db_profile)
            return db_profile

    def update(
        self,
        profile_id: UUID,
        *,
        name: str | None = None,
        timezone: str | None = None,
        last_organisation_id: UUID | None = None,
    ) -> Profile:
        """Update a profile's user-editable fields.

        Args:
            profile_id: Profile UUID.
            name: New display name (``None`` leaves the stored value untouched).
            timezone: New IANA timezone name (``None`` leaves the stored value untouched).
            last_organisation_id: The organisation the user last worked in
                (``None`` leaves the stored value untouched).

        Returns:
            The updated Profile.
        """
        with session_scope(self._engine) as session:
            db_profile = self.get(profile_id)
            if name is not None:
                db_profile.name = name
            if timezone is not None:
                db_profile.timezone = timezone
            if last_organisation_id is not None:
                db_profile.last_organisation_id = last_organisation_id
            save(session, db_profile)
            return db_profile

    def set_super_admin(self, profile_id: UUID, *, value: bool) -> Profile:
        """Set the platform-wide super-admin flag on a profile.

        Args:
            profile_id: Profile UUID.
            value: Whether the profile is a super-admin.

        Returns:
            The updated Profile, its ``organisations`` loaded.
        """
        with session_scope(self._engine) as session:
            db_profile = self.get(profile_id)
            db_profile.is_super_admin = value
            save(session, db_profile, "organisations")
            return db_profile

    def promote_super_admins(self, emails: Iterable[str]) -> builtins.list[Profile]:
        """Make super-admins of the profiles signed in under the given emails.

        Promote-only and idempotent: profiles already promoted, and emails no
        profile carries yet, are left alone. Emails match case-insensitively.

        Args:
            emails: The addresses to promote.

        Returns:
            The profiles this call promoted, oldest first.
        """
        addresses = sorted({email.lower() for email in emails})
        if not addresses:
            return []
        statement = (
            select(Profile)
            .where(func.lower(Profile.email).in_(addresses), col(Profile.is_super_admin).is_(False))
            .order_by(col(Profile.created_at), col(Profile.id))
        )
        with session_scope(self._engine) as session:
            promoted = builtins.list(session.exec(statement))
            for db_profile in promoted:
                db_profile.is_super_admin = True
                session.add(db_profile)
            commit(session)
            return promoted

    def delete(self, profile_id: UUID) -> None:
        """Delete a profile and everything anchored to it.

        Removes the user's identities, their tokens (login sessions and OAuth
        refresh tokens among them), the OAuth codes issued for them,
        organisation memberships, and the invitations they sent, then the
        profile row.

        Args:
            profile_id: Profile UUID.
        """
        with session_scope(self._engine) as session:
            db_profile = self.get(profile_id)
            for statement in (
                delete(Identity).where(col(Identity.profile_id) == profile_id),
                delete(Token).where(col(Token.user_id) == profile_id),
                delete(AuthorizationCode).where(col(AuthorizationCode.user_id) == profile_id),
                delete(UserOrganisation).where(col(UserOrganisation.user_id) == profile_id),
                delete(Invitation).where(col(Invitation.invited_by) == profile_id),
            ):
                session.connection().execute(statement)
            session.delete(db_profile)
            commit(session)

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _by_identity(session: Session, issuer: str, subject: str) -> Profile | None:
        """Read the profile an identity signs in to, in the caller's session.

        Args:
            session: The session to read in.
            issuer: The identity provider's issuer identifier.
            subject: The subject the provider names the person by.

        Returns:
            The profile, or ``None`` when no profile has that identity.
        """
        statement = (
            select(Profile)
            .join(Identity, col(Identity.profile_id) == col(Profile.id))
            .where(Identity.issuer == issuer, Identity.subject == subject)
        )
        return session.exec(statement).first()
