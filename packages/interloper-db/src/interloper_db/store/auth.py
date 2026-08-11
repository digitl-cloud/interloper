"""Auth persistence: profiles, sessions, organisations, memberships."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import delete, func, update
from sqlmodel import Session, select

from interloper_db.models import (
    AuthSession,
    Component,
    ComponentRelation,
    Invitation,
    Organisation,
    PersonalAccessToken,
    Profile,
    Quota,
    UserOrganisation,
)
from interloper_db.store.base import StoreBase

INVITATION_EXPIRY_DAYS = 7

SESSION_EXPIRY_DAYS = 30


def _as_utc(ts: datetime) -> datetime:
    """Treat naive timestamps as UTC (SQLite test databases drop the offset)."""
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _hash_token(token: str) -> str:
    """Hash a raw session token the way it is stored (only hashes persist).

    Returns:
        The SHA-256 hex digest.
    """
    return hashlib.sha256(token.encode()).hexdigest()


def _get_membership(session: Session, user_id: UUID, org_id: UUID) -> UserOrganisation | None:
    """Fetch a user's membership row in an organisation.

    Returns:
        The membership, or ``None`` when the user is not a member.
    """
    return session.exec(
        select(UserOrganisation).where(
            UserOrganisation.user_id == user_id,
            UserOrganisation.organisation_id == org_id,
        )
    ).first()


class AuthMixin(StoreBase):
    """Store methods for authentication and organisation management.

    Error contract (same as the component/run mixins): lookups return
    ``None`` when the row is absent — soft probes, like ``Registry.get`` —
    while mutations raise :class:`NotFoundError` on a missing target.
    """

    # -- Profiles -------------------------------------------------------------

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
            name: Display name (``None`` leaves the stored value untouched).
            avatar_url: Avatar URL (``None`` leaves the stored value untouched).

        Returns:
            The upserted Profile row.
        """
        with self._session() as session:
            statement = select(Profile).where(Profile.google_id == google_id)
            db_profile = session.exec(statement).first()

            if db_profile:
                db_profile.email = email
                if name is not None:
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

            session.commit()
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
        with self._session() as session:
            db_profile = session.get(Profile, user_id)
            if not db_profile:
                raise NotFoundError(f"Profile {user_id} not found")
            db_profile.is_super_admin = is_super_admin
            session.add(db_profile)
            session.commit()
            return db_profile

    def get_profile(self, user_id: UUID) -> Profile | None:
        """Get a profile by ID.

        Args:
            user_id: Profile UUID.

        Returns:
            The Profile or None.
        """
        with self._session() as session:
            return session.get(Profile, user_id)

    def list_all_profiles(self) -> list[tuple[Profile, list[Organisation]]]:
        """List every profile with the organisations it belongs to (super-admin only).

        Returns:
            List of ``(Profile, organisations)`` tuples.
        """
        with self._session() as session:
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
        with self._session() as session:
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
            session.commit()

    def get_profile_by_google_id(self, google_id: str) -> Profile | None:
        """Get a profile by Google OAuth subject identifier.

        Args:
            google_id: Google OAuth subject identifier.

        Returns:
            The Profile or None.
        """
        with self._session() as session:
            return session.exec(select(Profile).where(Profile.google_id == google_id)).first()

    # -- Sessions -------------------------------------------------------------

    def create_session(self, user_id: UUID, organisation_id: UUID | None = None) -> str:
        """Create a session and return the raw (unhashed) token.

        Args:
            user_id: Profile UUID.
            organisation_id: Optional org to bind to session.

        Returns:
            The raw session token (to be set as a cookie).
        """
        token = secrets.token_urlsafe(48)
        token_hash = _hash_token(token)

        with self._session() as session:
            db_session = AuthSession(
                user_id=user_id,
                organisation_id=organisation_id,
                token_hash=token_hash,
                expires_at=datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRY_DAYS),
            )
            session.add(db_session)
            session.commit()

        return token

    def resolve_session(self, token: str) -> tuple[Profile, AuthSession] | None:
        """Resolve a session token to a profile and session row.

        Args:
            token: The raw session token from the cookie.

        Returns:
            ``(Profile, Session)`` if valid, else ``None``.
        """
        token_hash = _hash_token(token)

        with self._session() as session:
            statement = select(AuthSession).where(AuthSession.token_hash == token_hash)
            db_session = session.exec(statement).first()
            if not db_session:
                return None

            if _as_utc(db_session.expires_at) < datetime.now(timezone.utc):
                session.delete(db_session)
                session.commit()
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
        token_hash = _hash_token(token)

        with self._session() as session:
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

            session.commit()

    def delete_user_sessions(self, user_id: UUID) -> None:
        """Delete all sessions for a user.

        Args:
            user_id: Profile UUID.
        """
        with self._session() as session:
            db_sessions = session.exec(select(AuthSession).where(AuthSession.user_id == user_id)).all()
            for db_session in db_sessions:
                session.delete(db_session)
            session.commit()

    # -- Organisations --------------------------------------------------------

    def create_organisation(self, name: str, creator_id: UUID | None = None) -> Organisation:
        """Create an organisation, optionally making the creator an admin.

        Args:
            name: Organisation name.
            creator_id: Profile UUID of the creating user. When provided, the
                user is added as an ``admin`` member. Pass ``None`` (e.g. for
                super-admin provisioning) to create an org with no members.

        Returns:
            The created Organisation row.
        """
        with self._session() as session:
            db_organisation = Organisation(name=name)
            session.add(db_organisation)
            session.flush()

            if creator_id is not None:
                session.add(UserOrganisation(
                    user_id=creator_id,
                    organisation_id=db_organisation.id,
                    role="admin",
                ))
            session.commit()
            session.refresh(db_organisation)
            return db_organisation

    def update_organisation(self, org_id: UUID, name: str) -> Organisation:
        """Rename an organisation.

        Args:
            org_id: Organisation UUID.
            name: New organisation name.

        Returns:
            The updated Organisation.

        Raises:
            NotFoundError: If the organisation is not found or deleted.
        """
        with self._session() as session:
            db_organisation = session.get(Organisation, org_id)
            if not db_organisation or db_organisation.deleted_at is not None:
                raise NotFoundError(f"Organisation {org_id} not found")
            db_organisation.name = name
            session.add(db_organisation)
            session.commit()
            return db_organisation

    def list_all_organisations(self) -> list[tuple[Organisation, int]]:
        """List every organisation with its member count (super-admin only).

        Returns:
            List of ``(Organisation, member_count)`` tuples.
        """
        with self._session() as session:
            organisations = session.exec(select(Organisation)).all()
            counts = dict(
                session.exec(
                    select(
                        UserOrganisation.organisation_id,
                        func.count(UserOrganisation.user_id),  # ty: ignore[invalid-argument-type]
                    ).group_by(UserOrganisation.organisation_id)  # ty: ignore[invalid-argument-type]
                ).all()
            )
            return [(org, counts.get(org.id, 0)) for org in organisations]

    def delete_organisation(self, org_id: UUID) -> None:
        """Soft-delete an organisation: purge its payload, keep the ledger.

        The org row survives with ``deleted_at`` stamped, and so do its
        runs, events, and backfills — execution history and the usage
        ledger must stay attributable for billing, even after the org is
        gone. Everything sensitive or live is removed: components and
        their relations (encrypted credentials, client config), tokens,
        quota overrides, invitations, and memberships; sessions and
        profiles pointing at the org are detached. Retained runs and
        backfills lose their component reference via the FK's SET NULL.
        Bulk statements — ordered children-first — so the cascade never
        depends on ORM-loaded state.

        Args:
            org_id: Organisation UUID.

        Raises:
            NotFoundError: If the organisation is not found or already deleted.
        """
        with self._session() as session:
            db_organisation = session.get(Organisation, org_id)
            if not db_organisation or db_organisation.deleted_at is not None:
                raise NotFoundError(f"Organisation {org_id} not found")

            # ty misreads SQLModel column comparisons in DML where() as plain bools.
            statements = (
                delete(ComponentRelation).where(ComponentRelation.org_id == org_id),  # ty: ignore[invalid-argument-type]
                delete(Component).where(Component.org_id == org_id),  # ty: ignore[invalid-argument-type]
                delete(PersonalAccessToken).where(PersonalAccessToken.organisation_id == org_id),  # ty: ignore[invalid-argument-type]
                delete(Quota).where(Quota.org_id == org_id),  # ty: ignore[invalid-argument-type]
                delete(Invitation).where(Invitation.organisation_id == org_id),  # ty: ignore[invalid-argument-type]
                delete(UserOrganisation).where(UserOrganisation.organisation_id == org_id),  # ty: ignore[invalid-argument-type]
                update(AuthSession).where(AuthSession.organisation_id == org_id).values(organisation_id=None),  # ty: ignore[invalid-argument-type]
                update(Profile).where(Profile.last_organisation_id == org_id).values(last_organisation_id=None),  # ty: ignore[invalid-argument-type]
            )
            connection = session.connection()
            for statement in statements:
                connection.execute(statement)

            db_organisation.deleted_at = datetime.now(timezone.utc)
            session.add(db_organisation)
            session.commit()

    def get_organisation(self, org_id: UUID, *, include_deleted: bool = False) -> Organisation | None:
        """Get an organisation by ID; soft-deleted orgs read as missing by default.

        Args:
            org_id: Organisation UUID.
            include_deleted: Also return soft-deleted organisations.

        Returns:
            The Organisation or None.
        """
        with self._session() as session:
            organisation = session.get(Organisation, org_id)
            if organisation and organisation.deleted_at is not None and not include_deleted:
                return None
            return organisation

    def list_user_organisations(self, user_id: UUID) -> list[Organisation]:
        """List all organisations a user belongs to.

        Args:
            user_id: Profile UUID.

        Returns:
            List of Organisation rows.
        """
        with self._session() as session:
            memberships = session.exec(
                select(UserOrganisation).where(UserOrganisation.user_id == user_id)
            ).all()
            organisation_ids = [membership.organisation_id for membership in memberships]
            if not organisation_ids:
                return []
            organisations = session.exec(
                select(Organisation).where(Organisation.id.in_(organisation_ids))  # ty: ignore[unresolved-attribute]
            ).all()
            return list(organisations)

    def get_user_role(self, user_id: UUID, org_id: UUID) -> str | None:
        """Get a user's role in an organisation.

        Args:
            user_id: Profile UUID.
            org_id: Organisation UUID.

        Returns:
            The role string or None if not a member.
        """
        with self._session() as session:
            membership = _get_membership(session, user_id, org_id)
            return membership.role if membership else None

    def list_org_members(self, org_id: UUID) -> list[tuple[Profile, str]]:
        """List all members of an organisation with their roles.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of ``(Profile, role)`` tuples.
        """
        with self._session() as session:
            memberships = session.exec(
                select(UserOrganisation).where(UserOrganisation.organisation_id == org_id)
            ).all()
            results: list[tuple[Profile, str]] = []
            for membership in memberships:
                db_profile = session.get(Profile, membership.user_id)
                if db_profile:
                    results.append((db_profile, membership.role))
            return results

    def add_org_member(self, org_id: UUID, user_id: UUID, role: str) -> bool:
        """Add a user to an organisation directly, without an invitation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID to add.
            role: Role to assign.

        Returns:
            True if added, False if the user is already a member (an
            idempotency signal, not a missing target).
        """
        with self._session() as session:
            if _get_membership(session, user_id, org_id):
                return False
            session.add(UserOrganisation(
                user_id=user_id,
                organisation_id=org_id,
                role=role,
            ))
            session.commit()
            return True

    def update_member_role(self, org_id: UUID, user_id: UUID, role: str) -> None:
        """Update a member's role within an organisation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID of the member.
            role: New role to assign.

        Raises:
            NotFoundError: If the user is not a member.
        """
        with self._session() as session:
            membership = _get_membership(session, user_id, org_id)
            if not membership:
                raise NotFoundError(f"User {user_id} is not a member of organisation {org_id}")
            membership.role = role
            session.add(membership)
            session.commit()

    def remove_org_member(self, org_id: UUID, user_id: UUID) -> None:
        """Remove a member from an organisation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID to remove.

        Raises:
            NotFoundError: If the user is not a member.
        """
        with self._session() as session:
            membership = _get_membership(session, user_id, org_id)
            if not membership:
                raise NotFoundError(f"User {user_id} is not a member of organisation {org_id}")
            session.delete(membership)
            session.commit()

    # -- Invitations -----------------------------------------------------------

    def create_invitation(
        self,
        org_id: UUID,
        email: str,
        role: str,
        invited_by: UUID,
    ) -> Invitation:
        """Create an invitation for a user to join an organisation.

        Args:
            org_id: Organisation UUID.
            email: Email to invite.
            role: Role to assign on acceptance.
            invited_by: Profile UUID of the inviter.

        Returns:
            The created Invitation row.
        """
        token = secrets.token_urlsafe(32)

        with self._session() as session:
            db_invitation = Invitation(
                organisation_id=org_id,
                email=email,
                role=role,
                token=token,
                invited_by=invited_by,
                expires_at=datetime.now(timezone.utc) + timedelta(days=INVITATION_EXPIRY_DAYS),
            )
            session.add(db_invitation)
            session.commit()
            session.refresh(db_invitation)
            return db_invitation

    def list_invitations(self, org_id: UUID) -> list[Invitation]:
        """List pending invitations for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Invitation rows.
        """
        with self._session() as session:
            db_invitations = session.exec(
                select(Invitation).where(Invitation.organisation_id == org_id)
            ).all()
            return list(db_invitations)

    def get_invitation_by_token(self, token: str) -> Invitation | None:
        """Resolve an invitation by its token.

        Args:
            token: The invitation token.

        Returns:
            The Invitation or None.
        """
        with self._session() as session:
            return session.exec(select(Invitation).where(Invitation.token == token)).first()

    def delete_invitation(self, invitation_id: UUID) -> None:
        """Delete an invitation.

        Args:
            invitation_id: Invitation UUID.

        Raises:
            NotFoundError: If the invitation is not found.
        """
        with self._session() as session:
            db_invitation = session.get(Invitation, invitation_id)
            if not db_invitation:
                raise NotFoundError(f"Invitation {invitation_id} not found")
            session.delete(db_invitation)
            session.commit()

    def has_pending_invitation(self, email: str) -> bool:
        """Check whether a non-expired invitation exists for an email.

        Args:
            email: Email address, matched case-insensitively.

        Returns:
            True when at least one pending invitation has not expired.
        """
        now = datetime.now(timezone.utc)
        with self._session() as session:
            db_invitations = session.exec(
                select(Invitation).where(func.lower(Invitation.email) == email.lower())
            ).all()
            return any(_as_utc(invitation.expires_at) > now for invitation in db_invitations)

    def accept_invitation(self, token: str, user_id: UUID) -> Organisation | None:
        """Accept an invitation: add user to org and delete the invitation.

        Args:
            token: The invitation token.
            user_id: Profile UUID of the accepting user.

        Returns:
            The Organisation joined, or None if invalid/expired.
        """
        with self._session() as session:
            db_invitation = session.exec(
                select(Invitation).where(Invitation.token == token)
            ).first()
            if not db_invitation:
                return None

            if _as_utc(db_invitation.expires_at) < datetime.now(timezone.utc):
                session.delete(db_invitation)
                session.commit()
                return None

            if not _get_membership(session, user_id, db_invitation.organisation_id):
                session.add(UserOrganisation(
                    user_id=user_id,
                    organisation_id=db_invitation.organisation_id,
                    role=db_invitation.role,
                ))

            db_organisation = session.get(Organisation, db_invitation.organisation_id)
            session.delete(db_invitation)
            session.commit()
            return db_organisation
