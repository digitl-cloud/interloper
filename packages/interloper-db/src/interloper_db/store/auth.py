"""Auth persistence: profiles, sessions, organisations, memberships."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import func
from sqlmodel import Session, select

from interloper_db.engine import get_engine
from interloper_db.models import Invitation, Organisation, Profile, UserOrganisation
from interloper_db.models import Session as SessionModel

INVITATION_EXPIRY_DAYS = 7

SESSION_EXPIRY_DAYS = 30


class AuthMixin:
    """Store methods for authentication and organisation management."""

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
            name: Display name.
            avatar_url: Avatar URL.

        Returns:
            The upserted Profile row.
        """
        with Session(get_engine()) as session:
            statement = select(Profile).where(Profile.google_id == google_id)
            db_profile = session.exec(statement).first()

            if db_profile:
                db_profile.email = email
                db_profile.name = name
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

    def get_profile(self, user_id: UUID) -> Profile | None:
        """Get a profile by ID.

        Args:
            user_id: Profile UUID.

        Returns:
            The Profile or None.
        """
        with Session(get_engine()) as session:
            return session.get(Profile, user_id)

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
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        with Session(get_engine()) as session:
            db_session = SessionModel(
                user_id=user_id,
                organisation_id=organisation_id,
                token_hash=token_hash,
                expires_at=datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRY_DAYS),
            )
            session.add(db_session)
            session.commit()

        return token

    def resolve_session(self, token: str) -> tuple[Profile, SessionModel] | None:
        """Resolve a session token to a profile and session row.

        Args:
            token: The raw session token from the cookie.

        Returns:
            ``(Profile, Session)`` if valid, else ``None``.
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        with Session(get_engine()) as session:
            statement = select(SessionModel).where(SessionModel.token_hash == token_hash)
            db_session = session.exec(statement).first()
            if not db_session:
                return None

            if db_session.expires_at < datetime.now(timezone.utc):
                session.delete(db_session)
                session.commit()
                return None

            db_profile = session.get(Profile, db_session.user_id)
            if not db_profile:
                return None

            # Expunge so objects survive session close
            session.expunge(db_session)
            session.expunge(db_profile)
            return db_profile, db_session

    def set_session_org(self, token: str, org_id: UUID, user_id: UUID | None = None) -> None:
        """Update the session's active organisation and persist preference on profile.

        Args:
            token: The raw session token.
            org_id: Organisation UUID to switch to.
            user_id: If provided, also update ``last_organisation_id`` on the profile.
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        with Session(get_engine()) as session:
            db_session = session.exec(
                select(SessionModel).where(SessionModel.token_hash == token_hash)
            ).first()
            if db_session:
                db_session.organisation_id = org_id  # type: ignore[assignment]
                session.add(db_session)

            if user_id:
                db_profile = session.get(Profile, user_id)
                if db_profile:
                    db_profile.last_organisation_id = org_id  # type: ignore[assignment]
                    session.add(db_profile)

            session.commit()

    def delete_user_sessions(self, user_id: UUID) -> None:
        """Delete all sessions for a user.

        Args:
            user_id: Profile UUID.
        """
        with Session(get_engine()) as session:
            db_sessions = session.exec(select(SessionModel).where(SessionModel.user_id == user_id)).all()
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
        with Session(get_engine()) as session:
            db_organisation = Organisation(name=name)
            session.add(db_organisation)
            session.flush()

            if creator_id is not None:
                session.add(UserOrganisation(
                    user_id=creator_id,
                    organisation_id=db_organisation.id,  # type: ignore[arg-type]
                    role="admin",
                ))
            session.commit()
            session.refresh(db_organisation)
            return db_organisation

    def update_organisation(self, org_id: UUID, name: str) -> Organisation | None:
        """Rename an organisation.

        Args:
            org_id: Organisation UUID.
            name: New organisation name.

        Returns:
            The updated Organisation, or None if not found.
        """
        with Session(get_engine()) as session:
            db_organisation = session.get(Organisation, org_id)
            if not db_organisation:
                return None
            db_organisation.name = name
            session.add(db_organisation)
            session.commit()
            session.refresh(db_organisation)
            return db_organisation

    def list_all_organisations(self) -> list[tuple[Organisation, int]]:
        """List every organisation with its member count (super-admin only).

        Returns:
            List of ``(Organisation, member_count)`` tuples.
        """
        with Session(get_engine()) as session:
            organisations = session.exec(select(Organisation)).all()
            counts = dict(
                session.exec(
                    select(
                        UserOrganisation.organisation_id,
                        func.count(UserOrganisation.user_id),  # type: ignore[arg-type]
                    ).group_by(UserOrganisation.organisation_id)  # type: ignore[arg-type]
                ).all()
            )
            return [(org, counts.get(org.id, 0)) for org in organisations]  # type: ignore[arg-type]

    def get_organisation(self, org_id: UUID) -> Organisation | None:
        """Get an organisation by ID.

        Args:
            org_id: Organisation UUID.

        Returns:
            The Organisation or None.
        """
        with Session(get_engine()) as session:
            return session.get(Organisation, org_id)

    def list_user_organisations(self, user_id: UUID) -> list[Organisation]:
        """List all organisations a user belongs to.

        Args:
            user_id: Profile UUID.

        Returns:
            List of Organisation rows.
        """
        with Session(get_engine()) as session:
            memberships = session.exec(
                select(UserOrganisation).where(UserOrganisation.user_id == user_id)
            ).all()
            organisation_ids = [membership.organisation_id for membership in memberships]
            if not organisation_ids:
                return []
            organisations = session.exec(
                select(Organisation).where(Organisation.id.in_(organisation_ids))  # type: ignore[union-attr]
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
        with Session(get_engine()) as session:
            membership = session.exec(
                select(UserOrganisation).where(
                    UserOrganisation.user_id == user_id,
                    UserOrganisation.organisation_id == org_id,
                )
            ).first()
            return membership.role if membership else None

    def list_org_members(self, org_id: UUID) -> list[tuple[Profile, str]]:
        """List all members of an organisation with their roles.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of ``(Profile, role)`` tuples.
        """
        with Session(get_engine()) as session:
            memberships = session.exec(
                select(UserOrganisation).where(UserOrganisation.organisation_id == org_id)
            ).all()
            results: list[tuple[Profile, str]] = []
            for membership in memberships:
                db_profile = session.get(Profile, membership.user_id)
                if db_profile:
                    session.expunge(db_profile)
                    results.append((db_profile, membership.role))
            return results

    def update_member_role(self, org_id: UUID, user_id: UUID, role: str) -> bool:
        """Update a member's role within an organisation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID of the member.
            role: New role to assign.

        Returns:
            True if updated, False if the user is not a member.
        """
        with Session(get_engine()) as session:
            membership = session.exec(
                select(UserOrganisation).where(
                    UserOrganisation.user_id == user_id,
                    UserOrganisation.organisation_id == org_id,
                )
            ).first()
            if not membership:
                return False
            membership.role = role
            session.add(membership)
            session.commit()
            return True

    def remove_org_member(self, org_id: UUID, user_id: UUID) -> bool:
        """Remove a member from an organisation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID to remove.

        Returns:
            True if removed, False if not found.
        """
        with Session(get_engine()) as session:
            membership = session.exec(
                select(UserOrganisation).where(
                    UserOrganisation.user_id == user_id,
                    UserOrganisation.organisation_id == org_id,
                )
            ).first()
            if not membership:
                return False
            session.delete(membership)
            session.commit()
            return True

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

        with Session(get_engine()) as session:
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
        with Session(get_engine()) as session:
            db_invitations = session.exec(
                select(Invitation).where(Invitation.organisation_id == org_id)
            ).all()
            for db_invitation in db_invitations:
                session.expunge(db_invitation)
            return list(db_invitations)

    def get_invitation_by_token(self, token: str) -> Invitation | None:
        """Resolve an invitation by its token.

        Args:
            token: The invitation token.

        Returns:
            The Invitation or None.
        """
        with Session(get_engine()) as session:
            db_invitation = session.exec(
                select(Invitation).where(Invitation.token == token)
            ).first()
            if db_invitation:
                session.expunge(db_invitation)
            return db_invitation

    def delete_invitation(self, invitation_id: UUID) -> bool:
        """Delete an invitation.

        Args:
            invitation_id: Invitation UUID.

        Returns:
            True if deleted, False if not found.
        """
        with Session(get_engine()) as session:
            db_invitation = session.get(Invitation, invitation_id)
            if not db_invitation:
                return False
            session.delete(db_invitation)
            session.commit()
            return True

    def accept_invitation(self, token: str, user_id: UUID) -> Organisation | None:
        """Accept an invitation: add user to org and delete the invitation.

        Args:
            token: The invitation token.
            user_id: Profile UUID of the accepting user.

        Returns:
            The Organisation joined, or None if invalid/expired.
        """
        with Session(get_engine()) as session:
            db_invitation = session.exec(
                select(Invitation).where(Invitation.token == token)
            ).first()
            if not db_invitation:
                return None

            if db_invitation.expires_at < datetime.now(timezone.utc):
                session.delete(db_invitation)
                session.commit()
                return None

            # Check if already a member
            existing_membership = session.exec(
                select(UserOrganisation).where(
                    UserOrganisation.user_id == user_id,
                    UserOrganisation.organisation_id == db_invitation.organisation_id,
                )
            ).first()
            if not existing_membership:
                session.add(UserOrganisation(
                    user_id=user_id,
                    organisation_id=db_invitation.organisation_id,
                    role=db_invitation.role,
                ))

            db_organisation = session.get(Organisation, db_invitation.organisation_id)
            session.delete(db_invitation)
            session.commit()
            if db_organisation:
                session.expunge(db_organisation)
            return db_organisation
