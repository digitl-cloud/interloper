"""Organisation persistence: the tenant, who belongs to it, and who is invited.

An organisation is the tenancy boundary every other facet scopes to, so this
one owns three tables rather than one: the org itself, the memberships that
carry a profile's role in it, and the invitations that are memberships not yet
accepted. Authentication (who a person is) is :mod:`~interloper_db.store.auth`;
this facet answers what they may do, and where.

Deleting an organisation reaches past those three tables into every facet that
scopes to it. That is the tenant purge, not a hidden coupling: it is bulk
statements, ordered children-first, and no other method here crosses over.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from interloper.errors import NotFoundError
from interloper.utils import assume_utc
from sqlalchemy import Engine, delete, func, update
from sqlmodel import Session, col, select

from interloper_db.models import (
    AuthSession,
    Component,
    ComponentRelation,
    Invitation,
    Organisation,
    PersonalAccessToken,
    Profile,
    Quota,
    Run,
    UserOrganisation,
)
from interloper_db.session import commit, session_scope

INVITATION_EXPIRY_DAYS = 7


class OrganisationStore:
    """Store methods for organisations, their members, and their invitations."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    # -- Organisations ---------------------------------------------------------

    def create(self, name: str, creator_id: UUID | None = None) -> Organisation:
        """Create an organisation, optionally making the creator an admin.

        Args:
            name: Organisation name.
            creator_id: Profile UUID of the creating user. When provided, the
                user is added as an ``admin`` member. Pass ``None`` (e.g. for
                super-admin provisioning) to create an org with no members.

        Returns:
            The created Organisation row.
        """
        with session_scope(self._engine) as session:
            db_organisation = Organisation(name=name)
            session.add(db_organisation)
            session.flush()

            if creator_id is not None:
                session.add(UserOrganisation(
                    user_id=creator_id,
                    organisation_id=db_organisation.id,
                    role="admin",
                ))
            commit(session)
            session.refresh(db_organisation)
            return db_organisation

    def update(self, org_id: UUID, name: str) -> Organisation:
        """Rename an organisation.

        Args:
            org_id: Organisation UUID.
            name: New organisation name.

        Returns:
            The updated Organisation.

        Raises:
            NotFoundError: If the organisation is not found or deleted.
        """
        with session_scope(self._engine) as session:
            db_organisation = session.get(Organisation, org_id)
            if not db_organisation or db_organisation.deleted_at is not None:
                raise NotFoundError(f"Organisation {org_id} not found")
            db_organisation.name = name
            session.add(db_organisation)
            commit(session)
            return db_organisation

    def list_all(self) -> list[tuple[Organisation, int]]:
        """List every organisation with its member count (super-admin only).

        Returns:
            List of ``(Organisation, member_count)`` tuples.
        """
        with session_scope(self._engine) as session:
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

    def delete(self, org_id: UUID) -> None:
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
        with session_scope(self._engine) as session:
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
            commit(session)

    def get(self, org_id: UUID, *, include_deleted: bool = False) -> Organisation | None:
        """Get an organisation by ID; soft-deleted orgs read as missing by default.

        Args:
            org_id: Organisation UUID.
            include_deleted: Also return soft-deleted organisations.

        Returns:
            The Organisation or None.
        """
        with session_scope(self._engine) as session:
            organisation = session.get(Organisation, org_id)
            if organisation and organisation.deleted_at is not None and not include_deleted:
                return None
            return organisation

    def list_activity(self, org_id: UUID, *, limit: int = 20) -> list[dict[str, Any]]:
        """A derived activity feed for one organisation, newest first.

        Composed purely from existing records — the organisation row,
        memberships, pending invitations, source components, and daily
        successful-run aggregates. There is no audit table, so events whose
        source rows are gone (accepted invitations' inviters, quota-change
        history) are not reconstructible and deliberately absent.

        Args:
            org_id: Organisation UUID.
            limit: Maximum entries to return (default 20), applied after the
                newest-first sort so the most recent activity always survives.

        Returns:
            Entries as ``{kind, when, subject, extra}`` dicts, ``when``
            always an aware UTC datetime.

        Raises:
            NotFoundError: If the organisation is not found.
        """

        def as_utc(value: Any) -> datetime:
            if isinstance(value, str):  # SQLite aggregates come back as text
                value = datetime.fromisoformat(value)
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value

        entries: list[dict[str, Any]] = []
        with session_scope(self._engine) as session:
            organisation = session.get(Organisation, org_id)
            if not organisation:
                raise NotFoundError(f"Organisation {org_id} not found")
            if organisation.created_at:
                entries.append({"kind": "org_created", "when": organisation.created_at, "subject": None, "extra": None})
            if organisation.deleted_at:
                entries.append({"kind": "org_deleted", "when": organisation.deleted_at, "subject": None, "extra": None})

            memberships = session.exec(
                select(UserOrganisation, Profile)
                .where(UserOrganisation.organisation_id == org_id, col(Profile.id) == UserOrganisation.user_id)
            ).all()
            for membership, profile in memberships:
                if membership.created_at:
                    entries.append({
                        "kind": "member_joined",
                        "when": membership.created_at,
                        "subject": profile.name or profile.email,
                        "extra": membership.role,
                    })

            invitations = session.exec(select(Invitation).where(Invitation.organisation_id == org_id)).all()
            inviter_ids = [invitation.invited_by for invitation in invitations]
            inviters = {
                profile.id: profile
                for profile in session.exec(select(Profile).where(col(Profile.id).in_(inviter_ids))).all()
            } if inviter_ids else {}
            for invitation in invitations:
                if invitation.created_at:
                    inviter = inviters.get(invitation.invited_by)
                    entries.append({
                        "kind": "invitation_sent",
                        "when": invitation.created_at,
                        "subject": invitation.email,
                        "extra": (inviter.name or inviter.email) if inviter else None,
                    })

            sources = session.exec(
                select(Component).where(col(Component.org_id) == org_id, col(Component.kind) == "source")
            ).all()
            for source in sources:
                if source.created_at:
                    entries.append({
                        "kind": "source_added",
                        "when": source.created_at,
                        "subject": source.name or source.key,
                        "extra": None,
                    })

            # func.date() buckets per calendar day on both Postgres and SQLite.
            day = func.date(col(Run.completed_at)).label("day")
            run_days = session.exec(
                select(day, func.count(), func.max(col(Run.completed_at)))
                .where(col(Run.org_id) == org_id, col(Run.status) == "success")
                .group_by(day)
            ).all()
            for _day, count, latest in run_days:
                entries.append({"kind": "runs_completed", "when": latest, "subject": str(count), "extra": None})

        for entry in entries:
            entry["when"] = as_utc(entry["when"])
        entries.sort(key=lambda entry: entry["when"], reverse=True)
        return entries[:limit]

    def list_for_user(self, user_id: UUID) -> list[Organisation]:
        """List all organisations a user belongs to.

        Args:
            user_id: Profile UUID.

        Returns:
            List of Organisation rows.
        """
        with session_scope(self._engine) as session:
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

    # -- Members ---------------------------------------------------------------

    def member_role(self, user_id: UUID, org_id: UUID) -> str | None:
        """Get a user's role in an organisation.

        Args:
            user_id: Profile UUID.
            org_id: Organisation UUID.

        Returns:
            The role string or None if not a member.
        """
        with session_scope(self._engine) as session:
            membership = self._get_membership(session, user_id, org_id)
            return membership.role if membership else None

    def list_members(self, org_id: UUID) -> list[tuple[Profile, str]]:
        """List all members of an organisation with their roles.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of ``(Profile, role)`` tuples.
        """
        with session_scope(self._engine) as session:
            memberships = session.exec(
                select(UserOrganisation).where(UserOrganisation.organisation_id == org_id)
            ).all()
            results: list[tuple[Profile, str]] = []
            for membership in memberships:
                db_profile = session.get(Profile, membership.user_id)
                if db_profile:
                    results.append((db_profile, membership.role))
            return results

    def add_member(self, org_id: UUID, user_id: UUID, role: str) -> bool:
        """Add a user to an organisation directly, without an invitation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID to add.
            role: Role to assign.

        Returns:
            True if added, False if the user is already a member (an
            idempotency signal, not a missing target).
        """
        with session_scope(self._engine) as session:
            if self._get_membership(session, user_id, org_id):
                return False
            session.add(UserOrganisation(
                user_id=user_id,
                organisation_id=org_id,
                role=role,
            ))
            commit(session)
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
        with session_scope(self._engine) as session:
            membership = self._get_membership(session, user_id, org_id)
            if not membership:
                raise NotFoundError(f"User {user_id} is not a member of organisation {org_id}")
            membership.role = role
            session.add(membership)
            commit(session)

    def remove_member(self, org_id: UUID, user_id: UUID) -> None:
        """Remove a member from an organisation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID to remove.

        Raises:
            NotFoundError: If the user is not a member.
        """
        with session_scope(self._engine) as session:
            membership = self._get_membership(session, user_id, org_id)
            if not membership:
                raise NotFoundError(f"User {user_id} is not a member of organisation {org_id}")
            session.delete(membership)
            commit(session)

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

        with session_scope(self._engine) as session:
            db_invitation = Invitation(
                organisation_id=org_id,
                email=email,
                role=role,
                token=token,
                invited_by=invited_by,
                expires_at=datetime.now(timezone.utc) + timedelta(days=INVITATION_EXPIRY_DAYS),
            )
            session.add(db_invitation)
            commit(session)
            session.refresh(db_invitation)
            return db_invitation

    def list_invitations(self, org_id: UUID) -> list[Invitation]:
        """List pending invitations for an organisation.

        Args:
            org_id: Organisation UUID.

        Returns:
            List of Invitation rows.
        """
        with session_scope(self._engine) as session:
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
        with session_scope(self._engine) as session:
            return session.exec(select(Invitation).where(Invitation.token == token)).first()

    def delete_invitation(self, invitation_id: UUID) -> None:
        """Delete an invitation.

        Args:
            invitation_id: Invitation UUID.

        Raises:
            NotFoundError: If the invitation is not found.
        """
        with session_scope(self._engine) as session:
            db_invitation = session.get(Invitation, invitation_id)
            if not db_invitation:
                raise NotFoundError(f"Invitation {invitation_id} not found")
            session.delete(db_invitation)
            commit(session)

    def has_pending_invitation(self, email: str) -> bool:
        """Check whether a non-expired invitation exists for an email.

        Args:
            email: Email address, matched case-insensitively.

        Returns:
            True when at least one pending invitation has not expired.
        """
        now = datetime.now(timezone.utc)
        with session_scope(self._engine) as session:
            db_invitations = session.exec(
                select(Invitation).where(func.lower(Invitation.email) == email.lower())
            ).all()
            return any(assume_utc(invitation.expires_at) > now for invitation in db_invitations)

    def accept_invitation(self, token: str, user_id: UUID) -> Organisation | None:
        """Accept an invitation: add user to org and delete the invitation.

        Args:
            token: The invitation token.
            user_id: Profile UUID of the accepting user.

        Returns:
            The Organisation joined, or None if invalid/expired.
        """
        with session_scope(self._engine) as session:
            db_invitation = session.exec(
                select(Invitation).where(Invitation.token == token)
            ).first()
            if not db_invitation:
                return None

            if assume_utc(db_invitation.expires_at) < datetime.now(timezone.utc):
                session.delete(db_invitation)
                commit(session)
                return None

            if not self._get_membership(session, user_id, db_invitation.organisation_id):
                session.add(UserOrganisation(
                    user_id=user_id,
                    organisation_id=db_invitation.organisation_id,
                    role=db_invitation.role,
                ))

            db_organisation = session.get(Organisation, db_invitation.organisation_id)
            session.delete(db_invitation)
            commit(session)
            return db_organisation

    @staticmethod
    def _get_membership(session: Session, user_id: UUID, org_id: UUID) -> UserOrganisation | None:
        """Fetch a user's membership row in an organisation.

        Args:
            session: Active database session.
            user_id: Profile UUID of the candidate member.
            org_id: Organisation UUID to look the membership up in.

        Returns:
            The membership, or ``None`` when the user is not a member.
        """
        return session.exec(
            select(UserOrganisation).where(
                UserOrganisation.user_id == user_id,
                UserOrganisation.organisation_id == org_id,
            )
        ).first()
