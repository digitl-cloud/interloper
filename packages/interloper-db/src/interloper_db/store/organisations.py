"""Organisation persistence: the tenant every other facet scopes to.

Who belongs to an organisation is :mod:`~interloper_db.store.members`, who is
invited is :mod:`~interloper_db.store.invitations`.

Deleting an organisation reaches past its own row into every facet that
scopes to it. That is the tenant purge, not a hidden coupling: it is bulk
statements, ordered children-first, and no other method here crosses over.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import Engine, delete, update
from sqlmodel import Session, col, func, select

from interloper_db.models import (
    AuthSession,
    Component,
    ComponentRelation,
    Invitation,
    Organisation,
    PersonalAccessToken,
    Profile,
    Quota,
    Role,
    Run,
    UserOrganisation,
)
from interloper_db.session import commit, session_scope
from interloper_db.store.page import Page, PageQuery


class OrganisationQuery(PageQuery):
    """Which organisations a listing reads.

    Attributes:
        user_id: Keep the organisations this profile is a member of; ``None``
            keeps every organisation.
        include_deleted: Keep soft-deleted organisations too.
    """

    user_id: UUID | None = None
    include_deleted: bool = False


@dataclass(frozen=True)
class ActivityEntry:
    """One event in an organisation's derived activity feed.

    Attributes:
        kind: What happened (``org_created``, ``member_joined``, …).
        when: When it happened, aware UTC.
        subject: Who or what it happened to, when the kind names one.
        extra: A detail the kind carries (a role, an inviter), or ``None``.
    """

    kind: str
    when: datetime
    subject: str | None = None
    extra: str | None = None


class OrganisationStore:
    """Store methods for organisations."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def get(self, org_id: UUID) -> Organisation:
        """Get an organisation by ID; soft-deleted organisations read as missing.

        Args:
            org_id: Organisation UUID.

        Returns:
            The organisation row.
        """
        with session_scope(self._engine) as session:
            return self._get_live(session, org_id)

    def list(self, query: OrganisationQuery) -> Page[Organisation]:
        """List organisations, oldest first.

        Args:
            query: Whose organisations, whether soft-deleted ones count, and
                the window to read.

        Returns:
            The page of organisations.
        """
        statement = select(Organisation).order_by(col(Organisation.created_at), col(Organisation.id))
        if query.user_id is not None:
            members = select(UserOrganisation.organisation_id).where(UserOrganisation.user_id == query.user_id)
            statement = statement.where(col(Organisation.id).in_(members))
        if not query.include_deleted:
            statement = statement.where(col(Organisation.deleted_at).is_(None))
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def create(self, name: str, creator_id: UUID | None = None) -> Organisation:
        """Create an organisation, optionally making the creator an admin.

        Args:
            name: Organisation name.
            creator_id: Profile UUID of the creating user, who becomes an
                ``admin`` member; ``None`` (super-admin provisioning) creates
                an organisation with no members.

        Returns:
            The created Organisation row.
        """
        with session_scope(self._engine) as session:
            db_organisation = Organisation(name=name)
            session.add(db_organisation)
            session.flush()
            if creator_id is not None:
                session.add(
                    UserOrganisation(user_id=creator_id, organisation_id=db_organisation.id, role=Role.ADMIN.value)
                )
            commit(session)
            session.refresh(db_organisation)
            return db_organisation

    def update(self, org_id: UUID, *, name: str) -> Organisation:
        """Rename an organisation.

        Args:
            org_id: Organisation UUID.
            name: New organisation name.

        Returns:
            The updated Organisation.
        """
        with session_scope(self._engine) as session:
            db_organisation = self._get_live(session, org_id)
            db_organisation.name = name
            session.add(db_organisation)
            commit(session)
            session.refresh(db_organisation)
            return db_organisation

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
        """
        with session_scope(self._engine) as session:
            db_organisation = self._get_live(session, org_id)
            for statement in (
                delete(ComponentRelation).where(col(ComponentRelation.org_id) == org_id),
                delete(Component).where(col(Component.org_id) == org_id),
                delete(PersonalAccessToken).where(col(PersonalAccessToken.organisation_id) == org_id),
                delete(Quota).where(col(Quota.org_id) == org_id),
                delete(Invitation).where(col(Invitation.organisation_id) == org_id),
                delete(UserOrganisation).where(col(UserOrganisation.organisation_id) == org_id),
                update(AuthSession).where(col(AuthSession.organisation_id) == org_id).values(organisation_id=None),
                update(Profile).where(col(Profile.last_organisation_id) == org_id).values(last_organisation_id=None),
            ):
                session.connection().execute(statement)
            db_organisation.deleted_at = datetime.now(timezone.utc)
            session.add(db_organisation)
            commit(session)

    def activity(self, org_id: UUID, query: PageQuery) -> Page[ActivityEntry]:
        """A derived activity feed for one organisation, newest first.

        Composed purely from existing records — the organisation row,
        memberships, pending invitations, source components, and daily
        successful-run aggregates. There is no audit table, so events whose
        source rows are gone (accepted invitations' inviters, quota-change
        history) are not reconstructible and deliberately absent.

        Args:
            org_id: Organisation UUID; a soft-deleted organisation still has
                its feed, ending in its deletion.
            query: The window to read.

        Returns:
            The page of entries, ``when`` always an aware UTC datetime.

        Raises:
            NotFoundError: If the organisation is not found.
        """
        entries: list[ActivityEntry] = []
        with session_scope(self._engine) as session:
            organisation = session.get(Organisation, org_id)
            if not organisation:
                raise NotFoundError(f"Organisation {org_id} not found")
            if organisation.created_at:
                entries.append(ActivityEntry("org_created", organisation.created_at))
            if organisation.deleted_at:
                entries.append(ActivityEntry("org_deleted", organisation.deleted_at))

            memberships = session.exec(
                select(UserOrganisation, Profile).where(
                    UserOrganisation.organisation_id == org_id, col(Profile.id) == UserOrganisation.user_id
                )
            ).all()
            for membership, profile in memberships:
                if membership.created_at:
                    subject = profile.name or profile.email
                    entries.append(ActivityEntry("member_joined", membership.created_at, subject, membership.role))

            invitations = session.exec(select(Invitation).where(Invitation.organisation_id == org_id)).all()
            inviter_ids = {invitation.invited_by for invitation in invitations}
            inviters = {
                profile.id: profile
                for profile in session.exec(select(Profile).where(col(Profile.id).in_(inviter_ids))).all()
            }
            for invitation in invitations:
                if invitation.created_at:
                    inviter = inviters.get(invitation.invited_by)
                    entries.append(
                        ActivityEntry(
                            "invitation_sent",
                            invitation.created_at,
                            invitation.email,
                            (inviter.name or inviter.email) if inviter else None,
                        )
                    )

            sources = session.exec(
                select(Component).where(col(Component.org_id) == org_id, col(Component.kind) == "source")
            ).all()
            for source in sources:
                if source.created_at:
                    entries.append(ActivityEntry("source_added", source.created_at, source.name or source.key))

            # func.date() buckets per calendar day on both Postgres and SQLite.
            day = func.date(col(Run.completed_at)).label("day")
            run_days = session.exec(
                select(day, func.count(), func.max(col(Run.completed_at)))
                .where(col(Run.org_id) == org_id, col(Run.status) == "success")
                .group_by(day)
            ).all()
            for _day, count, latest in run_days:
                if latest is not None:
                    entries.append(ActivityEntry("runs_completed", latest, str(count)))

        normalized = [
            ActivityEntry(entry.kind, self._as_utc(entry.when), entry.subject, entry.extra) for entry in entries
        ]
        normalized.sort(key=lambda entry: entry.when, reverse=True)
        return Page.window(normalized, query)

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _get_live(session: Session, org_id: UUID) -> Organisation:
        """Fetch an organisation that has not been soft-deleted.

        Args:
            session: Open session to read through.
            org_id: Organisation UUID.

        Returns:
            The organisation row.

        Raises:
            NotFoundError: If no live organisation carries that id.
        """
        organisation = session.get(Organisation, org_id)
        if not organisation or organisation.deleted_at is not None:
            raise NotFoundError(f"Organisation {org_id} not found")
        return organisation

    @staticmethod
    def _as_utc(value: Any) -> datetime:
        """An activity timestamp as an aware UTC datetime.

        Args:
            value: The stored timestamp; SQLite aggregates come back as text,
                and SQLite columns as naive datetimes.

        Returns:
            The aware datetime.
        """
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
