"""Organisation persistence: the tenant every other facet scopes to.

Who belongs to an organisation is :mod:`~interloper_db.store.members`, who is
invited is :mod:`~interloper_db.store.invitations`.

Deleting an organisation reaches past its own row into every facet that
scopes to it. That is the tenant purge, not a hidden coupling: it is bulk
statements, ordered children-first, and no other method here crosses over.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import Engine, delete, update
from sqlmodel import col, select

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
    UserOrganisation,
)
from interloper_db.session import commit, save, session_scope
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

        Raises:
            NotFoundError: If no live organisation carries that id.
        """
        with session_scope(self._engine) as session:
            organisation = session.get(Organisation, org_id)
            if not organisation or organisation.deleted_at is not None:
                raise NotFoundError(f"Organisation {org_id} not found")
            return organisation

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
            save(session, db_organisation)
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
            db_organisation = self.get(org_id)
            db_organisation.name = name
            save(session, db_organisation)
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
            db_organisation = self.get(org_id)
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
