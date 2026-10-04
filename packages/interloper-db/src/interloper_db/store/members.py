"""Membership persistence: who belongs to an organisation, and with which role.

A membership is one ``(organisation, profile)`` pair carrying a
:class:`~interloper_db.models.Role`. Every write validates the role, so a name
outside the vocabulary never reaches the table.
"""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlalchemy.orm import selectinload
from sqlmodel import Session, col, func, select

from interloper_db.models import Role, UserOrganisation
from interloper_db.session import commit, session_scope
from interloper_db.store.page import Page, PageQuery


class MemberStore:
    """Store methods for organisation memberships."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def role(self, org_id: UUID, user_id: UUID) -> str | None:
        """The role a profile holds in an organisation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID.

        Returns:
            The role name, or ``None`` when the profile is not a member.
        """
        with session_scope(self._engine) as session:
            membership = session.get(UserOrganisation, (user_id, org_id))
            return membership.role if membership else None

    def list(self, org_id: UUID, query: PageQuery) -> Page[UserOrganisation]:
        """List an organisation's members, oldest membership first.

        Args:
            org_id: Organisation UUID.
            query: The window to read.

        Returns:
            The page of memberships, each with its ``profile`` loaded.
        """
        statement = (
            select(UserOrganisation)
            .where(UserOrganisation.organisation_id == org_id)
            .options(selectinload(UserOrganisation.profile))  # ty: ignore[invalid-argument-type]
            .order_by(col(UserOrganisation.created_at), col(UserOrganisation.user_id))
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def count_by_org(self, org_ids: Sequence[UUID]) -> dict[UUID, int]:
        """How many members each organisation has, in one query.

        Args:
            org_ids: The organisations to count.

        Returns:
            The member count by organisation; one with no members is absent.
        """
        if not org_ids:
            return {}
        statement = (
            select(col(UserOrganisation.organisation_id), func.count())
            .where(col(UserOrganisation.organisation_id).in_(org_ids))
            .group_by(col(UserOrganisation.organisation_id))
        )
        with session_scope(self._engine) as session:
            return dict(session.exec(statement).all())

    def add(self, org_id: UUID, user_id: UUID, role: str) -> bool:
        """Add a profile to an organisation directly, without an invitation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID to add.
            role: Role to assign, one of :class:`Role`; any other name is a
                ``ConfigError``.

        Returns:
            True if added, False if the profile is already a member (an
            idempotency signal, not a missing target).
        """
        role = Role.parse(role).value
        with session_scope(self._engine) as session:
            if session.get(UserOrganisation, (user_id, org_id)):
                return False
            session.add(UserOrganisation(user_id=user_id, organisation_id=org_id, role=role))
            commit(session)
            return True

    def update(self, org_id: UUID, user_id: UUID, role: str) -> UserOrganisation:
        """Change a member's role.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID of the member.
            role: New role, one of :class:`Role`; any other name is a
                ``ConfigError``.

        Returns:
            The updated membership, with its ``profile`` loaded.
        """
        role = Role.parse(role).value
        with session_scope(self._engine) as session:
            membership = self._get(session, org_id, user_id)
            membership.role = role
            session.add(membership)
            commit(session)
            session.refresh(membership)
            _ = membership.profile  # load before the session closes; readers reach it detached
            return membership

    def delete(self, org_id: UUID, user_id: UUID) -> None:
        """Remove a member from an organisation.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID to remove.
        """
        with session_scope(self._engine) as session:
            session.delete(self._get(session, org_id, user_id))
            commit(session)

    @staticmethod
    def _get(session: Session, org_id: UUID, user_id: UUID) -> UserOrganisation:
        """Fetch a membership row.

        Args:
            session: Open session to read through.
            org_id: Organisation UUID.
            user_id: Profile UUID.

        Returns:
            The membership row.

        Raises:
            NotFoundError: If the profile is not a member of the organisation.
        """
        membership = session.get(UserOrganisation, (user_id, org_id))
        if not membership:
            raise NotFoundError(f"User {user_id} is not a member of organisation {org_id}")
        return membership
