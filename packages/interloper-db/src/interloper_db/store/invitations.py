"""Invitation persistence: memberships not yet accepted.

An invitation is addressed by email and redeemed by its token. It is read and
deleted within its organisation, so an id never acts as a cross-tenant handle;
redeeming takes the token alone, since that is all the recipient holds.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlmodel import col, func, select

from interloper_db.models import Invitation, Organisation, Role, UserOrganisation
from interloper_db.session import commit, save, session_scope
from interloper_db.store.page import Page, PageQuery

INVITATION_EXPIRY_DAYS = 7


class InvitationStore:
    """Store methods for organisation invitations."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def list(self, org_id: UUID, query: PageQuery) -> Page[Invitation]:
        """List an organisation's pending invitations, newest first.

        Args:
            org_id: Organisation UUID.
            query: The window to read.

        Returns:
            The page of invitations.
        """
        statement = (
            select(Invitation)
            .where(Invitation.organisation_id == org_id)
            .order_by(col(Invitation.created_at).desc(), col(Invitation.id))
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def create(self, org_id: UUID, *, email: str, role: str, invited_by: UUID) -> Invitation:
        """Invite an email address to join an organisation.

        Args:
            org_id: Organisation UUID.
            email: Address to invite.
            role: Role granted on acceptance, one of :class:`Role`; any other
                name is a ``ConfigError``.
            invited_by: Profile UUID of the inviter.

        Returns:
            The created Invitation row.
        """
        with session_scope(self._engine) as session:
            invitation = self._new(org_id, email=email, role=role, invited_by=invited_by)
            save(session, invitation)
            return invitation

    def reissue(self, invitation_id: UUID, *, org_id: UUID, invited_by: UUID) -> Invitation:
        """Replace an invitation with a fresh one: new token, new expiry, same address and role.

        The old link stops working in the same transaction the new one is
        issued in.

        Args:
            invitation_id: Invitation UUID.
            org_id: Organisation the invitation must belong to.
            invited_by: Profile UUID of whoever reissues it.

        Returns:
            The new Invitation row.
        """
        with session_scope(self._engine) as session:
            previous = self._lock(invitation_id, org_id)
            session.delete(previous)
            session.flush()
            invitation = self._new(org_id, email=previous.email, role=previous.role, invited_by=invited_by)
            save(session, invitation)
            return invitation

    def delete(self, invitation_id: UUID, *, org_id: UUID) -> None:
        """Withdraw one of an organisation's invitations.

        Args:
            invitation_id: Invitation UUID.
            org_id: Organisation the invitation must belong to; a mismatch
                reads as missing.
        """
        with session_scope(self._engine) as session:
            session.delete(self._lock(invitation_id, org_id))
            commit(session)

    def accept(self, token: str, user_id: UUID) -> Organisation | None:
        """Redeem an invitation: make the profile a member and consume the invitation.

        An expired invitation is deleted on sight. An unknown or expired token
        is an ordinary outcome of following an old link, so it reads as
        ``None`` rather than raising.

        Args:
            token: The invitation token.
            user_id: Profile UUID of the accepting user.

        Returns:
            The Organisation joined, or ``None`` when the token is unknown or expired.
        """
        with session_scope(self._engine) as session:
            invitation = session.exec(select(Invitation).where(Invitation.token == token)).first()
            if not invitation:
                return None
            if invitation.expires_at < datetime.now(timezone.utc):
                session.delete(invitation)
                commit(session)
                return None
            if not session.get(UserOrganisation, (user_id, invitation.organisation_id)):
                session.add(
                    UserOrganisation(user_id=user_id, organisation_id=invitation.organisation_id, role=invitation.role)
                )
            organisation = session.get(Organisation, invitation.organisation_id)
            session.delete(invitation)
            commit(session)
            return organisation

    def has_pending(self, email: str) -> bool:
        """Whether a non-expired invitation exists for an email address.

        Args:
            email: Email address, matched case-insensitively.

        Returns:
            True when at least one pending invitation has not expired.
        """
        now = datetime.now(timezone.utc)
        with session_scope(self._engine) as session:
            invitations = session.exec(select(Invitation).where(func.lower(Invitation.email) == email.lower())).all()
            return any(invitation.expires_at > now for invitation in invitations)

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _new(org_id: UUID, *, email: str, role: str, invited_by: UUID) -> Invitation:
        """A new invitation row with a fresh token and expiry, not yet persisted.

        Args:
            org_id: Organisation UUID.
            email: Address to invite.
            role: Role granted on acceptance, validated against :class:`Role`.
            invited_by: Profile UUID of the inviter.

        Returns:
            The row.
        """
        return Invitation(
            organisation_id=org_id,
            email=email,
            role=Role.parse(role).value,
            token=secrets.token_urlsafe(32),
            invited_by=invited_by,
            expires_at=datetime.now(timezone.utc) + timedelta(days=INVITATION_EXPIRY_DAYS),
        )

    def _lock(self, invitation_id: UUID, org_id: UUID) -> Invitation:
        """Load one of an organisation's invitations for a write, holding its row for the transaction.

        Args:
            invitation_id: Invitation UUID.
            org_id: Organisation the invitation must belong to.

        Returns:
            The Invitation row.

        Raises:
            NotFoundError: If no invitation carries that id in that organisation.
        """
        with session_scope(self._engine) as session:
            invitation = session.get(Invitation, invitation_id, with_for_update=True)
            if not invitation or invitation.organisation_id != org_id:
                raise NotFoundError(f"Invitation {invitation_id} not found")
            return invitation
