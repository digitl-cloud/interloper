"""Organisation invitation email: what it says."""

from __future__ import annotations

import html
from dataclasses import dataclass

from interloper_db import Profile
from interloper_db.models import Invitation
from interloper_db.store.invitations import INVITATION_EXPIRY_DAYS

from interloper_api.notifications.base import Email


@dataclass(frozen=True)
class InvitationEmail(Email):
    """An invitation to join an organisation, renderable and sendable.

    Attributes:
        org_name: Name of the organisation the recipient is invited to.
        inviter_name: Display name of the person who sent the invite.
        invite_url: Full URL to accept the invitation.
        logo_url: Absolute URL of the header logo image; the logo is omitted
            when None.
    """

    org_name: str
    inviter_name: str
    invite_url: str
    logo_url: str | None = None

    @classmethod
    def from_invitation(
        cls, invitation: Invitation, *, org_name: str, inviter: Profile, base_url: str
    ) -> InvitationEmail:
        """Address the email for a stored invitation, linking back to the app it was issued from.

        Args:
            invitation: The stored invitation, read for its token.
            org_name: Name of the organisation the recipient is invited to.
            inviter: The profile that issued the invitation, named by its
                display name or else its email.
            base_url: The app's base URL, under which the invite link and the
                hosted logo are served.

        Returns:
            The email, ready to deliver.
        """
        base_url = base_url.rstrip("/")
        return cls(
            org_name=org_name,
            inviter_name=inviter.name or inviter.email,
            invite_url=f"{base_url}/invite/{invitation.token}",
            logo_url=f"{base_url}/logo-email.png",
        )

    @property
    def subject(self) -> str:
        """The subject line, naming the organisation."""
        return f"You've been invited to join {self.org_name} on Interloper"

    def text(self) -> str:
        """Render the plain-text alternative of the invitation email.

        Returns:
            The plain-text email body.
        """
        return (
            f"{self.inviter_name} has invited you to join the {self.org_name} organisation on Interloper.\n\n"
            f"Accept the invitation:\n{self.invite_url}\n\n"
            f"This invitation expires in {INVITATION_EXPIRY_DAYS} days. If you weren't expecting it,\n"
            "you can safely ignore this email."
        )

    def _content_html(self) -> str:
        """Render the invitation: who invites the recipient where, and the accept button.

        Returns:
            The invitation's HTML, every interpolated value escaped.
        """
        org = self._emphasis_html(self.org_name)
        inviter = self._emphasis_html(self.inviter_name)
        return "\n".join(
            [
                self._heading_html(f"Join {html.escape(self.org_name)} on Interloper"),
                self._paragraph_html(f"{inviter} has invited you to join the {org} organisation on Interloper."),
                self._button_html("Accept invitation", self.invite_url),
            ]
        )

    def _footer_text(self) -> str:
        """Say when the invitation expires and that it can be ignored.

        Returns:
            The footer sentence.
        """
        return (
            f"This invitation expires in {INVITATION_EXPIRY_DAYS} days. "
            "If you weren't expecting it, you can safely ignore this email."
        )
