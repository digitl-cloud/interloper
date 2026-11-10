"""Super-admin promotion email: what the other super-admins are told."""

from __future__ import annotations

import html
from dataclasses import dataclass

from interloper_db import Profile

from interloper_api.notifications.base import Email


@dataclass(frozen=True)
class SuperAdminPromotionEmail(Email):
    """A notice to every super-admin that someone joined their ranks.

    Attributes:
        promoted_name: Display name of the profile promoted.
        promoted_email: Email of the profile promoted, so a shared or missing
            display name stays unambiguous.
        promoted_by: Display name of the super-admin who granted the access.
        admin_url: Full URL of the admin portal's user list, where the access
            can be reviewed and revoked.
        logo_url: Absolute URL of the header logo image; the logo is omitted
            when None.
    """

    promoted_name: str
    promoted_email: str
    promoted_by: str
    admin_url: str
    logo_url: str | None = None

    @classmethod
    def from_promotion(cls, promoted: Profile, *, promoted_by: Profile, base_url: str) -> SuperAdminPromotionEmail:
        """Describe a promotion, linking back to the admin portal it was made from.

        Args:
            promoted: The profile that became a super-admin.
            promoted_by: The super-admin who granted the access, named by its
                display name or else its email.
            base_url: The app's base URL, under which the admin portal and the
                hosted logo are served.

        Returns:
            The email, ready to deliver.
        """
        base_url = base_url.rstrip("/")
        return cls(
            promoted_name=promoted.name or promoted.email,
            promoted_email=promoted.email,
            promoted_by=promoted_by.name or promoted_by.email,
            admin_url=f"{base_url}/admin/users",
            logo_url=f"{base_url}/logo-email.png",
        )

    @property
    def subject(self) -> str:
        """The subject line, naming who was promoted."""
        return f"{self.promoted_name} is now a super admin on Interloper"

    @property
    def _promoted_label(self) -> str:
        """Name the promoted profile, adding the email when the name is not already it."""
        if self.promoted_name == self.promoted_email:
            return self.promoted_email
        return f"{self.promoted_name} ({self.promoted_email})"

    def text(self) -> str:
        """Render the plain-text alternative of the promotion email.

        Returns:
            The plain-text email body.
        """
        return (
            f"{self.promoted_by} granted super-admin access to {self._promoted_label} on Interloper.\n\n"
            "Super admins manage every organisation, user and quota on the instance.\n\n"
            f"Review super admins:\n{self.admin_url}\n\n"
            f"{self._footer_text()}"
        )

    def _content_html(self) -> str:
        """Render the notice: who promoted whom, and the review button.

        Returns:
            The notice's HTML, every interpolated value escaped.
        """
        promoted = self._emphasis_html(self._promoted_label)
        promoted_by = self._emphasis_html(self.promoted_by)
        return "\n".join(
            [
                self._heading_html(f"{html.escape(self.promoted_name)} is now a super admin"),
                self._paragraph_html(
                    f"{promoted_by} granted super-admin access to {promoted} on Interloper. "
                    "Super admins manage every organisation, user and quota on the instance."
                ),
                self._button_html("Review super admins", self.admin_url),
            ]
        )

    def _footer_text(self) -> str:
        """Say why the recipient got the notice and what to do if it is unexpected.

        Returns:
            The footer sentence.
        """
        return (
            "You're receiving this because you are a super admin on Interloper. "
            "If this change wasn't expected, revoke it from the admin portal."
        )
