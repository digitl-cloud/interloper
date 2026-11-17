"""Link confirmation email: what the owner of an address is asked before a new way to sign in joins their profile."""

from __future__ import annotations

from dataclasses import dataclass

from interloper_api.auth import LINK_EXPIRY
from interloper_api.notifications.base import Email


@dataclass(frozen=True)
class LinkConfirmationEmail(Email):
    """A request to confirm that a provider's account may sign in to the profile holding an address.

    Attributes:
        provider_label: The provider the new account signs in through.
        address: The address the profile holds, which the account claims.
        confirm_url: Full URL of the page that confirms the link.
    """

    provider_label: str
    address: str
    confirm_url: str

    @classmethod
    def from_link(cls, raw: str, *, provider_label: str, address: str, base_url: str) -> LinkConfirmationEmail:
        """Address the email for a link just requested, pointing at the app's confirmation page.

        Args:
            raw: The link's raw secret.
            provider_label: The provider the new account signs in through.
            address: The address the profile holds.
            base_url: The app's base URL, under which the confirmation page
                and the hosted logo are served.

        Returns:
            The email, ready to deliver.
        """
        base_url = base_url.rstrip("/")
        return cls(
            provider_label=provider_label,
            address=address,
            confirm_url=f"{base_url}/auth/link/{raw}",
            logo_url=f"{base_url}/logo-email.png",
        )

    @property
    def subject(self) -> str:
        """The subject line, naming the provider."""
        return f"Confirm signing in to Interloper with {self.provider_label}"

    def text(self) -> str:
        """Render the plain-text alternative of the confirmation email.

        Returns:
            The plain-text email body.
        """
        return (
            f"Someone signed in to Interloper with a {self.provider_label} account for {self.address}. "
            "Your Interloper account already uses this address, so that account becomes a way to sign in "
            f"to it only once you confirm:\n{self.confirm_url}\n\n"
            f"{self._footer_text()}"
        )

    def _content_html(self) -> str:
        """Render the request: which account claims the address, and the confirm button.

        Returns:
            The request's HTML, every interpolated value escaped.
        """
        account = self._emphasis_html(f"{self.provider_label} account for {self.address}")
        return "\n".join(
            [
                self._heading_html("Confirm a new way to sign in"),
                self._paragraph_html(
                    f"Someone signed in to Interloper with a {account}. Your Interloper account already uses "
                    "this address, so that account becomes a way to sign in to it only once you confirm."
                ),
                self._button_html("Review and confirm", self.confirm_url),
            ]
        )

    def _footer_text(self) -> str:
        """Say when the link expires and that ignoring it changes nothing.

        Returns:
            The footer sentence.
        """
        minutes = int(LINK_EXPIRY.total_seconds() // 60)
        return f"This link expires in {minutes} minutes. If this wasn't you, ignore this email: nothing changes."
