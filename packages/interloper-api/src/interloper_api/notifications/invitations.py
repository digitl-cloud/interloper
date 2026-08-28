"""Organisation invitation email: what it says and how it is delivered."""

from __future__ import annotations

import html
import logging
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from interloper_db.store.auth import INVITATION_EXPIRY_DAYS

logger = logging.getLogger(__name__)

# Email clients ignore <style> blocks and external assets, so everything is
# table-based with inline styles. Colors are the design tokens: navy #0B2A42
# (header), accent #2D7DF6 (button/links), grays from the app palette.
_FONT_STACK = "-apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"


@dataclass(frozen=True)
class InvitationEmail:
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

    def html(self) -> str:
        """Render the HTML body of the invitation email.

        The logo is a hosted PNG (``logo_url``), not an inline SVG: Gmail and
        Outlook strip ``<svg>`` and block ``data:`` URIs. The text wordmark stays
        next to it so the header still reads when images are blocked.

        Returns:
            The HTML email body.
        """
        org = html.escape(self.org_name)
        inviter = html.escape(self.inviter_name)
        url = html.escape(self.invite_url, quote=True)
        logo = (
            f'<img src="{html.escape(self.logo_url, quote=True)}" width="26" height="26" alt=""'
            ' style="vertical-align: middle; margin-right: 10px;">'
            if self.logo_url
            else ""
        )

        return f"""\
<!DOCTYPE html>
<html>
<body style="margin: 0; padding: 0; background: #fbfbfc;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background: #fbfbfc;">
        <tr>
            <td align="center" style="padding: 40px 16px;">
                <table role="presentation" width="560" cellpadding="0" cellspacing="0"
                       style="max-width: 560px; width: 100%; background: #ffffff; border: 1px solid #e8e8ec;
                              border-radius: 12px; overflow: hidden;">
                    <tr>
                        <td style="background: #0b2a42; padding: 16px 32px;">
                            {logo}<span style="font-family: {_FONT_STACK}; font-size: 17px; font-weight: 600;
                                         letter-spacing: -0.01em; color: #ffffff; vertical-align: middle;">
                                Interloper
                            </span>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 32px;">
                            <h1 style="margin: 0 0 12px; font-family: {_FONT_STACK}; font-size: 20px;
                                       font-weight: 700; letter-spacing: -0.01em; color: #1d1d1f;">
                                Join {org} on Interloper
                            </h1>
                            <p style="margin: 0 0 24px; font-family: {_FONT_STACK}; font-size: 14px;
                                      line-height: 1.6; color: #6b6b70;">
                                <strong style="color: #1d1d1f;">{inviter}</strong> has invited you to join the
                                <strong style="color: #1d1d1f;">{org}</strong> organisation on Interloper.
                            </p>
                            <a href="{url}"
                               style="display: inline-block; padding: 11px 22px; background: #2d7df6;
                                      font-family: {_FONT_STACK}; font-size: 14px; font-weight: 600;
                                      color: #ffffff; text-decoration: none; border-radius: 10px;">
                                Accept invitation
                            </a>
                            <p style="margin: 24px 0 0; font-family: {_FONT_STACK}; font-size: 12.5px;
                                      line-height: 1.6; color: #9a9aa0;">
                                Or copy and paste this link into your browser:<br>
                                <a href="{url}" style="color: #2d7df6; word-break: break-all;">{url}</a>
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 16px 32px; border-top: 1px solid #f0f0f3;">
                            <p style="margin: 0; font-family: {_FONT_STACK}; font-size: 12.5px;
                                      line-height: 1.6; color: #9a9aa0;">
                                This invitation expires in {INVITATION_EXPIRY_DAYS} days.
                                If you weren't expecting it, you can safely ignore this email.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""

    def send(self, smtp_config: Any, to: str) -> None:
        """Deliver the invitation over SMTP.

        Args:
            smtp_config: SmtpConfig instance with host, port, user, password,
                from_addr.
            to: Recipient email address.

        Raises:
            RuntimeError: If SMTP is not configured.
        """
        if not smtp_config.enabled:
            raise RuntimeError("SMTP is not configured. Set smtp.host, smtp.user, and smtp.password.")

        message = MIMEMultipart("alternative")
        message["Subject"] = f"You've been invited to join {self.org_name} on Interloper"
        message["From"] = smtp_config.from_addr
        message["To"] = to
        message.attach(MIMEText(self.text(), "plain"))
        message.attach(MIMEText(self.html(), "html"))

        logger.info("Sending invite email to %s", to)

        if smtp_config.port == 465:
            with smtplib.SMTP_SSL(smtp_config.host, smtp_config.port) as server:
                server.login(smtp_config.user, smtp_config.password)
                server.sendmail(smtp_config.from_addr, to, message.as_string())
        else:
            with smtplib.SMTP(smtp_config.host, smtp_config.port) as server:
                server.starttls()
                server.login(smtp_config.user, smtp_config.password)
                server.sendmail(smtp_config.from_addr, to, message.as_string())

        logger.info("Invite email sent to %s", to)
