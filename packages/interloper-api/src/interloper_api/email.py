"""SMTP email utilities for sending invitation emails."""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

logger = logging.getLogger(__name__)


def send_invite_email(
    smtp_config: Any,
    to: str,
    org_name: str,
    inviter_name: str,
    invite_url: str,
) -> None:
    """Send an organisation invitation email via SMTP.

    Args:
        smtp_config: SmtpConfig instance with host, port, user, password, from_addr.
        to: Recipient email address.
        org_name: Name of the organisation.
        inviter_name: Display name of the person who sent the invite.
        invite_url: Full URL to accept the invitation.

    Raises:
        RuntimeError: If SMTP is not configured.
        smtplib.SMTPException: If sending fails.
    """
    if not smtp_config.enabled:
        raise RuntimeError("SMTP is not configured. Set smtp.host, smtp.user, and smtp.password.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"You've been invited to join {org_name} on Interloper"
    msg["From"] = smtp_config.from_addr
    msg["To"] = to

    text = (
        f"{inviter_name} has invited you to join the {org_name} organisation on Interloper.\n\n"
        f"Click the link below to accept the invitation:\n{invite_url}\n\n"
        "This invitation expires in 7 days."
    )

    html = f"""\
<html>
<body style="font-family: sans-serif; color: #333;">
    <h2>You've been invited to join {org_name}</h2>
    <p>{inviter_name} has invited you to join the <strong>{org_name}</strong> organisation on Interloper.</p>
    <p>
        <a href="{invite_url}"
           style="display: inline-block; padding: 10px 20px; background: #2563eb;
                  color: #fff; text-decoration: none; border-radius: 6px;">
            Accept Invitation
        </a>
    </p>
    <p style="color: #666; font-size: 0.875rem;">This invitation expires in 7 days.</p>
</body>
</html>"""

    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))

    logger.info("Sending invite email to %s", to)

    if smtp_config.port == 465:
        with smtplib.SMTP_SSL(smtp_config.host, smtp_config.port) as server:
            server.login(smtp_config.user, smtp_config.password)
            server.sendmail(smtp_config.from_addr, to, msg.as_string())
    else:
        with smtplib.SMTP(smtp_config.host, smtp_config.port) as server:
            server.starttls()
            server.login(smtp_config.user, smtp_config.password)
            server.sendmail(smtp_config.from_addr, to, msg.as_string())

    logger.info("Invite email sent to %s", to)
