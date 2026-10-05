"""What every outbound email shares: the branded layout and SMTP delivery."""

from __future__ import annotations

import html
import logging
import smtplib
from abc import ABC, abstractmethod
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

logger = logging.getLogger(__name__)

# Email clients ignore <style> blocks and external assets, so everything is
# table-based with inline styles. Colors are the design tokens: navy #0B2A42
# (header), accent #2D7DF6 (button/links), grays from the app palette.
_FONT_STACK = "-apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"


class Email(ABC):
    """An email the API sends: subclasses write the words, this class the layout and delivery.

    Attributes:
        logo_url: Absolute URL of the header logo image; the logo is omitted
            when None.
    """

    logo_url: str | None

    # -- Content ---------------------------------------------------------------

    @property
    @abstractmethod
    def subject(self) -> str:
        """The subject line."""

    @abstractmethod
    def text(self) -> str:
        """Render the plain-text alternative of the email.

        Returns:
            The plain-text email body.
        """

    @abstractmethod
    def _content_html(self) -> str:
        """Render the email's own HTML: heading, message and call to action.

        Returns:
            The HTML placed in the body cell, with every interpolated value escaped.
        """

    @abstractmethod
    def _footer_text(self) -> str:
        """Say why the recipient got the email, in the footer.

        Returns:
            The footer sentence, as plain text.
        """

    # -- Rendering -------------------------------------------------------------

    def html(self) -> str:
        """Render the HTML body: the branded frame around the email's own content.

        The logo is a hosted PNG (``logo_url``), not an inline SVG: Gmail and
        Outlook strip ``<svg>`` and block ``data:`` URIs. The text wordmark stays
        next to it so the header still reads when images are blocked.

        Returns:
            The HTML email body.
        """
        logo = (
            f'<img src="{html.escape(self.logo_url, quote=True)}" width="26" height="26" alt=""'
            ' style="vertical-align: middle; margin-right: 10px;">'
            if self.logo_url
            else ""
        )
        footer = html.escape(self._footer_text(), quote=False)

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
                            {self._content_html()}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 16px 32px; border-top: 1px solid #f0f0f3;">
                            <p style="margin: 0; font-family: {_FONT_STACK}; font-size: 12.5px;
                                      line-height: 1.6; color: #9a9aa0;">
                                {footer}
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""

    @staticmethod
    def _heading_html(title: str) -> str:
        """Render the content heading.

        Args:
            title: The heading text, already escaped.

        Returns:
            The heading element.
        """
        return f"""<h1 style="margin: 0 0 12px; font-family: {_FONT_STACK}; font-size: 20px;
                                       font-weight: 700; letter-spacing: -0.01em; color: #1d1d1f;">
                                {title}
                            </h1>"""

    @staticmethod
    def _paragraph_html(body: str) -> str:
        """Render a message paragraph.

        Args:
            body: The paragraph's inner HTML, already escaped.

        Returns:
            The paragraph element.
        """
        return f"""<p style="margin: 0 0 24px; font-family: {_FONT_STACK}; font-size: 14px;
                                      line-height: 1.6; color: #6b6b70;">
                                {body}
                            </p>"""

    @staticmethod
    def _emphasis_html(value: str) -> str:
        """Render a highlighted value inside a paragraph, escaping it.

        Args:
            value: The raw value to highlight.

        Returns:
            The escaped value in a strong element.
        """
        return f'<strong style="color: #1d1d1f;">{html.escape(value)}</strong>'

    @staticmethod
    def _button_html(label: str, url: str) -> str:
        """Render the call-to-action button and its copy-paste fallback link.

        Args:
            label: The button label, as plain text.
            url: The raw destination URL.

        Returns:
            The button and the fallback paragraph.
        """
        href = html.escape(url, quote=True)
        return f"""<a href="{href}"
                               style="display: inline-block; padding: 11px 22px; background: #2d7df6;
                                      font-family: {_FONT_STACK}; font-size: 14px; font-weight: 600;
                                      color: #ffffff; text-decoration: none; border-radius: 10px;">
                                {html.escape(label)}
                            </a>
                            <p style="margin: 24px 0 0; font-family: {_FONT_STACK}; font-size: 12.5px;
                                      line-height: 1.6; color: #9a9aa0;">
                                Or copy and paste this link into your browser:<br>
                                <a href="{href}" style="color: #2d7df6; word-break: break-all;">{href}</a>
                            </p>"""

    # -- Delivery --------------------------------------------------------------

    def send(self, smtp_config: Any, to: str) -> None:
        """Deliver the email over SMTP.

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
        message["Subject"] = self.subject
        message["From"] = smtp_config.from_addr
        message["To"] = to
        message.attach(MIMEText(self.text(), "plain"))
        message.attach(MIMEText(self.html(), "html"))

        logger.info("Sending %s to %s", type(self).__name__, to)

        if smtp_config.port == 465:
            with smtplib.SMTP_SSL(smtp_config.host, smtp_config.port) as server:
                server.login(smtp_config.user, smtp_config.password)
                server.sendmail(smtp_config.from_addr, to, message.as_string())
        else:
            with smtplib.SMTP(smtp_config.host, smtp_config.port) as server:
                server.starttls()
                server.login(smtp_config.user, smtp_config.password)
                server.sendmail(smtp_config.from_addr, to, message.as_string())

        logger.info("%s sent to %s", type(self).__name__, to)

    def deliver(self, smtp_config: Any | None, to: str) -> None:
        """Send the email when SMTP is configured, never failing the caller.

        An email reports a change that is already stored, so an unconfigured or
        failing SMTP server is logged rather than raised.

        Args:
            smtp_config: SmtpConfig instance, or ``None`` when the app was
                started without one.
            to: Recipient email address.
        """
        if smtp_config is None or not smtp_config.enabled:
            logger.warning("SMTP not configured; %s to %s not sent", type(self).__name__, to)
            return
        try:
            self.send(smtp_config, to)
        except Exception:
            logger.exception("Failed to send %s to %s", type(self).__name__, to)
