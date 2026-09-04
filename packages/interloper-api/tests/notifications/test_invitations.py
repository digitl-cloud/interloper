"""Tests for the invitation email rendering and SMTP delivery."""

from __future__ import annotations

import smtplib
from collections.abc import Iterator
from types import SimpleNamespace
from typing import ClassVar

import pytest
from interloper_db.store.organisations import INVITATION_EXPIRY_DAYS
from typing_extensions import Self

from interloper_api.notifications import InvitationEmail


def test_render_invite_html_contains_content():
    body = InvitationEmail("Acme", "Ada Lovelace", "https://app.example.com/invite/tok").html()
    assert "Acme" in body
    assert "Ada Lovelace" in body
    assert 'href="https://app.example.com/invite/tok"' in body
    assert f"expires in {INVITATION_EXPIRY_DAYS} days" in body


def test_render_invite_html_escapes_interpolated_values():
    body = InvitationEmail("<b>Org</b>", "Eve <script>alert(1)</script>", "https://x").html()
    assert "<b>Org</b>" not in body
    assert "&lt;b&gt;Org&lt;/b&gt;" in body
    assert "<script>" not in body


def test_render_invite_html_logo_is_optional():
    with_logo = InvitationEmail("Acme", "Ada", "https://x", logo_url="https://x/logo-email.png").html()
    assert '<img src="https://x/logo-email.png"' in with_logo
    without_logo = InvitationEmail("Acme", "Ada", "https://x").html()
    assert "<img" not in without_logo


def test_render_invite_text_contains_link_and_expiry():
    text = InvitationEmail("Acme", "Ada", "https://x/invite/t").text()
    assert "https://x/invite/t" in text
    assert f"{INVITATION_EXPIRY_DAYS} days" in text


class FakeSmtpServer:
    """Context-managed smtplib stand-in recording the delivery calls."""

    instances: ClassVar[list[FakeSmtpServer]] = []

    def __init__(self, host: str, port: int) -> None:
        """Record the connection target.

        Args:
            host: The SMTP host connected to.
            port: The SMTP port connected to.
        """
        self.host = host
        self.port = port
        self.started_tls = False
        self.credentials: tuple[str, str] | None = None
        self.sent: tuple[str, str, str] | None = None
        FakeSmtpServer.instances.append(self)

    def __enter__(self) -> Self:
        """Enter the context.

        Returns:
            This server.
        """
        return self

    def __exit__(self, *args: object) -> None:
        """Leave the context.

        Args:
            *args: Exception triple, ignored.
        """

    def starttls(self) -> None:
        """Record that STARTTLS was negotiated."""
        self.started_tls = True

    def login(self, user: str, password: str) -> None:
        """Record the credentials presented.

        Args:
            user: The SMTP username.
            password: The SMTP password.
        """
        self.credentials = (user, password)

    def sendmail(self, from_addr: str, to: str, message: str) -> None:
        """Record the delivered message.

        Args:
            from_addr: The envelope sender.
            to: The recipient.
            message: The serialized message.
        """
        self.sent = (from_addr, to, message)


def _smtp_config(port: int = 587) -> SimpleNamespace:
    return SimpleNamespace(
        enabled=True,
        host="smtp.example.com",
        port=port,
        user="mailer",
        password="pw",
        from_addr="noreply@example.com",
    )


@pytest.fixture(autouse=True)
def reset_smtp_recorder() -> Iterator[None]:
    """Clear the recorded SMTP servers between tests.

    Yields:
        ``None``; the teardown clears the class-level recorder.
    """
    FakeSmtpServer.instances.clear()
    yield
    FakeSmtpServer.instances.clear()


class TestSend:
    """Delivery picks the transport from the port and carries both bodies."""

    def test_an_unconfigured_mailer_is_refused(self) -> None:
        email = InvitationEmail("Acme", "Ada", "https://x")

        with pytest.raises(RuntimeError, match="SMTP is not configured"):
            email.send(SimpleNamespace(enabled=False), "new@example.com")

    def test_port_587_negotiates_starttls(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smtplib, "SMTP", FakeSmtpServer)
        email = InvitationEmail("Acme", "Ada", "https://x")

        email.send(_smtp_config(port=587), "new@example.com")

        server = FakeSmtpServer.instances[0]
        assert (server.host, server.port) == ("smtp.example.com", 587)
        assert server.started_tls is True
        assert server.credentials == ("mailer", "pw")

    def test_port_465_uses_implicit_tls_without_starttls(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smtplib, "SMTP_SSL", FakeSmtpServer)
        email = InvitationEmail("Acme", "Ada", "https://x")

        email.send(_smtp_config(port=465), "new@example.com")

        server = FakeSmtpServer.instances[0]
        assert server.port == 465
        assert server.started_tls is False

    def test_the_message_carries_both_a_text_and_an_html_body(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A plain-text alternative keeps the invite readable in any client.
        monkeypatch.setattr(smtplib, "SMTP", FakeSmtpServer)
        email = InvitationEmail("Acme", "Ada", "https://app.example.com/invite/tok")

        email.send(_smtp_config(), "new@example.com")

        sent = FakeSmtpServer.instances[0].sent
        assert sent is not None
        from_addr, to, raw = sent
        assert (from_addr, to) == ("noreply@example.com", "new@example.com")
        assert "Content-Type: text/plain" in raw
        assert "Content-Type: text/html" in raw
        assert "Acme" in raw

    def test_the_subject_names_the_organisation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smtplib, "SMTP", FakeSmtpServer)

        InvitationEmail("Acme", "Ada", "https://x").send(_smtp_config(), "new@example.com")

        sent = FakeSmtpServer.instances[0].sent
        assert sent is not None
        assert "Subject: You've been invited to join Acme on Interloper" in sent[2]
