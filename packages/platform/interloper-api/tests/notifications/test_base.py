"""Tests for the shared email layout and SMTP delivery."""

from __future__ import annotations

import html
import smtplib
from collections.abc import Iterator
from dataclasses import dataclass
from types import SimpleNamespace
from typing import ClassVar

import pytest
from typing_extensions import Self

from interloper_api.notifications import Email


@dataclass(frozen=True)
class NoticeEmail(Email):
    """The smallest concrete email, so the shared behaviour is tested on its own.

    Attributes:
        topic: What the notice is about.
        logo_url: Absolute URL of the header logo image, or None.
    """

    topic: str
    logo_url: str | None = None

    @property
    def subject(self) -> str:
        """The subject line, naming the topic."""
        return f"Notice about {self.topic}"

    def text(self) -> str:
        """Render the plain-text body.

        Returns:
            The topic, as a sentence.
        """
        return f"Something happened to {self.topic}."

    def _content_html(self) -> str:
        """Render the body cell.

        Returns:
            A paragraph naming the topic.
        """
        return self._paragraph_html(f"Something happened to {self._emphasis_html(self.topic)}.")

    def _footer_text(self) -> str:
        """Render the footer sentence.

        Returns:
            A footer naming the topic, unescaped.
        """
        return f"Sent about <{self.topic}>."


class TestHtml:
    """Every email shares one frame: header, content cell, footer."""

    def test_the_frame_wraps_the_emails_own_content(self) -> None:
        body = NoticeEmail("Acme").html()
        assert body.startswith("<!DOCTYPE html>")
        assert "Interloper" in body
        assert "Something happened to" in body

    def test_the_footer_is_escaped(self) -> None:
        body = NoticeEmail("Acme").html()
        assert html.escape("Sent about <Acme>.") in body
        assert "<Acme>" not in body

    def test_the_logo_is_optional(self) -> None:
        assert '<img src="https://x/logo-email.png"' in NoticeEmail("Acme", logo_url="https://x/logo-email.png").html()
        assert "<img" not in NoticeEmail("Acme").html()

    def test_the_button_escapes_its_url_and_offers_a_fallback_link(self) -> None:
        button = Email._button_html("Open", 'https://x/?a=1&b="2"')
        assert 'href="https://x/?a=1&amp;b=&quot;2&quot;"' in button
        assert "Or copy and paste this link" in button


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
        email = NoticeEmail("Acme")

        with pytest.raises(RuntimeError, match="SMTP is not configured"):
            email.send(SimpleNamespace(enabled=False), "new@example.com")

    def test_port_587_negotiates_starttls(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smtplib, "SMTP", FakeSmtpServer)
        email = NoticeEmail("Acme")

        email.send(_smtp_config(port=587), "new@example.com")

        server = FakeSmtpServer.instances[0]
        assert (server.host, server.port) == ("smtp.example.com", 587)
        assert server.started_tls is True
        assert server.credentials == ("mailer", "pw")

    def test_port_465_uses_implicit_tls_without_starttls(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smtplib, "SMTP_SSL", FakeSmtpServer)
        email = NoticeEmail("Acme")

        email.send(_smtp_config(port=465), "new@example.com")

        server = FakeSmtpServer.instances[0]
        assert server.port == 465
        assert server.started_tls is False

    def test_the_message_carries_both_a_text_and_an_html_body(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A plain-text alternative keeps the email readable in any client.
        monkeypatch.setattr(smtplib, "SMTP", FakeSmtpServer)
        email = NoticeEmail("Acme")

        email.send(_smtp_config(), "new@example.com")

        sent = FakeSmtpServer.instances[0].sent
        assert sent is not None
        from_addr, to, raw = sent
        assert (from_addr, to) == ("noreply@example.com", "new@example.com")
        assert "Content-Type: text/plain" in raw
        assert "Content-Type: text/html" in raw
        assert "Acme" in raw

    def test_the_subject_is_the_emails_own(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smtplib, "SMTP", FakeSmtpServer)

        NoticeEmail("Acme").send(_smtp_config(), "new@example.com")

        sent = FakeSmtpServer.instances[0].sent
        assert sent is not None
        assert "Subject: Notice about Acme" in sent[2]


class TestDeliver:
    """Best-effort delivery: the change is already stored, so email never fails the caller."""

    @pytest.mark.parametrize("smtp_config", [None, SimpleNamespace(enabled=False)])
    def test_an_unconfigured_mailer_is_logged_and_skipped(
        self, smtp_config: SimpleNamespace | None, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING", logger="interloper_api.notifications.base"):
            NoticeEmail("Acme").deliver(smtp_config, "new@example.com")

        assert "SMTP not configured; NoticeEmail to new@example.com not sent" in caplog.text

    def test_a_configured_mailer_sends(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(smtplib, "SMTP", FakeSmtpServer)

        NoticeEmail("Acme").deliver(_smtp_config(), "new@example.com")

        sent = FakeSmtpServer.instances[0].sent
        assert sent is not None
        assert sent[1] == "new@example.com"

    def test_a_mailer_failure_is_logged_not_raised(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        def unreachable(host: str, port: int) -> None:
            raise OSError("smtp unreachable")

        monkeypatch.setattr(smtplib, "SMTP", unreachable)

        with caplog.at_level("ERROR", logger="interloper_api.notifications.base"):
            NoticeEmail("Acme").deliver(_smtp_config(), "new@example.com")

        assert "Failed to send NoticeEmail to new@example.com" in caplog.text
