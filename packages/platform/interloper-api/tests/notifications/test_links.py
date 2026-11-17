"""Tests for the email confirming a new way to sign in."""

from __future__ import annotations

from interloper_api.notifications import LinkConfirmationEmail


def _email() -> LinkConfirmationEmail:
    return LinkConfirmationEmail.from_link(
        "raw-secret", provider_label="Microsoft", address="ada@example.com", base_url="https://app.example.com/"
    )


def test_from_link_points_at_the_confirmation_page():
    email = _email()

    assert email.confirm_url == "https://app.example.com/auth/link/raw-secret"
    assert email.logo_url == "https://app.example.com/logo-email.png"


def test_the_subject_names_the_provider():
    assert _email().subject == "Confirm signing in to Interloper with Microsoft"


def test_the_text_names_the_account_links_to_the_confirmation_and_says_ignoring_changes_nothing():
    text = _email().text()

    assert "Microsoft account for ada@example.com" in text
    assert "https://app.example.com/auth/link/raw-secret" in text
    assert "expires in 60 minutes" in text
    assert "If this wasn't you, ignore this email: nothing changes." in text


def test_the_html_escapes_the_address_and_links_to_the_confirmation():
    email = LinkConfirmationEmail.from_link(
        "raw-secret", provider_label="Microsoft", address="<ada>@example.com", base_url="https://app.example.com"
    )

    body = email.html()

    assert "&lt;ada&gt;@example.com" in body
    assert "<ada>@example.com" not in body
    assert "https://app.example.com/auth/link/raw-secret" in body
