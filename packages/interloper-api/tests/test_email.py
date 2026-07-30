"""Tests for the invitation email rendering."""

from interloper_db.store.auth import INVITATION_EXPIRY_DAYS

from interloper_api.email import render_invite_html, render_invite_text


def test_render_invite_html_contains_content():
    body = render_invite_html("Acme", "Ada Lovelace", "https://app.example.com/invite/tok")
    assert "Acme" in body
    assert "Ada Lovelace" in body
    assert 'href="https://app.example.com/invite/tok"' in body
    assert f"expires in {INVITATION_EXPIRY_DAYS} days" in body


def test_render_invite_html_escapes_interpolated_values():
    body = render_invite_html("<b>Org</b>", "Eve <script>alert(1)</script>", "https://x")
    assert "<b>Org</b>" not in body
    assert "&lt;b&gt;Org&lt;/b&gt;" in body
    assert "<script>" not in body


def test_render_invite_html_logo_is_optional():
    with_logo = render_invite_html("Acme", "Ada", "https://x", logo_url="https://x/logo-email.png")
    assert '<img src="https://x/logo-email.png"' in with_logo
    without_logo = render_invite_html("Acme", "Ada", "https://x")
    assert "<img" not in without_logo


def test_render_invite_text_contains_link_and_expiry():
    text = render_invite_text("Acme", "Ada", "https://x/invite/t")
    assert "https://x/invite/t" in text
    assert f"{INVITATION_EXPIRY_DAYS} days" in text
