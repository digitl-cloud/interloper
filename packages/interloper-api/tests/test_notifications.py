"""Tests for the invitation email rendering."""

from interloper_db.store.auth import INVITATION_EXPIRY_DAYS

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
