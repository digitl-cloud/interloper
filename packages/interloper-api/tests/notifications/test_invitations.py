"""Tests for the invitation email content."""

from __future__ import annotations

from types import SimpleNamespace

from interloper_db.store.invitations import INVITATION_EXPIRY_DAYS

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


def test_from_invitation_links_back_to_the_app_it_was_issued_from():
    invitation = SimpleNamespace(token="tok")
    inviter = SimpleNamespace(name="Ada", email="ada@example.com")

    email = InvitationEmail.from_invitation(
        invitation,  # ty: ignore[invalid-argument-type]
        org_name="Acme",
        inviter=inviter,  # ty: ignore[invalid-argument-type]
        base_url="https://app.example.com/",
    )

    assert email.invite_url == "https://app.example.com/invite/tok"
    assert email.logo_url == "https://app.example.com/logo-email.png"
    assert (email.org_name, email.inviter_name) == ("Acme", "Ada")


def test_a_nameless_inviter_is_named_by_email():
    inviter = SimpleNamespace(name=None, email="ada@example.com")

    email = InvitationEmail.from_invitation(
        SimpleNamespace(token="tok"),  # ty: ignore[invalid-argument-type]
        org_name="Acme",
        inviter=inviter,  # ty: ignore[invalid-argument-type]
        base_url="https://x",
    )

    assert email.inviter_name == "ada@example.com"


def test_render_invite_text_contains_link_and_expiry():
    text = InvitationEmail("Acme", "Ada", "https://x/invite/t").text()
    assert "https://x/invite/t" in text
    assert f"{INVITATION_EXPIRY_DAYS} days" in text


def test_the_subject_names_the_organisation():
    assert InvitationEmail("Acme", "Ada", "https://x").subject == "You've been invited to join Acme on Interloper"


def test_render_invite_html_carries_the_expiry_in_the_footer():
    body = InvitationEmail("Acme", "Ada", "https://x").html()
    assert "you can safely ignore this email" in body
