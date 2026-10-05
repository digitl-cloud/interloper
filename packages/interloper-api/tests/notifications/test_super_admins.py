"""Tests for the super-admin promotion email content."""

from __future__ import annotations

from types import SimpleNamespace

from interloper_api.notifications import SuperAdminPromotionEmail


def _email(promoted_name: str = "Ada Lovelace", promoted_by: str = "Grace Hopper") -> SuperAdminPromotionEmail:
    return SuperAdminPromotionEmail(
        promoted_name=promoted_name,
        promoted_email="ada@example.com",
        promoted_by=promoted_by,
        admin_url="https://app.example.com/admin/users",
    )


def test_from_promotion_links_back_to_the_admin_portal():
    promoted = SimpleNamespace(name="Ada", email="ada@example.com")
    promoted_by = SimpleNamespace(name="Grace", email="grace@example.com")

    email = SuperAdminPromotionEmail.from_promotion(
        promoted,  # ty: ignore[invalid-argument-type]
        promoted_by=promoted_by,  # ty: ignore[invalid-argument-type]
        base_url="https://app.example.com/",
    )

    assert email.admin_url == "https://app.example.com/admin/users"
    assert email.logo_url == "https://app.example.com/logo-email.png"
    assert (email.promoted_name, email.promoted_email, email.promoted_by) == ("Ada", "ada@example.com", "Grace")


def test_nameless_profiles_are_named_by_email():
    email = SuperAdminPromotionEmail.from_promotion(
        SimpleNamespace(name=None, email="ada@example.com"),  # ty: ignore[invalid-argument-type]
        promoted_by=SimpleNamespace(name=None, email="grace@example.com"),  # ty: ignore[invalid-argument-type]
        base_url="https://x",
    )

    assert (email.promoted_name, email.promoted_by) == ("ada@example.com", "grace@example.com")
    assert "ada@example.com (ada@example.com)" not in email.text()


def test_the_subject_names_who_was_promoted():
    assert _email().subject == "Ada Lovelace is now a super admin on Interloper"


def test_the_text_names_both_sides_and_links_to_the_review():
    text = _email().text()
    assert "Grace Hopper granted super-admin access to Ada Lovelace (ada@example.com)" in text
    assert "https://app.example.com/admin/users" in text


def test_the_html_names_both_sides_and_links_to_the_review():
    body = _email().html()
    assert "Ada Lovelace (ada@example.com)" in body
    assert "Grace Hopper" in body
    assert 'href="https://app.example.com/admin/users"' in body
    assert "you are a super admin" in body


def test_the_html_escapes_interpolated_values():
    body = _email(promoted_name="<b>Ada</b>", promoted_by="Eve <script>alert(1)</script>").html()
    assert "<b>Ada</b>" not in body
    assert "&lt;b&gt;Ada&lt;/b&gt;" in body
    assert "<script>" not in body
