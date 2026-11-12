"""Tests for ``interloper_db.models.tokens``: what a token stands for, and each surface's kinds and resource."""

from __future__ import annotations

import pytest

from interloper_db.models import Audience, Token, TokenKind


class TestAudienceKinds:
    """The cookie takes sessions only; the bearers take personal and access tokens."""

    @pytest.mark.parametrize(
        ("audience", "kinds"),
        [
            (Audience.APP, (TokenKind.SESSION,)),
            (Audience.API, (TokenKind.PERSONAL, TokenKind.ACCESS)),
            (Audience.MCP, (TokenKind.PERSONAL, TokenKind.ACCESS)),
        ],
    )
    def test_each_surface_accepts_its_kinds(self, audience: Audience, kinds: tuple[TokenKind, ...]) -> None:
        assert audience.kinds == kinds


class TestAudienceResourceUrl:
    """A bearer surface is the resource at its endpoint on the app's public URL; the app's session is none."""

    @pytest.mark.parametrize(
        ("audience", "url"),
        [(Audience.API, "https://app.example.com/api"), (Audience.MCP, "https://app.example.com/mcp")],
    )
    def test_a_bearer_surface_is_its_endpoint_on_the_public_url(self, audience: Audience, url: str) -> None:
        assert audience.resource_url("https://app.example.com") == url

    def test_the_app_names_no_resource(self) -> None:
        with pytest.raises(ValueError, match="never for a resource"):
            Audience.APP.resource_url("https://app.example.com")


class TestToken:
    """A token stands for its user; the request names the organisation."""

    def test_token_carries_no_organisation(self) -> None:
        assert not hasattr(Token, "organisation_id")
