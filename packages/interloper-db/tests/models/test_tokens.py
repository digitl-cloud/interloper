"""Tests for ``interloper_db.models.tokens``: what a token stands for, and which kinds each surface accepts."""

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


class TestToken:
    """A token stands for its user; the request names the organisation."""

    def test_token_carries_no_organisation(self) -> None:
        assert not hasattr(Token, "organisation_id")
