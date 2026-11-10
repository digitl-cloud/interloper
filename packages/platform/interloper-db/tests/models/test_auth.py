"""Tests for ``interloper_db.models.auth``: the role vocabulary memberships are gated on."""

from __future__ import annotations

import pytest
from interloper.errors import ConfigError

from interloper_db.models import Role


class TestRoleParse:
    """A caller-supplied role name resolves to a role or is refused."""

    def test_a_known_name_resolves(self) -> None:
        assert Role.parse("editor") is Role.EDITOR

    def test_an_unknown_name_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError, match="Unknown role 'owner'; expected one of viewer, editor, admin"):
            Role.parse("owner")


class TestRoleAtLeast:
    """Each role admits everything the roles below it admit."""

    @pytest.mark.parametrize(
        ("role", "minimum", "admitted"),
        [
            ("admin", "viewer", True),
            ("editor", "editor", True),
            ("viewer", "editor", False),
            ("editor", "admin", False),
        ],
    )
    def test_ranks_order_the_roles(self, role: str, minimum: str, admitted: bool) -> None:
        assert Role.at_least(role, minimum) is admitted

    @pytest.mark.parametrize("role", [None, "owner"])
    def test_no_membership_or_an_unknown_role_admits_nothing(self, role: str | None) -> None:
        assert Role.at_least(role, "viewer") is False

    def test_an_unknown_minimum_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError):
            Role.at_least("admin", "owner")
