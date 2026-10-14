"""Tests for ``interloper_db.errors``."""

import importlib

import pytest
from interloper.errors import InterloperError

import interloper_db
from interloper_db import ConflictError, InUseError, NotFoundError, QuotaExceededError

PLATFORM_ERRORS = [
    "NotFoundError",
    "ConflictError",
    "InUseError",
    "QuotaExceededError",
    "HydrationError",
    "ComponentDriftError",
]


@pytest.mark.parametrize("name", PLATFORM_ERRORS)
def test_the_platform_errors_live_in_the_db_package(name: str) -> None:
    """Each platform error is exported by ``interloper_db`` and is still an ``InterloperError``."""
    error_class = getattr(interloper_db, name)

    assert issubclass(error_class, InterloperError)
    assert not hasattr(importlib.import_module("interloper.errors"), name)


class TestNotFoundError:
    """A missing record, reported in the words it was raised with."""

    def test_the_message_is_not_quoted_like_a_key_error(self):
        assert str(NotFoundError("Run 1 not found")) == "Run 1 not found"

    def test_it_still_catches_as_a_key_error(self):
        with pytest.raises(KeyError):
            raise NotFoundError("Run 1 not found")


class TestConflictError:
    """A request that is well-formed but refused by the record's current state."""

    def test_a_refused_deletion_is_a_conflict(self):
        assert issubclass(InUseError, ConflictError)

    def test_it_still_catches_as_a_value_error(self):
        with pytest.raises(ValueError, match="already success"):
            raise ConflictError("Run 1 is already success")


class TestInUseError:
    """A delete refused because other records point at the target."""

    def test_referrers_are_carried_for_the_api_to_surface(self):
        referrers: list[dict[str, str | None]] = [
            {"id": "1", "kind": "job", "key": "nightly", "name": "Nightly"}
        ]

        error = InUseError("Still referenced", referrers=referrers)

        assert str(error) == "Still referenced"
        assert error.referrers == referrers

    def test_no_referrers_becomes_an_empty_list(self):
        assert InUseError("Still referenced").referrers == []


class TestQuotaExceededError:
    """The structured fields an HTTP 429 renders from."""

    def test_the_quota_context_is_carried(self):
        error = QuotaExceededError("Too many sources", quota="max_sources", limit=10, used=10)

        assert str(error) == "Too many sources"
        assert (error.quota, error.limit, error.used) == ("max_sources", 10, 10)
