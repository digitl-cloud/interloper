"""Tests for the error formatting helpers."""

import pytest
from pydantic import BaseModel, ValidationError

from interloper.errors import InUseError, QuotaExceededError, format_exception


def test_format_exception_prefixes_type_name():
    """The type name leads so errors stay identifiable in event rows."""
    assert format_exception(ValueError("boom")) == "ValueError: boom"


def test_format_exception_message_less_exception():
    """A message-less exception still yields a non-empty string."""
    assert format_exception(TimeoutError()) == "TimeoutError"


def test_format_exception_quoted_message():
    """The message is rendered via str(), quirks included (KeyError quotes)."""
    assert format_exception(KeyError("missing")) == "KeyError: 'missing'"


def test_format_exception_validation_error_omits_input_values():
    """Pydantic input values (the decrypted payload for sensitive models) never reach the message."""

    class Conn(BaseModel):
        app_id: str
        app_secret: str

    with pytest.raises(ValidationError) as exc_info:
        Conn.model_validate({"token": "s3cret-value"})

    message = format_exception(exc_info.value)
    assert message.startswith("ValidationError: 2 validation error(s) for Conn")
    assert "app_id: Field required" in message
    assert "app_secret: Field required" in message
    assert "s3cret-value" not in message
    assert "\n" not in message


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
