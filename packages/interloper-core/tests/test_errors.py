"""Tests for the error formatting helpers."""

import importlib

import pytest
from pydantic import BaseModel, ValidationError

from interloper.errors import AssetNotFoundError, format_exception


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


def test_source_error_is_gone():
    """``SourceError`` was never raised; it is not part of the error vocabulary."""
    errors = importlib.import_module("interloper.errors")

    assert not hasattr(errors, "SourceError")


def test_asset_not_found_reads_without_key_error_quotes():
    """``str`` of an ``AssetNotFoundError`` is its message, not ``KeyError``'s quoted repr."""
    assert str(AssetNotFoundError("Asset 'x' not found")) == "Asset 'x' not found"
