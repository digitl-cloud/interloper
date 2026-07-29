"""Tests for the error formatting helpers."""

from interloper.errors import format_exception


def test_format_exception_prefixes_type_name():
    """The type name leads so errors stay identifiable in event rows."""
    assert format_exception(ValueError("boom")) == "ValueError: boom"


def test_format_exception_message_less_exception():
    """A message-less exception still yields a non-empty string."""
    assert format_exception(TimeoutError()) == "TimeoutError"


def test_format_exception_quoted_message():
    """The message is rendered via str(), quirks included (KeyError quotes)."""
    assert format_exception(KeyError("missing")) == "KeyError: 'missing'"
