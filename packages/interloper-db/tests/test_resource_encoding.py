"""Tests for the resource encrypt-on-write default logic.

These exercise ``ComponentMixin._encode_data`` directly (no DB), which is the
single place that decides whether a resource blob is encrypted.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest
from interloper.errors import ConfigError

from interloper_db.store.components import ComponentMixin


class _Encoder(ComponentMixin):
    """Minimal ComponentMixin carrier exposing only the cipher hook."""

    def __init__(self, encrypt: Callable[[bytes], bytes] | None = None) -> None:
        self._encrypt = encrypt


def _fake_encrypt(data: bytes) -> bytes:
    return b"ENC:" + data


def test_default_encrypts_when_key_is_configured() -> None:
    raw, encrypted = _Encoder(encrypt=_fake_encrypt)._encode_data({"a": 1}, None)
    assert encrypted is True
    assert raw == b"ENC:" + json.dumps({"a": 1}).encode()


def test_default_without_key_raises() -> None:
    # Fail closed: the default must never silently store a resource in plaintext.
    with pytest.raises(ConfigError):
        _Encoder(encrypt=None)._encode_data({"a": 1}, None)


def test_explicit_true_without_key_raises() -> None:
    with pytest.raises(ConfigError):
        _Encoder(encrypt=None)._encode_data({"a": 1}, True)


def test_explicit_false_stays_plaintext_even_with_key() -> None:
    raw, encrypted = _Encoder(encrypt=_fake_encrypt)._encode_data({"a": 1}, False)
    assert encrypted is False
    assert raw == json.dumps({"a": 1}).encode()


def test_explicit_false_without_key_stays_plaintext() -> None:
    # Opting out explicitly still works without a key (for non-secret resources).
    raw, encrypted = _Encoder(encrypt=None)._encode_data({"a": 1}, False)
    assert encrypted is False
    assert raw == json.dumps({"a": 1}).encode()
