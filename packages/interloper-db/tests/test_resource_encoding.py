"""Tests for the resource encrypt-on-write default logic.

These exercise ``ComponentStore._encode_data`` directly (no DB), which is the
single place that decides whether a resource blob is encrypted.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import interloper as il
import pytest
from interloper.errors import ConfigError
from sqlalchemy import create_engine

from interloper_db.store import Store
from interloper_db.store.components import ComponentStore


def _components(encrypt: Callable[[bytes], bytes] | None) -> ComponentStore:
    """The component facet of a store carrying only the cipher under test.

    Args:
        encrypt: Cipher the store encrypts resources with, or None for an
            instance with no encryption key configured.

    Returns:
        The facet whose ``_encode_data`` decides encryption. Its engine is
        never connected to: encoding is pure.
    """
    store = Store(catalog=il.Catalog(components={}), engine=create_engine("sqlite://"), encrypt=encrypt)
    return store.components


def _fake_encrypt(data: bytes) -> bytes:
    return b"ENC:" + data


def test_default_encrypts_when_key_is_configured() -> None:
    raw, encrypted = _components(_fake_encrypt)._encode_data({"a": 1}, None)
    assert encrypted is True
    assert raw == b"ENC:" + json.dumps({"a": 1}).encode()


def test_default_without_key_raises() -> None:
    # Fail closed: the default must never silently store a resource in plaintext.
    with pytest.raises(ConfigError):
        _components(None)._encode_data({"a": 1}, None)


def test_explicit_true_without_key_raises() -> None:
    with pytest.raises(ConfigError):
        _components(None)._encode_data({"a": 1}, True)


def test_explicit_false_stays_plaintext_even_with_key() -> None:
    raw, encrypted = _components(_fake_encrypt)._encode_data({"a": 1}, False)
    assert encrypted is False
    assert raw == json.dumps({"a": 1}).encode()


def test_explicit_false_without_key_stays_plaintext() -> None:
    # Opting out explicitly still works without a key (for non-secret resources).
    raw, encrypted = _components(None)._encode_data({"a": 1}, False)
    assert encrypted is False
    assert raw == json.dumps({"a": 1}).encode()
