"""Unit tests for resource-data encryption helpers."""

from __future__ import annotations

import base64

import pytest
from cryptography.fernet import Fernet, InvalidToken

from interloper_db.crypto import derive_fernet_key, make_cipher


def test_derive_fernet_key_is_valid_and_deterministic() -> None:
    key = derive_fernet_key("my-secret")
    # Same input → same key.
    assert derive_fernet_key("my-secret") == key
    # Different input → different key.
    assert derive_fernet_key("other-secret") != key
    # Valid Fernet key: url-safe base64 of 32 bytes.
    assert len(base64.urlsafe_b64decode(key)) == 32
    Fernet(key)  # does not raise


def test_cipher_round_trips() -> None:
    encrypt, decrypt = make_cipher("correct horse battery staple")
    plaintext = b'{"token": "s3cr3t"}'
    blob = encrypt(plaintext)
    assert blob != plaintext
    assert decrypt(blob) == plaintext


def test_cipher_accepts_arbitrary_key_strings() -> None:
    # The chart recommends `openssl rand -base64 32`, which is standard (not
    # url-safe) base64 — it must work without the operator massaging it.
    openssl_style = base64.b64encode(b"\x00" * 32 + b"\xff\xfe").decode()
    encrypt, decrypt = make_cipher(openssl_style)
    assert decrypt(encrypt(b"payload")) == b"payload"


def test_wrong_key_cannot_decrypt() -> None:
    encrypt, _ = make_cipher("key-a")
    _, decrypt_b = make_cipher("key-b")
    with pytest.raises(InvalidToken):
        decrypt_b(encrypt(b"payload"))


def test_empty_key_is_rejected() -> None:
    with pytest.raises(ValueError):
        make_cipher("")
