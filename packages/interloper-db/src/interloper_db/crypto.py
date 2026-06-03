"""Symmetric encryption for resource data at rest.

A single configured secret (``INTERLOPER_ENCRYPTION_KEY``) is turned into a
pair of ``(bytes) -> bytes`` callables that the :class:`~interloper_db.store.Store`
uses to encrypt/decrypt resource ``data`` blobs marked ``encrypted=True``.

The secret may be any non-empty string. It is run through SHA-256 and
url-safe base64-encoded to produce a valid 32-byte Fernet key, so operators
are not constrained to Fernet's exact key format (e.g. the chart's
``openssl rand -base64 32`` recommendation works as-is).

Fernet provides authenticated encryption (AES-128-CBC + HMAC-SHA256), so a
blob that was not produced with the active key fails to decrypt rather than
returning garbage.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Callable

from cryptography.fernet import Fernet


def derive_fernet_key(secret_key: str) -> bytes:
    """Derive a valid Fernet key from an arbitrary secret string.

    Args:
        secret_key: Any non-empty operator-provided secret.

    Returns:
        A 32-byte url-safe base64-encoded key suitable for :class:`Fernet`.
    """
    digest = hashlib.sha256(secret_key.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def make_cipher(secret_key: str) -> tuple[Callable[[bytes], bytes], Callable[[bytes], bytes]]:
    """Build ``(encrypt, decrypt)`` callables from a secret string.

    Args:
        secret_key: The operator-provided encryption secret. Must be non-empty.

    Returns:
        A ``(encrypt, decrypt)`` tuple, each a ``(bytes) -> bytes`` callable.

    Raises:
        ValueError: If ``secret_key`` is empty.
    """
    if not secret_key:
        raise ValueError("Cannot build a cipher from an empty secret key")
    fernet = Fernet(derive_fernet_key(secret_key))
    return fernet.encrypt, fernet.decrypt
