"""Organisation keys: the short name an organisation carries in the app's address.

A key is 1 to :data:`MAX_LENGTH` characters of lowercase ASCII letters and
digits, with single hyphens between them (``^[a-z0-9]+(?:-[a-z0-9]+)*$``).
It shares the app's top-level URL space with the app's own routes, so
:data:`RESERVED` words are refused: every page the app keeps at the top level,
the paths the deployment routes elsewhere (``/api``, ``/mcp``, the OAuth server)
and the app's asset paths. A name that yields no letter or digit becomes
:data:`FALLBACK`.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Container

from interloper.errors import ConfigError

MAX_LENGTH = 48

FALLBACK = "organisation"

RESERVED: frozenset[str] = frozenset(
    {
        "settings",
        "admin",
        "login",
        "invite",
        "auth",
        "welcome",
        "api",
        "mcp",
        "oauth",
        ".well-known",
        "_nuxt",
        "authorize",
    }
)

_PATTERN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_TRANSLITERATIONS = str.maketrans({"ß": "ss", "ø": "o", "æ": "ae", "œ": "oe", "đ": "d", "ł": "l", "þ": "th", "ð": "d"})
_NOT_ALPHANUMERIC = re.compile(r"[^a-z0-9]+")


def derive_key(name: str) -> str:
    """Derive a key from an organisation name.

    Accents fold to their ASCII letter, the letters Unicode does not decompose
    (``ß``, ``ø``, ``æ``, ``œ``, ``đ``, ``ð``, ``ł``, ``þ``) take their usual
    transliteration, everything else that is not a letter or digit becomes a
    hyphen, runs of hyphens collapse and the result is cut to
    :data:`MAX_LENGTH`. The result may be reserved or taken; :func:`unique_key`
    settles that.

    Args:
        name: The organisation name.

    Returns:
        The key, :data:`FALLBACK` when the name has no letter or digit.
    """
    folded = unicodedata.normalize("NFKD", name.lower().translate(_TRANSLITERATIONS))
    ascii_name = folded.encode("ascii", "ignore").decode()
    key = _NOT_ALPHANUMERIC.sub("-", ascii_name).strip("-")[:MAX_LENGTH].rstrip("-")
    return key or FALLBACK


def validate_key(key: str) -> None:
    """Refuse a key that is malformed or reserved.

    Args:
        key: The key to check.

    Raises:
        ConfigError: If the key is empty, longer than :data:`MAX_LENGTH`, not
            lowercase letters and digits joined by single hyphens, or reserved.
    """
    if not key or len(key) > MAX_LENGTH or not _PATTERN.fullmatch(key):
        raise ConfigError(
            f"Invalid key {key!r}: use 1 to {MAX_LENGTH} lowercase letters and digits, with single hyphens between them"
        )
    if key in RESERVED:
        raise ConfigError(f"Key {key!r} is reserved")


def unique_key(name: str, taken: Container[str]) -> str:
    """Derive a key from a name that is neither reserved nor in *taken*.

    The key of the name when free, else the name's key with ``-2``, ``-3`` and
    so on, the stem shortened so the suffix still fits :data:`MAX_LENGTH`.

    Args:
        name: The organisation name.
        taken: The keys already in use.

    Returns:
        A key that passes :func:`validate_key` and is not in *taken*.
    """
    base = derive_key(name)
    key, number = base, 1
    while key in taken or key in RESERVED:
        number += 1
        suffix = f"-{number}"
        key = base[: MAX_LENGTH - len(suffix)].rstrip("-") + suffix
    return key
