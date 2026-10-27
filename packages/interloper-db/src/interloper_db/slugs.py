"""Organisation slugs: the short name an organisation carries in the app's address.

A slug is 1 to :data:`MAX_LENGTH` characters of lowercase ASCII letters and
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


def slugify(name: str) -> str:
    """Derive a slug from an organisation name.

    Accents fold to their ASCII letter, the letters Unicode does not decompose
    (``ß``, ``ø``, ``æ``, ``œ``, ``đ``, ``ð``, ``ł``, ``þ``) take their usual
    transliteration, everything else that is not a letter or digit becomes a
    hyphen, runs of hyphens collapse and the result is cut to
    :data:`MAX_LENGTH`. The result may be reserved or taken; :func:`unique_slug`
    settles that.

    Args:
        name: The organisation name.

    Returns:
        The slug, :data:`FALLBACK` when the name has no letter or digit.
    """
    folded = unicodedata.normalize("NFKD", name.lower().translate(_TRANSLITERATIONS))
    ascii_name = folded.encode("ascii", "ignore").decode()
    slug = _NOT_ALPHANUMERIC.sub("-", ascii_name).strip("-")[:MAX_LENGTH].rstrip("-")
    return slug or FALLBACK


def validate_slug(slug: str) -> None:
    """Refuse a slug that is malformed or reserved.

    Args:
        slug: The slug to check.

    Raises:
        ConfigError: If the slug is empty, longer than :data:`MAX_LENGTH`, not
            lowercase letters and digits joined by single hyphens, or reserved.
    """
    if not slug or len(slug) > MAX_LENGTH or not _PATTERN.fullmatch(slug):
        raise ConfigError(
            f"Invalid slug {slug!r}: use 1 to {MAX_LENGTH} lowercase letters and digits, "
            "with single hyphens between them"
        )
    if slug in RESERVED:
        raise ConfigError(f"Slug {slug!r} is reserved")


def unique_slug(name: str, taken: Container[str]) -> str:
    """Derive a slug from a name that is neither reserved nor in *taken*.

    The slug of the name when free, else the name's slug with ``-2``, ``-3`` and
    so on, the stem shortened so the suffix still fits :data:`MAX_LENGTH`.

    Args:
        name: The organisation name.
        taken: The slugs already in use.

    Returns:
        A slug that passes :func:`validate_slug` and is not in *taken*.
    """
    base = slugify(name)
    slug, number = base, 1
    while slug in taken or slug in RESERVED:
        number += 1
        suffix = f"-{number}"
        slug = base[: MAX_LENGTH - len(suffix)].rstrip("-") + suffix
    return slug
