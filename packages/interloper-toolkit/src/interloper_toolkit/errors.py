"""Read-time classification of error text into structured fields.

Every error the platform records went through
:func:`interloper.errors.format_exception`, so it opens with the exception's
type name; what follows is the library's own message. Three families carry
an HTTP request in a recognisable shape: httpx (``Client error '429 …' for
url '…'``), google-api-core (``403 POST https://…: message``) and the
Facebook SDK (``Method:`` / ``Path:`` / ``Status:`` lines over a JSON body
with ``code`` and ``error_subcode``). Everything else is grouped by its
first line with numbers masked.
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from interloper_toolkit.models import ErrorCause

_TYPE_NAME = re.compile(r"^([A-Za-z_][\w.]*)(?::\s?|$)")
_URL = re.compile(r"https?://[^\s'\"<>]+")
_HTTPX_STATUS = re.compile(r"(?:Client|Server|Redirect|Informational) (?:error|response) '(\d{3})")
_GOOGLE_STATUS = re.compile(r"^(\d{3}) (?:([A-Z]+) https?://)?")
_FACEBOOK_STATUS = re.compile(r"^\s*Status:\s+(\d{3})\s*$", re.MULTILINE)
_FACEBOOK_METHOD = re.compile(r"^\s*Method:\s+([A-Z]+)\s*$", re.MULTILINE)
_VENDOR_CODE = re.compile(r'"code":\s*(-?\d+)')
_VENDOR_SUBCODE = re.compile(r'"error_subcode":\s*(-?\d+)')
_ID_SEGMENT = re.compile(r"^(?:\d+|act_\d+|[0-9a-f]{16,}|[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$", re.IGNORECASE)
_NUMBER = re.compile(r"\d+")
_SUMMARY_LIMIT = 200


def classify(text: str) -> ErrorCause:
    """Parse one recorded error text into its cause.

    Never raises: text nothing here recognises still gets a type (when it
    opens with one) and a fingerprint from its masked first line.

    Args:
        text: The error as stored, i.e. ``format_exception``'s output.

    Returns:
        The structured cause, with a ``fingerprint`` that identical causes
        share and a one-line ``summary``.
    """
    head = _TYPE_NAME.match(text)
    exception_type = head.group(1) if head else None
    message = text[head.end() :] if head else text

    status: int | None = None
    method: str | None = None
    if httpx := _HTTPX_STATUS.search(message):
        status = int(httpx.group(1))
    elif google := _GOOGLE_STATUS.match(message.strip()):
        status = int(google.group(1))
        method = google.group(2)
    elif facebook := _FACEBOOK_STATUS.search(message):
        status = int(facebook.group(1))
        if fb_method := _FACEBOOK_METHOD.search(message):
            method = fb_method.group(1)

    host = path = None
    if url := _URL.search(message):
        parts = urlsplit(url.group(0))
        host = parts.hostname
        path = "/".join(_mask_segment(segment) for segment in parts.path.split("/")) or "/"

    code = _VENDOR_CODE.search(message)
    subcode = _VENDOR_SUBCODE.search(message)
    vendor_code = int(code.group(1)) if code else None
    vendor_subcode = int(subcode.group(1)) if subcode else None

    first_line = next((line.strip() for line in message.splitlines() if line.strip()), "")
    if status is not None or host is not None:
        key = f"{exception_type}|{status}|{method}|{host}{path}|{vendor_code}|{vendor_subcode}"
        summary = " ".join(
            str(part)
            for part in (exception_type, status, method, f"{host}{path}" if host else None)
            if part is not None
        )
        if vendor_code is not None:
            summary += f" code={vendor_code}" + (f"/{vendor_subcode}" if vendor_subcode is not None else "")
    else:
        key = f"{exception_type}|{_NUMBER.sub('N', first_line)}"
        summary = f"{exception_type}: {first_line}" if exception_type and first_line else (exception_type or first_line)

    return ErrorCause(
        exception_type=exception_type,
        http_status=status,
        method=method,
        host=host,
        path=path,
        vendor_code=vendor_code,
        vendor_subcode=vendor_subcode,
        fingerprint=key,
        summary=summary[:_SUMMARY_LIMIT],
    )


def _mask_segment(segment: str) -> str:
    """Replace an identifier-looking path segment with ``{id}``, so paths group by endpoint.

    Args:
        segment: One segment of a URL path.

    Returns:
        ``{id}`` for a number, ``act_`` id, long hex string or UUID; the
        segment itself otherwise.
    """
    return "{id}" if _ID_SEGMENT.match(segment) else segment
