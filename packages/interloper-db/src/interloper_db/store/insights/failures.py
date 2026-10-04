"""Failures: what went wrong across runs, grouped by cause.

Every error the platform records went through
:func:`interloper.errors.format_exception`, so it opens with the exception's
type name; what follows is the library's own message. Three families carry
an HTTP request in a recognisable shape: httpx2 (``Client error '429 …' for
url '…'``), google-api-core (``403 POST https://…: message``) and the
Facebook SDK (``Method:`` / ``Path:`` / ``Status:`` lines over a JSON body
with ``code`` and ``error_subcode``). Everything else is grouped by its
first line with numbers masked.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import NamedTuple
from urllib.parse import urlsplit
from uuid import UUID

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


GROUP_KEYS = ("job", "asset", "cause")


@dataclass(frozen=True)
class ErrorCause:
    """What an error text says about its cause, parsed once at read time.

    ``fingerprint`` is what identical causes share and what error groups merge
    on; ``summary`` is the one line a reader wants.
    """

    fingerprint: str
    summary: str
    exception_type: str | None = None
    http_status: int | None = None
    method: str | None = None
    host: str | None = None
    path: str | None = None
    vendor_code: int | None = None
    vendor_subcode: int | None = None

    @classmethod
    def from_text(cls, text: str) -> ErrorCause:
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
        if httpx2 := _HTTPX_STATUS.search(message):
            status = int(httpx2.group(1))
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
            path = "/".join(cls._mask_segment(segment) for segment in parts.path.split("/")) or "/"

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
            summary = (
                f"{exception_type}: {first_line}" if exception_type and first_line else (exception_type or first_line)
            )

        return cls(
            fingerprint=key,
            summary=summary[:_SUMMARY_LIMIT],
            exception_type=exception_type,
            http_status=status,
            method=method,
            host=host,
            path=path,
            vendor_code=vendor_code,
            vendor_subcode=vendor_subcode,
        )

    @staticmethod
    def _mask_segment(segment: str) -> str:
        """Replace an identifier-looking path segment with ``{id}``, so paths group by endpoint.

        Args:
            segment: One segment of a URL path.

        Returns:
            ``{id}`` for a number, ``act_`` id, long hex string or UUID; the
            segment itself otherwise.
        """
        return "{id}" if _ID_SEGMENT.match(segment) else segment


class ErrorRow(NamedTuple):
    """Error events sharing one job, run, component, event type and error text, as the database groups them.

    Attributes:
        job_id: The run's target, or ``None`` when it was deleted.
        run_id: The run the events belong to.
        component_key: The component the events concern (``None`` for a
            run-level event).
        event_type: The events' type.
        error: The error text they share.
        count: How many events the row holds.
        first_seen: The earliest event's timestamp.
        last_seen: The latest event's timestamp.
    """

    job_id: UUID | None
    run_id: UUID
    component_key: str | None
    event_type: str
    error: str
    count: int
    first_seen: dt.datetime
    last_seen: dt.datetime


@dataclass
class ErrorGroup:
    """Failures sharing one grouping key, loudest first in a listing.

    ``failed_attempts`` counts every failed attempt, retried ones included;
    ``terminal_failures`` only those that were an operation's or a run's final
    word. The sample is the most recently seen run of the group and the first
    line of its error.

    Attributes:
        job_id: The job the failures ran under, when grouped by job.
        asset_key: The asset that failed, when grouped by asset.
        cause: The classified cause, when grouped by cause.
        failed_attempts: Every failed attempt in the group.
        terminal_failures: The failed attempts that were final.
        runs: The runs the group's failures belong to.
        first_seen: The earliest failure.
        last_seen: The latest failure.
        sample_run_id: The run of the latest failure.
        sample: The first line of the latest failure's error.
    """

    job_id: UUID | None
    asset_key: str | None
    cause: ErrorCause | None
    first_seen: dt.datetime
    last_seen: dt.datetime
    sample_run_id: UUID
    sample: str
    failed_attempts: int = 0
    terminal_failures: int = 0
    runs: set[UUID] = field(default_factory=set)

    @classmethod
    def merge(cls, rows: Iterable[ErrorRow], group_by: Sequence[str]) -> list[ErrorGroup]:
        """Merge database rows into groups on the chosen keys, loudest first.

        Each distinct error text is classified once, however many rows share it.

        Args:
            rows: The rows to merge.
            group_by: Any of ``job``, ``asset``, ``cause``; a key left out
                does not split the groups and is left unset on them.

        Returns:
            The groups, the most failed attempts first, then the most recently seen.
        """
        causes: dict[str, ErrorCause] = {}
        groups: dict[tuple[object, ...], ErrorGroup] = {}
        for row in rows:
            cause = causes.get(row.error) or causes.setdefault(row.error, ErrorCause.from_text(row.error))
            key = (
                row.job_id if "job" in group_by else None,
                row.component_key if "asset" in group_by else None,
                cause.fingerprint if "cause" in group_by else None,
            )
            sample = row.error.splitlines()[0].strip() if row.error else ""
            group = groups.get(key)
            if group is None:
                group = groups[key] = cls(
                    job_id=key[0],
                    asset_key=key[1],
                    cause=cause if "cause" in group_by else None,
                    first_seen=row.first_seen,
                    last_seen=row.last_seen,
                    sample_run_id=row.run_id,
                    sample=sample,
                )
            elif row.last_seen > group.last_seen:
                group.last_seen, group.sample_run_id, group.sample = row.last_seen, row.run_id, sample
            group.first_seen = min(group.first_seen, row.first_seen)
            group.failed_attempts += row.count
            if row.event_type != "operation_retried":
                group.terminal_failures += row.count
            group.runs.add(row.run_id)
        return sorted(groups.values(), key=lambda group: (-group.failed_attempts, -group.last_seen.timestamp()))


@dataclass(frozen=True)
class ErrorGroups:
    """The error groups of a window, and whether the database read hit its cap.

    Attributes:
        groups: The groups, loudest first.
        rows: How many grouped database rows were read.
        truncated: Whether the read stopped at its cap, so quieter groups may be missing.
    """

    groups: list[ErrorGroup]
    rows: int
    truncated: bool
