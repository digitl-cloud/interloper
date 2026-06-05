"""Tests for event persistence helpers."""

from __future__ import annotations

from interloper_db.store.runs import _sanitize_text


def test_sanitize_strips_nul_bytes() -> None:
    """NUL bytes (which Postgres text rejects) are removed."""
    assert _sanitize_text("a\x00b\x00c") == "abc"


def test_sanitize_passes_through_none() -> None:
    """``None`` stays ``None``."""
    assert _sanitize_text(None) is None


def test_sanitize_keeps_normal_text() -> None:
    """Ordinary text is returned unchanged."""
    assert _sanitize_text("hello world") == "hello world"


def test_sanitize_truncates_oversized() -> None:
    """Oversized values are capped and marked as truncated."""
    out = _sanitize_text("x" * 100, max_len=10)
    assert out is not None
    assert out.startswith("x" * 10)
    assert out.endswith("[truncated]")
    assert len(out) < 100
