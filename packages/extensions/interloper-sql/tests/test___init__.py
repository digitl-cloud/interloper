"""Tests for the package surface: the views always import, the SQLAlchemy half only with the postgres extra."""

import subprocess
import sys


def _import_with_blocked(module: str) -> subprocess.CompletedProcess[str]:
    """Import the package in a fresh interpreter with one module made unimportable.

    Args:
        module: The module to block, by setting its ``sys.modules`` entry to ``None``.

    Returns:
        The finished process, its stdout holding ``__all__`` when the import succeeded.
    """
    script = (
        f"import sys; sys.modules[{module!r}] = None; import interloper_sql; print(','.join(interloper_sql.__all__))"
    )
    return subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)


def test_the_views_import_without_sqlalchemy():
    completed = _import_with_blocked("sqlalchemy")
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "IbisView,ibis_view,latest,local_date,union"


def test_any_other_missing_module_still_fails_the_import():
    completed = _import_with_blocked("interloper_sql.types")
    assert completed.returncode != 0
    assert "interloper_sql.types" in completed.stderr
