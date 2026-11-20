"""Ibis expression nodes the portable views add, and the SQL each compiles to.

Loaded on first use, never with the package: a process that only describes views builds no
node. It enters Ibis through the package and the SQL compilers through theirs, as
``ibis.to_sql`` does, since on Python 3.14 two threads importing one package through different
submodules can deadlock.
"""

from __future__ import annotations

import ibis  # noqa: F401 - initialized before any of its submodules is entered
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import sqlglot.expressions as sge
from ibis.backends.sql import compilers  # noqa: F401 - initialized before its base module is entered
from ibis.backends.sql.compilers.base import SQLGlotCompiler
from ibis.expr import datatypes


class LocalDate(ops.Value):
    """The calendar day an instant falls on in a timezone, which may differ from row to row."""

    instant: ops.Value[datatypes.Timestamp]
    timezone: ops.Value[datatypes.String]

    dtype = datatypes.date
    shape = rlz.shape_like("args")


def _compile_local_date(
    compiler: SQLGlotCompiler, op: LocalDate, *, instant: sge.Expression, timezone: sge.Expression
) -> sge.Expression:
    """Compile a `LocalDate` as sqlglot's timezone conversion, cast to a date.

    Args:
        compiler: The Ibis SQL compiler of the target dialect.
        op: The node being compiled; its arguments arrive already compiled.
        instant: The compiled instant.
        timezone: The compiled IANA timezone name.

    Returns:
        The SQL expression, which sqlglot renders in the compiler's dialect.
    """
    return sge.Cast(this=sge.ConvertTimezone(target_tz=timezone, timestamp=instant), to=sge.DataType.build("date"))


# Ibis converts between zones only by a constant; sqlglot's ConvertTimezone takes a column and renders
# in each dialect's own syntax, so every Ibis SQL compiler learns this node from their common base
SQLGlotCompiler.visit_LocalDate = _compile_local_date  # ty: ignore[missing-attribute]
