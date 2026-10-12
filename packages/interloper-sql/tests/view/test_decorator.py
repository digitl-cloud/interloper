"""Tests for the sql_view decorator."""

from __future__ import annotations

import ibis
import interloper as il

from interloper_sql import SqlView, sql_view


def test_sql_view_builds_a_sqlview_with_the_function_as_data():
    @sql_view(tags=["Fact"])
    def totals(orders: il.Upstream) -> ibis.Table:
        return orders.data

    assert issubclass(totals, SqlView)
    assert totals.key == "totals"
    assert totals.tags == ["Fact"]
    assert totals.relations["orders"].key == "orders"
