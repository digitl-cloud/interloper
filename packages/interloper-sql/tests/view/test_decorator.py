"""Tests for the ibis_view decorator."""

from __future__ import annotations

import ibis
import interloper as il

from interloper_sql import IbisView, ibis_view


def test_ibis_view_builds_an_ibis_view_with_the_function_as_data():
    @ibis_view(tags=["Fact"])
    def totals(orders: il.Upstream) -> ibis.Table:
        return orders.data

    assert issubclass(totals, IbisView)
    assert totals.key == "totals"
    assert totals.tags == ["Fact"]
    assert totals.relations["orders"].key == "orders"


def test_the_bare_form_builds_an_ibis_view_too():
    @ibis_view
    def totals(orders: il.Upstream) -> ibis.Table:
        return orders.data

    assert issubclass(totals, IbisView)
    assert totals.key == "totals"
