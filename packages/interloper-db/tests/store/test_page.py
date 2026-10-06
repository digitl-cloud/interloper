"""Tests for the paging shapes (``interloper_db.store.page``)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pydantic import ValidationError
from sqlalchemy import Engine, event
from sqlmodel import Session, col, select

from interloper_db.models import Organisation
from interloper_db.store import Page, PageQuery
from interloper_db.store.page import MAX_PAGE_SIZE

_NAMES = ["a", "b", "c", "d", "e"]


@pytest.fixture
def session(auth_db: Engine) -> Iterator[Session]:
    """A session over a database holding one organisation per name in ``_NAMES``.

    Args:
        auth_db: The fixture database carrying the organisations table.

    Yields:
        An open session on that database.
    """
    with Session(auth_db) as session:
        session.add_all(Organisation(name=name) for name in _NAMES)
        session.commit()
        yield session


def _names(page: Page[Organisation]) -> list[str]:
    """The names of a page's organisations, in page order.

    Args:
        page: The page read.

    Returns:
        The names.
    """
    return [organisation.name for organisation in page.items]


class TestPageQuery:
    """The window's bounds are validated where the query is built."""

    def test_defaults_to_the_first_fifty(self):
        assert (PageQuery().limit, PageQuery().offset) == (50, 0)

    @pytest.mark.parametrize("limit", [1, MAX_PAGE_SIZE, None])
    def test_accepts_a_limit_within_bounds_or_none(self, limit: int | None):
        assert PageQuery(limit=limit).limit == limit

    @pytest.mark.parametrize("limit", [0, -1, MAX_PAGE_SIZE + 1])
    def test_rejects_a_limit_out_of_bounds(self, limit: int):
        with pytest.raises(ValidationError, match="limit"):
            PageQuery(limit=limit)

    def test_rejects_a_negative_offset(self):
        with pytest.raises(ValidationError, match="offset"):
            PageQuery(offset=-1)

    def test_the_cap_is_five_hundred(self):
        assert MAX_PAGE_SIZE == 500


class TestRead:
    """A listing statement read over a window, with the size of the whole match."""

    @pytest.mark.parametrize(
        ("limit", "offset", "names", "counts"),
        [
            pytest.param(2, 1, ["b", "c"], True, id="a full page counts the whole set"),
            pytest.param(2, 10, [], True, id="an empty page past the end still counts"),
            pytest.param(None, 0, _NAMES, False, id="the whole set is its own total"),
            pytest.param(None, 3, ["d", "e"], False, id="the rest of the set ends at its last row"),
            pytest.param(10, 0, _NAMES, False, id="a short first page holds the whole set"),
            pytest.param(2, 4, ["e"], False, id="a short later page ends the set"),
        ],
    )
    def test_reads_the_window_and_counts_only_when_the_page_cannot_tell(
        self, session: Session, limit: int | None, offset: int, names: list[str], counts: bool
    ):
        statement = select(Organisation).order_by(col(Organisation.name))
        executed: list[str] = []

        def record(_connection: object, _cursor: object, statement_text: str, *_rest: object) -> None:
            executed.append(statement_text)

        engine = session.get_bind()
        event.listen(engine, "before_cursor_execute", record)
        try:
            page = Page.read(session, statement, PageQuery(limit=limit, offset=offset))
        finally:
            event.remove(engine, "before_cursor_execute", record)

        assert _names(page) == names
        assert page.total == len(_NAMES)
        assert any("count(" in text.lower() for text in executed) is counts

    def test_the_total_ignores_the_ordering(self, session: Session):
        ascending = select(Organisation).order_by(col(Organisation.name))
        descending = select(Organisation).order_by(col(Organisation.name).desc())

        first = Page.read(session, ascending, PageQuery(limit=1))
        last = Page.read(session, descending, PageQuery(limit=1))

        assert (_names(first), _names(last)) == (["a"], ["e"])
        assert first.total == last.total == len(_NAMES)

    def test_the_total_follows_the_filter(self, session: Session):
        statement = select(Organisation).where(col(Organisation.name).in_(["a", "b"]))

        assert Page.read(session, statement, PageQuery(limit=1)).total == 2


class TestWindow:
    """A listing assembled in memory, windowed the same way."""

    def test_keeps_the_window_and_counts_every_item(self):
        page = Page[int].window(list(range(10)), PageQuery(limit=3, offset=4))

        assert page.items == [4, 5, 6]
        assert page.total == 10

    def test_no_limit_keeps_everything_from_the_offset(self):
        page = Page[int].window(list(range(5)), PageQuery(limit=None, offset=2))

        assert page.items == [2, 3, 4]
        assert page.total == 5

    def test_an_offset_past_the_end_is_empty(self):
        page = Page[int].window([1, 2], PageQuery(offset=5))

        assert page.items == []
        assert page.total == 2


class TestMap:
    def test_converts_every_item_and_keeps_the_total(self):
        page = Page[int](items=[1, 2, 3], total=7)

        mapped = page.map(str)

        assert mapped.items == ["1", "2", "3"]
        assert mapped.total == 7

    def test_leaves_the_original_page_untouched(self):
        page = Page[int](items=[1, 2], total=2)

        page.map(lambda value: value * 10)

        assert page.items == [1, 2]
