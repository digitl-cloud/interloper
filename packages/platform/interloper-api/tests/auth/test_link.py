"""Tests for ``interloper_api.auth.link``: a link proposal and its plain-values form.

The flow it carries runs in ``tests/routes/test_auth.py``; the signed link it
travels in, in ``tests/auth/test_base.py``.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from interloper_api.auth import LinkProposal


def _proposal() -> LinkProposal:
    return LinkProposal(
        issuer="https://login.microsoftonline.com/t/v2.0",
        subject="oid-1",
        provider="microsoft",
        profile_id=uuid4(),
        email="ada@contoso.com",
        destination="/jobs",
    )


def test_a_saved_proposal_reads_back_from_a_session_of_plain_values() -> None:
    session: dict[str, Any] = {}
    proposal = _proposal()

    proposal.save(session)

    assert all(isinstance(value, str) for value in session["link_proposal"].values())
    assert LinkProposal.from_session(session) == proposal


def test_taking_it_drops_it_from_the_session() -> None:
    session: dict[str, Any] = {"other": 1}
    proposal = _proposal()
    proposal.save(session)

    assert LinkProposal.take(session) == proposal
    assert LinkProposal.from_session(session) is None
    assert session == {"other": 1}


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param(None, id="none"),
        pytest.param("not a mapping", id="not a mapping"),
        pytest.param({"issuer": "x"}, id="missing fields"),
        pytest.param(
            {
                "issuer": "x",
                "subject": "y",
                "provider": "google",
                "profile_id": "not-a-uuid",
                "email": "e",
                "destination": "/",
            },
            id="a profile id that is no UUID",
        ),
    ],
)
def test_values_that_are_no_proposal_read_as_none(stored: Any) -> None:
    assert LinkProposal.from_mapping(stored) is None
    assert LinkProposal.from_session({"link_proposal": stored}) is None
