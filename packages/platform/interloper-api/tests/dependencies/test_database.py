"""Tests for ``interloper_api.dependencies.database``.

Exercised through ``create_app`` over a real store, with a probe extension
whose routes read the store, so what is counted is what a request costs the
database: transactions begun, statements sent, connections checked out.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.catalog import Catalog
from interloper_agent.settings import AgentSettings
from interloper_db import Organisation, Store, Token
from interloper_db.crypto import hash_token
from sqlalchemy import Engine, event
from sqlmodel import Session, select

from interloper_api.app import create_app
from interloper_api.settings import ApiSettings

PROBE_ROUTES = """\
from fastapi import APIRouter
from interloper_db import PageQuery

from interloper_api.dependencies import StoreDep, ViewerDep

router = APIRouter()


@router.get("/scope")
def scope(scope: ViewerDep) -> dict:
    return {"role": scope.role}


@router.api_route("/members", methods=["GET", "HEAD", "POST"])
def members(scope: ViewerDep, store: StoreDep) -> dict:
    return {"total": store.members.list(scope.organisation.id, PageQuery()).total}


@router.get("/boom")
def boom(scope: ViewerDep, store: StoreDep) -> dict:
    store.members.list(scope.organisation.id, PageQuery())
    raise RuntimeError("boom")
"""


class RoundTrips:
    """What the engine was asked for since the last reset.

    Attributes:
        begun: Transactions begun.
        statements: Statements sent, in order.
        checkouts: Connections checked out of the pool.
        checkins: Connections returned to it.
    """

    def __init__(self, engine: Engine) -> None:
        """Start listening on *engine* and its pool.

        Args:
            engine: The engine the store works through.
        """
        self.begun = 0
        self.statements: list[str] = []
        self.checkouts = 0
        self.checkins = 0
        event.listen(engine, "begin", self._begin)
        event.listen(engine, "before_cursor_execute", self._statement)
        event.listen(engine.pool, "checkout", self._checkout)
        event.listen(engine.pool, "checkin", self._checkin)

    def reset(self) -> None:
        """Forget everything counted so far."""
        self.begun = self.checkouts = self.checkins = 0
        self.statements.clear()

    def _begin(self, _connection: Any) -> None:
        self.begun += 1

    def _statement(self, _connection: Any, _cursor: Any, statement: str, *_: Any) -> None:
        self.statements.append(statement.split(maxsplit=1)[0])

    def _checkout(self, *_: Any) -> None:
        self.checkouts += 1

    def _checkin(self, *_: Any) -> None:
        self.checkins += 1


@pytest.fixture
def app(
    install: Callable[..., None], load_extensions: Callable[..., Any], fake_settings: ApiSettings, oauth_store: Store
) -> FastAPI:
    """The application, serving the probe extension's routes.

    Returns:
        The application.
    """
    install("probe_ext", "manifest_version: 1\nkey: probe\nroutes: probe_ext.routes:router\n", routes=PROBE_ROUTES)
    load_extensions()
    AgentSettings.get().enabled = False
    return create_app(store=oauth_store, catalog=Catalog(), settings=fake_settings)


@pytest.fixture
def organisation(oauth_store: Store) -> Organisation:
    """An organisation the caller is a viewer of.

    Returns:
        The organisation.
    """
    return oauth_store.organisations.create("Acme")


@pytest.fixture
def secret(oauth_store: Store, organisation: Organisation) -> str:
    """The session cookie of a viewer of the organisation, not yet used.

    Returns:
        The raw secret.
    """
    profile = oauth_store.profiles.upsert(
        issuer="https://accounts.google.com", subject="g-viewer", email="viewer@example.com", name="Viewer"
    )
    oauth_store.members.add(organisation.id, profile.id, "viewer")
    return oauth_store.tokens.create_session(profile.id)


@pytest.fixture
def client(app: FastAPI, secret: str) -> TestClient:
    """A client signed in as the viewer.

    Returns:
        The client, answering a server error with a 500 instead of raising it.
    """
    client = TestClient(app, raise_server_exceptions=False)
    client.cookies.set("session_token", secret)
    return client


@pytest.fixture
def round_trips(oauth_store: Store) -> RoundTrips:
    """The round trips counted on the store's engine.

    Returns:
        The counter.
    """
    return RoundTrips(oauth_store.engine)


def _last_used(store: Store, secret: str) -> datetime | None:
    """Read when a token was last used, as committed.

    Args:
        store: The store whose database holds the token.
        secret: The token's raw secret.

    Returns:
        The token's ``last_used_at``.
    """
    with Session(store.engine) as session:
        return session.exec(select(Token).where(Token.token_hash == hash_token(secret))).one().last_used_at


class TestReadRequests:
    """A GET or HEAD request's store calls share one session and its transaction."""

    @pytest.fixture(autouse=True)
    def _stamped(self, client: TestClient, organisation: Organisation, round_trips: RoundTrips) -> None:
        """Use the session once, so the requests under test fall inside the stamp's throttle."""
        client.get(f"/api/organisations/{organisation.id}/ext/probe/scope")
        round_trips.reset()

    @pytest.mark.parametrize("method", ["GET", "HEAD"])
    def test_one_transaction_and_one_checkout_serve_every_store_call(
        self, client: TestClient, organisation: Organisation, round_trips: RoundTrips, method: str
    ):
        response = client.request(method, f"/api/organisations/{organisation.id}/ext/probe/members")

        assert response.status_code == 200
        assert len(round_trips.statements) > 1
        assert (round_trips.begun, round_trips.checkouts, round_trips.checkins) == (1, 1, 1)

    def test_the_caller_and_their_organisation_are_read_in_one_statement(
        self, client: TestClient, organisation: Organisation, round_trips: RoundTrips
    ):
        response = client.get(f"/api/organisations/{organisation.id}/ext/probe/scope")

        assert response.json() == {"role": "viewer"}
        assert (round_trips.statements, round_trips.begun) == (["SELECT"], 1)

    def test_a_request_that_raises_leaves_no_session_open(
        self, client: TestClient, organisation: Organisation, round_trips: RoundTrips
    ):
        response = client.get(f"/api/organisations/{organisation.id}/ext/probe/boom")

        assert response.status_code == 500
        assert round_trips.checkouts == round_trips.checkins == 1
        assert client.get(f"/api/organisations/{organisation.id}/ext/probe/members").json() == {"total": 1}


class TestWriteRequests:
    """Any other method keeps a session per store call, as before."""

    def test_each_store_call_begins_its_own_transaction(
        self, client: TestClient, organisation: Organisation, round_trips: RoundTrips
    ):
        client.get(f"/api/organisations/{organisation.id}/ext/probe/scope")
        round_trips.reset()

        response = client.post(f"/api/organisations/{organisation.id}/ext/probe/members")

        assert response.status_code == 200
        assert (round_trips.begun, round_trips.checkouts) == (2, 2)


class TestUseStamp:
    """The once-a-minute ``last_used_at`` stamp commits on its own and stays throttled."""

    def test_the_first_read_stamps_and_commits_then_reads_on_in_a_fresh_transaction(
        self, client: TestClient, organisation: Organisation, round_trips: RoundTrips, oauth_store: Store, secret: str
    ):
        response = client.get(f"/api/organisations/{organisation.id}/ext/probe/members")

        assert response.json() == {"total": 1}
        assert round_trips.statements[:2] == ["SELECT", "UPDATE"]
        assert round_trips.begun == 2
        assert _last_used(oauth_store, secret) is not None

    def test_a_read_within_the_throttle_writes_nothing(
        self, client: TestClient, organisation: Organisation, round_trips: RoundTrips, oauth_store: Store, secret: str
    ):
        client.get(f"/api/organisations/{organisation.id}/ext/probe/scope")
        stamped = _last_used(oauth_store, secret)
        round_trips.reset()

        client.get(f"/api/organisations/{organisation.id}/ext/probe/members")

        assert "UPDATE" not in round_trips.statements
        assert round_trips.begun == 1
        assert _last_used(oauth_store, secret) == stamped
