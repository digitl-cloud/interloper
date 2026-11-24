"""Tests for ``interloper_api.requests.base``."""

from __future__ import annotations

import logging

import pytest
from fastapi import APIRouter, FastAPI, WebSocket
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from interloper_api.requests import RequestLog


@pytest.fixture
def client() -> TestClient:
    """An app with a route nested under a templated prefix, as the API's organisation routes are, behind `RequestLog`.

    Returns:
        A client that hands server errors back as responses.
    """
    app = FastAPI()
    api = APIRouter(prefix="/api")
    organisation = APIRouter()

    @organisation.get("/runs")
    def runs(org_id: str, status: int = 200) -> JSONResponse:
        return JSONResponse({"org": org_id}, status_code=status)

    @api.get("/boom")
    def boom() -> None:
        raise RuntimeError("boom")

    @app.websocket("/ws")
    async def socket(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.close()

    api.include_router(organisation, prefix="/organisations/{org_id}")
    app.include_router(api)
    app.add_middleware(RequestLog)
    return TestClient(app, raise_server_exceptions=False)


def _lines(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    records = [record for record in caplog.records if record.name.startswith("interloper_api")]
    return [(record.levelno, record.getMessage()) for record in records]


def test_a_server_error_is_an_error_line_naming_the_route_pattern(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="interloper_api")

    client.get("/api/organisations/7f3a/runs", params={"status": 500, "secret": "x"})

    ((level, message),) = _lines(caplog)
    assert level == logging.ERROR
    assert message.startswith("GET /api/organisations/{org_id}/runs 500 (")
    assert message.endswith(" ms)")
    assert "7f3a" not in message and "secret" not in message


def test_any_other_answer_is_a_debug_line(client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="interloper_api")

    client.get("/api/organisations/7f3a/runs", params={"status": 404})

    ((level, message),) = _lines(caplog)
    assert (level, message.split(" (")[0]) == (logging.DEBUG, "GET /api/organisations/{org_id}/runs 404")


def test_a_line_below_the_console_s_level_costs_no_route_lookup(
    client: TestClient, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(logging.INFO, logger="interloper_api")
    monkeypatch.setattr(RequestLog, "_route", staticmethod(lambda scope: pytest.fail("looked the route up")))

    client.get("/api/organisations/7f3a/runs")

    assert _lines(caplog) == []


def test_an_exception_is_logged_as_a_500(client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="interloper_api")

    client.get("/api/boom")

    ((level, message),) = _lines(caplog)
    assert (level, message.split(" (")[0]) == (logging.ERROR, "GET /api/boom 500")


def test_an_unrouted_path_is_named_as_asked_without_its_query(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="interloper_api")

    client.get("/api/nowhere", params={"token": "x"})

    ((_, message),) = _lines(caplog)
    assert message.split(" (")[0] == "GET /api/nowhere 404"


def test_a_websocket_is_not_a_request_line(client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="interloper_api")

    with client.websocket_connect("/ws"):
        pass

    assert _lines(caplog) == []
