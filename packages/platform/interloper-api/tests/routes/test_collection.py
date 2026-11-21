"""Tests for ``interloper_api.routes.collection``."""

from __future__ import annotations

from uuid import uuid4

import interloper as il
import pytest
import sqlalchemy
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import ReadOnlyError, Store

from interloper_api.app import ORG_PREFIX, install_error_handlers
from interloper_api.dependencies import get_store, require_viewer
from interloper_api.routes import collection as collection_module

_BASE_URL = f"http://testserver/organisations/{uuid4()}"


def _app(served_from: str | None, *, signed_in: bool = True) -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(collection_module.org_router, prefix=ORG_PREFIX)
    store = Store(il.Catalog(components={}), engine=sqlalchemy.create_engine("sqlite://"), served_from=served_from)
    app.dependency_overrides[get_store] = lambda: store
    if signed_in:
        app.dependency_overrides[require_viewer] = lambda: None
    return app


@pytest.mark.parametrize("served_from", ["collection.yaml", None])
def test_the_collection_says_where_it_is_served_from(served_from: str | None):
    response = TestClient(_app(served_from), base_url=_BASE_URL).get("/collection")

    assert response.status_code == 200
    assert response.json() == {"served_from": served_from}


def test_it_needs_a_member():
    response = TestClient(_app(None, signed_in=False), base_url=_BASE_URL).get("/collection")

    assert response.status_code == 401


def test_a_change_to_a_served_collection_answers_409_naming_the_files():
    app = FastAPI()
    install_error_handlers(app)

    @app.get("/refused")
    def refused() -> None:
        raise ReadOnlyError("The collection is served from collection.yaml: change it there")

    response = TestClient(app).get("/refused")

    assert response.status_code == 409
    assert "served from collection.yaml" in response.text
