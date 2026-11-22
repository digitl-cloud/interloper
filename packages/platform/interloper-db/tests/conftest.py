"""Shared fixtures: in-memory SQLite databases carrying the tables under test.

SQLite stands in for Postgres in unit tests (the models use portable types
on purpose). The engine sets each connection up as for ``interloper dev``,
foreign keys on, so ON DELETE CASCADE behaves like production.
"""

from __future__ import annotations

from collections.abc import Iterator

import interloper as il
import pytest
from sqlalchemy import Engine
from sqlalchemy.pool import StaticPool

from interloper_db import engine as engine_module
from interloper_db.models import (
    AuthorizationCode,
    Backfill,
    Component,
    ComponentRelation,
    Conversation,
    Event,
    Identity,
    Invitation,
    OAuthClient,
    Organisation,
    Profile,
    Quota,
    Run,
    Token,
    Usage,
    UserOrganisation,
    table_of,
)
from interloper_db.store import Store


@pytest.fixture
def component_db() -> Iterator[Engine]:
    """A fresh in-memory database with the two component tables, FKs enforced.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    for model in (Component, ComponentRelation, Quota, Usage):
        table_of(model).create(eng)
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def auth_db() -> Iterator[Engine]:
    """A fresh in-memory database with the auth tables, FKs enforced.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    auth_models = (Profile, Identity, Organisation, UserOrganisation, Invitation, Token)
    oauth_models = (OAuthClient, AuthorizationCode)
    org_data_models = (Component, ComponentRelation, Backfill, Run, Event, Quota, Usage, Conversation)
    for model in auth_models + oauth_models + org_data_models:
        table_of(model).create(eng)
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(auth_db: Engine) -> Store:
    """A store over the in-memory database (no catalog needed for these).

    Returns:
        A store with an empty catalog, reading and writing the fixture database.
    """
    return Store(catalog=il.Catalog(components={}))
