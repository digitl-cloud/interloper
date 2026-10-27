"""Shared fixtures for the API tests."""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import interloper as il
import pytest
from interloper.settings import ServerSettings
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import (
    AuthorizationCode,
    OAuthClient,
    Organisation,
    Profile,
    RefreshToken,
    Token,
    UserOrganisation,
)
from sqlalchemy import event
from sqlalchemy.pool import StaticPool


@pytest.fixture
def fake_settings() -> SimpleNamespace:
    """AppSettings-shaped namespace for the app and config-snapshot builders.

    Covers every field ``create_app`` and the config-snapshot builder read,
    with secrets planted so redaction can be proven.

    Returns:
        The settings-shaped namespace.
    """
    return SimpleNamespace(
        launcher=SimpleNamespace(
            type="kubernetes",
            config={
                "image": "ghcr.io/x:1",
                "namespace": "prod",
                "service_account_name": "sa",
                "ttl_seconds_after_finished": 300,
                "env": "l-secret",
                "image_pull_secrets": ["pull-secret"],
                "runner_config": {"max_workers": 4, "env": "nested-secret"},
            },
        ),
        runner=SimpleNamespace(type="async", config={"max_workers": 8, "env": "r-secret"}),
        agent=SimpleNamespace(enabled=True, model="google:gemini-2.5-flash"),
        auth=SimpleNamespace(
            allowed_domains=["digitlcloud.com"],
            super_admin_emails=["boss@example.com"],
            google_client_id="cid",
            google_client_secret="oauth-secret",
            google_redirect_uri="https://app/api/auth/google/callback",
            session_expiry_days=30,
            access_token_expiry_minutes=60,
            cookie_secure=True,
        ),
        cron=SimpleNamespace(enabled=True, reconcile_interval=10, batch_size=50, max_execution_delay=3600),
        worker=SimpleNamespace(enabled=True, poll_interval=5),
        reaper=SimpleNamespace(
            enabled=True,
            poll_interval=15,
            startup_timeout=600,
            heartbeat_interval=10,
            heartbeat_timeout=90,
            run_timeout=43200,
        ),
        smtp=SimpleNamespace(enabled=True, host="smtp.example.com", from_addr="noreply@x", password="smtp-secret"),
        otel=SimpleNamespace(
            enabled=True,
            protocol="grpc",
            endpoint="http://collector:4317",
            headers="secret-header=abc",
            traces=True,
            metrics=True,
            sample_ratio=1.0,
        ),
        mcp=SimpleNamespace(external_url="", token="mcp-secret"),
        server=ServerSettings(external_url="", port=3000),
        secrets=SimpleNamespace(encryption_key="key-material"),
        postgres=SimpleNamespace(password="pg-secret"),
        quota=SimpleNamespace(max_sources=None, max_assets_per_source=None, max_successful_runs_per_month=25),
        catalog=["interloper_assets.demo.basic.source.DemoSource"],
    )


@pytest.fixture
def oauth_store() -> Iterator[Store]:
    """A store over a fresh in-memory database carrying the login and OAuth tables.

    Yields:
        The store, its engine disposed once the test finishes.
    """
    engine = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Profile, Organisation, UserOrganisation, Token, OAuthClient, AuthorizationCode, RefreshToken):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}))
    finally:
        engine.dispose()
        engine_module._engine = None
