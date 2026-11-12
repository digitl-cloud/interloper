"""Shared fixtures for the API tests."""

from __future__ import annotations

import socket
from collections.abc import Callable, Iterator
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx2
import interloper as il
import pytest
from interloper.settings import AuthSettings, ServerSettings
from interloper_db import Store
from interloper_db import engine as engine_module
from interloper_db.models import (
    AuthorizationCode,
    Conversation,
    OAuthClient,
    Organisation,
    Profile,
    RefreshToken,
    Token,
    UserOrganisation,
)
from sqlalchemy import event
from sqlalchemy.pool import StaticPool

from interloper_api.authorization import ClientMetadataFetcher

PUBLIC_ADDRESS = "160.79.104.10"


class FakeDocumentHost:
    """The network client metadata documents are read over, faked: name resolution and the documents served.

    Attributes:
        addresses: What each host name resolves to; ``claude.ai`` to a public address.
        documents: The JSON served under 200 at each client id URL; any other URL is a 404.
        requests: Every request the fetcher sent.
        respond: Answers a request; :meth:`serve` unless a test replaces it.
    """

    def __init__(self) -> None:
        """Start with ``claude.ai`` resolvable and nothing served."""
        self.addresses: dict[str, list[str]] = {"claude.ai": [PUBLIC_ADDRESS]}
        self.documents: dict[str, Any] = {}
        self.requests: list[httpx2.Request] = []
        self.respond: Callable[[httpx2.Request], httpx2.Response] = self.serve

    def getaddrinfo(self, host: str, port: int, *args: Any, **kwargs: Any) -> list[Any]:
        """Resolve a host name from :attr:`addresses`, as ``socket.getaddrinfo`` does.

        Args:
            host: The host name.
            port: The port.
            *args: The lookup's other arguments, ignored.
            **kwargs: Its keyword arguments, ignored.

        Returns:
            One stream entry per address.

        Raises:
            socket.gaierror: When the name is unknown.
        """
        if host not in self.addresses:
            raise socket.gaierror("unknown host")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, port)) for address in self.addresses[host]]

    def serve(self, request: httpx2.Request) -> httpx2.Response:
        """Serve the document at the URL the request names through its ``Host`` header.

        Args:
            request: The request, sent to an address.

        Returns:
            200 with the document, or 404.
        """
        url = f"https://{request.headers['host']}{request.url.raw_path.decode()}"
        if url not in self.documents:
            return httpx2.Response(404)
        return httpx2.Response(200, json=self.documents[url])

    def fetcher(self) -> ClientMetadataFetcher:
        """Build a fetcher whose requests reach this host.

        Returns:
            The fetcher.
        """

        def handle(request: httpx2.Request) -> httpx2.Response:
            self.requests.append(request)
            return self.respond(request)

        return ClientMetadataFetcher(transport=httpx2.MockTransport(handle))


@pytest.fixture
def document_host(monkeypatch: pytest.MonkeyPatch) -> FakeDocumentHost:
    """The faked network client metadata documents are read over; name resolution goes through it.

    Returns:
        The host.
    """
    host = FakeDocumentHost()
    monkeypatch.setattr(socket, "getaddrinfo", host.getaddrinfo)
    return host


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
            type="in_process",
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
        auth=AuthSettings(
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
        server=ServerSettings(external_url="", port=3000),
        encryption=SimpleNamespace(key="key-material"),
        postgres=SimpleNamespace(password="pg-secret"),
        quota=SimpleNamespace(max_sources=None, max_assets_per_source=None, max_successful_runs_per_month=25),
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

    for model in (
        Profile,
        Organisation,
        UserOrganisation,
        Token,
        OAuthClient,
        AuthorizationCode,
        RefreshToken,
        Conversation,
    ):
        model.__table__.create(engine)  # ty: ignore[missing-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}))
    finally:
        engine.dispose()
        engine_module._engine = None
