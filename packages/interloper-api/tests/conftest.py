"""Shared fixtures for the API tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.fixture
def fake_settings() -> SimpleNamespace:
    """AppSettings-shaped namespace covering every field ``create_app`` and the
    config-snapshot builder read, with secrets planted to prove redaction."""
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
        agent=SimpleNamespace(enabled=True, model="gemini-2.5-flash"),
        auth=SimpleNamespace(
            allowed_domains=["digitlcloud.com"],
            super_admin_emails=["boss@example.com"],
            google_client_id="cid",
            google_client_secret="oauth-secret",
            google_redirect_uri="https://app/api/auth/google/callback",
            session_expiry_days=30,
            cookie_secure=True,
        ),
        cron=SimpleNamespace(enabled=True, reconcile_interval=10, batch_size=50, max_execution_delay=None),
        worker=SimpleNamespace(enabled=True, poll_interval=5),
        reaper=SimpleNamespace(enabled=True, timeout=3600, poll_interval=60),
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
        secrets=SimpleNamespace(encryption_key="key-material"),
        postgres=SimpleNamespace(password="pg-secret"),
        quota=SimpleNamespace(max_sources=None, max_assets_per_source=None, max_successful_runs_per_month=25),
        catalog=["interloper_assets.demo.source.DemoSource"],
    )
