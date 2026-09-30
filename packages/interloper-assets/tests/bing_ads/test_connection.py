"""Regression tests for the BingAds connection."""

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

from interloper_assets.bing_ads.connection import BingAdsConnection


class TestReportingServiceManagerWorkingDirectory:
    """Each manager must get its own working directory.

    The SDK defaults to a shared ``/tmp/BingAdsSDKPython`` created with a racy
    exists-then-makedirs, so concurrent report assets in one pod crash with
    ``FileExistsError`` (prod runs 7c88e1be / 1205e329).
    """

    def test_each_manager_gets_a_fresh_working_directory(self, monkeypatch):
        import bingads.v13.reporting.reporting_service_manager as rsm_module

        captured: list[dict[str, Any]] = []

        class FakeManager:
            def __init__(self, authorization_data: Any, **kwargs: Any):
                captured.append(kwargs)

        monkeypatch.setattr(rsm_module, "ReportingServiceManager", FakeManager)
        monkeypatch.setattr(BingAdsConnection, "authorization_data", lambda self, account_id: SimpleNamespace())

        connection = BingAdsConnection(
            client_id="cid",
            client_secret="secret",
            refresh_token="token",
            developer_token="dev",
        )
        connection.reporting_service_manager("123")
        connection.reporting_service_manager("456")

        dirs = [kwargs["working_directory"] for kwargs in captured]
        assert len(dirs) == 2
        assert dirs[0] != dirs[1]
        for path in dirs:
            assert os.path.isdir(path)
