"""Tests for the LinkedIn Organic connection.

The organization dropdown lists the organizations the member administers,
read from their access control entries and named by one batch lookup of those
organizations, and the connection check runs that lookup.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il

from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection
from interloper_assets.linkedin_organic.constants import BASE_URL

LOOKUP_URL = f"{BASE_URL}/organizationAcls?q=roleAssignee&role=ADMINISTRATOR&state=APPROVED&count=100"


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> LinkedinOrganicConnection:
    connection = LinkedinOrganicConnection(client_id="cid", client_secret="secret", refresh_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
    return connection


def _recording(sent: list[str], respond: Callable[[httpx2.Request], httpx2.Response]) -> Any:
    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(str(request.url))
        return respond(request)

    return handler


def _acls(elements: list[dict[str, Any]], next_href: str | None = None) -> httpx2.Response:
    links = [] if next_href is None else [{"rel": "next", "href": next_href}]
    return httpx2.Response(200, json={"elements": elements, "paging": {"start": 0, "count": 100, "links": links}})


class TestClient:
    def test_sends_linkedins_version_and_the_rest_li_protocol(self):
        connection = LinkedinOrganicConnection(client_id="cid", client_secret="secret", refresh_token="token")

        headers = connection.client.headers
        assert headers["LinkedIn-Version"] == "202607"
        assert headers["X-Restli-Protocol-Version"] == "2.0.0"
        assert headers["Accept-Encoding"] == "gzip, deflate, br"

    def test_the_api_is_built_on_the_client(self):
        connection = _connection(lambda _: _acls([]))

        assert connection.api.client is connection.client


def _names(results: dict[str, Any], errors: dict[str, Any] | None = None) -> httpx2.Response:
    statuses = {**dict.fromkeys(results, 200), **dict.fromkeys(errors or {}, 403)}
    return httpx2.Response(200, json={"results": results, "statuses": statuses, "errors": errors or {}})


class TestOrganizations:
    async def test_lists_the_administered_organizations_by_their_localized_name(self):
        sent: list[str] = []
        acls = [
            {"role": "ADMINISTRATOR", "state": "APPROVED", "organization": "urn:li:organization:1"},
            {"role": "ADMINISTRATOR", "state": "APPROVED", "organizationTarget": "urn:li:organization:2"},
            {"role": "ADMINISTRATOR", "state": "APPROVED"},
        ]
        names = {"1": {"id": 1, "localizedName": "Alpha"}, "2": {"id": 2, "localizedName": "Beta"}}

        def respond(request: httpx2.Request) -> httpx2.Response:
            return _names(names) if request.url.path == "/rest/organizations" else _acls(acls)

        organizations = await _connection(_recording(sent, respond)).organizations()

        assert organizations == [{"id": "1", "name": "Alpha"}, {"id": "2", "name": "Beta"}]
        assert sent == [LOOKUP_URL, f"{BASE_URL}/organizations?ids=List(1,2)"]

    async def test_an_organization_linkedin_does_not_name_is_listed_by_its_id(self):
        acls = [{"organization": "urn:li:organization:1"}, {"organization": "urn:li:organization:2"}]
        refused = {
            "2": {"status": 403, "message": "Viewer don't have permission to the ADMIN_ONLY VisibilityReduction"}
        }

        def respond(request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/rest/organizations":
                return _names({"1": {"id": 1, "localizedName": "Alpha"}}, errors=refused)
            return _acls(acls)

        organizations = await _connection(respond).organizations()

        assert organizations == [{"id": "1", "name": "Alpha"}, {"id": "2", "name": "2"}]

    async def test_follows_the_next_link(self):
        sent: list[str] = []
        next_href = "/rest/organizationAcls?count=100&q=roleAssignee&role=ADMINISTRATOR&start=100&state=APPROVED"

        def respond(request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/rest/organizations":
                return _names({"1": {"localizedName": "Alpha"}, "2": {"localizedName": "Beta"}})
            if "start=100" in str(request.url):
                return _acls([{"organization": "urn:li:organization:2"}])
            return _acls([{"organization": "urn:li:organization:1"}], next_href=next_href)

        organizations = await _connection(_recording(sent, respond)).organizations()

        assert organizations == [{"id": "1", "name": "Alpha"}, {"id": "2", "name": "Beta"}]
        assert sent == [
            LOOKUP_URL,
            f"https://api.linkedin.com{next_href}",
            f"{BASE_URL}/organizations?ids=List(1,2)",
        ]

    async def test_no_administered_organization_asks_for_no_names(self):
        sent: list[str] = []

        assert await _connection(_recording(sent, lambda _: _acls([]))).organizations() == []
        assert sent == [LOOKUP_URL]


class TestCheck:
    async def test_a_working_token_passes(self):
        assert await _connection(lambda _: _acls([])).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_refused_name_lookup_reports_linkedins_reason(self):
        def respond(request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/rest/organizations":
                return httpx2.Response(403, json={"status": 403, "message": "Not enough permissions"})
            return _acls([{"organization": "urn:li:organization:1"}])

        result = await _connection(respond).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False,
            live=True,
            category="error",
            message="LinkedIn refused the request (HTTP 403): Not enough permissions",
        )

    async def test_a_refused_lookup_reports_linkedins_reason(self):
        refused = httpx2.Response(403, json={"status": 403, "message": "Not enough permissions"})

        result = await _connection(lambda _: refused).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False,
            live=True,
            category="error",
            message="LinkedIn refused the request (HTTP 403): Not enough permissions",
        )


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert (
            LinkedinOrganicConnection(client_id="cid", client_secret="secret", refresh_token="token").client.retry
            == il.HttpRetryPolicy()
        )
