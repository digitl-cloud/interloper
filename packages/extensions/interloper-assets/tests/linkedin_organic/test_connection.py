"""Tests for the LinkedIn Organic connection.

The organization dropdown lists the organizations the member administers,
named by decorating each access control entry with its organization, and the
connection check runs that lookup.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il

from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection
from interloper_assets.linkedin_organic.constants import BASE_URL

LOOKUP_URL = (
    f"{BASE_URL}/organizationAcls?q=roleAssignee&role=ADMINISTRATOR&state=APPROVED"
    "&projection=%28elements%2A%28organization~%28id%2ClocalizedName%29%29%2Cpaging%29&count=100"
)


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


class TestOrganizations:
    async def test_lists_the_administered_organizations_by_their_localized_name(self):
        sent: list[str] = []
        elements = [
            {"organization": "urn:li:organization:1", "organization~": {"id": 1, "localizedName": "Alpha"}},
            {"organization": "urn:li:organization:2"},
            {"role": "ADMINISTRATOR"},
        ]

        organizations = await _connection(_recording(sent, lambda _: _acls(elements))).organizations()

        assert organizations == [{"id": "1", "name": "Alpha"}, {"id": "2", "name": "2"}]
        assert sent == [LOOKUP_URL]

    async def test_follows_the_next_link(self):
        sent: list[str] = []
        next_href = (
            "/rest/organizationAcls?count=100"
            "&projection=%28elements%2A%28organization~%28id%2ClocalizedName%29%29%2Cpaging%29"
            "&q=roleAssignee&role=ADMINISTRATOR&start=100&state=APPROVED"
        )

        def respond(request: httpx2.Request) -> httpx2.Response:
            if "start=100" in str(request.url):
                return _acls([{"organization": "urn:li:organization:2"}])
            return _acls([{"organization": "urn:li:organization:1"}], next_href=next_href)

        organizations = await _connection(_recording(sent, respond)).organizations()

        assert organizations == [{"id": "1", "name": "1"}, {"id": "2", "name": "2"}]
        assert sent == [LOOKUP_URL, f"https://api.linkedin.com{next_href}"]


class TestCheck:
    async def test_a_working_token_passes(self):
        assert await _connection(lambda _: _acls([])).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_refused_lookup_reports_linkedins_reason(self):
        refused = httpx2.Response(403, json={"status": 403, "message": "Not enough permissions"})

        result = await _connection(lambda _: refused).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False,
            live=True,
            category="error",
            message="LinkedIn refused the request (HTTP 403): Not enough permissions",
        )
