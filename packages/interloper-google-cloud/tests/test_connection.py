"""Tests for GoogleCloudConnection helpers."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from interloper_google_cloud.connection import _list_buckets, _list_projects


def _client_returning(pages: list[dict[str, Any]]) -> AsyncMock:
    """Build an httpx client mock whose GETs return *pages* in order."""
    client = AsyncMock()
    responses = []
    for page in pages:
        resp = MagicMock()
        resp.json.return_value = page
        responses.append(resp)
    client.get.side_effect = responses
    return client


class TestListBuckets:
    """Bucket listing pages through the storage API and sorts by name."""

    async def test_paginates_and_sorts(self):
        client = _client_returning(
            [
                {"items": [{"name": "zeta"}], "nextPageToken": "t1"},
                {"items": [{"name": "Alpha"}]},
            ]
        )
        buckets = await _list_buckets(client, "token", "test-proj")
        assert buckets == [{"name": "Alpha"}, {"name": "zeta"}]
        assert client.get.call_count == 2
        first_params = client.get.call_args_list[0].kwargs["params"]
        assert first_params["project"] == "test-proj"
        assert "pageToken" not in first_params
        assert client.get.call_args_list[1].kwargs["params"]["pageToken"] == "t1"

    async def test_empty(self):
        client = _client_returning([{}])
        assert await _list_buckets(client, "token", "test-proj") == []


class TestListProjects:
    """Project listing builds display labels and follows pagination."""

    async def test_labels_and_sorting(self):
        client = _client_returning(
            [
                {
                    "projects": [
                        {"id": "proj-b", "friendlyName": "Beta"},
                        {"id": "proj-a"},
                    ]
                }
            ]
        )
        projects = await _list_projects(client, "token")
        assert projects == [
            {"project_id": "proj-b", "name": "Beta (proj-b)"},
            {"project_id": "proj-a", "name": "proj-a (proj-a)"},
        ]
