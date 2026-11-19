"""Tests for Google's refusal of a REST request.

Google's APIs refuse with one error object (``code``, ``message``, ``status``),
whose ``details`` carry the specific reason on the Ads APIs, and its OAuth token
endpoint with ``error`` and ``error_description``. These tests pin the reason
read from each.
"""

from __future__ import annotations

from typing import Any

import httpx2
import pytest

from interloper_assets.google.errors import GoogleApiError


def _refusal(status: int, body: Any) -> httpx2.Response:
    request = httpx2.Request("GET", "https://example.googleapis.com/v1/things")
    if isinstance(body, bytes):
        return httpx2.Response(status, content=body, request=request)
    return httpx2.Response(status, json=body, request=request)


class TestFromResponse:
    def test_reads_googles_status_and_message(self):
        refusal = {"code": 403, "message": "The caller does not have permission", "status": "PERMISSION_DENIED"}

        error = GoogleApiError.from_response(_refusal(403, {"error": refusal}))

        assert str(error) == (
            "Google refused the request (HTTP 403, PERMISSION_DENIED): The caller does not have permission"
        )

    def test_the_ads_apis_reason_is_read_from_the_failure_details(self):
        failure = {
            "@type": "type.googleapis.com/google.ads.googleads.v23.errors.GoogleAdsFailure",
            "errors": [
                {"errorCode": {"authorizationError": "DEVELOPER_TOKEN_NOT_APPROVED"}, "message": "Test accounts only."},
                {"errorCode": {"authorizationError": "USER_PERMISSION_DENIED"}, "message": "No access to 222."},
            ],
            "requestId": "r1",
        }
        response = _refusal(
            403,
            {
                "error": {
                    "code": 403,
                    "message": "The caller does not have permission",
                    "status": "PERMISSION_DENIED",
                    "details": [failure],
                }
            },
        )

        error = GoogleApiError.from_response(response)

        assert str(error) == (
            "Google refused the request (HTTP 403, PERMISSION_DENIED): Test accounts only.; No access to 222."
        )

    def test_reads_the_token_endpoints_error_and_description(self):
        response = _refusal(400, {"error": "invalid_grant", "error_description": "Token has been expired or revoked."})

        error = GoogleApiError.from_response(response)

        assert str(error) == "Google refused the request (HTTP 400, invalid_grant): Token has been expired or revoked."

    @pytest.mark.parametrize("body", [b"<html>Bad Gateway</html>", {"unexpected": True}, {"error": {}}])
    def test_a_body_without_googles_error_reads_the_status_reason(self, body: Any):
        error = GoogleApiError.from_response(_refusal(502, body))

        assert str(error) == "Google refused the request (HTTP 502): Bad Gateway"

    def test_is_the_http_error_of_the_refused_response(self):
        response = _refusal(404, {"error": {"code": 404, "message": "Not found", "status": "NOT_FOUND"}})

        error = GoogleApiError.from_response(response)

        assert isinstance(error, httpx2.HTTPStatusError)
        assert error.response is response
        assert error.request is response.request
