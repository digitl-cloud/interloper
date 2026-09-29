"""Tests for ``interloper_toolkit.errors``."""

from __future__ import annotations

from interloper_toolkit.errors import classify

HTTPX_429 = (
    "HTTPStatusError: Client error '429 Too Many Requests' for url "
    "'https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/50042019896?x=1'\n"
    "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429"
)
HTTPX_401 = (
    "HTTPStatusError: Client error '401 Unauthorized' for url "
    "'https://api.thetradedesk.com/v3/myreports/reportexecution/query/advertisers'\n"
    "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/401"
)
GOOGLE_403 = (
    "Forbidden: 403 POST https://bigquery.googleapis.com/bigquery/v2/projects/dc-swarovski/jobs?prettyPrint=false: "
    "Access Denied: Dataset dc-swarovski:raw: Permission bigquery.tables.create denied"
)
FACEBOOK = (
    "FacebookRequestError: \n\n"
    "  Message: Call was not successful\n"
    "  Method:  GET\n"
    "  Path:    https://graph.facebook.com/v23.0/act_1234567890/insights\n"
    "  Params:  {'level': 'ad', 'time_range': {'since': '2026-09-28'}}\n\n"
    "  Status:  400\n"
    "  Response:\n"
    "    {\n"
    '      "error": {\n'
    '        "message": "(#17) User request limit reached",\n'
    '        "type": "OAuthException",\n'
    '        "is_transient": true,\n'
    '        "code": 17,\n'
    '        "error_subcode": 2446079,\n'
    '        "fbtrace_id": "AbCdEf"\n'
    "      }\n"
    "    }\n"
)
VALIDATION = "ValidationError: 1 validation error(s) for FacebookAdsConnection: access_token: Field required"


class TestClassify:
    def test_httpx_status_and_endpoint_with_ids_masked(self):
        cause = classify(HTTPX_429)

        assert (cause.exception_type, cause.http_status) == ("HTTPStatusError", 429)
        assert cause.host == "sellingpartnerapi-eu.amazon.com"
        assert cause.path == "/reports/2021-06-30/reports/{id}"
        assert cause.summary == "HTTPStatusError 429 sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/{id}"

    def test_same_endpoint_with_different_ids_shares_a_fingerprint(self):
        assert classify(HTTPX_429).fingerprint == classify(HTTPX_429.replace("50042019896", "777")).fingerprint
        assert classify(HTTPX_429).fingerprint != classify(HTTPX_401).fingerprint

    def test_google_status_method_and_host(self):
        cause = classify(GOOGLE_403)

        assert (cause.exception_type, cause.http_status, cause.method) == ("Forbidden", 403, "POST")
        assert cause.host == "bigquery.googleapis.com"
        assert cause.path == "/bigquery/v2/projects/dc-swarovski/jobs"

    def test_facebook_status_method_path_and_vendor_codes(self):
        cause = classify(FACEBOOK)

        assert (cause.http_status, cause.method) == (400, "GET")
        assert cause.path == "/v23.0/{id}/insights"
        assert (cause.vendor_code, cause.vendor_subcode) == (17, 2446079)
        assert cause.summary.endswith("graph.facebook.com/v23.0/{id}/insights code=17/2446079")

    def test_text_without_a_request_groups_by_its_masked_first_line(self):
        cause = classify(VALIDATION)

        assert cause.exception_type == "ValidationError"
        assert cause.http_status is None
        assert cause.fingerprint == classify(VALIDATION.replace("1 validation", "3 validation")).fingerprint
        assert cause.summary.startswith("ValidationError: 1 validation error(s)")

    def test_a_bare_type_name_and_free_text_never_raise(self):
        assert classify("ReadTimeout").summary == "ReadTimeout"
        assert classify("ReadTimeout").exception_type == "ReadTimeout"
        assert classify("something went wrong at 3pm").exception_type is None
        assert classify("").fingerprint == "None|"
