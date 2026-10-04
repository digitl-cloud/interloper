"""Tests for ``interloper_db.store.insights.failures``."""

from __future__ import annotations

import datetime as dt
from uuid import uuid4

from interloper_db.store.insights.failures import ErrorCause, ErrorGroup, ErrorRow

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


class TestErrorCause:
    def test_httpx_status_and_endpoint_with_ids_masked(self):
        cause = ErrorCause.from_text(HTTPX_429)

        assert (cause.exception_type, cause.http_status) == ("HTTPStatusError", 429)
        assert cause.host == "sellingpartnerapi-eu.amazon.com"
        assert cause.path == "/reports/2021-06-30/reports/{id}"
        assert cause.summary == "HTTPStatusError 429 sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/{id}"

    def test_same_endpoint_with_different_ids_shares_a_fingerprint(self):
        fingerprint = ErrorCause.from_text(HTTPX_429).fingerprint

        assert fingerprint == ErrorCause.from_text(HTTPX_429.replace("50042019896", "777")).fingerprint
        assert fingerprint != ErrorCause.from_text(HTTPX_401).fingerprint

    def test_google_status_method_and_host(self):
        cause = ErrorCause.from_text(GOOGLE_403)

        assert (cause.exception_type, cause.http_status, cause.method) == ("Forbidden", 403, "POST")
        assert cause.host == "bigquery.googleapis.com"
        assert cause.path == "/bigquery/v2/projects/dc-swarovski/jobs"

    def test_facebook_status_method_path_and_vendor_codes(self):
        cause = ErrorCause.from_text(FACEBOOK)

        assert (cause.http_status, cause.method) == (400, "GET")
        assert cause.path == "/v23.0/{id}/insights"
        assert (cause.vendor_code, cause.vendor_subcode) == (17, 2446079)
        assert cause.summary.endswith("graph.facebook.com/v23.0/{id}/insights code=17/2446079")

    def test_text_without_a_request_groups_by_its_masked_first_line(self):
        cause = ErrorCause.from_text(VALIDATION)

        assert cause.exception_type == "ValidationError"
        assert cause.http_status is None
        assert cause.fingerprint == ErrorCause.from_text(VALIDATION.replace("1 validation", "3 validation")).fingerprint
        assert cause.summary.startswith("ValidationError: 1 validation error(s)")

    def test_a_bare_type_name_and_free_text_never_raise(self):
        assert ErrorCause.from_text("ReadTimeout").summary == "ReadTimeout"
        assert ErrorCause.from_text("ReadTimeout").exception_type == "ReadTimeout"
        assert ErrorCause.from_text("something went wrong at 3pm").exception_type is None
        assert ErrorCause.from_text("").fingerprint == "None|"


_T0 = dt.datetime(2026, 9, 29, 4, tzinfo=dt.timezone.utc)


def _row(error: str, *, event_type: str = "operation_failed", count: int = 1, minute: int = 0, **fields) -> ErrorRow:
    seen = _T0 + dt.timedelta(minutes=minute)
    values = {"job_id": None, "run_id": uuid4(), "component_key": "orders", **fields}
    return ErrorRow(error=error, event_type=event_type, count=count, first_seen=seen, last_seen=seen, **values)


class TestErrorGroupMerge:
    def test_one_cause_across_runs_merges_and_retries_are_not_terminal(self):
        job = uuid4()
        rows = [
            _row(HTTPX_429, job_id=job, event_type="operation_retried", count=3),
            _row(HTTPX_429.replace("50042019896", "777"), job_id=job, minute=5),
        ]

        [group] = ErrorGroup.merge(rows, ("job", "cause"))

        assert (group.job_id, group.failed_attempts, group.terminal_failures) == (job, 4, 1)
        assert group.runs == {row.run_id for row in rows}
        assert (group.sample_run_id, group.last_seen) == (rows[1].run_id, rows[1].last_seen)
        assert group.sample.startswith("HTTPStatusError: Client error '429")

    def test_only_the_chosen_keys_split_groups_loudest_first(self):
        rows = [_row(HTTPX_429, component_key="a"), _row(HTTPX_401, component_key="b", count=2)]

        assert [g.asset_key for g in ErrorGroup.merge(rows, ("asset",))] == ["b", "a"]
        [merged] = ErrorGroup.merge(rows, ())
        assert (merged.asset_key, merged.cause, merged.failed_attempts) == (None, None, 3)
