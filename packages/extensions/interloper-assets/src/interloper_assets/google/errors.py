"""Google's refusal of a REST request, with Google's own reason."""

from __future__ import annotations

from typing import Any

import httpx2


class GoogleApiError(httpx2.HTTPStatusError):
    """Google refused a request, with its own status and reason.

    It is the HTTP error status Google answered, so whatever reads an
    ``httpx2.HTTPStatusError`` reads it unchanged. Its message is Google's
    reason rather than httpx's, which names the request URL and only the
    status. Google's APIs refuse with one error object (``code``, ``message``,
    ``status``); the Ads APIs put the specific reason in its ``details``, as a
    failure listing ``errors``. Google's OAuth token endpoint refuses with
    ``error`` and ``error_description`` instead. None of them names a
    credential.
    """

    @classmethod
    def from_response(cls, response: httpx2.Response) -> GoogleApiError:
        """Read Google's refusal from an error response.

        Args:
            response: A response with an error status.

        Returns:
            The refusal, with Google's status and reason, or the HTTP status's reason when the body is not one of
            Google's errors.
        """
        try:
            body = response.json()
        except ValueError:
            body = None
        status, reason = cls._read(body)
        qualifier = f"HTTP {response.status_code}, {status}" if status else f"HTTP {response.status_code}"
        message = f"Google refused the request ({qualifier}): {reason or response.reason_phrase}"
        return cls(message, request=response.request, response=response)

    @staticmethod
    def _read(body: Any) -> tuple[str | None, str | None]:
        """Read the status and reason out of a refusal's body.

        Args:
            body: The decoded body, or None when it is not JSON.

        Returns:
            Google's status, or the OAuth error code, and its reason; either is None when the body does not say.
        """
        error = body.get("error") if isinstance(body, dict) else None
        if isinstance(error, str):
            return error, body.get("error_description")
        if not isinstance(error, dict):
            return None, None
        failures = [
            failure["message"]
            for detail in error.get("details") or []
            for failure in detail.get("errors") or []
            if failure.get("message")
        ]
        return error.get("status"), "; ".join(failures) or error.get("message")
