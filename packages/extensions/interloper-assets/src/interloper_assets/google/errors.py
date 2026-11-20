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
        return cls.from_body(body, response)

    @classmethod
    def from_body(cls, body: Any, response: httpx2.Response) -> GoogleApiError:
        """Read Google's refusal from a decoded error body.

        A stream that fails after it started has already answered 200, and
        closes its array on the error object instead, so the status named is
        the error's own ``code`` when it gives one.

        Args:
            body: The decoded error body, Google's ``{"error": ...}``, or None when the body is not JSON.
            response: The response the body came in.

        Returns:
            The refusal, with Google's status and reason, or the HTTP status's reason when the body is not one of
            Google's errors.
        """
        code, status, reason = cls._read(body)
        code = code or response.status_code
        qualifier = f"HTTP {code}, {status}" if status else f"HTTP {code}"
        message = f"Google refused the request ({qualifier}): {reason or response.reason_phrase}"
        return cls(message, request=response.request, response=response)

    @staticmethod
    def _read(body: Any) -> tuple[int | None, str | None, str | None]:
        """Read the code, status and reason out of a refusal's body.

        Args:
            body: The decoded body, or None when it is not JSON.

        Returns:
            Google's HTTP code, its status or the OAuth error code, and its reason; each is None when the body does
            not say.
        """
        error = body.get("error") if isinstance(body, dict) else None
        if isinstance(error, str):
            return None, error, body.get("error_description")
        if not isinstance(error, dict):
            return None, None, None
        failures = [
            failure["message"]
            for detail in error.get("details") or []
            for failure in detail.get("errors") or []
            if failure.get("message")
        ]
        code = error.get("code")
        return code if isinstance(code, int) else None, error.get("status"), "; ".join(failures) or error.get("message")
