"""The API's own line per request, in place of uvicorn's access log."""

from interloper_api.requests.base import RequestLog

__all__ = ["RequestLog"]
