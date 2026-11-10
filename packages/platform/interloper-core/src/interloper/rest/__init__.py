"""REST client with pagination and authentication support."""

from interloper.rest.auth import HTTPBearerAuth, OAuth2Auth, OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth
from interloper.rest.client import AsyncRESTClient, RESTClient
from interloper.rest.paginator import (
    BasePaginator,
    HeaderLinkPaginator,
    JSONCursorPaginator,
    JSONLinkPaginator,
    OffsetPaginator,
    PageNumberPaginator,
    RangePaginator,
    SinglePagePaginator,
)

__all__ = [
    "AsyncRESTClient",
    "BasePaginator",
    "HTTPBearerAuth",
    "HeaderLinkPaginator",
    "JSONCursorPaginator",
    "JSONLinkPaginator",
    "OAuth2Auth",
    "OAuth2ClientCredentialsAuth",
    "OAuth2RefreshTokenAuth",
    "OffsetPaginator",
    "PageNumberPaginator",
    "RESTClient",
    "RangePaginator",
    "SinglePagePaginator",
]
