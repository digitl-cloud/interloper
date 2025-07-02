"""This module contains the paginator classes for the REST client."""
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Generator
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class Paginator(ABC):
    """An abstract class for paginators."""

    @abstractmethod
    def paginate(self, client: httpx.Client, path: str) -> Generator[Any]:
        """Paginate through a resource.

        Args:
            client: The httpx client.
            path: The path to the resource.

        Yields:
            The items in the resource.
        """
        ...


# TODO: implementation should be robust, as in, it should handle more exit strategies. `total_pages_path`?
# And be tested!
class PageNumberPaginator(Paginator):
    """A paginator that uses page numbers."""

    def __init__(
        self,
        initial_page: int = 1,
        page_param: str = "page",
        total_pages_path: str | None = None,
        max_pages: int = 50,
    ):
        """Initialize the page number paginator.

        Args:
            initial_page: The initial page number.
            page_param: The name of the page parameter.
            total_pages_path: The path to the total pages in the response.
            max_pages: The maximum number of pages to fetch.
        """
        self.initial_page = initial_page
        self.page_param = page_param
        self.total_pages_path = total_pages_path
        self.max_pages = max_pages

    def paginate(self, client: httpx.Client, path: str) -> Generator[Any]:
        """Paginate through a resource.

        Args:
            client: The httpx client.
            path: The path to the resource.

        Yields:
            The items in the resource.
        """
        page = self.initial_page

        while True:
            response = client.get(path, params={self.page_param: page})
            response.raise_for_status()
            data = response.json()
            yield data

            if self.total_pages_path:
                total_pages = _json_path_extract(data, self.total_pages_path)
                if total_pages and page >= total_pages:
                    break
            if page >= self.max_pages:
                logger.warning(f"PageNumberPaginator reached max pages ({self.max_pages})")
                break
            if not data:
                break
            page += 1


def _json_path_extract(data: dict, path: str) -> Any:
    if not isinstance(path, str) or not path:
        raise ValueError("Path must be a non-empty string.")

    # Basic path validation: allow alphanumerics, underscores, and dots
    if not re.fullmatch(r"[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*", path):
        raise ValueError("Invalid path format. Use dot-separated keys, e.g., 'store.book.title'.")

    keys = path.split(".")
    current = data

    for key in keys:
        if not isinstance(current, dict):
            raise TypeError(f"Expected dict at key '{key}', but got {type(current).__name__}.")
        if key not in current:
            raise ValueError(f"Path '{path}' is not valid. Key '{key}' was not found.")
        current = current[key]

    return current
