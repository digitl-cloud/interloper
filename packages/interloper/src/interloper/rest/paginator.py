import re
from abc import ABC, abstractmethod
from collections.abc import Generator
from typing import Any

import httpx


class Paginator(ABC):
    @abstractmethod
    def paginate(self, client: httpx.Client, path: str) -> Generator[Any]: ...


# TODO: implementation should be robust, as in, it should handle more exit strategies. `total_pages_path`?
# And be tested!
class PageNumberPaginator(Paginator):
    def __init__(
        self,
        initial_page: int = 1,
        page_param: str = "page",
        data_path: str | None = None,
        max_pages: int = 10,
    ):
        self.initial_page = initial_page
        self.page_param = page_param
        self.data_path = data_path
        self.max_pages = max_pages

    def paginate(self, client: httpx.Client, path: str) -> Generator[Any]:
        page = self.initial_page

        while True:
            response = client.get(path, params={self.page_param: page})
            response.raise_for_status()

            data = response.json()
            if self.data_path:
                data = _json_path_extract(data, self.data_path)
            yield data

            if not data or page >= self.max_pages:
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
