from typing import Any

import pytest

import interloper as itlp


@pytest.fixture
def io() -> itlp.IO:
    class IO(itlp.IO):
        def write(self, context: itlp.IOContext, data: Any) -> None:
            pass

        def read(self, context: itlp.IOContext) -> Any:
            return "data"

    return IO()
