"""This module contains the IO specification."""

from typing import Any

from pydantic import BaseModel

from interloper.io.base import IO
from interloper.utils.loader import import_from_path


class IOSpec(BaseModel):
    """A specification for an IO.

    Attributes:
        path: The path to the IO class.
        init: The arguments to initialize the IO class.
    """

    path: str
    init: dict[str, Any]

    def to_io(self) -> IO:
        """Create an IO from the specification.

        Returns:
            An IO object.
        """
        IOType: type[IO] = import_from_path(self.path)
        return IOType(**self.init)
