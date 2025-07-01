"""This module contains the execution strategy enums."""
from enum import Enum, auto


class ExecutionStategy(Enum):
    """The execution strategy for a DAG."""

    NOT_PARTITIONED = auto()
    PARTITIONED_MULTI_RUNS = auto()
    PARTITIONED_SINGLE_RUN = auto()
    MIXED = auto()


class MaterializationStrategy(Enum):
    """The materialization strategy for an asset."""

    FLEXIBLE = auto()
    STRICT = auto()
