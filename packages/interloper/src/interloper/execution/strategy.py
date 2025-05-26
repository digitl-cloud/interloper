from enum import Enum, auto


class ExecutionStategy(Enum):
    NOT_PARTITIONED = auto()
    PARTITIONED_MULTI_RUNS = auto()
    PARTITIONED_SINGLE_RUN = auto()
    MIXED = auto()


class MaterializationStrategy(Enum):
    FLEXIBLE = auto()
    STRICT = auto()
