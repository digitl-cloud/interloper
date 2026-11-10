"""Operations: the units of work runners execute, and the workloads that provide them."""

from interloper.operation.base import Effect, Operation, OperationContext, OperationResult, Workload

__all__ = [
    "Effect",
    "Operation",
    "OperationContext",
    "OperationResult",
    "Workload",
]
