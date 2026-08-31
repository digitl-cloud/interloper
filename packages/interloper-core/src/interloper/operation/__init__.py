"""Operations: the units of work runners execute, and the workloads that provide them."""

from interloper.operation.base import Operation, OperationContext, OperationResult, Workload

__all__ = [
    "Operation",
    "OperationContext",
    "OperationResult",
    "Workload",
]
