import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

T = TypeVar("T")
logger = logging.getLogger(__name__)


class Reconciler(ABC, Generic[T]):
    @abstractmethod
    def reconcile(
        self,
        data: T,
        table_schema: dict[str, str],
    ) -> T: ...


class JSONReconciler(Reconciler[list[dict[str, Any]]]):
    def reconcile(
        self,
        data: list[dict[str, Any]],
        table_schema: dict[str, str],
    ) -> list[dict[str, Any]]:
        return data
