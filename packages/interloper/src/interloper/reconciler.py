import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")
logger = logging.getLogger(__name__)


class Reconciler(ABC, Generic[T]):
    @abstractmethod
    def reconcile(
        self,
        data: T,
        schema: dict[str, str],
    ) -> T: ...
