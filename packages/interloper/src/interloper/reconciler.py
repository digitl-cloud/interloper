"""This module contains the reconciler classes."""
import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

T = TypeVar("T")
logger = logging.getLogger(__name__)


class Reconciler(ABC, Generic[T]):
    """An abstract class for reconcilers."""

    @abstractmethod
    def reconcile(
        self,
        data: T,
        table_schema: dict[str, str],
    ) -> T:
        """Reconcile the data with the table schema.

        Args:
            data: The data to reconcile.
            table_schema: The schema of the table.

        Returns:
            The reconciled data.
        """
        ...


class JSONReconciler(Reconciler[list[dict[str, Any]]]):
    """A reconciler for JSON data."""

    def reconcile(
        self,
        data: list[dict[str, Any]],
        table_schema: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Reconcile the JSON data with the table schema.

        Args:
            data: The data to reconcile.
            table_schema: The schema of the table.

        Returns:
            The reconciled data.
        """
        return data
