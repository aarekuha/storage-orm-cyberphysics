from abc import ABCMeta
from abc import abstractmethod
from typing import Any

from .operation_result import OperationResult


class StorageFrame(metaclass=ABCMeta):
    _db_instance: Any

    @abstractmethod
    def add(self, item_or_items) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def bulk_create(self, items) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def clear(self, item) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def get(self, item, start_index: int = 0, end_index: int = -1) -> list:
        raise NotImplementedError
