from abc import ABCMeta
from abc import abstractmethod
from typing import Any
from typing import Union

from .storage_item import StorageItem
from .operation_result import OperationResult


class StorageFrame(metaclass=ABCMeta):
    _db_instance: Any

    @abstractmethod
    def add(
        cls,
        item_or_items: Union[StorageItem, list[StorageItem]],
    ) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def bulk_create(cls, items: list[StorageItem]) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def clear(cls, item: StorageItem) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def get(
        cls,
        item: StorageItem,
        start_item: int = 0,
        end_item: int = -1,
    ) -> list[StorageItem]:
        raise NotImplementedError
