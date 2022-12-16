from abc import ABCMeta, abstractproperty
from abc import abstractmethod
from typing import Any
from typing import Union

from .storage_item import StorageItem
from .storage_frame import StorageFrame
from .operation_result import OperationResult


class StorageORM(metaclass=ABCMeta):
    _db_instance: Any

    @abstractmethod
    def __init__(
        self,
        client: Any = None,
        host: Union[str, None] = None,
        port: int = 6379,
        db: int = 0,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def save(self, item) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def bulk_create(self, items: list) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def bulk_delete(self, items: list) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def delete(self, item) -> OperationResult:
        raise NotImplementedError

    @abstractproperty
    def frame(self) -> StorageFrame:
        raise NotImplementedError
