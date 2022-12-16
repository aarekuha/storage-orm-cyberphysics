import redis
import logging
from typing import Union
from typing import TypeVar

from .redis_frame import RedisFrame
from .redis_item import RedisItem
from .redis_item import T as SubclassItemType
from ..operation_result import OperationResult
from ..operation_result import OperationStatus

from ..storage_orm import StorageORM

ChildItem = TypeVar('ChildItem', bound=RedisItem)


class RedisORM(StorageORM):
    """ Работа с БД Redis через объектное представление """
    _pipe: redis.client.Pipeline
    _client: redis.Redis
    _frame: RedisFrame

    def __init__(
        self,
        client: Union[redis.Redis, None] = None,
        host: Union[str, None] = None,
        port: int = 6379,
        db: int = 0,
    ) -> None:
        if client:
            self._client = client
        elif host:
            self._client = redis.Redis(host=host, port=port, db=db)
        else:
            raise Exception("StorageORM-init must contains redis_client or host values...")

        self._pipe = self._client.pipeline()
        if not RedisItem._db_instance:
            RedisItem._set_global_instance(db_instance=self._client)

        self._frame = RedisFrame(client=self._client)

    @property
    def frame(self) -> RedisFrame:
        """ Подготовленный frame для работы со списками значений """
        return self._frame

    def save(self, item: RedisItem) -> OperationResult:
        """ Одиночная вставка """
        return item.save()

    def bulk_create(self, items: list[SubclassItemType]) -> OperationResult:
        """ Групповая вставка """
        try:
            if hasattr(items[0], "_ttl") and items[0]._ttl:
                for redis_item in items:
                    for key, value in redis_item.mapping.items():
                        self._pipe.set(name=key, value=value, ex=redis_item._ttl)
            else:
                for redis_item in items:
                    self._pipe.mset(mapping=redis_item.mapping)
            self._pipe.execute()
            return OperationResult(status=OperationStatus.success)
        except Exception as exception:
            self._on_error_actions(exception=exception)
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )

    def bulk_delete(self, items: list[ChildItem]) -> OperationResult:
        """
            Удаление списка элементов
        """
        try:
            for redis_item in items:
                self._pipe.delete(*[key for key in redis_item.mapping.keys()])
            self._pipe.execute()
            return OperationResult(status=OperationStatus.success)
        except Exception as exception:
            self._on_error_actions(exception=exception)
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )

    def delete(self, item: RedisItem) -> OperationResult:
        """
            Удаление одного элемента
        """
        return item.delete()

    def _on_error_actions(self, exception: Exception) -> None:
        """
            Действия, выполняющиеся в случае возникновения исключения
                во время вставки, сохранения, получения данных из БД
        """
        logging.exception(exception)
