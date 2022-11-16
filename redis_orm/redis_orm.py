import redis
import logging

from .item import Item
from .operation_result import OperationResult
from .operation_result import OperationStatus


class RedisORM:
    """ Работа с БД Redis через объектное представление """
    _pipe: redis.client.Pipeline
    _client: redis.Redis

    def __init__(self, redis_client: redis.Redis) -> None:
        self._client = redis_client
        self._pipe = redis_client.pipeline()
        if not Item._db_instance:
            Item._set_global_instance(db_instance=redis_client)

    def save(self, item: Item) -> OperationResult:
        """ Одиночная вставка """
        try:
            self._client.mset(mapping=item.mapping)
            return OperationResult(status=OperationStatus.success)
        except Exception as exception:
            logging.exception(exception)
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )

    def bulk_create(self, items: list[Item]) -> OperationResult:
        """ Групповая вставка """
        try:
            for redis_item in items:
                self._pipe.mset(mapping=redis_item.mapping)
            self._pipe.execute()
            return OperationResult(status=OperationStatus.success)
        except Exception as exception:
            logging.exception(exception)
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )
