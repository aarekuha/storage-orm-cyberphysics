import redis
import pickle
import logging
from typing import Any
from typing import Union
from redis.commands.core import Script

from .redis_item import RedisItem
from .redis_item import T as SubclassItemType
from ..operation_result import OperationResult
from ..operation_result import OperationStatus

from ..storage_frame import StorageFrame


class RedisFrame(StorageFrame):
    """
    Работа с искусственно ограниченным frame'ом объектов

    Frame - массив сериализованных объектов определенного размера
    Размер frame'а задается в объекте типа RedisItem, в блоке Meta -> frame_size
    Хранимый скрипт - lua-скрипт, который добавляется в Redis путем
                      вызова register_script. Он позволяет выполнять
                      установленную последовательность действий без
                      переключения контекста (python-redis), а также
                      все операции выполняются "за один раз", что
                      гарантирует атомарность всего процесса

    """
    DEFAULT_QUEUE_SIZE: int = 100
    # Префикс записей frame'ов. Нужен для исключения потенциальных пересечений
    FRAME_PREFIX: str = "frame."
    QUEUE_START_INDEX: int = 0
    QUEUE_END_INDEX: int = -1

    _pipe: redis.client.Pipeline
    _client: redis.Redis
    _queue_add: Script  # Хранимый скрипт Redis

    def __init__(self, client: redis.Redis) -> None:
        self._client = client
        self._pipe = client.pipeline()
        # Инициализация хранимого скрипта в Redis для атомарного добавления
        #   в конец одного элемента и удаления первого элемента, если размер
        #   списка достиг лимита
        # Сигнатура вызова после регистрации:
        #   self._queue_add(keys=["example_key"], args=["my_value", 10], client=...)
        #   , где 10 - это размер списка (лимит)
        #         client - это клиет, в котором будет вызван скрипт (им может быть
        #                  pipeline)
        self._queue_add = self._client.register_script("""
            redis.call('rpush', KEYS[1], ARGV[1])
            if redis.call('llen', KEYS[1]) > tonumber(ARGV[2]) then
                redis.call('lpop', KEYS[1])
            end
        """)
        # Подрезка списков, согласно установленной в subclass'е величине
        RedisItem._on_init_ltrim = self.ltrim_by_item

    def add(
        self,
        item_or_items: Union[SubclassItemType, list[SubclassItemType]],
    ) -> OperationResult:
        """
        Добавление объект(а/ов) во frame

        В списке объектов приемлемо использовать разнородные данные
            (т.е. переданные объекты могут не принадлежать одному классу)

        """
        try:
            if isinstance(item_or_items, RedisItem):
                self._add_item(item=item_or_items)
            else:
                [self._add_item(item) for item in item_or_items]  # type: ignore
        except Exception as exception:
            self._pipe.reset()
            logging.exception(exception)
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )
        self._pipe.execute()
        return OperationResult(status=OperationStatus.success)

    def _get_frame_size(self, item: RedisItem) -> int:
        if hasattr(item, "_frame_size") and item._frame_size:
            queue_size = item._frame_size
        else:
            queue_size = self.DEFAULT_QUEUE_SIZE
        return queue_size

    def _add_item(self, item: RedisItem) -> None:
        """
        Добавление объекта в БД

        - сериализация данных объекта
        - вызов хранимого скрипта Redis для атомарной операции
          наполнения ограниченного списка

        """
        # item._table содержит строку с подставленными параметрами текущего объекта
        key: str = self._make_key(item=item)
        object_key: bytes = key.encode()
        values: tuple = tuple(
            item.__dict__[key]
            for key in sorted(item.__annotations__.keys())
            if not key.startswith("_")
        )
        serialized_object: bytes = pickle.dumps(values)
        queue_size: int = self._get_frame_size(item=item)
        # Вызов запускает хранимый скрипт, который нуждается в
        #   - KEYS[1] (keys[0]) - ключ объекта Redis
        #   - ARGV[1] (args[0]) - данные для хранения
        #   - ARGV[2] (args[1]) - лимит размера списка
        self._queue_add(
            keys=[object_key],
            args=[serialized_object, queue_size],
            client=self._pipe,
        )
        # _pipe может быть использован в цикле для списка объектов, поэтому
        #   осознанно не выполняем (execute)

    def bulk_create(self, items: list[SubclassItemType]) -> OperationResult:
        return self.add(item_or_items=items)

    def clear(self, item: RedisItem) -> OperationResult:
        """ Удаление frame'а из БД """
        try:
            key: str = self._make_key(item=item)
            self._client.delete(key)
        except Exception as exception:
            logging.exception(exception)
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )
        return OperationResult(status=OperationStatus.success)

    def _values_to_items(
        self,
        values: list[tuple],
        item: SubclassItemType,
    ) -> list[SubclassItemType]:
        """ Десериализация и создание объектов типа item.__class__ со списком значений """
        T: type = item.__class__  # Класс десериализуемого объекта, нужен для вызова конструктора
        # Получение параметров класса (subsytem, tag_id и т.п.)
        params: dict[str, Any] = {key: item.__dict__[key] for key in item._keys_positions.keys()}
        # Сортировка имён для правильного расположения значений в атрибуты
        attr_names: list[str] = list(sorted(T.__annotations__.keys()))
        # Формирование словарей для инициализации объектов
        init_dicts: list[dict] = [dict(zip(attr_names, item_values)) for item_values in values]
        result: list[SubclassItemType] = [T(**(init_dict | params)) for init_dict in init_dicts]
        return result

    def _make_key(self, item: RedisItem) -> str:
        return f"{self.FRAME_PREFIX}{item._table}"

    def ltrim_by_item(self, item: RedisItem) -> None:
        """ Форматирование(обрезка) длины очереди в соответствии с frame_size Item'а """
        key: str = self._make_key(item=item)
        total_items_count: int = self._client.llen(key)
        queue_size: int = self._get_frame_size(item=item)
        if total_items_count > queue_size:
            self._client.ltrim(key, -queue_size, self.QUEUE_END_INDEX)

    def get(
        self,
        item: SubclassItemType,
        start_index: int = QUEUE_START_INDEX,
        end_index: int = QUEUE_END_INDEX,
    ) -> list[SubclassItemType]:
        """
        Получение данных из БД и приведение их к соответствующим типам объектов

        item: RedisItem - объект с подготовленными для поиска параметрами
        start_index: int - индекс начального элемента frame'а (отсчет начинается с 0,)
        end_index: int - индекс последнего элемента (включительно)

        * start_index - самый старый объект, end_index - самый свежий объект

        """
        key: str = self._make_key(item=item)
        serialized_values: list[bytes] = self._client.lrange(key, start_index, end_index)
        # Десериализация полученных данных
        all_values_list: list[tuple] = [pickle.loads(v) for v in serialized_values]
        # Формирование объектов из полученных данных
        items: list[SubclassItemType] = self._values_to_items(
            values=all_values_list,
            item=item,
        )
        return items
