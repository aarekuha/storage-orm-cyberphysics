from __future__ import annotations
import logging
import re
import copy
import redis
import itertools
from typing import Any
from typing import cast
from typing import Union
from typing import Mapping
from typing import Type
from typing import TypeVar
from typing import Callable

from ..storage_item import StorageItem
from ..operation_result import OperationResult
from ..operation_result import OperationStatus

from ..exceptions import MultipleGetParamsException
from ..exceptions import NotEnoughParamsException

# Redis: переопределения типов для корректной работы линтеров
_Value = Union[bytes, float, int, str]
_Key = Union[str, bytes]
ResponseT = Any

T = TypeVar('T', bound='RedisItem')
IN_SUFFIX = "__in"
KEYS_DELIMITER = "."


class RedisItem(StorageItem):
    _table: str
    _keys_positions: dict[str, int]
    _params: Mapping[_Key, _Value]
    _db_instance: Union[redis.Redis, None] = None
    _on_init_ltrim: Union[Callable[[RedisItem,], None], None] = None
    _frame_size: int = 0
    _ttl: Union[int, None] = None

    class Meta:
        table: str = ""  # Pattern имени записи, например, "subsystem.{subsystem_id}.tag.{tag_id}"
        ttl: Union[int, None] = None  # Время жизни объекта в базе данных
        frame_size: Union[int, None] = 100  # Максимальный размер frame'а

    def __init_subclass__(cls) -> None:
        cls._keys_positions = {
            index.replace("{", "").replace("}", ""): key
            for key, index in enumerate(cls.Meta.table.split(KEYS_DELIMITER))
            if index.startswith("{") and index.endswith("}")
        }

    @classmethod
    def _make_kwargs_from_objects(cls: Type[T], objects: list[T]) -> dict:
        """
            Конкатенация атрибутов объектов и их подготовка для
                использования в качестве фильтров
        """
        result_kwargs: dict = {}
        for obj in objects:
            for key, position in obj._keys_positions.items():
                value: str = obj._table.split(KEYS_DELIMITER)[position]
                if key in result_kwargs:
                    result_kwargs[key] += str(value)
                else:
                    result_kwargs[key] = str(value)

        for key in result_kwargs.keys():
            result_kwargs[key] = f"[{result_kwargs[key]}]"

        return result_kwargs

    def set_ttl(self, new_ttl: int) -> None:
        """ Установка настройки времени жизни объекта 'на лету' """
        setattr(self, "_ttl", new_ttl)

    def set_frame_size(self, new_frame_size: int = 0) -> None:
        """ Установка настройки максимального размера frame'а 'на лету' """
        meta_frame_size: int | None = self.Meta.frame_size if hasattr(self.Meta, "frame_size") else None
        old_frame_size: int = self._frame_size or meta_frame_size or 0
        # При одинаковых значениях ничего не делать
        if old_frame_size == new_frame_size:
            return
        # Если передан пустой аргумент, нужно установить значение по умолчанию
        if not new_frame_size and self.Meta.frame_size:
            new_frame_size = self.Meta.frame_size
        # Аргумент, который используется для дальнейшей проверки и работы
        setattr(self, "_frame_size", new_frame_size)
        # Проверка необходимости подрезки frame'а
        if old_frame_size > new_frame_size:
            # Подрезка фрейма в БД
            if self._on_init_ltrim:
                self._on_init_ltrim(self)

    def __init__(self, **kwargs) -> None:
        # Установка атрибутов из конструктора
        for config_key in ("ttl", "frame_size"):
            if config_key in kwargs.keys():
                setattr(self, f"_{config_key}", kwargs[config_key])
                del kwargs[config_key]
        # Формирование полей модели из переданных дочернему классу аргументов
        [self.__dict__.__setitem__(key, value) for key, value in kwargs.items()]  # type: ignore
        # Формирование изолированной среды с данными класса для дальнейшей работы с БД
        self._table = self.__class__.Meta.table.format(**kwargs)
        self._params = {
            key: kwargs.get(key, None)
            for key in self.__class__.__annotations__
        }
        # Перегрузка методов для экземпляра класса
        self.using = self.instance_using  # type: ignore
        if self._on_init_ltrim:
            self.set_frame_size()
            self._on_init_ltrim(self)

    def __getattr__(self, attr_name: str):
        return object.__getattribute__(self, attr_name)

    @classmethod
    def _set_global_instance(cls: Type[T], db_instance: redis.Redis) -> None:
        """ Установка глобальной ссылки на БД во время первого подключения """
        cls._db_instance = db_instance

    @classmethod
    def _get_keys_list(cls: Type[T], prefix: str) -> list[bytes]:
        """ Формирование ключей для поиска в БД на основе префикса и атрибутов класса"""
        return [f"{prefix}.{key}".encode() for key in cls.__annotations__.keys()]

    @staticmethod
    def _is_connected(db_instance: redis.Redis) -> bool:
        """ Проверка наличия подключения к серверу """
        try:
            db_instance.ping()  # type: ignore
        except redis.exceptions.ConnectionError:
            return False

        return True

    @classmethod
    def get(cls: Type[T], _item: Union[T, None] = None, **kwargs) -> Union[T, None]:
        """
            Получение одного объекта по выбранному фильтру

                StorageItem.get(subsystem_id=10, tag_id=55)
                StorageItem.get(_item=StorageItem(subsystem_id=10))
        """
        if not cls._db_instance or not cls._is_connected(db_instance=cls._db_instance):
            raise Exception("Redis database not connected...")
        if len(kwargs) and _item:
            raise Exception(f"{cls.__name__}.get() has _item and kwargs. It's not possible.")
        filter: str
        if _item:
            filter = _item._table
        else:
            # Разобрать готовым методом аргуметы в список фильтров
            filters_list: list[str] = cls._get_filters_by_kwargs(**kwargs)
            if len(filters_list) > 1:
                raise MultipleGetParamsException(
                    f"{cls.__name__} invalid (uses __in) params to get method..."
                )
            filter = filters_list[0]
            # Использование маски для выборки одного объекта не предусмотрено
            if not filter or "*" in filter:
                raise NotEnoughParamsException(
                    f"{cls.__name__} not enough params to get method..."
                )
        keys: list[bytes] = cls._get_keys_list(prefix=filter)
        values: list[bytes] = cast(list[bytes], cls._db_instance.mget(keys))
        if not [v for v in values if v]:
            return None
        finded_objects: list[T] = cls._objects_from_db_items(items=dict(zip(keys, values)))
        result: Union[T, None] = finded_objects[0]
        return result

    @classmethod
    def filter(cls: Type[T], _items: Union[list[T], None] = None, **kwargs) -> list[T]:
        """
            Получение объектов по фильтру переданных аргументов, например:

                StorageItem.filter(subsystem_id=10, tag_id=55)
                StorageItem.filter(_items=[StorageItem(subsystem_id=10), ...])
        """
        if not cls._db_instance or not cls._is_connected(db_instance=cls._db_instance):
            raise Exception("Redis database not connected...")
        if not len(kwargs) and not _items:
            raise Exception(f"{cls.__name__}.filter() has empty filter. OOM possible.")
        if len(kwargs) and _items:
            raise Exception(f"{cls.__name__}.filter() has _items and kwargs. It's not possible.")
        filters_list: list[str]
        keys: list[bytes] = []
        if _items:
            filters_list = [item._table for item in _items]
            for filter in filters_list:
                keys += cls._get_keys_list(prefix=filter)
        else:
            # Формирование списка фильтров для возможности поиска входящих в список
            filters_list = cls._get_filters_by_kwargs(**kwargs)
            for filter in filters_list:
                keys_list: list[bytes] = cls._get_keys_list(prefix=filter)
                if [key for key in keys_list if "*" in str(key)]:
                    # Если не передан один из параметров и нужен поиск по ключам
                    keys += cls._db_instance.keys(pattern=filter + ".*")
                else:
                    # Если все параметры присутствуют, то можно использовать только
                    #   имена атрибутов
                    keys += keys_list

        values: list[bytes] = cast(list[bytes], cls._db_instance.mget(keys))
        # Очистка пустых значений полученных данных
        if not [v for v in values if v]:
            return []

        result: list[T] = cls._objects_from_db_items(items=dict(zip(keys, values)))

        return result

    @classmethod
    def _objects_from_db_items(cls: Type[T], items: dict[bytes, bytes]) -> list[T]:
        """ Формирование cls(RedisItem)-объектов из данных базы """
        # Подготовка базовых данных для формирования объектов из ключей
        #   (уникальные ключи, без имён полей)
        tables: set[str] = {
            str(key).rsplit(KEYS_DELIMITER, 1)[0]
            for key in items.keys()
        }
        result_items: list[T] = []
        for table in tables:
            # Отбор полей с префиксом текущей table
            fields_src: list[bytes] = list(
                filter(lambda item: str(item).startswith(table), items)
            )
            fields: dict[str, Any] = {}
            for field in fields_src:
                # Формирование атрибутов объекта из присутствующих полей
                key: str = field.decode().rsplit(KEYS_DELIMITER, 1)[1]
                # Приведение типа к соответствующему полю cls
                if cls.__annotations__[key] is str:
                    fields[key] = items[field].decode()
                else:
                    try:
                        fields[key] = cls.__annotations__[key](items[field])
                    except TypeError:
                        logging.warning(
                            f"Type cast exception: {key=}, {field=}, "
                            f"{items[field]=}, {cls.__annotations__[key]=}"
                        )
                        fields[key] = None

            # Формирование Meta из table класса и префикса полученных данных
            table_args: dict = {}
            src_values: list[str] = table.split('.')
            for key, position in cls._keys_positions.items():
                table_args[key] = src_values[position]

            result_items.append(cls(**(fields | table_args)))

        return result_items

    @staticmethod
    def _get_list_of_prepared_kwargs(**kwargs: dict) -> list[dict]:
        """
            Подготовка списка фильтров из словарей:
                - исходный словарь разделить:
                    - базовый (без списков в значениях)
                    - расширенный (со списками в значениях)
                - получить множество комбинаций расширенного словаря
                - скомбинировать

            Examples:
                >>>
                kwargs = {"param1__in": [1, 2], "param2__in": [3, 4]}
                result = [
                    {"param1": 1, "param2": 3},
                    {"param1": 1, "param2": 4},
                    {"param1": 2, "param2": 3},
                    {"param1": 2, "param2": 4},
                ]
        """
        basic_kwargs: dict = {}
        extend_kwargs: dict = {}
        # Разделение на словари "с" и "без" списков в значениях
        for key, value in kwargs.items():
            if not key.endswith(IN_SUFFIX):
                basic_kwargs[key] = value
            else:
                extend_kwargs[key.strip(IN_SUFFIX)] = value
        # Формирование итоговых словарей
        result_kwargs: list[dict] = []
        if extend_kwargs:
            # Получить множество комбинаций расширенного словаря
            mixed_kwargs: list[dict] = list(
                dict(zip(extend_kwargs.keys(), values))
                for values in itertools.product(*extend_kwargs.values())
            )
            # Обогатить расширенные словари базовым
            result_kwargs = [mixed_item | basic_kwargs for mixed_item in mixed_kwargs]
        else:
            result_kwargs = [basic_kwargs]

        return result_kwargs

    @classmethod
    def _get_filters_by_kwargs(cls: Type[T], **kwargs: dict) -> list[str]:
        """ Подготовка списка паттернов поиска """
        table: str = cls.Meta.table
        # Шаблон для поиска аргументов, которе не были переданы
        patterns: list[str] = re.findall(r'\{[^\}]*\}', table)
        str_filters: list[str] = []
        # Получение сырого списка фильтров
        prepared_kwargs_list: list[dict] = cls._get_list_of_prepared_kwargs(**kwargs)
        # Замена аргументов, которые не переданы, на звездочку
        for prepared_kwargs in prepared_kwargs_list:
            for pattern in patterns:
                clean_key: str = pattern.strip("{").strip("}")
                if clean_key not in prepared_kwargs:
                    table = table.replace(pattern, "*")
            # Заполнение паттерна поиска
            str_filters.append(table.format(**prepared_kwargs))

        return str_filters

    @property
    def mapping(self) -> Mapping[_Key, _Value]:
        """ Формирование ключей и значений для БД """
        return {
            KEYS_DELIMITER.join([self._table, str(key)]): value
            for key, value in self._params.items()
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self._table=}, "
            f"{self._keys_positions=}, {self._params=})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self._params == other._params and self._table == other._table

        return False

    def instance_using(self: T, db_instance: Union[redis.Redis, None] = None) -> T:
        """
            Выполнение операций с БД путём direct-указания используемого
            подключения, например:

                another_client: redis.Redis = redis.Redis(host="8.8.8.8", db=12)
                storage_item_instance.using(db_instance=another_client).save()

            Создаётся копия объекта для работы через "неглобальное" подключение к Redis
        """
        copied_instance: T = copy.copy(self)
        copied_instance._db_instance = db_instance
        return copied_instance

    @classmethod
    def using(cls: Type[T], db_instance: Union[redis.Redis, None] = None) -> T:
        """
            Выполнение операций с БД путём direct-указания используемого
            подключения, например:

                another_client: redis.Redis = redis.Redis(host="8.8.8.8", db=12)
                StorageItem.using(db_instance=another_client).get(subsystem_id=10)

            Создаётся копия класса для работы через "неглобальное" подключение к Redis
        """
        class CopiedClass(cls):  # type: ignore
            _db_instance = db_instance
        CopiedClass.__annotations__.update(cls.__annotations__)
        CopiedClass.__name__ = cls.__name__
        return cast(T, CopiedClass)

    def save(self) -> OperationResult:
        """ Одиночная вставка """
        if not self._db_instance or not self._is_connected(db_instance=self._db_instance):
            raise Exception("Redis database not connected...")
        try:
            for key, value in self.mapping.items():
                expiration: Union[int, None] = self._ttl if hasattr(self, "_ttl") else None
                self._db_instance.set(name=key, value=value, ex=expiration)
            return OperationResult(status=OperationStatus.success)
        except Exception as exception:
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )

    def delete(self) -> OperationResult:
        """
            Удаление одного элемента
        """
        if not self._db_instance or not self._is_connected(db_instance=self._db_instance):
            raise Exception("Redis database not connected...")
        try:
            self._db_instance.delete(*[key for key in self.mapping.keys()])
            return OperationResult(status=OperationStatus.success)
        except Exception as exception:
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )
