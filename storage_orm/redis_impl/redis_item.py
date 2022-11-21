from __future__ import annotations
import re
import copy
import redis
from typing import Any
from typing import cast
from typing import Union
from typing import Mapping
from typing import Type
from typing import TypeVar

from ..storage_item import StorageItem
from ..operation_result import OperationResult
from ..operation_result import OperationStatus

# Redis: переопределения типов для корректной работы линтеров
_Value = Union[bytes, float, int, str]
_Key = Union[str, bytes]
ResponseT = Any

T = TypeVar('T', bound='RedisItem')


class RedisItem(StorageItem):
    _table: str
    _params: Mapping[_Key, _Value]
    _db_instance: redis.Redis | None = None

    class Meta:
        table = ""  # Pattern имени записи, например, "subsystem.{subsystem_id}.tag.{tag_id}"

    def __init__(self, **kwargs):
        # Формирование полей модели из переданных дочернему классу аргументов
        [self.__dict__.__setitem__(key, value) for key, value in kwargs.items()]
        # Формирование изолированной среды с данными класса для дальнейшей работы с БД
        self._table = self.__class__.Meta.table.format(**kwargs)
        self._params = {key: kwargs.get(key, None) for key in self.__class__.__annotations__}
        # Перегрузка методов для экземпляра класса
        self.using = self.instance_using  # type: ignore

    def __getattr__(self, attr_name: str):
        return object.__getattribute__(self, attr_name)

    @classmethod
    def _set_global_instance(cls: Type[T], db_instance: redis.Redis) -> None:
        """ Установка глобальной ссылки на БД во время первого подключения """
        cls._db_instance = db_instance

    @classmethod
    def get(cls: Type[T], **kwargs) -> list[T]:
        """
            Получение объектов по фильтру переданных аргументов, например:

                StorageItem.get(subsystem_id=10, tag_id=55)
        """
        if not cls._db_instance:
            raise Exception("Redis database not connected...")
        if not len(kwargs):
            raise Exception(f"{cls.__name__}.get() has empty filter. OOM possible.")
        # Выборка ключей из базы по подготовленному паттерну и формирование результата
        keys: list[bytes]
        cursor, keys = cls._db_instance.scan(match=cls._get_filter_by_kwargs(kwargs=kwargs))
        # Выбор ключей до истощения курсора (выборка производится постранично)
        while cursor:
            cursor, more_keys = cls._db_instance.scan(
                cursor=cursor,
                match=cls._get_filter_by_kwargs(kwargs=kwargs)
            )
            keys += more_keys
        values: list[bytes] = cast(list[bytes], cls._db_instance.mget(keys))
        result: list[T] = cls._objects_from_db_items(items=dict(zip(keys, values)))
        return result

    @classmethod
    def _objects_from_db_items(cls: Type[T], items: dict[bytes, bytes]) -> list[T]:
        """ Формирование cls(RedisItem)-объектов из данных базы """
        # Подготовка базовых данных для формирования объектов из ключей (уникальные ключи, без имён полей)
        tables: set[str] = {str(key).rsplit(".", 1)[0] for key in items.keys()}
        result_items: list[T] = []
        for table in tables:
            # Отбор полей с префиксом текущей table
            fields_src: list[bytes] = list(filter(lambda item: str(item).startswith(table), items))
            fields: dict[str, Any] = {}
            for field in fields_src:
                # Формирование атрибутов объекта из присутствующий полей
                key: str = field.decode().rsplit(".", 1)[1]
                # Приведение типа к соответствующему полю cls
                if cls.__annotations__[key] is str:
                    fields[key] = items[field].decode()
                else:
                    fields[key] = cls.__annotations__[key](items[field])

            # Формирование Meta из table класса и префикса полученных данных
            table_keys: list[str] = cls._get_keys_from_table(table=cls.Meta.table)
            table_values: list[str] = cls._get_keys_from_table(table=table)
            table_args: dict = dict(zip(table_keys, table_values))

            result_items.append(cls(**(fields | table_args)))

        return result_items

    @classmethod
    def _get_filter_by_kwargs(cls: Type[T], kwargs: dict) -> str:
        # Подготовка паттерна поиска
        table: str = cls.Meta.table
        # Шаблон для поиска аргументов, которе не были переданы
        patterns: list[str] = re.findall(r'\{[^\}]*\}', table)
        # Замена аргументов, которые не переданы на звездочку
        for pattern in patterns:
            clean_key: str = pattern.strip("{").strip("}")
            if not clean_key in kwargs:
                table = table.replace(pattern, "*")
        # Заполнение паттерна поиска
        filter_string: str = table.format(**kwargs) + ".*"
        return filter_string

    @staticmethod
    def _get_keys_from_table(table: str) -> list[str]:
        """
            Сбор данных на "ключевых" позициях table
            Например,
                table = "subsystem.{subsystem_id}.tag.{tag_id}"
                                         ^                ^
                table = "subsystem.10.tag.55"
                                    ^      ^
        """
        table_keys: list[str] = []
        for key in table.split(".")[::-2]:
            table_keys.append(key.strip("{").strip("}"))
        return table_keys

    @property
    def mapping(self) -> Mapping[_Key, _Value]:
        """ Формирование ключей и значений для БД """
        return {".".join([self._table, str(key)]): value for key, value in self._params.items()}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._table=}, {self._params=})"

    def __eq__(self, other: Type[T]) -> bool:
        if isinstance(other, self.__class__):
            return self._params == other._params and self._table == other._table

        return False

    def instance_using(self: T, db_instance: redis.Redis = None) -> T:
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
    def using(cls: Type[T], db_instance: redis.Redis = None) -> T:
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
        return cast(T, CopiedClass)

    def save(self) -> OperationResult:
        """ Одиночная вставка """
        if not self._db_instance:
            raise Exception("Redis database not connected...")
        try:
            self._db_instance.mset(mapping=self.mapping)
            return OperationResult(status=OperationStatus.success)
        except Exception as exception:
            self._on_error_actions(exception=exception)
            return OperationResult(
                status=OperationStatus.failed,
                message=str(exception),
            )
