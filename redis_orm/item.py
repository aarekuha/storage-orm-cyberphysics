from __future__ import annotations
import re
import redis
from typing import Mapping
from typing import Union
from typing import cast
from typing import Any

# Переопределения типов для корректной работы линтеров
_Value = Union[bytes, float, int, str]
_Key = Union[str, bytes]


class Item(object):
    table: str
    params: Mapping[_Key, _Value]
    _db_instance: redis.Redis | None = None

    def __init__(self, **kwargs):
        # Формирование полей модели из переданных дочернему классу аргументов
        [self.__dict__.__setitem__(key, value) for key, value in kwargs.items()]
        # Формирование изолированной среды с данными класса для дальнейшей работы с БД
        self.table = self.__class__.Meta.table.format(**kwargs)
        self.params = {key: kwargs.get(key, None) for key in self.__class__.__annotations__}

    def __getattr__(self, attr_name: str):
        return object.__getattribute__(self, attr_name)

    @classmethod
    def _set_global_instance(cls, db_instance: redis.Redis) -> None:
        """ Установка глобальной ссылки на БД во время первого подключения """
        cls._db_instance = db_instance

    @classmethod
    def get(cls, **kwargs):
        """ Запрос данных из БД """
        if not cls._db_instance:
            raise Exception("Redis database not connected...")
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
        filter_string: str = table.format(**kwargs)
        # Выборка данных из базы по подгтовленному паттерну
        _, keys = cls._db_instance.scan(match=filter_string + '.*')
        # Формирование результата
        return dict(zip(keys, cls._db_instance.mget(keys)))

    @property
    def mapping(self) -> Mapping[_Key, _Value]:
        """ Формирование ключей и значений для БД """
        return {".".join([self.table, str(key)]): value for key, value in self.params.items()}

    def __str__(self) -> str:
        return f"{self.table=}, {self.params=}"

    @classmethod
    def using(cls, redis_instance: redis.Redis) -> Item:
        """
            Создание копии класса для работы через
                "неглобальное" подключение к Redis
        """
        class CopiedClass(cls):
            _db_instance = redis_instance
        CopiedClass.__annotations__.update(cls.__annotations__)
        return cast(Item, CopiedClass)

    class Meta:
        table = ""  # Pattern имени записи, например, "subsystem.{subsystem_id}.tag.{tag_id}"
