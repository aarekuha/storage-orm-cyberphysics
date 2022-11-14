from typing import Mapping
from typing import Union

# Переопределения типов для корректной работы линтеров
_Value = Union[bytes, float, int, str]
_Key = Union[str, bytes]


class Item(object):
    table: str
    params: Mapping[_Key, _Value]

    def __init__(self, **kwargs):
        # Формирование полей модели из переданных дочернему классу аргументов
        [self.__dict__.__setitem__(key, value) for key, value in kwargs.items()]
        # Формирование изолированной среды с данными класса для дальнейшей работы с БД
        self.table = self.__class__.Meta.table.format(**kwargs)
        self.params = {key: kwargs.get(key, None) for key in self.__class__.__annotations__}

    def __getattr__(self, attr_name: str):
        return object.__getattribute__(self, attr_name)

    @property
    def mapping(self) -> Mapping[_Key, _Value]:
        """ Формирование ключа для БД """
        return {".".join([self.table, str(key)]): value for key, value in self.params.items()}

    def __str__(self) -> str:
        return f"{self.table=}, {self.params=}"

    class Meta:
        table = ""  # Pattern имени записи, например, "subsystem.{subsystem_id}.tag.{tag_id}"
