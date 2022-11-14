from typing import Any
from typing import Union
from dataclasses import dataclass

from .expression import Expression
from .operator import Operator


@dataclass
class FieldBase:
    """ Базовая модель для атрибутов класса, при работе с БД """
    _type: type
    value: Union[int, float] = 0

    def __init__(self, T: type) -> None:
        """ T: тип хранимых данных """
        self._type = T

    def __eq__(self, other: Any) -> Expression:
        """ Формирование запроса для сравнений """
        return Expression(
            left=self,
            right=other,
            op=Operator.EQ,
        )


def Field(*args, **kwargs) -> Any:
    """ Factory для корректной работы линтеров """
    return FieldBase(*args, **kwargs)
