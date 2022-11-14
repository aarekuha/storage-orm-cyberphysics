from enum import Enum


class Operator(Enum):
    EQ = "equals"  # Сравнение на соответствие
    IN = "in"  # Присутствует в списке
    AND = "&"  # Наличие обоих Выражений
    OR = "|"  # Наличие одного из выражений

    def __str__(self):
        return str(self.name)
