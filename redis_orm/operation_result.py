from enum import Enum


class OperationStatus(Enum):
    """ Статус операции записи/чтения из БД """
    success = True
    failed = False


class OperationResult:
    """ Результат записи/чтения из БД """
    status: OperationStatus
    message: str

    def __init__(self, status: OperationStatus | bool, message: str = "") -> None:
        self.status = OperationStatus(status)
        self.message = message

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: status={self.status}, message={self.message}"
