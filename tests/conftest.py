import pytest
from typing import Union
from pytest_mock_resources import create_redis_fixture

from storage_orm import RedisItem
from storage_orm import RedisFrame


test_redis = create_redis_fixture()


@pytest.fixture
def test_item(test_input_dict: dict) -> RedisItem:
    """ Тестовый экземплар класса """
    class TestItem(RedisItem):
        """ Тестовый пример класса """
        attr1: str
        attr2: int
        attr3: float
        attr4: bytes

        class Meta:
            # Префикс записи в БД
            table = "param1.{param1}.param2.{param2}"
            ttl = None
            frame_size = 100

    return TestItem(**test_input_dict)


@pytest.fixture
def test_input_dict() -> dict[str, Union[str, bytes, float, int]]:
    """ Тестовый словарь """
    return {
        "param1": "param_value_1",
        "param2": "param_value_2",
        "attr1": "attr_value_1",  # str
        "attr2": 19,  # int
        "attr3": 99.9,  # float
        "attr4": b"attr_value_4",  # bytes
    }


@pytest.fixture
def test_frame(test_redis) -> RedisFrame:
    return RedisFrame(client=test_redis)
