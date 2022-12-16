import copy
import redis

from storage_orm import RedisItem
from storage_orm import RedisFrame


def test_add_one_item(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """ Проверка на добавление одного элемента """
    test_frame.add(item_or_items=test_item)
    key: str = test_frame._make_key(item=test_item)
    # Должен добавиться только один элемент
    assert test_redis.llen(key) == 1
    # Должен быть создан один ключ
    assert len(test_redis.keys()) == 1


def test_add_another_one_item(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """ Проверка на добавление ещё одного элемента """
    test_frame.add(item_or_items=test_item)
    key: str = test_frame._make_key(item=test_item)
    # Должен добавиться только один элемент
    assert test_redis.llen(key) == 1
    # Должен быть создан один ключ
    assert len(test_redis.keys()) == 1
    # Добавить еще один элемент к существующему ключу
    test_frame.add(item_or_items=test_item)
    # Должен добавиться еще один элемент
    assert test_redis.llen(key) == 2
    # Должен быть создан один ключ
    assert len(test_redis.keys()) == 1


def test_add_multiple_items(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """ Добавление нескольких элементов """
    COUNT_OF_ITEMS: int = 10
    test_frame.add(item_or_items=[test_item for _ in range(COUNT_OF_ITEMS)])
    key: str = test_frame._make_key(item=test_item)
    # Количество элементов во frame'е должно составлять COUNT_OF_ITEMS
    assert test_redis.llen(key) == COUNT_OF_ITEMS
    # Должен быть создан один ключ
    assert len(test_redis.keys()) == 1


def test_add_more_multiple_items(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """ Добавление двух пачек по COUNT_OF_ITEMS элементов к одному ключу """
    COUNT_OF_ITEMS: int = 10
    key: str = test_frame._make_key(item=test_item)
    test_frame.add(item_or_items=[test_item for _ in range(COUNT_OF_ITEMS)])
    # Количество элементов во frame'е должно составлять COUNT_OF_ITEMS
    assert test_redis.llen(key) == COUNT_OF_ITEMS
    # Должен быть создан один ключ
    assert len(test_redis.keys()) == 1
    # Добавить еще пачку элементов
    test_frame.add(item_or_items=[test_item for _ in range(COUNT_OF_ITEMS)])
    # Количество элементов во frame'е должно составлять COUNT_OF_ITEMS
    assert test_redis.llen(key) == COUNT_OF_ITEMS * 2
    # Должен быть создан один ключ
    assert len(test_redis.keys()) == 1


def test_get_frame_size(
    test_frame: RedisFrame,
    test_item: RedisItem,
) -> None:
    """ Получение максимального количества элементов в одном frame'е """
    expected_size: int = 33
    test_item._frame_size = 33
    result_size: int = test_frame._get_frame_size(item=test_item)
    assert result_size == expected_size
    # Когда размер не определен, должно устанавливаться значение по умолчанию
    del test_item._frame_size
    result_size = test_frame._get_frame_size(item=test_item)
    assert result_size == test_frame.DEFAULT_QUEUE_SIZE


def test_add_multiple_different_items(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """ Добавление нескольких разнородных элементов """
    test_item_1: RedisItem = copy.copy(test_item)
    test_item_2: RedisItem = copy.copy(test_item)
    test_item_2._table = test_item_2._table.replace("param1", "another_param1")
    test_frame.add(item_or_items=[test_item_1, test_item_2])
    key: str = test_frame._make_key(item=test_item_1)
    assert test_redis.llen(key) == 1
    key = test_frame._make_key(item=test_item_2)
    assert test_redis.llen(key) == 1
    # Должно быть создано два ключа
    assert len(test_redis.keys()) == 2


def test_clear(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """ Создание двух разных элементов, заполнение БД, очистка одного из элементов """
    test_item_1: RedisItem = copy.copy(test_item)
    test_item_2: RedisItem = copy.copy(test_item)
    test_item_2._table = test_item_2._table.replace("param1", "another_param1")
    COUNT_OF_ITEMS: int = 10
    key: str = test_frame._make_key(item=test_item)
    test_frame.add(item_or_items=[test_item_1 for _ in range(COUNT_OF_ITEMS)])
    test_frame.add(item_or_items=[test_item_2 for _ in range(COUNT_OF_ITEMS * 2)])
    assert test_redis.llen(key) == COUNT_OF_ITEMS
    key = test_frame._make_key(item=test_item_2)
    assert test_redis.llen(key) == COUNT_OF_ITEMS * 2
    # Должно быть создано два ключа
    assert len(test_redis.keys()) == 2
    # Очистить все записи первого элемента
    test_frame.clear(item=test_item_1)
    assert len(test_redis.keys()) == 1


def test_get_all(
    test_frame: RedisFrame,
    test_item: RedisItem,
) -> None:
    COUNT_OF_ITEMS: int = 13
    test_frame.add(item_or_items=[test_item for _ in range(COUNT_OF_ITEMS)])
    result: list[RedisItem] = test_frame.get(item=test_item)
    assert len(result) == COUNT_OF_ITEMS
    assert result[0] == test_item


def test_get_first_last(test_frame: RedisFrame) -> None:
    """ Проверка на выбор крайних элементов """
    COUNT_OF_ITEMS: int = 13
    SHARED_PARAMS: dict = {"param1": 1, "attr2": 0}

    class TestItem(RedisItem):
        attr1: int
        attr2: int

        class Meta:
            table = "param1.{param1}"
            frame_size = COUNT_OF_ITEMS + 2  # Дополнительно первый и последний

    # Общая характеристика элементво
    item: RedisItem = TestItem(**SHARED_PARAMS, attr1=0)
    # Элемент добавленный первым
    test_item_oldest: RedisItem = TestItem(**SHARED_PARAMS, attr1=-1)
    test_frame.add(item_or_items=test_item_oldest)
    test_item_middle: RedisItem = TestItem(**SHARED_PARAMS, attr1=50)
    test_frame.add(item_or_items=[test_item_middle for _ in range(COUNT_OF_ITEMS)])
    # Элемент добавленный последним
    test_item_newest: RedisItem = TestItem(**SHARED_PARAMS, attr1=999)
    test_frame.add(item_or_items=test_item_newest)
    # Получение самого старого объекта
    result: list[RedisItem] = test_frame.get(item=item, start_index=0, end_index=0)
    assert len(result) == 1
    assert result[0] == test_item_oldest
    # Получение самого свежего объекта
    result = test_frame.get(item=item, start_index=-1, end_index=-1)
    assert len(result) == 1
    assert result[-1] == test_item_newest
    # При это количество записей во frame'е должно соответствовать лимиту
    result = test_frame.get(item=item)
    assert len(result) == COUNT_OF_ITEMS + 2


def test_add_squeeze_out_oldest(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """
    Проверка на то, что самый старый элемент, при добавлении нового
        выдавливается из frame'а
    """
    COUNT_OF_ITEMS: int = RedisFrame.DEFAULT_QUEUE_SIZE + 999
    test_frame.add(item_or_items=[test_item for _ in range(COUNT_OF_ITEMS)])
    key = test_frame._make_key(item=test_item)
    current_frame_size: int = test_redis.llen(key)
    assert current_frame_size == RedisFrame.DEFAULT_QUEUE_SIZE


def test_item_set_frame_size(
    test_frame: RedisFrame,
    test_redis: redis.Redis,
) -> None:
    """ Подрезка frame'а при изменении frame_size 'на лету' """
    INIT_FRAME_SIZE: int = 100

    class TestItem(RedisItem):
        class Meta:
            table = "param1.{param1}"
            frame_size = INIT_FRAME_SIZE

    # Подготовка данных для образца
    test_item: RedisItem = TestItem(param1=1)
    test_frame.add([test_item for _ in range(INIT_FRAME_SIZE)])
    key: str = test_frame._make_key(item=test_item)
    db_frame_len: int = test_redis.llen(key)
    assert db_frame_len == INIT_FRAME_SIZE
    # Обрезка половины frame'а
    NEW_FRAME_SIZE: int = round(INIT_FRAME_SIZE / 2)
    test_item._db_instance = test_redis
    test_item.set_frame_size(NEW_FRAME_SIZE)

    assert test_item._frame_size == NEW_FRAME_SIZE
    db_frame_len = test_redis.llen(key)
    assert db_frame_len == NEW_FRAME_SIZE
