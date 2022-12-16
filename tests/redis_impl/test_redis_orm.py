import pytest
import copy
import redis
from typing import Union

from storage_orm import RedisORM
from storage_orm import RedisItem


def test_empty_constructor() -> None:
    """ Отсутствие аргументов для подключения """
    with pytest.raises(Exception) as exception:
        RedisORM()

    assert "must contains" in str(exception.value)


def test_save_item(
    test_redis: redis.Redis,
    test_item: RedisItem,
) -> None:
    """ Проверка сохранения данных """
    RedisORM(client=test_redis).save(item=test_item)
    for key, value in test_item.mapping.items():
        db_item: Union[bytes, None] = test_redis.get(key)
        # Подготовка проверяемого значение
        expected_value: Union[bytes, None] = None
        if isinstance(value, str):
            expected_value = value.encode()
        elif isinstance(value, bytes):
            expected_value = value
        else:
            expected_value = str(value).encode()
        assert db_item == expected_value


def test_delete(test_redis: redis.Redis, test_item: RedisItem) -> None:
    """ Проверка вызова метода delete для одного элемента """
    RedisORM(client=test_redis).save(item=test_item)
    count_of_item_fields: int = len(test_item._params)
    count_of_db_items: int = len(test_redis.keys())
    assert count_of_item_fields == count_of_db_items


def test_bulk_create_rewrite_one_item(test_redis: redis.Redis, test_item: RedisItem) -> None:
    """
    Вызов метода группового сохранения должен создать по одной записи для каждого атрибута
    В тесте используется с разными атрибутами (значениями) один и тот же элемент
    Для него должна быть создана только одна группа записей в БД
    """
    items_count: int = 11
    items: list[RedisItem] = []
    for i in range(items_count):
        another_item: RedisItem = copy.copy(test_item)
        # Изменить значение атрибутов - запись в БД должна получиться та же
        another_item._params = {key: i for key in another_item._params.keys()}
        items.append(another_item)
    RedisORM(client=test_redis).bulk_create(items=items)
    count_of_db_items: int = len(test_redis.keys())
    count_of_item_fields: int = len(test_item._params)
    assert count_of_db_items == count_of_item_fields


def test_bulk_create_different_items(test_redis: redis.Redis, test_item: RedisItem) -> None:
    """ Вызов метода группового сохранения должен создать определенное количество записей """
    items_count: int = 11
    items: list[RedisItem] = []
    for i in range(items_count):
        another_item: RedisItem = copy.copy(test_item)
        # Дополнить ключи различными значениями счетчика,
        #   чтобы получились разные записи
        another_item._table += str(i)
        items.append(another_item)
    RedisORM(client=test_redis).bulk_create(items=items)
    count_of_db_items: int = len(test_redis.keys())
    count_of_item_fields: int = len(test_item._params)
    total_keys_expected: int = count_of_item_fields * items_count
    assert count_of_db_items == total_keys_expected


def test_bulk_delete(test_redis: redis.Redis, test_item: RedisItem) -> None:
    """ Вызов метода группового удаления должен удалить записи переданных объектов """
    items_count: int = 11
    # Создать элементы для проверки
    items: list[RedisItem] = []
    for i in range(items_count):
        another_item: RedisItem = copy.copy(test_item)
        # Дополнить ключи различными значениями счетчика,
        #   чтобы получились разные записи
        another_item._table += str(i)
        items.append(another_item)
    RedisORM(client=test_redis).bulk_create(items=items)
    count_of_db_items: int = len(test_redis.keys())
    count_of_item_fields: int = len(test_item._params)
    total_keys_expected: int = count_of_item_fields * items_count
    assert count_of_db_items == total_keys_expected
    # Удаление почти всех объектов (один оставить)
    RedisORM(client=test_redis).bulk_delete(items=items[:-1])
    count_of_db_items = len(test_redis.keys())
    count_of_item_fields = len(test_item._params)
    # Должны остаться значения только одного объекта
    total_keys_expected = count_of_item_fields
    assert count_of_db_items == total_keys_expected


def test_init_global_db_connection(test_redis: redis.Redis) -> None:
    """
    При первом подключении должна устанавливаться глобальная
        ссылка на него
    """
    # Удалить установленное подключение
    RedisItem._db_instance = None
    # Создать новое и проверить, что именно оно проинициализировалось
    RedisORM(client=test_redis)
    assert id(RedisItem._db_instance) == id(test_redis)


def test_noreinit_global_db_connection(test_redis: redis.Redis) -> None:
    """
    При первом подключении должна устанавливаться глобальная
        ссылка на него, которая не должна заменяться, при последующих
        подключениях
    """
    # Установить стороннее подключение в случае отсутствия
    if RedisItem._db_instance is None:
        RedisORM(client=redis.Redis())
    # Создать новое и проверить, что сохранилось первое подключение
    RedisORM(client=test_redis)
    assert id(RedisItem._db_instance) != id(test_redis)
