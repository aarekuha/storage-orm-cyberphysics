import pytest
import redis
from pytest import MonkeyPatch

from storage_orm import RedisItem
from storage_orm import MultipleGetParamsException
from storage_orm import NotEnoughParamsException


def _get_prefix(src_dict: dict) -> str:
    """ Искусственное формирование префикса из данных словаря """
    expected_prefix: str = ".".join([
        f"{key}.{value}"
        for key, value in src_dict.items()
        if key.startswith("param")
    ])
    return expected_prefix


def test_new_item_redis_model(test_item: RedisItem, test_input_dict: dict[str, str]) -> None:
    """ Переданные параметры должны быть погружены в экземпляр класса """
    for key in test_input_dict.keys():
        assert test_item.__dict__[key] == test_input_dict[key]


def test_filter_not_instance(test_item: RedisItem, test_input_dict: dict[str, str]) -> None:
    """ Осмысленное исключение, при отсутствии инициированного подключения к БД """
    with pytest.raises(Exception) as exception:
        any_dict_key: str = next(iter(test_input_dict))
        test_item.filter(_items=None, **{any_dict_key: test_input_dict[any_dict_key]})

    assert "not connected" in str(exception.value)


def test_filter_oom_exclude(test_item: RedisItem, monkeypatch: MonkeyPatch) -> None:
    """ Для исключения ООМ должна быть проверка на запрос данных без фильтра """
    with monkeypatch.context() as patch:
        # Установить фейковое подключение, чтобы пройти проверку на его отсутствие
        patch.setattr(RedisItem, "_db_instance", redis.Redis)
        # Мок на пинг установленного подключения
        patch.setattr(RedisItem._db_instance, "ping", lambda: True)
        with pytest.raises(Exception) as exception:
            test_item.filter()

    assert "empty filter" in str(exception.value)


def test_get_filters_by_kwargs_all(
    test_item: RedisItem,
    test_input_dict: dict,
) -> None:
    """ Проверка формирования фильтра для таблицы """
    expected_prefix: str = _get_prefix(src_dict=test_input_dict)
    expected_filters_list: list[str] = [expected_prefix]
    assert test_item._get_filters_by_kwargs(**test_input_dict) == expected_filters_list


@pytest.mark.parametrize("param_key", ["param1", "param2"])
def test_get_filters_by_kwargs_one_of_two(
    test_item: RedisItem,
    test_input_dict: dict,
    param_key: str,  # Исключаемый параметр
) -> None:
    """
        Проверка формирования фильтра для таблицы c одним параметром
        Параметры поочередно удаляются и проверяется формирование с одинм параметром
        То есть, при наличии значения для param1 должен сформироваться префикс для
            фильтра следующего вида:
                param1.param_value_1.param2.*
            а при наличии только значения для param2:
                param1.*.param2.param_value_2
    """
    modified_dict: dict = test_input_dict.copy()
    # В строке фильтра текущий параметр (param*) должен приобрести значение *
    modified_dict[param_key] = "*"
    expected_prefix: str = _get_prefix(src_dict=modified_dict)
    expected_filters_list: list[str] = [expected_prefix]
    # Удалить из передаваемых аргументов текущий параметр
    del modified_dict[param_key]

    assert test_item._get_filters_by_kwargs(**modified_dict) == expected_filters_list


def test_mapping(test_item: RedisItem, test_input_dict: dict) -> None:
    """ Проверка формирования карты значений атрибутов """
    # Подготовка префикса атрибура
    expected_prefix: str = _get_prefix(src_dict=test_input_dict)
    # Подготовка атрибутов с префиксом для проверки
    expected_dict: dict = {
        f"{expected_prefix}.{key}": value
        for key, value in test_input_dict.items()
        if not key.startswith("param")
    }
    assert test_item.mapping == expected_dict


def test_using_when_empty(test_item: RedisItem) -> None:
    """ Проверка подстановки используемого клиента БД, когда он не определен """
    # Проверка отсутствия установленного клиента
    assert test_item._db_instance is None
    # Использование клиента 1
    tmp_redis_1: redis.Redis = redis.Redis()
    assert test_item.using(db_instance=tmp_redis_1)._db_instance == tmp_redis_1
    # Проверка, что инстанс БД не был заменён
    assert test_item._db_instance is None


def test_using_when_defined(test_item: RedisItem, monkeypatch: MonkeyPatch) -> None:
    """ Проверка подстановки используемого клиента БД, когда он ранее определен """
    with monkeypatch.context() as patch:
        # Назначение глобального инстанса и его проверка
        tmp_redis_1: redis.Redis = redis.Redis()
        patch.setattr(test_item, "_db_instance", tmp_redis_1)
        assert test_item._db_instance == tmp_redis_1
        # Использование клиента 2 на ранее установленном клиенте
        tmp_redis_2: redis.Redis = redis.Redis()
        assert test_item.using(db_instance=tmp_redis_2)._db_instance == tmp_redis_2
        # Проверка, что инстанс БД не был заменён
        assert test_item._db_instance == tmp_redis_1


def test_save_when_instance_not_defined(test_item: RedisItem) -> None:
    """
        Сохранение объекта в БД.
        Определение исключения при попытке сохранения до установки подключения
    """
    with pytest.raises(Exception) as exception:
        test_item.save()

    assert "not connected" in str(exception.value)


def test_save(test_item: RedisItem, test_redis: redis.Redis) -> None:
    """ Сохранение объекта в БД """
    expected_keys_count: int = len(test_item.mapping.keys())
    test_item.using(db_instance=test_redis).save()
    db_keys_count: int = len(test_redis.keys())
    assert db_keys_count == expected_keys_count


def test_objects_from_db_items(
    test_input_dict: dict[str, str],
    test_item: RedisItem,
) -> None:
    """ Формирование объектов из полученных данных """
    expected_item: RedisItem = test_item.__class__(**test_input_dict)
    expected_prefix: str = _get_prefix(src_dict=test_input_dict)
    test_data: dict[bytes, bytes] = {
        f"{expected_prefix}.{key}".encode(): value if isinstance(value, bytes) else str(value).encode()
        for key, value in test_input_dict.items()
        if key.startswith("attr")
    }
    assert test_item._objects_from_db_items(items=test_data) == [expected_item,]


def test_get_multiple_params_exception(monkeypatch: MonkeyPatch) -> None:
    """ Выброс исключения, во время использования метода get(), когда не найдено ни одной записи """
    with monkeypatch.context() as patch, pytest.raises(MultipleGetParamsException):
        patch.setattr(RedisItem, "_db_instance", redis.Redis)
        # Мок на пинг установленного подключения
        patch.setattr(RedisItem._db_instance, "ping", lambda: True)
        RedisItem.get(subsystem_id__in=[1, 2])


def test_get_not_enough_params(monkeypatch: MonkeyPatch) -> None:
    """ Выброс исключения, во время использования метода get(), когда найдено несколько записей """
    with monkeypatch.context() as patch, pytest.raises(NotEnoughParamsException):
        patch.setattr(RedisItem, "_db_instance", redis.Redis)
        # Мок на пинг установленного подключения
        patch.setattr(RedisItem._db_instance, "ping", lambda: True)
        RedisItem.get()


@pytest.mark.parametrize(
    "input_kwargs, expected_kwargs", [
        ({"param1": "1", "param2": "2"}, [{"param1": "1", "param2": "2"}]),
        (
            {"param1__in": [1, 2], "param2": "2"},
            [
                {"param1": 1, "param2": "2"},
                {"param1": 2, "param2": "2"},
            ],
        ),
        (
            {"param1__in": [1, 2], "param2__in": [3, 4]},
            [
                {"param1": 1, "param2": 3},
                {"param1": 1, "param2": 4},
                {"param1": 2, "param2": 3},
                {"param1": 2, "param2": 4},
            ],
        ),
        (
            {"param1": "6", "param2__in": [3, 4]},
            [
                {"param1": "6", "param2": 3},
                {"param1": "6", "param2": 4},
            ],
        ),
    ],
)
def test_get_list_of_prepared_kwargs(input_kwargs: dict, expected_kwargs: dict) -> None:
    """ Формирование элементов для использования в паттерне поиска """
    assert RedisItem._get_list_of_prepared_kwargs(**input_kwargs) == expected_kwargs


@pytest.mark.parametrize(
    "prefix, expected_keys_list", [
        (
            "param1.*.param2.*",
            [
                b"param1.*.param2.*.attr1",
                b"param1.*.param2.*.attr2",
                b"param1.*.param2.*.attr3",
                b"param1.*.param2.*.attr4",
            ],
        ),
        (
            "param1.12.param2.99",
            [
                b"param1.12.param2.99.attr1",
                b"param1.12.param2.99.attr2",
                b"param1.12.param2.99.attr3",
                b"param1.12.param2.99.attr4",
            ],
        ),
    ],
)
def test_get_keys_list(test_item: RedisItem, prefix: str, expected_keys_list: list[bytes]) -> None:
    """ Формирование ключей для поиска в БД на основе префикса и атрибутов класса"""
    assert test_item._get_keys_list(prefix=prefix) == expected_keys_list


def test_delete(test_item: RedisItem, test_redis: redis.Redis) -> None:
    """ Проверка вызова метода delete один раз для удаления элемента """
    # Создать один элемент для проверки
    expected_keys_count: int = len(test_item.mapping.keys())
    test_item.using(db_instance=test_redis).save()
    db_keys_count: int = len(test_redis.keys())
    assert db_keys_count == expected_keys_count
    # Удалить элемент и проверить, что база опустела
    test_item.using(db_instance=test_redis).delete()
    db_keys_count = len(test_redis.keys())
    assert db_keys_count == 0


def test_set_ttl(test_item: RedisItem) -> None:
    """ Проверка изменения ttl соответствующим вызовом метода """
    prev_ttl: int = test_item._ttl or 1
    new_ttl: int = prev_ttl * 2
    test_item.set_ttl(new_ttl)
    assert test_item._ttl == new_ttl


def test_set_frame_size(test_item: RedisItem) -> None:
    """ Проверка изменения frame_size соответствующим вызовом метода """
    prev_frame_size: int = test_item._frame_size or 1
    new_frame_size: int = prev_frame_size * 2
    test_item.set_frame_size(new_frame_size)
    assert test_item._frame_size == new_frame_size
