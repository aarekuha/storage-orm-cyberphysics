from typing import Union

from storage_orm import StorageORM
from storage_orm import RedisORM
from storage_orm import RedisItem
from storage_orm import OperationResult


class ExampleItem(RedisItem):
    # Атрибуты объекта с указанием типа данных (в процессе сбора данных из БД приводится тип)
    date_time: int
    any_value: float

    class Meta:
        # Системный префикс записи в Redis
        # Ключи указанные в префиксе обязательны для передачи в момент создания экземпляра
        table = "subsystem.{subsystem_id}.tag.{tag_id}"


# Во время первого подключения устанавливается глобальное подключение к Redis
orm: StorageORM = RedisORM(host="localhost", port=8379)

# Создание трёх записей с последовательным subsystem_id
items: list[ExampleItem] = []
for i in range(3):
    items.append(ExampleItem(subsystem_id=1+i, tag_id=15, date_time=100+i, any_value=17.+i))
result_of_operation: OperationResult = orm.bulk_create(items=items)
print(result_of_operation)

# Получение одной записи по фильтру
another_item: ExampleItem = ExampleItem(subsystem_id=1, tag_id=15)
item_by_object: Union[ExampleItem, None] = ExampleItem.get(_item=another_item)
print(f"{item_by_object=}")

# Получение всех записей по фильтру
another_items: list[ExampleItem] = [ExampleItem(subsystem_id=1, tag_id=15)]
item_by_objects: list[ExampleItem] = ExampleItem.filter(_items=another_items)
print(f"{item_by_objects=}")
