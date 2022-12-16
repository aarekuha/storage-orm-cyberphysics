import random

from storage_orm import StorageORM
from storage_orm import RedisORM
from storage_orm import RedisItem
from storage_orm import OperationResult


class ExampleItem(RedisItem):
    # Атрибуты объекта с указанием типа данных (в процессе сбора данных из БД приводится тип)
    date_time: int
    any_value: str

    class Meta:
        # Системный префикс записи в Redis
        # Ключи указанные в префиксе обязательны для передачи в момент создания экземпляра
        table = "subsystem.{subsystem_id}.tag.{tag_id}"


# Во время первого подключения устанавливается глобальное подключение к Redis
orm: StorageORM = RedisORM(host="localhost", port=8379)

# Создание единичной записи
example_item: ExampleItem = ExampleItem(subsystem_id=3, tag_id=15, date_time=100, any_value=17.)
result_of_operation: OperationResult = example_item.save()
print(f"After save: {ExampleItem.get(subsystem_id=3, tag_id=15)=}")
# Удаление единичной записи
result_of_operation = example_item.delete()
print(f"After delete: {ExampleItem.get(subsystem_id=3, tag_id=15)=}")

# Создание нескольких записей
# Подготовка данных
example_items: list[ExampleItem] = []
for i in range(10):
    subsystem_id: int = i % 10
    example_item = ExampleItem(
        subsystem_id=subsystem_id,
        another_key_value=i,
        tag_id=10 + (15 * random.randint(0, 1)),
        date_time=i*100,
        any_value=random.random() * 10,
    )
    example_items.append(example_item)
result_of_operation = orm.bulk_create(items=example_items)
print("After save:")
for item in ExampleItem.filter(_items=example_items):
    print(f"{item=}")
# Удаление нескольких записей
result_of_operation = orm.bulk_delete(items=example_items)
print(f"After delete: {ExampleItem.filter(_items=example_items)=}")
