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

# Создание единичной записи
example_item: ExampleItem = ExampleItem(subsystem_id=3, tag_id=15, date_time=100, any_value=17.)
result_of_operation: OperationResult = example_item.save()
print(result_of_operation)

# Получение записей
getted_items: list[ExampleItem] = ExampleItem.get(subsystem_id=3, tag_id=15)
print(f"{getted_items=}")
