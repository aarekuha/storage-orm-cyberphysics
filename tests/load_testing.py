from time import monotonic

from storage_orm import RedisORM
from storage_orm import RedisItem
from storage_orm import StorageORM

COUNT: int = 1000


class TestItem(RedisItem):
    attr1: int
    attr2: str

    class Meta:
        table = "param1.{param1}.param2.{param2}"


start_time: float = monotonic()
redis_orm: StorageORM = RedisORM(host="localhost", port=8379, db=1)
redis_orm.bulk_create([TestItem(attr1=i, attr2=str(i), param1=i%5, param2=i%3) for i in range(COUNT)])
total_time: float = monotonic() - start_time
print(f"StorageORM -> Objects count: {COUNT}, total time: {total_time}")
