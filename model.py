from __future__ import annotations
import redis


from redis_orm import RedisORM
from redis_orm import Field
from redis_orm import Item


class TestModel(Item):
    date_time: float = Field(float)
    subsystem_id: int = Field(int)

    class Meta:
        table = "subsystem.{subsystem_id}.tag.{tag_id}"


orm = RedisORM(redis_client=redis.Redis(host="localhost", port=8379))

test_model = TestModel(value=1, date_time=100, subsystem_id=10, tag_id=55)
test_model2 = TestModel(value=2, date_time=200, subsystem_id=20, tag_id=110)
# orm.bulk_create([test_model, test_model2])
orm.select(TestModel).where(TestModel.subsystem_id == 5)
# print(test_model.params)
