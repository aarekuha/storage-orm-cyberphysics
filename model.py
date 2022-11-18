from __future__ import annotations
import redis

from storage_orm import StorageORM
from storage_orm import StorageItem


class TestModel(StorageItem):
    date_time: float
    any_value: int

    class Meta:
        table = "subsystem.{subsystem_id}.tag.{tag_id}"

redis_client = redis.Redis(host="localhost", port=8379)
orm = StorageORM(client=redis_client)

test_models: list[TestModel] = []
test_models.append(TestModel(any_value=1, date_time=100, subsystem_id=10, tag_id=56))
test_models.append(TestModel(any_value=2, date_time=100, subsystem_id=10, tag_id=57))
test_models.append(TestModel(any_value=3, date_time=100, subsystem_id=20, tag_id=57))
test_models.append(TestModel(any_value=4, date_time=100, subsystem_id=20, tag_id=58))
print(orm.bulk_create(test_models))

# test_model = TestModel(value=1, date_time=100, subsystem_id=10, tag_id=55)
# test_model2 = TestModel(value=2, date_time=200, subsystem_id=20, tag_id=110)
# print(orm.save(test_model))
# print(orm.bulk_create([test_model, test_model2]))

# print(f"{TestModel.using(redis_client).get(tag_id=110)}")

# print(f"{TestModel.get(subsystem_id=10)}")
# print(f"{TestModel.get(subsystem_id=20)}")
# print(f"{TestModel.get(tag_id=55)}")
ts: list[TestModel] = TestModel.get(tag_id=57)
for t in ts:
    print(f"{t._table}: {t.date_time=}, {t.any_value=}")

# st = {"date_time'": '100', "any_value'": b'1', 'tag_id': '56', 'subsystem_id': '10'}
# test_model = TestModel(**st)
# print(test_model)
# orm.save(test_model)

# print(f"{TestModel.get(tag_id=110)}")
