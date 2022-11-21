from storage_orm import RedisItem
from storage_orm.redis_impl.redis_orm import RedisORM

a = "subsystem.{subsystem_id}.{tag_id}"
splits = a.split(".")


class MyItem(RedisItem):
    attr1: int

    class Meta:
        table = "subsystem.{subsystem_id}.{tag_id}"


redis_orm: RedisORM = RedisORM(host="localhost", port=8379)
# my_item: MyItem = MyItem(attr1=12, subsystem_id=55, tag_id=66)
# my_item.save()

my_item: list[MyItem] = MyItem.get(subsystem=55)
print(my_item)
