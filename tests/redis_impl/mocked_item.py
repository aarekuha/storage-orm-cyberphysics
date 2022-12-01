from storage_orm import RedisItem


class MockedItem(RedisItem):
    calls_count: int
    delete_calls_count: int

    def __init__(self) -> None:
        self.calls_count = 0
        self.delete_calls_count = 0

    def save(self) -> None:
        self.calls_count += 1

    @property
    def mapping(self) -> dict:
        return dict()

    # def delete(self) -> None:
    #     self.delete_calls_count += 1