import redis


class MockedRedis(redis.Redis):
    calls_count: int

    def __init__(self) -> None:
        self.calls_count = 0

    def mset(self, **_) -> None:
        self.calls_count += 1
