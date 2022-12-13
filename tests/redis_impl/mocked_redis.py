from __future__ import annotations
import redis

from typing import Callable


class MockedRedis(redis.Redis):
    calls_count: int
    set_calls_count: int
    execute_calls_count: int
    delete_calls_count: int
    _pipe: MockedRedis

    def __init__(self, is_pipe: bool = False) -> None:
        self.calls_count = 0
        self.set_calls_count = 0
        self.execute_calls_count = 0
        self.delete_calls_count = 0
        if not is_pipe:
            self._pipe = self.__class__(is_pipe=True)

    def mset(self, **_) -> None:
        self.calls_count += 1

    def set(self, *args, **_) -> None:
        self.set_calls_count += 1

    def execute(self, **_) -> None:
        self.execute_calls_count += 1

    def pipeline(self, **_) -> MockedRedis:
        return self._pipe

    def delete(self, *_) -> None:
        self.delete_calls_count += 1

    def __repr__(self) -> str:
        return self.__class__.__name__

    def register_script(self, *_) -> Callable:
        return lambda *_: _

    @classmethod
    def ping(cls) -> bool:
        return True
