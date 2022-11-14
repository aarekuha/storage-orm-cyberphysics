from typing import Any
from dataclasses import dataclass

from .operator import Operator


@dataclass
class Expression:
    left: Any
    right: Any
    op: Operator

    def __repr__(self) -> str:
        return f"{self.left} {self.op} {self.right}"
