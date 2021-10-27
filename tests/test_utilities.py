from dataclasses import dataclass
from typing import Optional

from diamond_miner.utilities import common_parameters


def test_common_parameters():
    @dataclass
    class C1:
        a: int
        b: Optional[str]

    @dataclass
    class C2:
        b: Optional[str]
        c: float

    assert common_parameters(C1(a=1, b="Hello"), C2) == {"b": "Hello"}
    assert common_parameters(C1(a=1, b=None), C2) == {"b": None}
