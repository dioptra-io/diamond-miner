from dataclasses import dataclass

from diamond_miner.utilities import common_parameters


def test_common_parameters():
    @dataclass
    class C1:
        a: int
        b: str | None

    @dataclass
    class C2:
        b: str | None
        c: float

    assert common_parameters(C1(a=1, b="Hello"), C2) == {"b": "Hello"}
    assert common_parameters(C1(a=1, b=None), C2) == {"b": None}
