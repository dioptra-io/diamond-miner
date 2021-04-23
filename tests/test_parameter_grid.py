import pytest

from diamond_miner.grid import ParameterGrid


def test_parameter_grid():
    grid = ParameterGrid(["a", "b"], range(3))
    assert grid.size == (2, 3)
    assert len(grid) == 6
    assert grid[1, 2] == ["b", 2]
    assert grid[5] == ["b", 2]
    with pytest.raises(IndexError):
        _ = grid[6]
    assert list(grid) == [["a", 0], ["b", 0], ["a", 1], ["b", 1], ["a", 2], ["b", 2]]
    assert list(grid.shuffled(seed=42)) == [
        ["a", 1],
        ["b", 1],
        ["b", 0],
        ["a", 0],
        ["b", 2],
        ["a", 2],
    ]
