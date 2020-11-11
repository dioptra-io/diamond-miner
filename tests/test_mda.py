from reader.mda import stopping_point


def test_stopping_point():
    # Note that k is shifted by 1 compared to the D-Miner paper,
    # in order to match the notations in the
    # "Collecter un nombre inconnu de coupons" paper.
    assert stopping_point(1, 0.05) == 0
    assert stopping_point(2, 0.05) == 6
    assert stopping_point(3, 0.05) == 11
    assert stopping_point(11, 0.05) == 57
    # There is a typo in the D-Miner paper:
    # n(101) = 765, not 757.
    assert stopping_point(101, 0.05) == 765
