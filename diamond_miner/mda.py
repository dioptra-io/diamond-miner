from math import ceil, log

from diamond_miner.defaults import DEFAULT_FAILURE_RATE


def stopping_point(k: int, eps: float = DEFAULT_FAILURE_RATE) -> int:
    """
    Return the number `n_k` of probes that guarantees that the probability of not
    detecting `k` outgoing load-balanced edges is lower than `eps`[@veitch2009failure;@jacquet2018collecter].

    Examples:
        >>> stopping_point(1, 0.05)
        0
        >>> stopping_point(2, 0.05)
        6
        >>> stopping_point(3, 0.05)
        11
        >>> stopping_point(11, 0.05)
        57
        >>> stopping_point(101, 0.05)
        765

    Note:
        There is a typo in the D-Miner paper: n(101) = 765, not 757.
    """
    assert (k >= 1) and (0 <= eps <= 1)
    if k == 1:
        return 0
    return ceil(log(eps / k) / log((k - 1) / k))
