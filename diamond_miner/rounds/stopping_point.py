from math import ceil, log


def stopping_point(k, eps=0.05):
    """
    Return the number `n_k` of probes that guarantees that the probability of not
    detecting `k` outgoing load-balanced edges is lower than `eps`.
    Recurrent formula in [1], improved direct formula in [2].
    [1] "Failure Control in Multipath Route Tracing"
    https://www.researchgate.net/publication/224500381_Failure_Control_in_Multipath_Route_Tracing
    [2] "Collecter un nombre inconnu de coupons"
    https://hal.inria.fr/hal-01787252/document
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

    NOTE: There is a typo in the D-Miner paper: n(101) = 765, not 757.
    """
    assert (k >= 1) and (0 <= eps <= 1)
    if k == 1:
        return 0
    return ceil(log(eps / k) / log((k - 1) / k))
