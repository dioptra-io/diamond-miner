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
    """
    assert (k >= 1) and (0 <= eps <= 1)
    if k == 1:
        return 0
    return ceil(log(eps / k) / log((k - 1) / k))
