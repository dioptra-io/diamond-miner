"""
*D-Miner is the first Internet-scale system that captures a multipath view of the topology.
By combining and adapting state-of-the-art multipath detection and high speed randomized topology discovery techniques,
D-Miner permits discovery of the Internet’s multipath topology in 2.5 days when probing at 100kpps.* :cite:`DiamondMiner2020`

There are two implementations of Diamond-Miner:

- `diamond-miner-cpp <https://github.com/dioptra-io/diamond-miner-cpp>`_ +
  `diamond-miner-wrapper <https://github.com/dioptra-io/diamond-miner-wrapper>`_,
  the original implementation in C++ that have been used for the NSDI 2020 paper :cite:`DiamondMiner2020`.
  This implementation is not maintained anymore.
- This implementation, `diamond-miner <https://github.com/dioptra-io/diamond-miner>`_, a rewrite of the core algorithm in Python and ClickHouse SQL.
  This implementation is maintained and used in production. It supports IPv4 and IPv6.
"""
