# Introduction

> D-Miner is the first Internet-scale system that captures a multipath view of the topology.
> By combining and adapting state-of-the-art multipath detection and high speed randomized topology discovery techniques,
> D-Miner permits discovery of the Internet’s multipath topology in 2.5 days[^1] when probing at 100kpps.[^2]

## Implementations

There are two implementations of Diamond-Miner:

- [`diamond-miner-cpp`](https://github.com/dioptra-io/diamond-miner-cpp) the original implementation in C++ that have been used for the NSDI 2020 paper[^2].
  This implementation is not maintained anymore.
- This implementation, [`diamond-miner`](https://github.com/dioptra-io/diamond-miner), a rewrite of the core algorithm in Python and ClickHouse SQL.
  This implementation is maintained and used in production. It supports IPv4 and IPv6.

## Installation

Diamond-Miner requires Python 3.10+.

```bash
pip install diamond-miner
```

## Publication

Diamond-Miner has been presented and published at [NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/vermeulen).
If you use Diamond-Miner, please cite the following paper:

```bibtex
--8<-- "CITATION.bib"
```

[^1]: As of v0.1.0, diamond-miner can discover the multipath topology in less than a day when probing at 100k pps.
[^2]: Vermeulen, Kevin, et al. ["Diamond-Miner: Comprehensive Discovery of the Internet's Topology Diamonds."](https://www.usenix.org/system/files/nsdi20-paper-vermeulen.pdf) _17th USENIX Symposium on Networked Systems Design and Implementation (NSDI 20)_. 2020.
