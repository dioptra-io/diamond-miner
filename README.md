# Diamond-Miner 💎

[![Tests](https://img.shields.io/github/workflow/status/dioptra-io/diamond-miner/Tests?logo=github)](https://github.com/dioptra-io/diamond-miner/actions/workflows/tests.yml)
[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/diamond-miner?logo=codecov&logoColor=white&token=RKZSQ2CL4J)](https://app.codecov.io/gh/dioptra-io/diamond-miner)
[![Documentation](https://img.shields.io/badge/documentation-online-blue.svg?logo=read-the-docs&logoColor=white)](https://dioptra-io.github.io/diamond-miner/)
[![PyPI](https://img.shields.io/pypi/v/diamond-miner?logo=pypi&logoColor=white)](https://pypi.org/project/diamond-miner/)

> D-Miner is the first Internet-scale system that captures a multipath view of the topology.
> By combining and adapting state-of-the-art multipath detection and high speed randomized topology discovery techniques,
> D-Miner permits discovery of the Internet’s multipath topology in 2.5 days¹ when probing at 100kpps. [[1]](#references)

¹ As of v0.1.0, diamond-miner can discover the multipath topology in less than a day when probing at 100k pps.

## 🚀 Quickstart

Diamond-Miner is implemented as a Python library which computes the probes to send.
To actually send the probes, we recommend [Caracal](https://github.com/dioptra-io/caracal) but you can use the tool of your liking, such as [yarrp](https://github.com/cmand/yarrp) or [scamper](https://www.caida.org/catalog/software/scamper/) as long as you convert its output to the expected format described in the documentation.

To get started, install Diamond-Miner and head over to the [documentation](https://dioptra-io.github.io/diamond-miner/):
```bash
pip install diamond-miner
```

## Publications

Diamond-Miner has been presented and published at [NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/vermeulen).
Since then, the code has been refactored and separated in the [`diamond-miner`](https://github.com/dioptra-io/diamond-miner) and [`caracal`](https://github.com/dioptra-io/caracal) repositories.
The code as it was at the time of the publication is available in the [`diamond-miner-cpp`](https://github.com/dioptra-io/diamond-miner-cpp) and [`diamond-miner-wrapper`](https://github.com/dioptra-io/diamond-miner-wrapper) repositories.

If you use Diamond-Miner, please cite the following paper:
```bibtex
@inproceedings {DiamondMiner2020,
  author = {Kevin Vermeulen and Justin P. Rohrer and Robert Beverly and Olivier Fourmaux and Timur Friedman},
  title = {Diamond-Miner: Comprehensive Discovery of the Internet{\textquoteright}s Topology Diamonds },
  booktitle = {17th {USENIX} Symposium on Networked Systems Design and Implementation ({NSDI} 20)},
  year = {2020},
  isbn = {978-1-939133-13-7},
  address = {Santa Clara, CA},
  pages = {479--493},
  url = {https://www.usenix.org/conference/nsdi20/presentation/vermeulen},
  publisher = {{USENIX} Association},
  month = feb,
}
```

## Authors

Diamond-Miner is developed and maintained by the [Dioptra group](https://dioptra.io) at Sorbonne Université in Paris, France.
The initial version has been written by [Kévin Vermeulen](https://github.com/kvermeul), with subsequents refactoring and improvements by [Maxime Mouchet](https://github.com/maxmouchet) and [Matthieu Gouel](https://github.com/matthieugouel).

## References

[1] Vermeulen, Kevin, et al. ["Diamond-Miner: Comprehensive Discovery of the Internet's Topology Diamonds."](https://www.usenix.org/system/files/nsdi20-paper-vermeulen.pdf) _17th USENIX Symposium on Networked Systems Design and Implementation (NSDI 20)_. 2020.

## License & Dependencies

This software is released under the [MIT license](/LICENSE), in accordance with the license of its dependencies.

Name                                             | License                                    | Usage
-------------------------------------------------|--------------------------------------------|------
[aioch](https://github.com/mymarilyn/aioch)      | [MIT](https://opensource.org/licenses/MIT) | Interacting with ClickHouse
[clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver)      | [MIT](https://opensource.org/licenses/MIT) | Interacting with ClickHouse
[pygfc](https://github.com/maxmouchet/gfc)       | [MIT](https://opensource.org/licenses/MIT) | Generating random permutations
[python-zstandard](https://github.com/indygreg/python-zstandard) | [3-clause BSD](https://opensource.org/licenses/BSD-3-Clause) | Compression
