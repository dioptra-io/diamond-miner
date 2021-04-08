# Diamond-Miner :gem:

[![Tests](https://github.com/dioptra-io/diamond-miner/actions/workflows/quality.yml/badge.svg)](https://github.com/dioptra-io/diamond-miner/actions/workflows/quality.yml)
[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/diamond-miner?logo=codecov&logoColor=white&token=RKZSQ2CL4J)](https://app.codecov.io/gh/dioptra-io/diamond-miner)

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

## NSDI 2020 paper

Diamond-Miner has been presented and published at [NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/vermeulen).
Since then, the code has been refactored and separated in the [diamond-miner](https://github.com/dioptra-io/diamond-miner) and [caracal](https://github.com/dioptra-io/caracal) repositories.
The code as it was at the time of the publication is available in the [`nsdi2020`](https://github.com/dioptra-io/diamond-miner/releases/tag/nsdi2020) tag.

## Authors

Diamond-Miner is developed and maintained by the [Dioptra team](https://dioptra.io) at Sorbonne Université in Paris, France.
The initial version has been written by [Kévin Vermeulen](https://github.com/kvermeul), with subsequents refactoring and improvements by [Maxime Mouchet](https://github.com/maxmouchet) and [Matthieu Gouel](https://github.com/matthieugouel).

## License & Dependencies

This software is released under the [MIT license](/LICENSE), in accordance with the license of its dependencies.

Name                                             | License                                    | Usage
-------------------------------------------------|--------------------------------------------|------
[clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver)      | [MIT](https://opensource.org/licenses/MIT) | Interacting with ClickHouse
[pygfc](https://github.com/maxmouchet/gfc)       | [MIT](https://opensource.org/licenses/MIT) | Generating random permutations
