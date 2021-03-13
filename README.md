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

## Dependencies

This software is released under the MIT license, in accordance with the license of its dependencies.

Name                                             | License                                    | Usage
-------------------------------------------------|--------------------------------------------|------
[aioch](https://github.com/mymarilyn/aioch)      | [MIT](https://opensource.org/licenses/MIT) | Interacting with ClickHouse
[pygfc](https://github.com/maxmouchet/gfc)       | [MIT](https://opensource.org/licenses/MIT) | Generating random permutations
