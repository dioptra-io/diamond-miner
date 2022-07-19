# Usage


This library contains three main components:

- Database queries that implements most of the algorithms in ClickHouse SQL.
- Flow mappers, to map between flow IDs and (address, port) offsets.
- Probe generators, to generate randomized probes on-the-fly.

The example below show how to use these components together with [`caracal`](https://github.com/dioptra-io/caracal)
in order to discover load-balanced paths.
Refer to the reference section of the documentation to learn more about the different functions available.

## How to run the examples

```bash
python -m venv venv
source venv/bin/activate
venv/bin/pip install diamond-miner pycaracal pych-client
docker run --rm -d -p 8123:8123 clickhouse/clickhouse-server:22.6
```

## Yarrp

[Yarrp](https://github.com/cmand/yarrp) is a high-speed single-path traceroute tool.
Since Diamond-Miner is a generalization of Yarrp, it is easy to re-implement Yarrp with this library.

```bash
python examples/yarrp.py
# ??? links discovered
```

```python title="examples/yarrp.py"
--8<-- "examples/yarrp.py"
```

## Diamond-Miner

```bash
python examples/diamond-miner.py
# 8 links discovered
```

```python title="examples/diamond-miner.py"
--8<-- "examples/diamond-miner.py"
```

## Scaling Diamond-Miner

For a more complex example, to handle measurements with billion of probes and results, see:

- [`iris/commons/clickhouse.py`](https://github.com/dioptra-io/iris/blob/main/iris/commons/clickhouse.py)
- [`iris/worker/inner_pipeline/diamond_miner.py`](https://github.com/dioptra-io/iris/blob/main/iris/worker/inner_pipeline/diamond_miner.py)

## Alternative probing tools

You can use other tools such as Scamper, as long as ...
Input format:
Output format:
