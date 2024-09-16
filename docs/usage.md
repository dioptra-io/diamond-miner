# Usage

The `diamond-miner` library contains three principal components:

- Database queries that implements most of the algorithms in ClickHouse SQL.
- Flow mappers, to map between flow IDs and (address, port) offsets.
- Probe generators, to generate randomized probes on-the-fly.

These components can be pieced together to conduct various kind of topology measurements.

## How to run the examples

To run the examples below, you need a running [ClickHouse](https://clickhouse.com) server:
```bash
docker run --rm -d -p 8123:8123 clickhouse/clickhouse-server:22.8
```

You also need [`pycaracal`](https://github.com/dioptra-io/caracal) and [`pych-client`](https://github.com/dioptra-io/pych-client).
We recommend that you install them in a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
pip install diamond-miner pycaracal pych-client
```

## Yarrp

[Yarrp](https://github.com/cmand/yarrp) is a high-speed single-path traceroute tool.
Since Diamond-Miner is a generalization of Yarrp, it is easy to re-implement Yarrp with this library.

```bash
python examples/yarrp.py
# 2 links discovered
```

```python title="examples/yarrp.py"
--8<-- "examples/yarrp.py"
```

## Diamond-Miner

Diamond-Miner needs to remember how many probes were sent to each TTL.
As such, instead of generating the probes on-the-fly as in the Yarrp example, we first store in the database the
number of probes to send at each round, and we then generate a _probes file_ containing one line per probe.
This file is given as an input to pycaracal.

```bash
python examples/diamond-miner.py
# 8 links discovered
```

```python title="examples/diamond-miner.py"
--8<-- "examples/diamond-miner.py"
```

## Scaling Diamond-Miner

You may find the previous example to run slowly for a large number of prefixes and/or results.

- To speed up `InsertResults`, you can first split the input file in multiple parts, and run this query in
parallel over each part.
- To speed up `InsertPrefixes` and `InsertLinks`, you can run these queries in parallel over subsets of the probing space.
For example:
- 
```python
from diamond_miner.queries import InsertPrefixes
from diamond_miner.subsets import subsets_for
from pych_client import ClickHouseClient

with ClickHouseClient() as client:
    query = InsertPrefixes()
    # First option: define subsets manually
    subsets = ["1.0.0.0/23", "1.0.2.0/23"]
    # Second option: compute subsets automatically with `subsets_for`
    subsets = subsets_for(query, client, measurement_id)
    query.execute_concurrent(client, measurement_id, subsets=subsets, concurrent_requests=8)
```

You can see such techniques implemented in [Iris](https://github.com/dioptra-io/iris) source code:

- [`iris/commons/clickhouse.py`](https://github.com/dioptra-io/iris/blob/main/iris/commons/clickhouse.py)
- [`iris/worker/inner_pipeline/diamond_miner.py`](https://github.com/dioptra-io/iris/blob/main/iris/worker/inner_pipeline/diamond_miner.py)

## Alternative probing tools

This library is designed to work with [`pycaracal`](https://github.com/dioptra-io/caracal) as the probing tool.
However, you can use the tool of your choice, such as [`scamper`](https://www.caida.org/catalog/software/scamper/)
as long as you can convert the results to the following format:
```csv
capture_timestamp,probe_protocol,probe_src_addr,probe_dst_addr,probe_src_port,probe_dst_port,probe_ttl,quoted_ttl,reply_src_addr,reply_protocol,reply_icmp_type,reply_icmp_code,reply_ttl,reply_size,reply_mpls_labels,rtt,round
1658244381,1,::ffff:132.227.78.108,::ffff:1.0.0.1,24000,0,3,1,::ffff:134.157.254.124,1,11,0,253,56,"[]",30,1
1658244381,1,::ffff:132.227.78.108,::ffff:1.0.0.5,24000,0,3,1,::ffff:134.157.254.124,1,11,0,253,56,"[]",57,1
...
```
