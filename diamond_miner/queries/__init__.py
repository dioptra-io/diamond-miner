"""
Wrappers around ClickHouse SQL queries.

The queries operate on different kind of tables.
Refer to the following superclasses for more information:
[Query][diamond_miner.queries.Query],
[LinksQuery][diamond_miner.queries.LinksQuery],
[PrefixesQuery][diamond_miner.queries.PrefixesQuery],
[ProbesQuery][diamond_miner.queries.ProbesQuery],
[ResultsQuery][diamond_miner.queries.ResultsQuery].
"""
from .count import Count
from .count_rows import CountLinksPerPrefix, CountProbesPerPrefix, CountResultsPerPrefix
from .create_links_table import CreateLinksTable
from .create_prefixes_table import CreatePrefixesTable
from .create_probes_table import CreateProbesTable
from .create_results_table import CreateResultsTable
from .create_tables import CreateTables
from .drop_tables import DropTables
from .get_invalid_prefixes import (
    GetInvalidPrefixes,
    GetPrefixesWithAmplification,
    GetPrefixesWithLoops,
)
from .get_links import GetLinks
from .get_links_from_results import GetLinksFromResults
from .get_mda_probes import GetMDAProbes
from .get_nodes import GetNodes
from .get_prefixes import GetPrefixes
from .get_probes import GetProbes, GetProbesDiff
from .get_results import GetResults
from .get_sliding_prefixes import GetSlidingPrefixes
from .insert_links import InsertLinks
from .insert_mda_probes import InsertMDAProbes
from .insert_prefixes import InsertPrefixes
from .insert_results import InsertResults
from .query import (
    LinksQuery,
    PrefixesQuery,
    ProbesQuery,
    Query,
    ResultsQuery,
    StoragePolicy,
    links_table,
    prefixes_table,
    probes_table,
    results_table,
)

__all__ = (
    "Count",
    "CountLinksPerPrefix",
    "CountProbesPerPrefix",
    "CountResultsPerPrefix",
    "CreateLinksTable",
    "CreatePrefixesTable",
    "CreateProbesTable",
    "CreateResultsTable",
    "CreateTables",
    "DropTables",
    "GetLinks",
    "GetLinksFromResults",
    "GetMDAProbes",
    "GetNodes",
    "GetPrefixes",
    "GetProbes",
    "GetProbesDiff",
    "GetResults",
    "GetSlidingPrefixes",
    "GetInvalidPrefixes",
    "GetPrefixesWithAmplification",
    "GetPrefixesWithLoops",
    "InsertMDAProbes",
    "InsertLinks",
    "InsertPrefixes",
    "InsertResults",
    "Query",
    "LinksQuery",
    "PrefixesQuery",
    "ProbesQuery",
    "ResultsQuery",
    "StoragePolicy",
    "links_table",
    "prefixes_table",
    "probes_table",
    "results_table",
)
