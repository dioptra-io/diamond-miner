# Development

This library is developed on GitHub in the [`dioptra-io/diamond-miner`](https://github.com/dioptra-io/diamond-miner) repository.

```bash
git clone git@github.com:dioptra-io/diamond-miner.git
cd diamond-miner/

# Create and enter a virtual environment
python -m venv venv
source venv/bin/activate

# Compile the Cython code and install the dependencies (once)
pip install -e .

# Install the pre-commit hooks (once)
pre-commit install

# Edit some files...
# Run pip install again if you have edited Cython files (`.pyx`)

# Run the tests
pytest

# Preview the documentation
mkdocs serve --watch diamond_miner --watch docs

# Commit...

# Tag a new version
bumpversion patch # or minor/major
```

## Test data

Most tests require a running instance of ClickHouse with pre-populated tables.
To start a ClickHouse server and insert the test data:
```bash
docker run --rm -d -p 8123:8123 clickhouse/clickhouse-server:22.6
python tests/data/insert.py
```

To use a different server, set the `DIAMOND_MINER_TEST_DATABASE_URL` environment variable (`http://localhost:8123` by default).
