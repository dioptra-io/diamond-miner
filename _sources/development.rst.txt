Development
===========

.. code-block:: bash

    git clone git@github.com:dioptra-io/diamond-miner.git
    cd diamond-miner/

    # Compile the Cython code and install the dependencies
    poetry install

    # Run the tests (assuming a ClickHouse server listening on localhost)
    poetry run tests/data/insert.py
    poetry run pytest
