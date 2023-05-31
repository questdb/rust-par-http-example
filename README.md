# rust-par-http-example

A Rust example time-slicing a query and fetching the results in parallel into a Pola.rs dataframe.

Uses the TSBS benchmark data as sample data.

At a high-level you need to:
1. To run the example, install Rust from https://rustup.rs/.
1. To generate sample data, install Python 3.10 and Poetry from https://python-poetry.org/docs/#installation.
1. Install QuestDB from https://questdb.io/docs/get-started/binaries/
1. Run QuestDB with `questdb start`
1. Insert sample data via the Python TSBS script.
1. Build and run the release binary with `cargo run --release -- <ARGS>`.

Details below.

## Starting the QuestDB server

These instructions are for Linux, other OSes would differ slightly.

```shell
mkdir -p questdb
mkdir -p questdb/data
cd questdb
wget https://github.com/questdb/questdb/releases/download/7.1.3/questdb-7.1.3-rt-linux-amd64.tar.gz
tar -xzf questdb-7.1.3-rt-linux-amd64.tar.gzar
./questdb-7.1.3-rt-linux-amd64/bin/questdb.sh start -d data  # Serve from the "data" dir 
```

Next, double-check that the server is running and serving requests over HTTP.

Visit: http://localhost:9000/

> :information_source: Once you're done, stop QuestDB by running `./questdb-7.1.3-rt-linux-amd64/bin/questdb.sh stop`.


## Insert sample data

Once you have a QuestDB instance running, you can insert sample data via the Python TSBS script.

This is included as a submodule in this repo.

First, set up the project so it picks up the right dependencies:

```shell
git submodule update --init
cd py-tsbs-benchmark
pyenv local 3.10  # assuming you have pyenv installed
poetry env use 3.10
poetry install
```

Then run the script to insert the test data:

```shell
poetry run bench_pandas --row-count 5000000 --workers 6 --send
```

You should see something like:

```
Running with params:
    {'debug': False,
     'host': 'localhost',
     'http_port': 9000,
     'ilp_port': 9009,
     'op': 'dataframe',
     'row_count': 5000000,
     'scale': 4000,
     'seed': 1352199176117871185,
     'send': True,
     'shell': False,
     'validation_query_timeout': 120.0,
     'worker_chunk_row_count': 10000,
     'workers': 6,
     'write_ilp': None}
Table cpu does not exist
Created table cpu
Serialized:
  5000000 rows in 1.66s: 3.00 mil rows/sec.
  ILP Buffer size: 2326.00 MiB: 1397.32 MiB/sec.
Sent:
  5000000 rows in 3.35s: 1.49 mil rows/sec.
  ILP Buffer size: 2326.00 MiB: 693.75 MiB/sec.
```

Run the rest of the commands from the git checkout root directory: `cd ..`.


## Compile the Rust binary

```shell
cargo build --release
```
