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

## Configuring QuestDB

Now that we've started the DB for the first time, we can tweak its default generated configuration.

Temporarily, stop the server:

```shell
cd questdb  # if you're not in the questdb dir already
./questdb-7.1.3-rt-linux-amd64/bin/questdb.sh stop -d data
```

Edit `questdb/data/conf/server.conf` and add the following settings:

```ini
http.connection.pool.initial.capacity=16
http.worker.count=16
```

Then start QuestDB again:

```shell
./questdb-7.1.3-rt-linux-amd64/bin/questdb.sh start -d data
```


## Insert 5 million rows of sample data

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

## Run the Rust binary

To see all options:

```shell
cargo run --release -- --help
```

To run the example:

```shell
cargo run --release -- --concurrency 5
```

```
    Finished release [optimized] target(s) in 0.08s
     Running `target/release/rust-par-http-example --concurrency 8`
shape: (5_000_000, 21)
┌───────────┬────────────────┬─────────────────┬──────┬───┬─────────────┬─────────────┬──────────────────┬─────────────────────┐
│ hostname  ┆ region         ┆ datacenter      ┆ rack ┆ … ┆ usage_steal ┆ usage_guest ┆ usage_guest_nice ┆ timestamp           │
│ ---       ┆ ---            ┆ ---             ┆ ---  ┆   ┆ ---         ┆ ---         ┆ ---              ┆ ---                 │
│ str       ┆ str            ┆ str             ┆ str  ┆   ┆ f64         ┆ f64         ┆ f64              ┆ datetime[ns]        │
╞═══════════╪════════════════╪═════════════════╪══════╪═══╪═════════════╪═════════════╪══════════════════╪═════════════════════╡
│ host_0    ┆ ap-southeast-2 ┆ ap-southeast-2b ┆ 96   ┆ … ┆ 0.726996    ┆ 0.0         ┆ 0.0              ┆ 2016-01-01 00:00:00 │
│ host_1    ┆ eu-west-1      ┆ eu-west-1b      ┆ 52   ┆ … ┆ 1.022753    ┆ 1.711183    ┆ 0.0              ┆ 2016-01-01 00:00:10 │
│ host_2    ┆ us-west-1      ┆ us-west-1b      ┆ 69   ┆ … ┆ 0.0         ┆ 0.472402    ┆ 0.312164         ┆ 2016-01-01 00:00:20 │
│ host_3    ┆ us-west-2      ┆ us-west-2c      ┆ 8    ┆ … ┆ 0.0         ┆ 0.0         ┆ 1.496152         ┆ 2016-01-01 00:00:30 │
│ …         ┆ …              ┆ …               ┆ …    ┆ … ┆ …           ┆ …           ┆ …                ┆ …                   │
│ host_3996 ┆ us-west-2      ┆ us-west-2a      ┆ 67   ┆ … ┆ 23.045458   ┆ 76.46893    ┆ 17.091646        ┆ 2017-08-01 16:52:40 │
│ host_3997 ┆ us-west-2      ┆ us-west-2b      ┆ 63   ┆ … ┆ 20.375169   ┆ 78.043473   ┆ 17.870002        ┆ 2017-08-01 16:52:50 │
│ host_3998 ┆ eu-west-1      ┆ eu-west-1b      ┆ 53   ┆ … ┆ 21.004499   ┆ 78.341154   ┆ 18.880808        ┆ 2017-08-01 16:53:00 │
│ host_3999 ┆ us-east-1      ┆ us-east-1c      ┆ 87   ┆ … ┆ 19.05504    ┆ 78.094993   ┆ 19.263652        ┆ 2017-08-01 16:53:10 │
└───────────┴────────────────┴─────────────────┴──────┴───┴─────────────┴─────────────┴──────────────────┴─────────────────────┘
elapsed: 1.861212519s
Row throughput: 2686420 rows/sec
Data throughput: 715 MiB/sec (of downloaded CSV data)
```

## Finally, stop QuestDB

```shell
cd questdb
./questdb-7.1.3-rt-linux-amd64/bin/questdb.sh stop -d data
```
