#[cfg(feature = "dataframe")]
use std::{fs::File, io, path::Path};

use clap::Parser;
// The dataframe feature is enabled by default: It decodes the CSV response from the server
// and constructs a Pola.rs DataFrame.
// This is to provide a representative example of what performance can be obtained, including the
// cost of decoding the CSV response.
// To benchmark without this feature, see the README.md's "Synthetic HTTP benchmark" section.
#[cfg(feature = "dataframe")]
use csv_async::{AsyncReaderBuilder, StringRecord};
#[cfg(feature = "dataframe")]
use futures::stream::TryStreamExt;
#[cfg(feature = "dataframe")]
use polars::{
    export::chrono::NaiveDateTime,
    frame::DataFrame, // in-memory table
    prelude::{
        ChunkedBuilder, concat, Float64Type, Int64Type, IntoLazy, PrimitiveChunkedBuilder,
        TimeUnit, Utf8ChunkedBuilder,
    },
    series::Series, // column
};
#[cfg(feature = "dataframe")]
use polars_io::parquet::ParquetWriter;
use reqwest::Url;
use tokio;

// async CSV reader

// extension trait for working with streams of async values

/// Query the TSBS dataset into memory from concurrent connections.
#[derive(Parser,Clone)]
struct Args {
    /// QuestDB Host
    #[clap(long, default_value = "localhost")]
    host: String,

    /// QuestDB HTTP port
    #[clap(long, default_value = "443")]
    port: u16,

    #[clap(long, default_value = "")]
    user: String,

    #[clap(long, default_value = "")]
    password: String,

    /// Maximum number of io threads [default: CPU count]
    #[clap(long)]
    threads: Option<usize>,

    /// Number of concurrent HTTP requests
    #[clap(long, default_value = "1")]
    concurrency: usize,

    /// Number of rows to query
    #[clap(long, default_value = "26226829")]
    tot_rows: usize,

    /// Write the result to Parquet format at this path
    #[cfg(feature = "dataframe")]
    #[clap(long)]
    to_parquet: Option<String>,
}

#[cfg(feature = "dataframe")]
enum ColumnBuilder {
    Utf8(Utf8ChunkedBuilder),
    Double(PrimitiveChunkedBuilder<Float64Type>),
    Timestamp(PrimitiveChunkedBuilder<Int64Type>),
}

#[cfg(feature = "dataframe")]
impl ColumnBuilder {
    fn new_utf8(name: &str, capacity: usize, bytes_capacity: usize) -> Self {
        Self::Utf8(Utf8ChunkedBuilder::new(name, capacity, bytes_capacity))
    }

    fn new_double(name: &str, capacity: usize) -> Self {
        Self::Double(PrimitiveChunkedBuilder::new(name, capacity))
    }

    fn new_timestamp(name: &str, capacity: usize) -> Self {
        Self::Timestamp(PrimitiveChunkedBuilder::new(name, capacity))
    }
}

#[cfg(feature = "dataframe")]
fn new_column_builder(col_name: &str, capacity: usize) -> anyhow::Result<ColumnBuilder> {
    let column = match col_name {
        "TradeDate" => ColumnBuilder::new_timestamp(col_name, capacity),
        "OQI" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "Bid" => ColumnBuilder::new_double(col_name, capacity),
        "Ask" => ColumnBuilder::new_double(col_name, capacity),
        "ODMP" => ColumnBuilder::new_double(col_name, capacity),
        "SettlementPrice" => ColumnBuilder::new_double(col_name, capacity),
        "Last" => ColumnBuilder::new_double(col_name, capacity),
        "SRC" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "ExpirationDate" => ColumnBuilder::new_timestamp(col_name, capacity),
        "StrikePrice" => ColumnBuilder::new_double(col_name, capacity),
        "C1PM1" => ColumnBuilder::new_double(col_name, capacity),
        "COP" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "AssetType" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "AssetClass" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "SuperROOT" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "ROOT" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "UDL" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "SettlementOQI" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "RateOQI" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "UDL_OQI" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "TTM" => ColumnBuilder::new_double(col_name, capacity),
        "ATTM" => ColumnBuilder::new_double(col_name, capacity),
        "Rate" => ColumnBuilder::new_double(col_name, capacity),
        "Spot" => ColumnBuilder::new_double(col_name, capacity),
        "Moneyness" => ColumnBuilder::new_double(col_name, capacity),
        "RIC" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "RICRoot" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "UnderlyingRIC" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "SettlementRIC" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "PutCallFlag" => ColumnBuilder::new_utf8(col_name, capacity, capacity),
        "IV" => ColumnBuilder::new_double(col_name, capacity),
        "delta" => ColumnBuilder::new_double(col_name, capacity),
        "gamma" => ColumnBuilder::new_double(col_name, capacity),
        "theta" => ColumnBuilder::new_double(col_name, capacity),
        "rho" => ColumnBuilder::new_double(col_name, capacity),
        "vega" => ColumnBuilder::new_double(col_name, capacity),
        _ => return Err(anyhow::anyhow!("unknown column {:?}", col_name)),
    };
    Ok(column)
}

/// Parse a CSV response into a Polars DataFrame.
/// Also returns the total number of bytes read over the network.
#[cfg(feature = "dataframe")]
async fn to_dataframe(
    response: reqwest::Response,
    start_row: usize,
    end_row: usize,
) -> anyhow::Result<(u64, DataFrame)> {

    // We use the `reqwest` library to query HTTP.
    // It returns a stream that first needs to be converted into an async reader before we can use it.
    let stream = response.bytes_stream();
    let stream = stream.map_err(|e| io::Error::new(io::ErrorKind::Other, e));
    let async_reader = stream.into_async_read();

    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .create_reader(async_reader);

    // We parse the header and create typed column builders for each column.
    // We hard-code this for simplicity, but if we wanted to make this more generic,
    // we could instead issue a JSON query with `limit 0` just to get the schema and set up
    // the column builders dynamically.
    let mut col_builders = Vec::new();
    let row_count = end_row - start_row;
    let headers = csv_reader.headers().await?;
    let mut header_names = Vec::new();

    for header in headers.iter() {
        let column = new_column_builder(header, row_count)?;
        col_builders.push(column);
        header_names.push(header.to_string());
    }

    // We loop through the CSV data as it's streamed in from the server.
    // We re-use a single record to avoid allocating a new one for each row.
    let mut row_count = 0;
    let mut record = StringRecord::new();
    while csv_reader.read_record(&mut record).await? {

        if row_count % 100000 == 0 {
            eprintln!("row: {}", row_count);
        }

        row_count += 1;
        continue;
        for (i, value) in record.iter().enumerate() {
            let column = &mut col_builders[i];
            // We parse each cell and append it to the appropriate column builder.
            match column {
                ColumnBuilder::Utf8(builder) => {
                    if value == "" {
                        builder.append_null();
                    } else {
                        builder.append_value(value);
                    }
                }
                ColumnBuilder::Double(builder) => {
                    // eprintln!("value: {}, name: {}", value, header_names[i]);
                    if value == "" {
                        builder.append_null();
                    } else {
                        builder.append_value(value.parse::<f64>()?)
                    }
                }
                ColumnBuilder::Timestamp(builder) => {
                    if value == "" {
                        builder.append_null();
                    } else {
                        let ts = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S.%fZ")?;
                        let parsed = ts.timestamp_nanos();
                        builder.append_value(parsed);
                    }
                }
            }
        }
    }

    let response_byte_len = csv_reader.position().byte();

    // We finally construct series (columns) from the builders and assemble them into a DataFrame.
    let series = col_builders
        .into_iter()
        .map(|column| match column {
            ColumnBuilder::Utf8(builder) => Series::from(builder.finish()),
            ColumnBuilder::Double(builder) => Series::from(builder.finish()),
            ColumnBuilder::Timestamp(builder) => {
                let dt_chunked = builder.finish().into_datetime(TimeUnit::Nanoseconds, None);
                Series::from(dt_chunked)
            }
        })
        .collect::<Vec<_>>();
    let df = DataFrame::new(series)?;
    Ok((response_byte_len, df))
}

async fn run(args: Args) -> anyhow::Result<()> {
    // We use the `/exp` endpoint (rather than /exec which returns JSON) for efficiency.
    let base_url = format!("https://{}:{}/exp", args.host, args.port);
    let tot_rows = args.tot_rows;
    let rows_per_spawn = tot_rows / args.concurrency;
    if rows_per_spawn == 0 {
        return Err(anyhow::anyhow!(
            "total rows must be greater than concurrency"
        ));
    }

    // We create an HTTP client that can support compression, should this be enabled on the server.
    // This saves on bandwidth, but actually decreases performance if there's no network bottleneck.
    // Your mileage may vary.
    // If you want to test with compression enabled, tweak the QuestDB `server.conf` setting:
    // http.allow.deflate.before.send=true
    let client = reqwest::ClientBuilder::new()
        .deflate(true)
        .gzip(true)
        .build()?;

    // We divide the total number of rows into ranges for each concurrent task.
    let ranges = (0..args.concurrency)
        .map(|i| {
            let start_row = i * rows_per_spawn;
            let mut end_row = start_row + rows_per_spawn;
            if i == args.concurrency - 1 {
                end_row = tot_rows;
            }
            (start_row, end_row)
        })
        .collect::<Vec<_>>();

    println!("ranges: {:?}", ranges);

    let start_time = std::time::Instant::now();

    let handles: Vec<_> = ranges
        .iter()
        .map(|(start_row, end_row)| {
            let start_row = *start_row;
            let end_row = *end_row;
            let base_url = base_url.clone();
            let client = client.clone();
            let args = args.clone();
            tokio::task::spawn(async move {
                let query = format!("select * from PJG limit {},{}", start_row, end_row);
                println!("query: {}", query);
                let url = Url::parse_with_params(&base_url, &[("query", query)])?;
                let response = client
                    .get(url)
                    .basic_auth(args.user, Some(args.password))
                    .send().await?;

                #[cfg(feature = "dataframe")]
                {
                    let (byte_count, frame) = to_dataframe(response, start_row, end_row).await?;
                    Ok::<_, anyhow::Error>((byte_count, frame))
                }

                #[cfg(not(feature = "dataframe"))]
                {
                    let byte_count = response.bytes().await?.len() as u64;
                    Ok::<_, anyhow::Error>(byte_count)
                }
            })
        })
        .collect();

    #[cfg(feature = "dataframe")]
        let mut frames: Vec<_> = Vec::new();
    let mut tot_bytes = 0;
    for handle in handles {
        #[cfg(feature = "dataframe")]
            let (byte_count, frame) = handle.await??;

        #[cfg(not(feature = "dataframe"))]
            let byte_count = handle.await??;

        tot_bytes += byte_count;

        #[cfg(feature = "dataframe")]
        frames.push(frame.lazy());
    }

    // We concatenate the dataframes from each task into a single dataframe.
    // Pola.rs uses Arrow chuncked arrays, so this is a zero-copy operation.
    #[cfg(feature = "dataframe")]
        let mut frame: DataFrame = concat(&frames, false, false)?.collect()?;

    let elapsed = start_time.elapsed();

    #[cfg(feature = "dataframe")]
    println!("{}", frame);

    println!("elapsed: {:?}", elapsed);
    println!(
        "Row throughput: {} rows/sec",
        (tot_rows as f64 / elapsed.as_secs_f64()) as u64
    );
    let bytes_throughput = (tot_bytes as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64()) as u64;
    println!(
        "Data throughput: {} MiB/sec (of downloaded CSV data)",
        bytes_throughput
    );

    // If you want to load the data into Python you can first export it via
    // `--to-parquet dest/path.parquet`.
    // The following will allow you to load it back:
    // >>> import pandas as pd
    // >>> df = pd.read_parquet('dest/path.parquet')
    #[cfg(feature = "dataframe")]
    if let Some(path) = args.to_parquet {
        println!("Writing dataframe to {} as parquet", path);
        let path = Path::new(&path);
        let file = File::options().write(true).create(true).open(path)?;
        let writer = ParquetWriter::new(file);
        writer.finish(&mut frame)?;
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    // Async IO is handled via the Tokio runtime.
    // You can experiment with the number of IO threads via the `--threads` flag,
    // but the default should be fine.
    let args = Args::parse();
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();
    if let Some(threads) = args.threads {
        rt_builder.worker_threads(threads);
    }
    let rt = rt_builder.build()?;
    rt.block_on(async { run(args).await })
}
