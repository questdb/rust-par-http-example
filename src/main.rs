use clap::Parser;
use reqwest::Url;
use tokio;

// The dataframe feature is enabled by default: It decodes the CSV response from the server
// and constructs a Pola.rs DataFrame.
// This is to provide a representative example of what performance can be obtained, including the
// cost of decoding the CSV response.
// To benchmark without this feature, see the README.md's "Synthetic HTTP benchmark" section.
#[cfg(feature = "dataframe")]
use csv_async::{AsyncReaderBuilder, StringRecord}; // async CSV reader

#[cfg(feature = "dataframe")]
use futures::stream::TryStreamExt; // extension trait for working with streams of async values

#[cfg(feature = "dataframe")]
use std::{fs::File, io, path::Path};

#[cfg(feature = "dataframe")]
use polars::{
    export::chrono::NaiveDateTime,
    frame::DataFrame, // in-memory table
    prelude::{
        concat, ChunkedBuilder, Float64Type, Int64Type, IntoLazy, PrimitiveChunkedBuilder,
        TimeUnit, Utf8ChunkedBuilder,
    },
    series::Series, // column
};

#[cfg(feature = "dataframe")]
use polars_io::parquet::ParquetWriter;

/// Query the TSBS dataset into memory from concurrent connections.
#[derive(Parser)]
struct Args {
    /// QuestDB Host
    #[clap(long, default_value = "localhost")]
    host: String,

    /// QuestDB HTTP port
    #[clap(long, default_value = "9000")]
    port: u16,

    /// Maximum number of io threads [default: CPU count]
    #[clap(long)]
    threads: Option<usize>,

    /// Number of concurrent HTTP requests
    #[clap(long, default_value = "1")]
    concurrency: usize,

    /// Number of rows to query
    #[clap(long, default_value = "5000000")]
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

    fn new_double(name: &str) -> Self {
        Self::Double(PrimitiveChunkedBuilder::new(name, 0))
    }

    fn new_timestamp(name: &str) -> Self {
        Self::Timestamp(PrimitiveChunkedBuilder::new(name, 0))
    }
}

#[cfg(feature = "dataframe")]
fn new_column_builder(col_name: &str, capacity: usize) -> anyhow::Result<ColumnBuilder> {
    let column = match col_name {
        "hostname" => ColumnBuilder::new_utf8(col_name, capacity, 9 * capacity),
        "region" => ColumnBuilder::new_utf8(col_name, capacity, 12 * capacity),
        "datacenter" => ColumnBuilder::new_utf8(col_name, capacity, 13 * capacity),
        "rack" => ColumnBuilder::new_utf8(col_name, capacity, 1 * capacity),
        "os" => ColumnBuilder::new_utf8(col_name, capacity, 13 * capacity),
        "arch" => ColumnBuilder::new_utf8(col_name, capacity, 3 * capacity),
        "team" => ColumnBuilder::new_utf8(col_name, capacity, 3 * capacity),
        "service" => ColumnBuilder::new_utf8(col_name, capacity, 2 * capacity),
        "service_version" => ColumnBuilder::new_utf8(col_name, capacity, 1 * capacity),
        "service_environment" => ColumnBuilder::new_utf8(col_name, capacity, 7 * capacity),
        "usage_user" => ColumnBuilder::new_double(col_name),
        "usage_system" => ColumnBuilder::new_double(col_name),
        "usage_idle" => ColumnBuilder::new_double(col_name),
        "usage_nice" => ColumnBuilder::new_double(col_name),
        "usage_iowait" => ColumnBuilder::new_double(col_name),
        "usage_irq" => ColumnBuilder::new_double(col_name),
        "usage_softirq" => ColumnBuilder::new_double(col_name),
        "usage_steal" => ColumnBuilder::new_double(col_name),
        "usage_guest" => ColumnBuilder::new_double(col_name),
        "usage_guest_nice" => ColumnBuilder::new_double(col_name),
        "timestamp" => ColumnBuilder::new_timestamp(col_name),
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
    for header in headers.iter() {
        let column = new_column_builder(header, row_count)?;
        col_builders.push(column);
    }

    // We loop through the CSV data as it's streamed in from the server.
    // We re-use a single record to avoid allocating a new one for each row.
    let mut record = StringRecord::new();
    while csv_reader.read_record(&mut record).await? {
        for (i, value) in record.iter().enumerate() {
            let column = &mut col_builders[i];
            // We parse each cell and append it to the appropriate column builder.
            match column {
                ColumnBuilder::Utf8(builder) => builder.append_value(value),
                ColumnBuilder::Double(builder) => builder.append_value(value.parse::<f64>()?),
                ColumnBuilder::Timestamp(builder) => {
                    let ts = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S.%fZ")?;
                    let parsed = ts.timestamp_nanos();
                    builder.append_value(parsed);
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
    let base_url = format!("http://{}:{}/exp", args.host, args.port);
    let tot_rows = args.tot_rows;
    let rows_per_spawn = tot_rows / args.concurrency;
    if tot_rows % args.concurrency != 0 {
        return Err(anyhow::anyhow!(
            "total rows {} must be divisible by concurrency {}",
            tot_rows,
            args.concurrency
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
            let end_row = start_row + rows_per_spawn;
            (start_row, end_row)
        })
        .collect::<Vec<_>>();

    let start_time = std::time::Instant::now();

    let handles: Vec<_> = ranges
        .iter()
        .map(|(start_row, end_row)| {
            let start_row = *start_row;
            let end_row = *end_row;
            let base_url = base_url.clone();
            let client = client.clone();
            tokio::task::spawn(async move {
                let query = format!("select * from cpu limit {},{}", start_row, end_row);
                let url = Url::parse_with_params(&base_url, &[("query", query)])?;
                let response = client.get(url).send().await?;

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
