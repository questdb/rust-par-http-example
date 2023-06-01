use clap::Parser;
use csv_async::AsyncReaderBuilder;
use futures::stream::TryStreamExt;
use futures::{stream, StreamExt};
use polars::export::chrono::NaiveDateTime;
use polars::frame::DataFrame;
use polars::prelude::{ChunkedBuilder, concat, Float64Type, Int64Type, IntoLazy, LazyFrame, PrimitiveChunkedBuilder, TimeUnit, Utf8ChunkedBuilder};
use polars::series::Series;
use reqwest::Url;
use std::io;
use tokio;

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
}

enum ColumnVec {
    Utf8(Utf8ChunkedBuilder),
    Double(PrimitiveChunkedBuilder<Float64Type>),
    Timestamp(PrimitiveChunkedBuilder<Int64Type>),
}

impl ColumnVec {
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

fn new_column(name: &str, capacity: usize) -> anyhow::Result<ColumnVec> {
    let column = match name {
        "hostname" => ColumnVec::new_utf8(name, capacity, 9 * capacity),
        "region" => ColumnVec::new_utf8(name, capacity, 12 * capacity),
        "datacenter" => ColumnVec::new_utf8(name, capacity, 13 * capacity),
        "rack" => ColumnVec::new_utf8(name, capacity, 1 * capacity),
        "os" => ColumnVec::new_utf8(name, capacity, 13 * capacity),
        "arch" => ColumnVec::new_utf8(name, capacity, 3 * capacity),
        "team" => ColumnVec::new_utf8(name, capacity, 3 * capacity),
        "service" => ColumnVec::new_utf8(name, capacity, 2 * capacity),
        "service_version" => ColumnVec::new_utf8(name, capacity, 1 * capacity),
        "service_environment" => ColumnVec::new_utf8(name, capacity, 7 * capacity),
        "usage_user" => ColumnVec::new_double(name),
        "usage_system" => ColumnVec::new_double(name),
        "usage_idle" => ColumnVec::new_double(name),
        "usage_nice" => ColumnVec::new_double(name),
        "usage_iowait" => ColumnVec::new_double(name),
        "usage_irq" => ColumnVec::new_double(name),
        "usage_softirq" => ColumnVec::new_double(name),
        "usage_steal" => ColumnVec::new_double(name),
        "usage_guest" => ColumnVec::new_double(name),
        "usage_guest_nice" => ColumnVec::new_double(name),
        "timestamp" => ColumnVec::new_timestamp(name),
        _ => return Err(anyhow::anyhow!("unknown column {:?}", name)),
    };
    Ok(column)
}

async fn to_dataframe(response: reqwest::Response, start_row: usize, end_row: usize) -> anyhow::Result<DataFrame> {
    let stream = response.bytes_stream();
    let stream = stream.map_err(|e| io::Error::new(io::ErrorKind::Other, e));
    let async_reader = stream.into_async_read();

    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .create_reader(async_reader);

    let mut columns = Vec::new();

    let row_count = end_row - start_row;
    let headers = csv_reader.headers().await?;
    for header in headers.iter() {
        let column = new_column(header, row_count)?;
        columns.push(column);
    }

    let mut records = csv_reader.byte_records();

    while let Some(record) = records.next().await {
        let record = record?;
        for (i, value) in record.iter().enumerate() {
            let value = std::str::from_utf8(value)?;
            let column = &mut columns[i];
            match column {
                ColumnVec::Utf8(vec) => vec.append_value(value.to_owned()),
                ColumnVec::Double(vec) => vec.append_value(value.parse::<f64>()?),
                ColumnVec::Timestamp(vec) => {
                    let ts = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S.%fZ")?;
                    let parsed = ts.timestamp_nanos();
                    vec.append_value(parsed);
                }
            }
        }
    }

    let series = columns
        .into_iter()
        .map(|column| match column {
            ColumnVec::Utf8(vec) => Series::from(vec.finish()),
            ColumnVec::Double(vec) => Series::from(vec.finish()),
            ColumnVec::Timestamp(vec) => {
                let dt_chunked = vec.finish().into_datetime(TimeUnit::Nanoseconds, None);
                Series::from(dt_chunked)
            }
        })
        .collect::<Vec<_>>();

    let df = DataFrame::new(series)?;
    Ok(df)
}

async fn run(args: Args) -> anyhow::Result<()> {
    let base = format!("http://{}:{}/exp", args.host, args.port);
    let tot_rows = 5000000;
    let rows_per_spawn = tot_rows / args.concurrency;
    if tot_rows % args.concurrency != 0 {
        return Err(anyhow::anyhow!(
            "total rows {} must be divisible by concurrency {}",
            tot_rows,
            args.concurrency
        ));
    }

    let client = reqwest::Client::new();
    let urls = (0..args.concurrency)
        .map(|i| {
            let start_row = i * rows_per_spawn;
            let end_row = start_row + rows_per_spawn;
            (start_row, end_row)
        })
        .collect::<Vec<_>>();

    let start_time = std::time::Instant::now();
    let results = stream::iter(urls)
        .map(|(start_row, end_row)| {
            let client = &client;
            let base = base.clone();
            async move {
                let query = format!("select * from cpu limit {},{}", start_row, end_row);
                let url = Url::parse_with_params(&base, &[("query", query)])?;
                let response = client.get(url).send().await?;
                let frame = to_dataframe(response, start_row, end_row).await?;
                Ok::<_, anyhow::Error>((start_row, end_row, frame))
            }
        })
        .buffer_unordered(args.concurrency);
    let frames_or_err = results.collect::<Vec<_>>().await;
    let mut frames = frames_or_err.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
    frames.sort_by_key(|(start_row, _, _)| *start_row);
    let frames: Vec<LazyFrame> = frames.into_iter().map(|(_, _, frame)| frame.lazy()).collect();
    let frame: DataFrame = concat(&frames, false, false)?.collect()?;
    let elapsed = start_time.elapsed();
    println!("{}", frame);
    println!("elapsed: {:?}", elapsed);
    println!("rows/sec: {}", (tot_rows as f64 / elapsed.as_secs_f64()) as u64);
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();
    if let Some(threads) = args.threads {
        rt_builder.worker_threads(threads);
    }
    let rt = rt_builder.build()?;
    rt.block_on(async { run(args).await })
}
