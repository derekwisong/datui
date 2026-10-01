//! Compares the peak memory of a streamed export with a collected one.
//!
//! One export per process, so the process's peak resident memory is that
//! export's. Generate a Parquet file once, then export it each way under a
//! tool that reports the peak, such as GNU time:
//!
//! ```text
//! cargo build --release -p datui-lib --example export_memory
//! BIN=target/release/examples/export_memory
//! $BIN generate data.parquet 100000000
//! /usr/bin/time -v $BIN export data.parquet out.csv streamed
//! /usr/bin/time -v $BIN export data.parquet out.csv collected
//! ```
//!
//! `streamed` and `collected` are the `polars_streaming` setting: on, an
//! uncompressed CSV or a Parquet export streams; off, it is collected. A
//! `.csv.gz`, `.json`, `.jsonl`, `.arrow` or `.avro` destination is collected
//! either way.

use std::path::{Path, PathBuf};
use std::time::Instant;

use datui_lib::CompressionFormat;
use datui_lib::export::{self, ExportOptions, ExportRequest};
use datui_lib::export_modal::ExportFormat;
use datui_lib::output_file::Overwrite;
use polars::prelude::*;

/// Rows per generated batch, and so per row group.
const BATCH: i64 = 1_000_000;

fn main() -> color_eyre::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["generate", path, rows] => generate(Path::new(path), rows.parse()?),
        ["export", from, to, engine @ ("streamed" | "collected")] => {
            run(Path::new(from), PathBuf::from(to), engine == "streamed")
        }
        _ => {
            eprintln!(
                "usage: export_memory generate FILE ROWS\n       \
                 export_memory export FROM TO streamed|collected"
            );
            std::process::exit(2);
        }
    }
}

/// A simple table: an id, a timestamp, two floats and two strings, one of them
/// low-cardinality, written a batch at a time so generating holds one batch.
fn generate(path: &Path, rows: i64) -> color_eyre::Result<()> {
    let batch = |start: i64, len: i64| -> PolarsResult<DataFrame> {
        let ids: Vec<i64> = (start..start + len).collect();
        let mut df = df!(
            "id" => &ids,
            "at" => ids.iter().map(|i| 1_700_000_000_000 + i * 1_000).collect::<Vec<_>>(),
            "price" => ids.iter().map(|i| (i % 10_007) as f64 / 3.0).collect::<Vec<_>>(),
            "qty" => ids.iter().map(|i| (i % 7 != 0).then_some((i % 101) as f64)).collect::<Vec<_>>(),
            "region" => ids.iter().map(|i| ["north", "south", "east", "west"][(i % 4) as usize]).collect::<Vec<_>>(),
            "note" => ids.iter().map(|i| format!("order {i} of customer {}", i % 50_021)).collect::<Vec<_>>(),
        )?;
        df.apply("at", |c| {
            c.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                .expect("a datetime")
        })?;
        Ok(df)
    };
    let first = batch(0, BATCH.min(rows))?;
    let mut writer = ParquetWriter::new(std::fs::File::create(path)?).batched(first.schema())?;
    writer.write_batch(&first)?;
    let mut start = BATCH;
    while start < rows {
        writer.write_batch(&batch(start, BATCH.min(rows - start))?)?;
        start += BATCH;
    }
    writer.finish()?;
    println!("wrote {rows} rows to {}", path.display());
    Ok(())
}

fn run(from: &Path, to: PathBuf, streaming: bool) -> color_eyre::Result<()> {
    let name = to.to_string_lossy();
    let format = ExportFormat::from_path(&name).unwrap_or(ExportFormat::Csv);
    let compression = CompressionFormat::from_extension(&to);
    let request = ExportRequest {
        format,
        options: ExportOptions {
            csv_delimiter: b',',
            csv_include_header: true,
            source_file: false,
            csv_compression: compression.filter(|_| format == ExportFormat::Csv),
            json_compression: compression.filter(|_| format == ExportFormat::Json),
            ndjson_compression: compression.filter(|_| format == ExportFormat::Ndjson),
        },
        overwrite: Overwrite::Replace,
        path: to,
    };
    let lf = LazyFrame::scan_parquet(PlRefPath::try_from_path(from)?, Default::default())?;
    let route = request.route(streaming);
    let started = Instant::now();
    export::run(lf, &request, streaming, |_| {})?;
    println!(
        "{route:?} {format:?} in {:.1}s, {} bytes",
        started.elapsed().as_secs_f64(),
        std::fs::metadata(&request.path)?.len()
    );
    Ok(())
}
