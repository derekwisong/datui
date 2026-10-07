//! The readers of the formats Polars reads: Parquet, delimited text, JSON, Arrow IPC,
//! Avro, ORC and Excel.

use color_eyre::Result;

use super::{BASE, EVERYWHERE, Kind, Reader, ScanIn, Signature, Trusted, Unnamed};
#[cfg(feature = "cloud")]
use crate::error_display::FileError;
use crate::export_modal::ExportFormat;
use crate::python_script::{self as py, Python};
use crate::scan::Scan;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::types::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMillisecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use orc_rust::ArrowReaderBuilder;
use polars::prelude::*;

use super::Read;
use crate::unfinished::Writer;
use crate::{OpenOptions, ParseStringsTarget};

/// A prefix of CSV in an object store, read with the flags the user gave as they are
/// for a local file.
#[cfg(feature = "cloud")]
fn bucket_csv(input: super::BucketIn<'_>) -> Result<polars::prelude::LazyFrame> {
    use polars::prelude::{LazyCsvReader, LazyFileListReader};
    let super::BucketIn {
        url,
        path,
        cloud,
        glob,
        options,
        format,
    } = input;
    let named = std::path::Path::new(url);
    let failed = |e: polars::prelude::PolarsError| {
        FileError::new(
            named,
            format!("could not read it as {}: {}", format.name(), said(&e)),
        )
    };
    let reader = || {
        LazyCsvReader::new(path.clone())
            .with_cloud_options(Some(cloud.clone()))
            .with_glob(glob)
    };
    // Each object has its own header lines, and the scan reads them all as one; the
    // names cannot come from one of them.
    if options.header_rows().is_some() {
        return Err(FileError::new(
            named,
            "--header-rows reads a file's own lines, so it cannot read these in place. Download the files, or name the header with --skip-lines.",
        )
        .into());
    }
    let nv = super::csv::build_null_values_with(options, None, || {
        super::csv::csv_schema_for_null_values(reader(), options)
    })?;
    // No `--infer-types` here: its sample would be a second read of the bucket. Nor
    // Polars' `try_parse_dates`, which fails the whole read on a value it cannot parse,
    // even one like those it inferred the type from. Timestamps stay text, and so do
    // padded numbers: `--skip-initial-space` only takes their padding off.
    let lf = super::csv::configure_csv_reader(reader(), options, nv.as_ref())
        .finish()
        .and_then(|lf| crate::csv_dialect::name_columns(lf, None))
        .and_then(|lf| {
            if !options.skip_initial_space {
                return Ok(lf);
            }
            crate::csv_dialect::skip_initial_space(lf, |column| {
                super::csv::csv_null_values_for(options, column)
            })
        })
        .map_err(failed)?;
    super::csv::apply_skip_tail_rows_csv(lf, options)
        .map_err(|e| crate::error_display::in_file(named, e))
}

/// A prefix of NDJSON in an object store.
#[cfg(feature = "cloud")]
fn bucket_json_lines(input: super::BucketIn<'_>) -> Result<polars::prelude::LazyFrame> {
    use polars::prelude::LazyFileListReader;
    polars::prelude::LazyJsonLineReader::new(input.path)
        .with_cloud_options(Some(input.cloud))
        .finish()
        .map_err(|e| {
            FileError::new(
                std::path::Path::new(input.url),
                format!("could not read it as {}: {}", input.format.name(), said(&e)),
            )
            .into()
        })
}

/// IPC files in an object store, by range from their footers. A prefix is listed
/// before it gets here (`cloud_arrow`), so this is a glob: a stream among its objects
/// has no footer, and is read by its folder, which downloads it.
#[cfg(feature = "cloud")]
fn bucket_arrow(input: super::BucketIn<'_>) -> Result<polars::prelude::LazyFrame> {
    let url = input.url;
    let args = polars::prelude::UnifiedScanArgs {
        cloud_options: Some(input.cloud),
        glob: input.glob,
        ..Default::default()
    };
    polars::prelude::LazyFrame::scan_ipc(input.path, Default::default(), args).map_err(|e| {
        let folder = url
            .split('*')
            .next()
            .and_then(|head| head.rsplit_once('/'))
            .map_or(url, |(folder, _)| folder);
        FileError::new(
            std::path::Path::new(url),
            format!(
                "could not read it as Arrow IPC files: {}. A glob reads IPC files in place; Arrow streams are read by their folder: open {folder}/",
                said(&e)
            ),
        )
        .into()
    })
}

/// Polars' words for `e`, as a clause: tidied, without its full stop.
#[cfg(feature = "cloud")]
fn said(e: &polars::prelude::PolarsError) -> String {
    let said = crate::error_display::user_message_from_polars(e);
    said.trim_end_matches('.').to_string()
}

/// The frame of a state a Polars reader built, with what the read did to its rows for
/// Copy as Python.
fn frame(read: Read, input: ScanIn<'_>) -> Result<Scan> {
    let lf = resolved(read.lf)?;
    input.report.read_python = read.python;
    input.report.read_notes = read.notes;
    input.report.typing = read.typing;
    if let (Some(units), Some(delimited)) = (read.units, input.report.delimited.as_mut()) {
        let mut merged = (**delimited).clone();
        merged.units = units;
        *delimited = std::sync::Arc::new(merged);
    }
    Ok(lf.into())
}

/// `lf` with its schema resolved, as an open needs it: a file Polars cannot read
/// fails here, at the scan.
pub(crate) fn resolved(mut lf: LazyFrame) -> Result<LazyFrame> {
    lf.collect_schema()?;
    Ok(lf)
}

/// A JSON reader's frame. JSON is read into memory whole, so its sample costs no read
/// of the file.
fn json_frame(lf: LazyFrame, input: ScanIn<'_>) -> Result<Scan> {
    apply_parse_dates_to_json_lazyframe(resolved(lf)?, input.options, &mut input.report.read_python)
        .map(Scan::from)
}

fn scan_parquet(input: ScanIn<'_>) -> Result<Scan> {
    frame(each(input.paths, parquet)?.into(), input)
}

fn scan_csv(input: ScanIn<'_>) -> Result<Scan> {
    let read = match input.paths {
        [one] => super::csv::read_delimited(one, b',', input.options, &Writer::default())?,
        many => super::csv::from_csv_paths(many, input.options)?,
    };
    frame(read, input)
}

/// TSV and PSV: one file, with the descriptor's separator.
fn scan_delimited(input: ScanIn<'_>) -> Result<Scan> {
    let separator = input.format.separator().unwrap_or(b',');
    let read =
        super::csv::read_delimited(input.path(), separator, input.options, &Writer::default())?;
    frame(read, input)
}

fn scan_json(input: ScanIn<'_>) -> Result<Scan> {
    let lf = each(input.paths, |p| json(p, JsonFormat::Json))?;
    json_frame(lf, input)
}

/// NDJSON, read whole; followed, scanned, so the frame reads more of it as it grows.
fn scan_json_lines(input: ScanIn<'_>) -> Result<Scan> {
    if input.options.follow {
        let path = input.paths[0].clone();
        return crate::follow::scan_lines(
            &path,
            input.options,
            false,
            &mut input.report.read_python,
        )
        .map(Scan::from);
    }
    let lf = each(input.paths, |p| json(p, JsonFormat::JsonLines))?;
    json_frame(lf, input)
}

/// Arrow IPC files are scanned where they are; streams, which have no footer, are
/// converted first. The first file says for a list: a `datasets` cache is all streams,
/// and the conversion reads each file anyway.
fn scan_arrow(input: ScanIn<'_>) -> Result<Scan> {
    let paths = input.paths;
    if crate::ipc_stream::starts_with_stream(paths) {
        return Ok(Scan::Streams(paths.to_vec()));
    }
    let lf = match paths {
        [one] => ipc(one)?,
        // Polars reads every IPC file's footer for the schema, and fails on a stream
        // among them: only then is each file looked at.
        many => match each(many, ipc).and_then(resolved) {
            Ok(lf) => lf,
            Err(_) if crate::ipc_stream::any_stream(many) => {
                return Ok(Scan::Streams(many.to_vec()));
            }
            Err(e) => return Err(e),
        },
    };
    frame(lf.into(), input)
}

fn scan_avro(input: ScanIn<'_>) -> Result<Scan> {
    frame(each(input.paths, avro)?.into(), input)
}

fn scan_orc(input: ScanIn<'_>) -> Result<Scan> {
    frame(each(input.paths, orc)?.into(), input)
}

fn scan_excel(input: ScanIn<'_>) -> Result<Scan> {
    let (lf, detail) = crate::excel::read(input.path(), input.options)?;
    input.report.opened = Some(std::sync::Arc::new(crate::members::Opened {
        detail: Some(std::sync::Arc::new(detail)),
        ..Default::default()
    }));
    frame(lf.into(), input)
}

pub(crate) const PARQUET: Reader = Reader {
    preview: Some(super::Preview::RowGroup),
    python: Some(Python {
        call: "pl.scan_parquet",
        eager: false,
        glob_flag: true,
        arguments: Some(py::parquet_arguments),
    }),
    scan: scan_parquet,
    facts: Some(super::Facts {
        read: crate::parquet_footer::facts,
        footer: true,
    }),
    // `PAR1` at both ends of a file, because at the front alone it is a truncated
    // write: the footer is what a reader needs.
    signatures: &[Signature {
        says: |head, file| {
            head.starts_with(b"PAR1") && file.is_none_or(crate::discover::has_parquet_magic)
        },
        kind: Kind::Magic,
        trusted: Trusted {
            open: Unnamed::NoExtension,
            ..EVERYWHERE
        },
    }],
    export: Some(ExportFormat::Parquet),
    ..BASE
};

/// CSV, TSV and PSV each export as themselves: TSV and PSV are CSV presets.
pub(crate) const CSV: Reader = Reader {
    #[cfg(feature = "cloud")]
    bucket_scan: Some(bucket_csv),
    preview: Some(super::Preview::Scan),
    python: Some(Python {
        call: "pl.scan_csv",
        eager: false,
        glob_flag: true,
        arguments: Some(py::csv_arguments),
    }),
    scan: scan_csv,
    export: Some(ExportFormat::Csv),
    ..BASE
};

pub(crate) const TSV: Reader = Reader {
    scan: scan_delimited,
    // No prefix of it is read in place.
    #[cfg(feature = "cloud")]
    bucket_scan: None,
    export: Some(ExportFormat::Tsv),
    ..CSV
};

pub(crate) const PSV: Reader = Reader {
    export: Some(ExportFormat::Psv),
    ..TSV
};

pub(crate) const JSON: Reader = Reader {
    python: Some(Python {
        call: "pl.read_json",
        eager: true,
        glob_flag: false,
        arguments: None,
    }),
    scan: scan_json,
    export: Some(ExportFormat::Json),
    ..BASE
};

pub(crate) const JSONL: Reader = Reader {
    #[cfg(feature = "cloud")]
    bucket_scan: Some(bucket_json_lines),
    preview: Some(super::Preview::Scan),
    // `scan_ndjson` has no `glob` flag: a name with a glob character is escaped.
    python: Some(Python {
        call: "pl.scan_ndjson",
        eager: false,
        glob_flag: false,
        arguments: Some(py::ndjson_arguments),
    }),
    scan: scan_json_lines,
    export: Some(ExportFormat::Ndjson),
    ..BASE
};

pub(crate) const ARROW: Reader = Reader {
    #[cfg(feature = "cloud")]
    bucket_scan: Some(bucket_arrow),
    preview: Some(super::Preview::Scan),
    python: Some(Python {
        call: "pl.scan_ipc",
        eager: false,
        glob_flag: true,
        arguments: Some(py::arrow_arguments),
    }),
    scan: scan_arrow,
    facts: Some(super::Facts {
        read: super::facts::arrow,
        footer: false,
    }),
    signatures: &[
        Signature {
            says: |head, _| head.starts_with(b"ARROW1"),
            kind: Kind::Magic,
            trusted: Trusted {
                open: Unnamed::Never,
                ..EVERYWHERE
            },
        },
        // A stream has no magic, only its schema message: a file's is read whole to be
        // sure, what is piped in is judged on its first bytes.
        Signature {
            says: |head, file| match file {
                Some(file) => crate::ipc_stream::is_stream_file(file),
                None => crate::ipc_stream::is_stream_head(head),
            },
            kind: Kind::Structure,
            trusted: Trusted {
                open: Unnamed::NoExtension,
                ..EVERYWHERE
            },
        },
    ],
    export: Some(ExportFormat::Ipc),
    ..BASE
};

pub(crate) const AVRO: Reader = Reader {
    python: Some(Python {
        call: "pl.read_avro",
        eager: true,
        glob_flag: false,
        arguments: None,
    }),
    scan: scan_avro,
    facts: Some(super::Facts {
        read: super::facts::avro,
        footer: false,
    }),
    signatures: &[Signature {
        says: |head, _| head.starts_with(b"Obj\x01"),
        kind: Kind::Magic,
        trusted: Trusted {
            open: Unnamed::Never,
            ..EVERYWHERE
        },
    }],
    export: Some(ExportFormat::Avro),
    ..BASE
};

/// Datui writes no ORC. Its magic is three letters a text file may start with, so it
/// is believed only of a file a listing looks inside.
pub(crate) const ORC: Reader = Reader {
    scan: scan_orc,
    facts: Some(super::Facts {
        read: super::facts::orc,
        footer: false,
    }),
    signatures: &[Signature {
        says: |head, _| head.starts_with(b"ORC"),
        kind: Kind::Magic,
        trusted: Trusted {
            pipe: false,
            open: Unnamed::Never,
            listing: true,
            tables: false,
        },
    }],
    ..BASE
};

/// Datui writes no workbook.
pub(crate) const EXCEL: Reader = Reader {
    python: Some(Python {
        call: "pl.read_excel",
        eager: true,
        glob_flag: false,
        arguments: Some(py::excel_arguments),
    }),
    scan: scan_excel,
    // A workbook whose sheets its directory lists: `.xlsx` and `.xlsm`, which are zip
    // files. Never piped, and never asked of a listing, which would open every zip.
    signatures: &[Signature {
        says: crate::excel::is_listable,
        kind: Kind::Magic,
        trusted: Trusted {
            pipe: false,
            open: Unnamed::Any,
            listing: false,
            tables: true,
        },
    }],
    tables: Some(crate::excel::sheets),
    ..BASE
};

/// One frame of `paths`, each read by `read`, stacked in order.
fn each(paths: &[PathBuf], read: impl Fn(&Path) -> Result<LazyFrame>) -> Result<LazyFrame> {
    match paths {
        [] => Err(color_eyre::eyre::eyre!("No paths provided")),
        [one] => read(one),
        many => {
            let frames = many.iter().map(|p| read(p)).collect::<Result<Vec<_>>>()?;
            Ok(concat(frames.as_slice(), Default::default())?)
        }
    }
}

pub(super) fn parquet(path: &Path) -> Result<LazyFrame> {
    let args = ScanArgsParquet {
        glob: crate::source::expands_as_glob(path),
        ..Default::default()
    };
    Ok(LazyFrame::scan_parquet(
        PlRefPath::try_from_path(path)?,
        args,
    )?)
}

/// An Arrow IPC / Feather v2 file, scanned.
pub(super) fn ipc(path: &Path) -> Result<LazyFrame> {
    let args = UnifiedScanArgs {
        glob: crate::source::expands_as_glob(path),
        ..Default::default()
    };
    Ok(LazyFrame::scan_ipc(
        PlRefPath::try_from_path(path)?,
        Default::default(),
        args,
    )?)
}

/// An Avro file, read whole.
pub(super) fn avro(path: &Path) -> Result<LazyFrame> {
    let file = File::open(path)?;
    Ok(polars::io::avro::AvroReader::new(file).finish()?.lazy())
}

/// An ORC file, read whole through orc-rust's Arrow batches; see
/// `docs/formats/columnar-and-json.md`.
pub(super) fn orc(path: &Path) -> Result<LazyFrame> {
    let file = File::open(path)?;
    let reader = ArrowReaderBuilder::try_new(file)
        .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?
        .build();
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?;
    Ok(arrow_record_batches_to_dataframe(&batches)?.lazy())
}

/// A JSON or NDJSON file, read whole.
fn json(path: &Path, format: JsonFormat) -> Result<LazyFrame> {
    let file = File::open(path)?;
    Ok(JsonReader::new(file)
        .with_json_format(format)
        .finish()?
        .lazy())
}

/// Load multiple Parquet files and concatenate them into one LazyFrame (same schema assumed).
/// How the files of one dataset are stacked into one table.
///
/// `diagonal`, so a file written before a column existed brings the rest of its
/// rows instead of refusing the whole directory; the column reads null for it, and
/// the Notes say which files have it. `to_supertypes`, because a CSV column is
/// typed by inference per file — one `N/A` makes `amount` a String in one file and
/// an Int64 in the next — and without widening, name agreement is not enough to
/// stack them.
///
/// Both are opt-ins everywhere else: DuckDB's `union_by_name`, pyarrow's
/// `unify_schemas`, Spark's `mergeSchema`. They are the default here because a
/// library that unions silently becomes wrong analysis downstream, while datui
/// says what it did in the Notes and keeps `Enter` on the row conservative — a
/// directory whose files are not one table is gone inside, not unioned, and this is
/// what the `(all files)` row behind it reads with.
///
/// **Only for the formats that rule can judge**, which is CSV and NDJSON here, and
/// Parquet through `lenient_scan` elsewhere. Arrow, Avro, ORC and `.json` keep
/// their columns nowhere cheap to reach, so nothing looks at them before the open
/// and nothing could say what a union of them had done — a silent union with no
/// gate in front of it and no note behind it is the pairing this whole change
/// exists to remove, not something to spread further.
///
/// Identical schemas stack exactly as before: diagonal over one schema is vertical,
/// and nothing is widened where nothing differs. Arrow streams converted beside IPC
/// files read in place stack with it too: the streams and the files are one
/// directory's table, read two ways (`App::scan_arrow_parts`).
pub(crate) fn union_of_files() -> polars::prelude::UnionArgs {
    polars::prelude::UnionArgs {
        diagonal: true,
        to_supertypes: true,
        ..Default::default()
    }
}

/// Convert Arrow (arrow crate 57) RecordBatches to Polars DataFrame by value (ORC uses
/// arrow 57; Polars uses polars-arrow, so we cannot use Series::from_arrow).
fn arrow_record_batches_to_dataframe(batches: &[RecordBatch]) -> Result<DataFrame> {
    if batches.is_empty() {
        return Ok(DataFrame::empty());
    }
    let mut all_dfs = Vec::with_capacity(batches.len());
    for batch in batches {
        let n_cols = batch.num_columns();
        let schema = batch.schema();
        let mut series_vec = Vec::with_capacity(n_cols);
        for (i, col) in batch.columns().iter().enumerate() {
            let name = schema.field(i).name().as_str();
            let s = arrow_array_to_polars_series(name, col)?;
            series_vec.push(s.into());
        }
        let df = DataFrame::new_infer_height(series_vec)?;
        all_dfs.push(df);
    }
    let mut out = all_dfs.remove(0);
    for df in all_dfs {
        out = out.vstack(&df)?;
    }
    Ok(out)
}

fn arrow_array_to_polars_series(name: &str, array: &dyn Array) -> Result<Series> {
    use arrow::datatypes::DataType as ArrowDataType;
    let len = array.len();
    match array.data_type() {
        ArrowDataType::Int8 => {
            let a = array
                .as_primitive_opt::<Int8Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int8 array"))?;
            let v: Vec<Option<i8>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Int16 => {
            let a = array
                .as_primitive_opt::<Int16Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int16 array"))?;
            let v: Vec<Option<i16>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Int32 => {
            let a = array
                .as_primitive_opt::<Int32Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int32 array"))?;
            let v: Vec<Option<i32>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Int64 => {
            let a = array
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int64 array"))?;
            let v: Vec<Option<i64>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::UInt8 => {
            let a = array
                .as_primitive_opt::<UInt8Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt8 array"))?;
            let v: Vec<Option<i64>> = (0..len)
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i) as i64)
                    }
                })
                .collect();
            Ok(Series::new(name.into(), v).cast(&DataType::UInt8)?)
        }
        ArrowDataType::UInt16 => {
            let a = array
                .as_primitive_opt::<UInt16Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt16 array"))?;
            let v: Vec<Option<i64>> = (0..len)
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i) as i64)
                    }
                })
                .collect();
            Ok(Series::new(name.into(), v).cast(&DataType::UInt16)?)
        }
        ArrowDataType::UInt32 => {
            let a = array
                .as_primitive_opt::<UInt32Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt32 array"))?;
            let v: Vec<Option<u32>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::UInt64 => {
            let a = array
                .as_primitive_opt::<UInt64Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt64 array"))?;
            let v: Vec<Option<u64>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Float32 => {
            let a = array
                .as_primitive_opt::<Float32Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Float32 array"))?;
            let v: Vec<Option<f32>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Float64 => {
            let a = array
                .as_primitive_opt::<Float64Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Float64 array"))?;
            let v: Vec<Option<f64>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Boolean => {
            let a = array
                .as_boolean_opt()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Boolean array"))?;
            let v: Vec<Option<bool>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Utf8 => {
            let a = array
                .as_string_opt::<i32>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Utf8 array"))?;
            let v: Vec<Option<String>> = (0..len)
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_string())
                    }
                })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::LargeUtf8 => {
            let a = array
                .as_string_opt::<i64>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected LargeUtf8 array"))?;
            let v: Vec<Option<String>> = (0..len)
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_string())
                    }
                })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Date32 => {
            let a = array
                .as_primitive_opt::<Date32Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Date32 array"))?;
            let v: Vec<Option<i32>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Date64 => {
            let a = array
                .as_primitive_opt::<Date64Type>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Date64 array"))?;
            let v: Vec<Option<i64>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        ArrowDataType::Timestamp(_, _) => {
            let a = array
                .as_primitive_opt::<TimestampMillisecondType>()
                .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Timestamp array"))?;
            let v: Vec<Option<i64>> = (0..len)
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect();
            Ok(Series::new(name.into(), v))
        }
        other => Err(color_eyre::eyre::eyre!(
            "ORC: unsupported column type {:?} for column '{}'",
            other,
            name
        )),
    }
}

/// Dates and timestamps a JSON file holds as strings, typed the way a CSV's are.
/// JSON already says which values are numbers, so a string only ever becomes a
/// date, datetime or time, and one that is none of those is left as it was read.
pub(crate) fn apply_parse_dates_to_json_lazyframe(
    lf: LazyFrame,
    options: &OpenOptions,
    read: &mut Vec<String>,
) -> Result<LazyFrame> {
    if !options.parse_dates {
        return Ok(lf);
    }
    super::csv::type_string_columns(
        lf,
        &ParseStringsTarget::All,
        options.parse_strings_sample_rows,
        super::csv::StringTypes {
            dates: true,
            numbers: false,
        },
        read,
        &[],
        &mut Vec::new(),
    )
}

#[cfg(test)]
mod reader_errors {
    use crate::FileFormat;
    use crate::readers::bad_input::each_names_its_file;

    /// Bytes no Polars reader can read name their file, in the one shape, at the scan
    /// or at the first rows.
    #[test]
    fn errors_name_the_file() {
        let garbage: &[u8] = b"\x00\x01\x02 this is not a file of any format \xff\xfe";
        for (format, name) in [
            (FileFormat::Parquet, "bad.parquet"),
            (FileFormat::Arrow, "bad.arrow"),
            (FileFormat::Avro, "bad.avro"),
            (FileFormat::Orc, "bad.orc"),
            (FileFormat::Excel, "bad.xlsx"),
            (FileFormat::Json, "bad.json"),
            (FileFormat::Jsonl, "bad.jsonl"),
        ] {
            each_names_its_file(format, &[(name, garbage, "")]);
        }
        each_names_its_file(FileFormat::Csv, &[("ragged.csv", b"a,b\n1,2\n\"3,4\n", "")]);
    }
}
