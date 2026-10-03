//! The readers of the formats Polars reads: Parquet, delimited text, JSON, Arrow IPC,
//! Avro, ORC and Excel.

use color_eyre::Result;

use super::{BASE, EVERYWHERE, Kind, Reader, ScanIn, Signature, Trusted, Unnamed};
#[cfg(feature = "cloud")]
use crate::error_display::FileError;
use crate::export_modal::ExportFormat;
use crate::python_script::{self as py, Python};
use crate::scan::Scan;
use crate::widgets::datatable::DataTableState;

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
    let nv = DataTableState::build_null_values_with(options, None, || {
        DataTableState::csv_schema_for_null_values(reader(), options)
    })?;
    // No `--parse-strings` here: its sample would be a second read of the bucket. Nor
    // Polars' `try_parse_dates`, which fails the whole read on a value it cannot parse,
    // even one like those it inferred the type from. Timestamps stay text, and so do
    // padded numbers: `--skip-initial-space` only takes their padding off.
    let lf = DataTableState::configure_csv_reader(reader(), options, nv.as_ref())
        .finish()
        .and_then(|lf| crate::csv_dialect::name_columns(lf, None))
        .and_then(|lf| {
            if !options.skip_initial_space {
                return Ok(lf);
            }
            crate::csv_dialect::skip_initial_space(lf, |column| {
                DataTableState::csv_null_values_for(options, column)
            })
        })
        .map_err(failed)?;
    DataTableState::apply_skip_tail_rows_csv(lf, options)
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
fn frame(state: DataTableState, input: ScanIn<'_>) -> Result<Scan> {
    input.report.read_python = state.read_python().to_vec();
    Ok(state.into_lf().into())
}

/// A JSON reader's frame. JSON is read into memory whole, so its sample costs no read
/// of the file.
fn json_frame(state: DataTableState, input: ScanIn<'_>) -> Result<Scan> {
    DataTableState::apply_parse_dates_to_json_lazyframe(
        state.into_lf(),
        input.options,
        &mut input.report.read_python,
    )
    .map(Scan::from)
}

fn scan_parquet(input: ScanIn<'_>) -> Result<Scan> {
    let state = match input.paths {
        [one] => DataTableState::from_parquet(one, input.options)?,
        many => DataTableState::from_parquet_paths(many, input.options)?,
    };
    frame(state, input)
}

fn scan_csv(input: ScanIn<'_>) -> Result<Scan> {
    let state = match input.paths {
        [one] => DataTableState::from_csv(one, input.options)?,
        many => DataTableState::from_csv_paths(many, input.options)?,
    };
    frame(state, input)
}

/// TSV and PSV: one file, with the descriptor's separator.
fn scan_delimited(input: ScanIn<'_>) -> Result<Scan> {
    let separator = input.format.separator().unwrap_or(b',');
    let state = DataTableState::from_delimited(input.path(), separator, input.options)?;
    frame(state, input)
}

fn scan_json(input: ScanIn<'_>) -> Result<Scan> {
    let state = match input.paths {
        [one] => DataTableState::from_json(one, input.options)?,
        many => DataTableState::from_json_paths(many, input.options)?,
    };
    json_frame(state, input)
}

/// NDJSON, read whole; followed, scanned, so the frame reads more of it as it grows.
fn scan_json_lines(input: ScanIn<'_>) -> Result<Scan> {
    if input.options.follow {
        let path = input.paths[0].clone();
        return crate::follow::scan_lines(&path, input.options, &mut input.report.read_python)
            .map(Scan::from);
    }
    let state = match input.paths {
        [one] => DataTableState::from_json_lines(one, input.options)?,
        many => DataTableState::from_json_lines_paths(many, input.options)?,
    };
    json_frame(state, input)
}

/// Arrow IPC files are scanned where they are; streams, which have no footer, are
/// converted first. The first file says for a list: a `datasets` cache is all streams,
/// and the conversion reads each file anyway.
fn scan_arrow(input: ScanIn<'_>) -> Result<Scan> {
    let paths = input.paths;
    if crate::ipc_stream::starts_with_stream(paths) {
        return Ok(Scan::Streams(paths.to_vec()));
    }
    let state = match paths {
        [one] => DataTableState::from_ipc(one, input.options)?,
        // Polars reads every IPC file's footer for the schema, and fails on a stream
        // among them: only then is each file looked at.
        many => match DataTableState::from_ipc_paths(many, input.options) {
            Ok(state) => state,
            Err(_) if crate::ipc_stream::any_stream(many) => {
                return Ok(Scan::Streams(many.to_vec()));
            }
            Err(e) => return Err(e),
        },
    };
    frame(state, input)
}

fn scan_avro(input: ScanIn<'_>) -> Result<Scan> {
    let state = match input.paths {
        [one] => DataTableState::from_avro(one, input.options)?,
        many => DataTableState::from_avro_paths(many, input.options)?,
    };
    frame(state, input)
}

fn scan_orc(input: ScanIn<'_>) -> Result<Scan> {
    let state = match input.paths {
        [one] => DataTableState::from_orc(one, input.options)?,
        many => DataTableState::from_orc_paths(many, input.options)?,
    };
    frame(state, input)
}

fn scan_excel(input: ScanIn<'_>) -> Result<Scan> {
    let state = DataTableState::from_excel(input.path(), input.options)?;
    frame(state, input)
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
    ..BASE
};

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
