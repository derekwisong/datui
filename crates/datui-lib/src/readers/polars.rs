//! The readers of the formats Polars reads: Parquet, delimited text, JSON, Arrow IPC,
//! Avro, ORC and Excel.

use color_eyre::Result;

use super::{BASE, EVERYWHERE, Kind, Reader, ScanIn, Signature, Trusted, Unnamed};
use crate::OpenOptions;
use crate::export_modal::ExportFormat;
use crate::scan::Scan;
use crate::widgets::datatable::DataTableState;

/// The paging options every Polars reader takes, in the order they take them.
type Paging = (
    Option<usize>,
    Option<usize>,
    Option<usize>,
    Option<usize>,
    bool,
    usize,
);

fn paging(options: &OpenOptions) -> Paging {
    (
        options.pages_lookahead,
        options.pages_lookback,
        options.max_buffered_rows,
        options.max_buffered_mb,
        options.row_numbers,
        options.row_start_index,
    )
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
    let (a, b, c, d, e, f) = paging(input.options);
    let state = match input.paths {
        [one] => DataTableState::from_parquet(one, a, b, c, d, e, f)?,
        many => DataTableState::from_parquet_paths(many, a, b, c, d, e, f)?,
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
    let (a, b, c, d, e, f) = paging(input.options);
    let state = match input.paths {
        [one] => DataTableState::from_json(one, a, b, c, d, e, f)?,
        many => DataTableState::from_json_paths(many, a, b, c, d, e, f)?,
    };
    json_frame(state, input)
}

fn scan_json_lines(input: ScanIn<'_>) -> Result<Scan> {
    let (a, b, c, d, e, f) = paging(input.options);
    let state = match input.paths {
        [one] => DataTableState::from_json_lines(one, a, b, c, d, e, f)?,
        many => DataTableState::from_json_lines_paths(many, a, b, c, d, e, f)?,
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
    let (a, b, c, d, e, f) = paging(input.options);
    let state = match paths {
        [one] => DataTableState::from_ipc(one, a, b, c, d, e, f)?,
        // Polars reads every IPC file's footer for the schema, and fails on a stream
        // among them: only then is each file looked at.
        many => match DataTableState::from_ipc_paths(many, a, b, c, d, e, f) {
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
    let (a, b, c, d, e, f) = paging(input.options);
    let state = match input.paths {
        [one] => DataTableState::from_avro(one, a, b, c, d, e, f)?,
        many => DataTableState::from_avro_paths(many, a, b, c, d, e, f)?,
    };
    frame(state, input)
}

fn scan_orc(input: ScanIn<'_>) -> Result<Scan> {
    let (a, b, c, d, e, f) = paging(input.options);
    let state = match input.paths {
        [one] => DataTableState::from_orc(one, a, b, c, d, e, f)?,
        many => DataTableState::from_orc_paths(many, a, b, c, d, e, f)?,
    };
    frame(state, input)
}

fn scan_excel(input: ScanIn<'_>) -> Result<Scan> {
    let (a, b, c, d, e, f) = paging(input.options);
    let sheet = input.options.excel_sheet.as_deref();
    let state = DataTableState::from_excel(input.path(), a, b, c, d, e, f, sheet)?;
    frame(state, input)
}

pub(crate) const PARQUET: Reader = Reader {
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

/// CSV, TSV and PSV export as CSV: the export's own delimiter option says the rest.
pub(crate) const CSV: Reader = Reader {
    scan: scan_csv,
    export: Some(ExportFormat::Csv),
    ..BASE
};

pub(crate) const TSV: Reader = Reader {
    scan: scan_delimited,
    ..CSV
};

pub(crate) const PSV: Reader = TSV;

pub(crate) const JSON: Reader = Reader {
    scan: scan_json,
    export: Some(ExportFormat::Json),
    ..BASE
};

pub(crate) const JSONL: Reader = Reader {
    scan: scan_json_lines,
    export: Some(ExportFormat::Ndjson),
    ..BASE
};

pub(crate) const ARROW: Reader = Reader {
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
    scan: scan_excel,
    ..BASE
};
