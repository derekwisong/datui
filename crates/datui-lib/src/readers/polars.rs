//! The readers of the formats Polars reads: Parquet, delimited text, JSON, Arrow IPC,
//! Avro, ORC and Excel.

use super::{BASE, EVERYWHERE, Kind, Reader, Signature, Trusted, Unnamed};
use crate::export_modal::ExportFormat;

pub(crate) const PARQUET: Reader = Reader {
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
    export: Some(ExportFormat::Csv),
    ..BASE
};

pub(crate) const TSV: Reader = CSV;

pub(crate) const PSV: Reader = CSV;

pub(crate) const JSON: Reader = Reader {
    export: Some(ExportFormat::Json),
    ..BASE
};

pub(crate) const JSONL: Reader = Reader {
    export: Some(ExportFormat::Ndjson),
    ..BASE
};

pub(crate) const ARROW: Reader = Reader {
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
pub(crate) const EXCEL: Reader = BASE;
