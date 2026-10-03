//! The readers of the formats Polars reads: Parquet, delimited text, JSON, Arrow IPC,
//! Avro, ORC and Excel.

use super::{BASE, Reader};
use crate::export_modal::ExportFormat;

pub(crate) const PARQUET: Reader = Reader {
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
    export: Some(ExportFormat::Ipc),
    ..BASE
};

pub(crate) const AVRO: Reader = Reader {
    export: Some(ExportFormat::Avro),
    ..BASE
};

/// Datui writes no ORC.
pub(crate) const ORC: Reader = BASE;

/// Datui writes no workbook.
pub(crate) const EXCEL: Reader = BASE;
