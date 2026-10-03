//! The readers: what datui does with a file of each format.
//!
//! A format's descriptor ([`crate::FileFormat::descriptor`], in datui-cli) says what is
//! true of it without a file to read. Its [`Reader`] holds the code: the tables a file
//! of it lists and the format a view of it exports to by default. Each format's reader
//! lives beside its parser (`crate::sqlite::READER`), and those of the formats Polars
//! reads in [`polars`]. [`of`] maps every format to its reader, exhaustively, so a
//! format without one does not compile.

use std::path::Path;

use color_eyre::Result;

use crate::FileFormat;
use crate::export_modal::ExportFormat;
use crate::members::Table;

pub(crate) mod polars;

/// Lists the tables of a file of a format.
pub(crate) type ListTables = fn(&Path) -> Result<Vec<Table>>;

/// The code behind one format.
pub(crate) struct Reader {
    /// The tables a file of it lists on the home screen, read cheaply: a database's
    /// schema, an archive's directory. Only for a format whose descriptor says it holds
    /// tables that are listed.
    pub tables: Option<ListTables>,
    /// What a view of it is exported as unless the user picks: the format itself where
    /// datui writes it.
    pub export: Option<ExportFormat>,
}

/// The reader of a format nothing is written for: no tables, no export default.
pub(crate) const BASE: Reader = Reader {
    tables: None,
    export: None,
};

/// The reader of `format`.
pub(crate) fn of(format: FileFormat) -> &'static Reader {
    match format {
        FileFormat::Parquet => &polars::PARQUET,
        FileFormat::Csv => &polars::CSV,
        FileFormat::Tsv => &polars::TSV,
        FileFormat::Psv => &polars::PSV,
        FileFormat::Json => &polars::JSON,
        FileFormat::Jsonl => &polars::JSONL,
        FileFormat::Arrow => &polars::ARROW,
        FileFormat::Avro => &polars::AVRO,
        FileFormat::Orc => &polars::ORC,
        FileFormat::Excel => &polars::EXCEL,
        FileFormat::Safetensors => &crate::model_files::SAFETENSORS,
        FileFormat::Gguf => &crate::model_files::GGUF,
        FileFormat::Nmea => &crate::gps::NMEA,
        FileFormat::Gpx => &crate::gps::GPX,
        FileFormat::Audio => &crate::audio::READER,
        FileFormat::Midi => &crate::midi::READER,
        FileFormat::Sqlite => &crate::sqlite::READER,
        FileFormat::Vcd => &crate::vcd::READER,
        FileFormat::Fix => &crate::fix::READER,
        FileFormat::Sdf => &crate::sdf::READER,
        FileFormat::Numpy => &crate::numpy::READER,
        FileFormat::Elf => &crate::elf::READER,
        FileFormat::Ulog => &crate::ulog::READER,
        FileFormat::Dataflash => &crate::dataflash::READER,
        FileFormat::Candump => &crate::candump::READER,
    }
}

/// The format a view read as `format` is exported as by default.
pub(crate) fn export_default(format: FileFormat) -> Option<ExportFormat> {
    of(format).export
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A format whose descriptor says its tables are listed has a way to list them, and
    /// only such a format does.
    #[test]
    fn readers_agree_with_their_descriptors() {
        for format in FileFormat::ALL {
            let reader = of(format);
            assert_eq!(
                reader.tables.is_some(),
                format.holds_tables(),
                "{}",
                format.name()
            );
        }
    }
}
