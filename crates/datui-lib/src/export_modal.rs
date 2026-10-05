//! Export modal state and focus management.

use crate::CompressionFormat;
use crate::widgets::text_input::TextInput;
use polars::prelude::{LazyFrame, PolarsResult};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    #[default]
    Csv,
    /// CSV with the tab as its delimiter.
    Tsv,
    /// CSV with `|` as its delimiter.
    Psv,
    Parquet,
    Json,
    Ndjson,
    /// Arrow IPC / Feather v2
    Ipc,
    Avro,
}

impl ExportFormat {
    pub const ALL: [Self; 8] = [
        Self::Csv,
        Self::Tsv,
        Self::Psv,
        Self::Parquet,
        Self::Json,
        Self::Ndjson,
        Self::Ipc,
        Self::Avro,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Tsv => "TSV",
            Self::Psv => "PSV",
            Self::Parquet => "Parquet",
            Self::Json => "JSON",
            Self::Ndjson => "NDJSON",
            Self::Ipc => "Arrow",
            Self::Avro => "Avro",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Psv => "psv",
            Self::Parquet => "parquet",
            Self::Json => "json",
            Self::Ndjson => "jsonl",
            Self::Ipc => "arrow",
            Self::Avro => "avro",
        }
    }

    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "csv" => Some(Self::Csv),
            "tsv" => Some(Self::Tsv),
            "psv" => Some(Self::Psv),
            "parquet" => Some(Self::Parquet),
            "json" => Some(Self::Json),
            "ndjson" | "jsonl" => Some(Self::Ndjson),
            "arrow" | "ipc" | "feather" => Some(Self::Ipc),
            "avro" => Some(Self::Avro),
            _ => None,
        }
    }

    /// Whether the format stores list, array and struct columns as they are.
    /// The others get them as JSON text (`nested_json`).
    pub fn holds_nesting(self) -> bool {
        !self.is_delimited()
    }

    /// CSV and its presets, which write delimited text.
    pub fn is_delimited(self) -> bool {
        matches!(self, Self::Csv | Self::Tsv | Self::Psv)
    }

    /// The delimiter a preset sets; `None` for CSV, whose delimiter is the user's.
    pub fn preset_delimiter(self) -> Option<u8> {
        match self {
            Self::Tsv => Some(b'\t'),
            Self::Psv => Some(b'|'),
            _ => None,
        }
    }

    /// `lf` as this format can write it: binary as base64 and dates as text for
    /// CSV and JSON, nested columns as JSON and durations as ISO 8601 for CSV, and the types
    /// Avro lacks cast to ones it has. Planned, not run.
    pub fn prepare(self, lf: LazyFrame) -> PolarsResult<LazyFrame> {
        match self {
            Self::Csv | Self::Tsv | Self::Psv => crate::nested_json::lazy_as_json(lf),
            Self::Json | Self::Ndjson => crate::nested_json::lazy_for_json(lf),
            Self::Avro => crate::avro_types::lazy_for_avro(lf),
            Self::Parquet | Self::Ipc => Ok(lf),
        }
    }

    pub fn supports_compression(self) -> bool {
        self.is_delimited() || matches!(self, Self::Json | Self::Ndjson)
    }

    /// The format a path's extension names, looking through a trailing compression
    /// extension so `out.csv.gz` still names CSV. None for a bare or unknown extension.
    pub fn from_path(path: &str) -> Option<Self> {
        let path = std::path::Path::new(path);
        let ext = path.extension()?.to_str()?;
        if let Some(format) = Self::from_extension(ext) {
            return Some(format);
        }
        if matches!(ext.to_lowercase().as_str(), "gz" | "zst" | "bz2" | "xz") {
            let stem = path.file_stem()?.to_str()?;
            return Self::from_extension(stem.rsplit('.').next()?);
        }
        None
    }
}

/// The export form's fields.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ExportFocus {
    #[default]
    FormatSelector,
    PathInput,
    CsvDelimiter,
    CsvIncludeHeader,
    /// The compression of the chosen format; offered by the formats that take one.
    Compression,
    /// Add a column naming the file each row came from. Only offered for a dataset
    /// whose files disagree, since that is where a null and an absent cell differ and
    /// the source file is what tells them apart downstream.
    SourceFile,
}

/// The compressions a delimited or JSON export steps through, in order.
pub const COMPRESSION_OPTIONS: [Option<CompressionFormat>; 5] = [
    None,
    Some(CompressionFormat::Gzip),
    Some(CompressionFormat::Zstd),
    Some(CompressionFormat::Bzip2),
    Some(CompressionFormat::Xz),
];

pub struct ExportModal {
    pub active: bool,
    pub focus: ExportFocus,
    pub selected_format: ExportFormat,
    pub path_input: TextInput,
    // CSV options
    pub csv_delimiter_input: TextInput,
    pub csv_include_header: bool,
    /// Add a column naming the file each row came from. See `ExportFocus::SourceFile`.
    pub source_file: bool,
    /// Whether this dataset has files to name. Set when the modal opens.
    pub offer_source_file: bool,
    /// Whether the view has list, array or struct columns, which a format
    /// without nesting writes as JSON; the dialog says so. Set when it opens.
    pub nested_columns: bool,
    /// Whether a column or struct field of the view has a name Avro does not
    /// allow; the dialog says those are renamed. Set when it opens.
    pub avro_renames: bool,
    pub csv_compression: Option<CompressionFormat>,
    // JSON options
    pub json_compression: Option<CompressionFormat>,
    // NDJSON options
    pub ndjson_compression: Option<CompressionFormat>,
    pub history_limit: usize,
    /// Why the form cannot export, or why its last write failed, said inline on
    /// its own status line. Cleared by typing in the path.
    pub path_error: Option<String>,
}

impl ExportModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(
        &mut self,
        default_format: Option<ExportFormat>,
        history_limit: usize,
        theme: &crate::config::Theme,
        file_delimiter: Option<u8>,
    ) {
        self.active = true;
        self.focus = ExportFocus::PathInput;
        self.history_limit = history_limit;
        if let Some(format) = default_format {
            self.selected_format = format;
        }
        // Ctrl+P / Ctrl+N recall the paths exported to before.
        self.path_input = TextInput::new()
            .with_history("export_path".to_string())
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.path_input.clear();
        self.csv_delimiter_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        // `--delimiter` if the file was read with one, else a comma.
        let delimiter_char = file_delimiter.unwrap_or(b',');
        self.csv_delimiter_input
            .suggest(format!("{}", delimiter_char as char));
        self.csv_include_header = true;
        self.source_file = false;
        self.offer_source_file = false;
        self.nested_columns = false;
        self.avro_renames = false;
        self.csv_compression = None;
        self.json_compression = None;
        self.ndjson_compression = None;
        self.path_error = None;
    }

    pub fn close(&mut self) {
        self.active = false;
        self.focus = ExportFocus::FormatSelector;
        self.path_input.clear();
        self.path_error = None;
    }

    /// Hide behind a child confirmation without discarding the form; `resume`
    /// brings it back exactly as typed. `close` is the discard.
    pub fn suspend(&mut self) {
        self.active = false;
    }

    pub fn resume(&mut self) {
        self.active = true;
    }

    /// Follow the typed path's extension with the format picker, so `out.csv` never
    /// silently receives Parquet bytes. An extension that names no format leaves the
    /// picker alone, and an explicit format picked after typing stands, because this
    /// runs only when the path itself changes.
    pub fn sync_format_to_path(&mut self) {
        let value = self.path_input.value().trim().to_string();
        if let Some(format) = ExportFormat::from_path(&value) {
            self.selected_format = format;
            // A trailing compression extension is part of what the path asks for:
            // `out.csv.gz` left at Compression: None writes plain bytes to a .gz name.
            if format.supports_compression()
                && let Some(comp) = CompressionFormat::from_extension(std::path::Path::new(&value))
            {
                self.set_compression_for(format, Some(comp));
            }
        }
    }

    /// The other direction of [`Self::sync_format_to_path`]: a format picked after
    /// typing rewrites the path's format extension, so `out.csv` never silently
    /// receives Parquet bytes from the picker side either. A path whose extension
    /// names no format is left alone. A compression suffix survives when the new
    /// format supports one and is dropped when it cannot.
    pub fn sync_path_to_format(&mut self) {
        let value = self.path_input.value().trim().to_string();
        if value.is_empty() || ExportFormat::from_path(&value).is_none() {
            return;
        }
        let path = std::path::Path::new(&value);
        let compression = CompressionFormat::from_extension(path)
            .filter(|_| self.selected_format.supports_compression());
        // Strip the compression suffix, then the format extension, textually:
        // Path::set_extension would also eat the `v2` of `data.v2`.
        let mut base = value.as_str();
        if CompressionFormat::from_extension(path).is_some()
            && let Some((rest, _)) = base.rsplit_once('.')
        {
            base = rest;
        }
        if let Some((rest, ext)) = base.rsplit_once('.')
            && ExportFormat::from_extension(ext).is_some()
        {
            base = rest;
        }
        let new_path = match compression {
            Some(comp) => format!(
                "{base}.{}.{}",
                self.selected_format.extension(),
                comp.extension()
            ),
            None => format!("{base}.{}", self.selected_format.extension()),
        };
        self.path_input.set_value(new_path);
    }

    /// Set the compression field the given format reads at export time.
    fn set_compression_for(&mut self, format: ExportFormat, comp: Option<CompressionFormat>) {
        match format {
            ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => {
                self.csv_compression = comp
            }
            ExportFormat::Json => self.json_compression = comp,
            ExportFormat::Ndjson => self.ndjson_compression = comp,
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => {}
        }
    }

    /// The compression the chosen format writes with; `None` for one that takes none.
    pub fn compression(&self) -> Option<CompressionFormat> {
        match self.selected_format {
            ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => self.csv_compression,
            ExportFormat::Json => self.json_compression,
            ExportFormat::Ndjson => self.ndjson_compression,
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => None,
        }
    }

    /// Step the chosen format's compression through [`COMPRESSION_OPTIONS`].
    pub fn step_compression(&mut self, delta: i8) {
        let next = crate::form::step_value(&COMPRESSION_OPTIONS, self.compression(), delta);
        self.set_compression_for(self.selected_format, next);
    }

    /// Step the format, carrying the typed path's extension with it.
    pub fn step_format(&mut self, delta: i8) {
        self.selected_format =
            crate::form::step_value(&ExportFormat::ALL, self.selected_format, delta);
        self.sync_path_to_format();
        // A format without a delimiter row or compression takes focus off it.
        crate::form::Form::settle_focus(self);
    }

    /// The fields this modal offers, in the order Tab walks them.
    ///
    /// Built as a list rather than a match per field: the options differ by format and
    /// one of them depends on the dataset, and a hand-written state machine over both
    /// has an arm for every pair.
    pub fn focus_order(&self) -> Vec<ExportFocus> {
        crate::form::Form::fields(self)
            .into_iter()
            .map(|(field, _)| field)
            .collect()
    }
}

impl crate::form::Form for ExportModal {
    type Field = ExportFocus;

    fn fields(&self) -> Vec<(ExportFocus, crate::form::FieldKind)> {
        use crate::form::FieldKind::{Checkbox, Choice, Text};
        let mut fields = vec![
            (ExportFocus::FormatSelector, Choice),
            (ExportFocus::PathInput, Text),
        ];
        if self.selected_format == ExportFormat::Csv {
            // The presets say the delimiter.
            fields.push((ExportFocus::CsvDelimiter, Text));
        }
        if self.selected_format.is_delimited() {
            fields.push((ExportFocus::CsvIncludeHeader, Checkbox));
        }
        if self.selected_format.supports_compression() {
            fields.push((ExportFocus::Compression, Choice));
        }
        if self.offer_source_file {
            fields.push((ExportFocus::SourceFile, Checkbox));
        }
        fields
    }

    fn focused(&self) -> ExportFocus {
        self.focus
    }

    fn set_focused(&mut self, field: ExportFocus) {
        self.focus = field;
        self.path_input.set_focused(field == ExportFocus::PathInput);
        self.csv_delimiter_input
            .set_focused(field == ExportFocus::CsvDelimiter);
    }
}

impl Default for ExportModal {
    fn default() -> Self {
        Self {
            active: false,
            focus: ExportFocus::FormatSelector,
            selected_format: ExportFormat::Csv,
            path_input: TextInput::new(),
            csv_delimiter_input: TextInput::new(),
            csv_include_header: true,
            source_file: false,
            offer_source_file: false,
            nested_columns: false,
            avro_renames: false,
            csv_compression: None,
            json_compression: None,
            ndjson_compression: None,
            history_limit: 1000,
            path_error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_path_reads_the_extension_and_looks_through_compression() {
        assert_eq!(ExportFormat::from_path("out.csv"), Some(ExportFormat::Csv));
        assert_eq!(
            ExportFormat::from_path("a/b/out.PARQUET"),
            Some(ExportFormat::Parquet)
        );
        assert_eq!(
            ExportFormat::from_path("out.csv.gz"),
            Some(ExportFormat::Csv)
        );
        assert_eq!(
            ExportFormat::from_path("out.jsonl"),
            Some(ExportFormat::Ndjson)
        );
        assert_eq!(ExportFormat::from_path("out"), None);
        assert_eq!(ExportFormat::from_path("out.dat"), None);
        // A bare compression extension names no format either way.
        assert_eq!(ExportFormat::from_path("out.gz"), None);
    }

    /// TSV and PSV are CSV presets: their names pick them, their delimiter is set, and
    /// the form offers the header and compression without a delimiter row.
    #[test]
    fn tsv_and_psv_are_presets_with_their_delimiter_set() {
        assert_eq!(ExportFormat::from_path("out.tsv"), Some(ExportFormat::Tsv));
        assert_eq!(
            ExportFormat::from_path("out.psv.zst"),
            Some(ExportFormat::Psv)
        );
        assert_eq!(ExportFormat::Tsv.preset_delimiter(), Some(b'\t'));
        assert_eq!(ExportFormat::Psv.preset_delimiter(), Some(b'|'));
        assert_eq!(ExportFormat::Csv.preset_delimiter(), None);
        let mut modal = ExportModal::new();
        for format in [ExportFormat::Tsv, ExportFormat::Psv] {
            modal.selected_format = format;
            let order = modal.focus_order();
            assert!(!order.contains(&ExportFocus::CsvDelimiter), "{format:?}");
            assert!(order.contains(&ExportFocus::CsvIncludeHeader), "{format:?}");
            assert!(order.contains(&ExportFocus::Compression), "{format:?}");
        }
        modal.selected_format = ExportFormat::Csv;
        modal.path_input.set_value("out.csv");
        modal.selected_format = ExportFormat::Tsv;
        modal.sync_path_to_format();
        assert_eq!(modal.path_input.value(), "out.tsv");
    }

    #[test]
    fn sync_format_follows_a_known_extension_and_only_that() {
        let mut modal = ExportModal::new();
        modal.selected_format = ExportFormat::Parquet;
        modal.path_input.set_value("out.csv");
        modal.sync_format_to_path();
        assert_eq!(modal.selected_format, ExportFormat::Csv);

        modal.selected_format = ExportFormat::Parquet;
        modal.path_input.set_value("out.dat");
        modal.sync_format_to_path();
        assert_eq!(modal.selected_format, ExportFormat::Parquet);
    }

    #[test]
    fn a_compression_suffix_in_the_path_sets_compression() {
        let mut modal = ExportModal::new();
        modal.path_input.set_value("out.csv.gz");
        modal.sync_format_to_path();
        assert_eq!(modal.selected_format, ExportFormat::Csv);
        assert_eq!(modal.csv_compression, Some(CompressionFormat::Gzip));

        let mut modal = ExportModal::new();
        modal.path_input.set_value("out.jsonl.zst");
        modal.sync_format_to_path();
        assert_eq!(modal.selected_format, ExportFormat::Ndjson);
        assert_eq!(modal.ndjson_compression, Some(CompressionFormat::Zstd));

        // A plain path leaves an explicitly chosen compression standing.
        let mut modal = ExportModal::new();
        modal.csv_compression = Some(CompressionFormat::Gzip);
        modal.path_input.set_value("out.csv");
        modal.sync_format_to_path();
        assert_eq!(modal.csv_compression, Some(CompressionFormat::Gzip));
    }

    /// A field the new format does not show never keeps focus: stepping the
    /// format settles it on one that is shown.
    #[test]
    fn focus_stays_on_a_shown_field_as_the_format_steps() {
        let mut modal = ExportModal::new();
        for format in ExportFormat::ALL {
            modal.selected_format = format;
            for field in modal.focus_order() {
                modal.selected_format = format;
                crate::form::Form::set_focused(&mut modal, field);
                for delta in [1, -1, 1, 1] {
                    modal.step_format(delta);
                    assert!(
                        modal.focus_order().contains(&modal.focus),
                        "{field:?} from {format:?} lands on {:?} at {:?}",
                        modal.focus,
                        modal.selected_format
                    );
                }
            }
        }
    }

    #[test]
    fn picking_a_format_rewrites_the_typed_extension() {
        let mut modal = ExportModal::new();
        modal.path_input.set_value("out.csv");
        modal.selected_format = ExportFormat::Parquet;
        modal.sync_path_to_format();
        assert_eq!(modal.path_input.value(), "out.parquet");

        // The compression suffix goes when the new format cannot carry it...
        let mut modal = ExportModal::new();
        modal.path_input.set_value("data.v2.csv.gz");
        modal.selected_format = ExportFormat::Parquet;
        modal.sync_path_to_format();
        assert_eq!(modal.path_input.value(), "data.v2.parquet");

        // ...and stays when it can.
        let mut modal = ExportModal::new();
        modal.path_input.set_value("out.csv.gz");
        modal.selected_format = ExportFormat::Ndjson;
        modal.sync_path_to_format();
        assert_eq!(modal.path_input.value(), "out.jsonl.gz");

        // An extension naming no format is the user's to keep.
        let mut modal = ExportModal::new();
        modal.path_input.set_value("out.dat");
        modal.selected_format = ExportFormat::Parquet;
        modal.sync_path_to_format();
        assert_eq!(modal.path_input.value(), "out.dat");
    }
}
