//! Export modal state and focus management.

use crate::CompressionFormat;
use crate::widgets::text_input::TextInput;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    #[default]
    Csv,
    Parquet,
    Json,
    Ndjson,
    /// Arrow IPC / Feather v2
    Ipc,
    Avro,
}

impl ExportFormat {
    pub const ALL: [Self; 6] = [
        Self::Csv,
        Self::Parquet,
        Self::Json,
        Self::Ndjson,
        Self::Ipc,
        Self::Avro,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
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
        !matches!(self, Self::Csv)
    }

    pub fn supports_compression(self) -> bool {
        matches!(self, Self::Csv | Self::Json | Self::Ndjson)
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ExportFocus {
    #[default]
    FormatSelector,
    PathInput,
    // CSV options
    CsvDelimiter,
    CsvIncludeHeader,
    CsvCompression,
    // JSON options
    JsonCompression,
    // NDJSON options
    NdjsonCompression,
    /// Add a column naming the file each row came from. Only offered for a dataset
    /// whose files disagree, since that is where a null and an absent cell differ and
    /// the source file is what tells them apart downstream.
    SourceFile,
}

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
    pub csv_compression: Option<CompressionFormat>,
    // JSON options
    pub json_compression: Option<CompressionFormat>,
    // NDJSON options
    pub ndjson_compression: Option<CompressionFormat>,
    // Compression selection index (the row cycles through the choices)
    pub compression_selection_idx: usize,
    pub history_limit: usize,
    /// Why the form cannot export yet, said inline on its own status line.
    /// Set by Enter on an invalid form, cleared by typing in the path.
    pub path_error: Option<&'static str>,
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
        self.path_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.path_input.clear();
        self.csv_delimiter_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        // `--delimiter` if the file was read with one, else a comma.
        let delimiter_char = file_delimiter.unwrap_or(b',');
        self.csv_delimiter_input
            .set_value(format!("{}", delimiter_char as char));
        self.csv_include_header = true;
        self.source_file = false;
        self.offer_source_file = false;
        self.nested_columns = false;
        self.csv_compression = None;
        self.json_compression = None;
        self.ndjson_compression = None;
        self.compression_selection_idx = 0;
        self.path_error = None;
    }

    pub fn close(&mut self) {
        self.active = false;
        self.focus = ExportFocus::FormatSelector;
        self.path_input.clear();
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
            ExportFormat::Csv => self.csv_compression = comp,
            ExportFormat::Json => self.json_compression = comp,
            ExportFormat::Ndjson => self.ndjson_compression = comp,
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => {}
        }
    }

    /// The fields this modal offers, in the order Tab walks them.
    ///
    /// Built as a list rather than a match per field: the options differ by format and
    /// one of them depends on the dataset, and a hand-written state machine over both
    /// has an arm for every pair.
    pub fn focus_order(&self) -> Vec<ExportFocus> {
        let mut order = vec![ExportFocus::FormatSelector, ExportFocus::PathInput];
        match self.selected_format {
            ExportFormat::Csv => order.extend([
                ExportFocus::CsvDelimiter,
                ExportFocus::CsvIncludeHeader,
                ExportFocus::CsvCompression,
            ]),
            ExportFormat::Json => order.push(ExportFocus::JsonCompression),
            ExportFormat::Ndjson => order.push(ExportFocus::NdjsonCompression),
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => {}
        }
        if self.offer_source_file {
            order.push(ExportFocus::SourceFile);
        }
        order
    }

    /// Where `focus` sits in that list; 0 for a field the current format does not offer.
    fn focus_index(&self) -> usize {
        self.focus_order()
            .iter()
            .position(|field| *field == self.focus)
            .unwrap_or(0)
    }

    pub fn next_focus(&mut self) {
        let order = self.focus_order();
        let new_focus = order[(self.focus_index() + 1) % order.len()];
        self.focus = new_focus;
        // Initialize compression selection index when focusing on compression
        if matches!(
            self.focus,
            ExportFocus::CsvCompression
                | ExportFocus::JsonCompression
                | ExportFocus::NdjsonCompression
        ) {
            self.init_compression_selection();
        }
    }

    pub fn prev_focus(&mut self) {
        let order = self.focus_order();
        let new_focus = order[(self.focus_index() + order.len() - 1) % order.len()];
        self.focus = new_focus;
        // Initialize compression selection index when focusing on compression
        if matches!(
            self.focus,
            ExportFocus::CsvCompression
                | ExportFocus::JsonCompression
                | ExportFocus::NdjsonCompression
        ) {
            self.init_compression_selection();
        }
    }

    pub fn init_compression_selection(&mut self) {
        const COMPRESSION_OPTIONS: [Option<CompressionFormat>; 5] = [
            None,
            Some(CompressionFormat::Gzip),
            Some(CompressionFormat::Zstd),
            Some(CompressionFormat::Bzip2),
            Some(CompressionFormat::Xz),
        ];

        let compression = match self.focus {
            ExportFocus::CsvCompression => self.csv_compression,
            ExportFocus::JsonCompression => self.json_compression,
            ExportFocus::NdjsonCompression => self.ndjson_compression,
            _ => return,
        };

        // Find current index based on selected compression
        self.compression_selection_idx = COMPRESSION_OPTIONS
            .iter()
            .position(|&opt| opt == compression)
            .unwrap_or(0);
    }

    pub fn cycle_compression(&mut self) {
        const COMPRESSION_OPTIONS: [Option<CompressionFormat>; 5] = [
            None,
            Some(CompressionFormat::Gzip),
            Some(CompressionFormat::Zstd),
            Some(CompressionFormat::Bzip2),
            Some(CompressionFormat::Xz),
        ];

        let compression = match self.focus {
            ExportFocus::CsvCompression => &mut self.csv_compression,
            ExportFocus::JsonCompression => &mut self.json_compression,
            ExportFocus::NdjsonCompression => &mut self.ndjson_compression,
            _ => return,
        };

        // Move to next
        self.compression_selection_idx =
            (self.compression_selection_idx + 1) % COMPRESSION_OPTIONS.len();
        *compression = COMPRESSION_OPTIONS[self.compression_selection_idx];
    }

    pub fn cycle_compression_backward(&mut self) {
        const COMPRESSION_OPTIONS: [Option<CompressionFormat>; 5] = [
            None,
            Some(CompressionFormat::Gzip),
            Some(CompressionFormat::Zstd),
            Some(CompressionFormat::Bzip2),
            Some(CompressionFormat::Xz),
        ];

        let compression = match self.focus {
            ExportFocus::CsvCompression => &mut self.csv_compression,
            ExportFocus::JsonCompression => &mut self.json_compression,
            ExportFocus::NdjsonCompression => &mut self.ndjson_compression,
            _ => return,
        };

        // Move to previous
        self.compression_selection_idx = if self.compression_selection_idx == 0 {
            COMPRESSION_OPTIONS.len() - 1
        } else {
            self.compression_selection_idx - 1
        };
        *compression = COMPRESSION_OPTIONS[self.compression_selection_idx];
    }

    pub fn select_compression(&mut self, compression: Option<CompressionFormat>) {
        match self.focus {
            ExportFocus::CsvCompression => {
                self.csv_compression = compression;
            }
            ExportFocus::JsonCompression => {
                self.json_compression = compression;
            }
            ExportFocus::NdjsonCompression => {
                self.ndjson_compression = compression;
            }
            _ => {}
        }
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
            csv_compression: None,
            json_compression: None,
            ndjson_compression: None,
            compression_selection_idx: 0,
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
