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

    pub fn supports_compression(self) -> bool {
        matches!(self, Self::Csv | Self::Json | Self::Ndjson)
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
    // Footer buttons
    ExportButton,
    CancelButton,
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
    pub csv_compression: Option<CompressionFormat>,
    // JSON options
    pub json_compression: Option<CompressionFormat>,
    // NDJSON options
    pub ndjson_compression: Option<CompressionFormat>,
    // Compression selection index (for horizontal radio buttons)
    pub compression_selection_idx: usize,
    pub history_limit: usize,
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
        // The separator the file was read with, so a round trip keeps its layout.
        let delimiter_char = file_delimiter.unwrap_or(b',');
        self.csv_delimiter_input
            .set_value(format!("{}", delimiter_char as char));
        self.csv_include_header = true;
        self.source_file = false;
        self.offer_source_file = false;
        self.csv_compression = None;
        self.json_compression = None;
        self.ndjson_compression = None;
        self.compression_selection_idx = 0;
    }

    pub fn close(&mut self) {
        self.active = false;
        self.focus = ExportFocus::FormatSelector;
        self.path_input.clear();
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
        order.extend([ExportFocus::ExportButton, ExportFocus::CancelButton]);
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
            csv_compression: None,
            json_compression: None,
            ndjson_compression: None,
            compression_selection_idx: 0,
            history_limit: 1000,
        }
    }
}
