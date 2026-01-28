//! Export modal state and focus management.

use crate::widgets::text_input::TextInput;
use crate::CompressionFormat;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    #[default]
    Csv,
    Parquet,
    Json,
    Ndjson,
}

impl ExportFormat {
    pub const ALL: [Self; 4] = [Self::Csv, Self::Parquet, Self::Json, Self::Ndjson];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Parquet => "Parquet",
            Self::Json => "JSON",
            Self::Ndjson => "NDJSON",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::Json => "json",
            Self::Ndjson => "jsonl",
        }
    }

    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "csv" => Some(Self::Csv),
            "parquet" => Some(Self::Parquet),
            "json" => Some(Self::Json),
            "ndjson" | "jsonl" => Some(Self::Ndjson),
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
        config_delimiter: Option<u8>,
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
        // Priority: 1) Config delimiter (user preference), 2) File delimiter (what was used/autodetected), 3) Comma (default)
        let delimiter_char = config_delimiter.or(file_delimiter).unwrap_or(b',');
        // Use set_value to properly sync to textarea
        self.csv_delimiter_input
            .set_value(format!("{}", delimiter_char as char));
        self.csv_include_header = true;
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

    pub fn next_focus(&mut self) {
        let new_focus = match self.focus {
            ExportFocus::FormatSelector => ExportFocus::PathInput,
            ExportFocus::PathInput => match self.selected_format {
                ExportFormat::Csv => ExportFocus::CsvDelimiter,
                ExportFormat::Json => ExportFocus::JsonCompression,
                ExportFormat::Ndjson => ExportFocus::NdjsonCompression,
                ExportFormat::Parquet => ExportFocus::ExportButton,
            },
            ExportFocus::CsvDelimiter => ExportFocus::CsvIncludeHeader,
            ExportFocus::CsvIncludeHeader => ExportFocus::CsvCompression,
            ExportFocus::CsvCompression => ExportFocus::ExportButton,
            ExportFocus::JsonCompression => ExportFocus::ExportButton,
            ExportFocus::NdjsonCompression => ExportFocus::ExportButton,
            ExportFocus::ExportButton => ExportFocus::CancelButton,
            ExportFocus::CancelButton => ExportFocus::FormatSelector,
        };
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
        let new_focus = match self.focus {
            ExportFocus::FormatSelector => ExportFocus::CancelButton,
            ExportFocus::PathInput => ExportFocus::FormatSelector,
            ExportFocus::CsvDelimiter => ExportFocus::PathInput,
            ExportFocus::CsvIncludeHeader => ExportFocus::CsvDelimiter,
            ExportFocus::CsvCompression => ExportFocus::CsvIncludeHeader,
            ExportFocus::JsonCompression => ExportFocus::PathInput,
            ExportFocus::NdjsonCompression => ExportFocus::PathInput,
            ExportFocus::ExportButton => match self.selected_format {
                ExportFormat::Csv => ExportFocus::CsvCompression,
                ExportFormat::Json => ExportFocus::JsonCompression,
                ExportFormat::Ndjson => ExportFocus::NdjsonCompression,
                ExportFormat::Parquet => ExportFocus::PathInput,
            },
            ExportFocus::CancelButton => ExportFocus::ExportButton,
        };
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
            csv_compression: None,
            json_compression: None,
            ndjson_compression: None,
            compression_selection_idx: 0,
            history_limit: 1000,
        }
    }
}
