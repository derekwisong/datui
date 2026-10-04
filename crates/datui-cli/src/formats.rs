//! The formats datui reads, one [`Descriptor`] each.
//!
//! A descriptor holds what is true of a format without a reader to ask: its name, the
//! extensions that say it, how a file of it is read locally and remotely, whether it
//! holds tables and which flag picks one, and what the loading screen says while it is
//! converted. Everything that asks one of these questions asks here, so `--format`'s
//! help, the docs' format table, the home screen and the open cannot disagree.
//!
//! What needs a reader (the bytes that say a format, the scan, the Info tab, Copy as
//! Python, the export default) is `datui_lib::readers`, keyed by the same
//! [`FileFormat`]. Its docs list where a format is still named by the app because it
//! changes what the app does: Parquet's partitions, Arrow's streams, SQLite's tables.
//!
//! Adding a format: a variant, its descriptor below, and its line in
//! [`FileFormat::descriptor`] and [`FileFormat::ALL`]. The match is exhaustive, so a
//! variant without a descriptor does not compile; `every_format_is_listed` catches a
//! variant left out of `ALL`.

use std::path::Path;

/// A format datui reads, as `--format` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileFormat {
    Parquet,
    Csv,
    Tsv,
    Psv,
    Json,
    Jsonl,
    Arrow,
    Avro,
    Orc,
    Excel,
    Safetensors,
    Gguf,
    Nmea,
    Gpx,
    Audio,
    Midi,
    Sqlite,
    Vcd,
    Fix,
    Sdf,
    Numpy,
    Elf,
    Ulog,
    Dataflash,
    Candump,
    Text,
    Journal,
}

/// What datui knows of a format without reading a file of it.
#[derive(Debug)]
pub struct Descriptor {
    /// The name `--format` takes and the home screen counts (`12 parquet`). Lowercase
    /// and singular, one per format rather than per extension.
    pub name: &'static str,
    /// What the format is called in a sentence: `SQLite`, `Arrow IPC`.
    pub title: &'static str,
    /// The extensions that say it, lowercase and without the dot. The first is the one
    /// a glob over a directory of it names.
    pub extensions: &'static [&'static str],
    /// Whole-name endings that say it before the extension does:
    /// `model.safetensors.index.json` is SafeTensors, read as the shards it names.
    pub name_endings: &'static [&'static str],
    /// How a file of it on disk, as its extension says, is read.
    pub read: ReadMode,
    /// How a compressed file of it is read.
    pub compressed: Compressed,
    /// How an Arrow IPC stream of it is read, for the one format that has streams.
    pub stream: Option<ReadMode>,
    /// How one HTTP(S) file of it is read.
    pub http: RemoteRead,
    /// How one object of it in a bucket is read.
    pub bucket_object: RemoteRead,
    /// How a bucket prefix or glob of it is read as one table, or `None` when it is
    /// browsed into and opened an object at a time.
    pub bucket_prefix: Option<RemoteRead>,
    /// Whether many files of it are read as one table.
    pub many_files: bool,
    /// How a line of it is read, for text read a line at a time: what `--follow` reads
    /// as it grows.
    pub lines: Option<Lines>,
    /// The tables a file of it can hold, and how one is picked. The home screen lists
    /// them as places inside the file (`shop.db/orders`, `run.npz/weights`) that
    /// recents record and `--table` names.
    pub tables: Option<Tables>,
    /// What the Info panel shows of a file of it besides the Schema tab.
    pub summary: Summary,
    /// Whether a file of it declares its columns' types, which the Schema tab then
    /// calls known rather than inferred.
    pub declares_types: bool,
    /// What the loading screen and the control bar say while a file of it is read into
    /// files of its own before it is scanned.
    pub conversion: Option<Conversion>,
}

/// How a line of a line-oriented text format is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lines {
    /// Split into columns by this separator, when `--delimiter` does not say.
    Delimited(u8),
    /// One JSON object.
    Json,
    /// The line itself, blank or not.
    Text,
}

/// How a compressed file of a format is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compressed {
    /// It is not: the open refuses it.
    Refused,
    /// Decompressed once, to a file or with `[file_loading] decompress_in_memory` into memory.
    Decompressed,
    /// Decompressed as it is read into the files it is converted to.
    ReadThrough,
}

/// What the Info panel shows of a file of a format besides its Schema tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Summary {
    /// A tab of its own, so named (`SQLite`, `Model`), which the format's reader fills
    /// from what the open read.
    Tab(&'static str),
    /// No tab, and why: the Schema tab is all the file says.
    None(&'static str),
}

/// Why a text format has no tab of its own.
const TEXT_ONLY: Summary = Summary::None("text holds its rows and nothing else");

/// The tables in a file of a format.
#[derive(Debug)]
pub struct Tables {
    /// The table a file of several opens when none is named, as the home screen says
    /// it (`its symbols`, `its first table`): Enter there opens it and → lists them
    /// all. `None`: Enter lists them.
    pub opens: Option<&'static str>,
    /// Whether the home screen lists them by name rather than in the file's order.
    pub by_name: bool,
    /// What the flag takes for this format, for its help: `a table or view by name`.
    pub help: &'static str,
}

/// What the open says while a file is read into files of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Conversion {
    /// The loading screen's line: `Reading FIX log`.
    pub label: &'static str,
    /// The control bar's, shorter: `Reading FIX log...`.
    pub status: &'static str,
}

/// The fields most formats share, spread into each descriptor: a plain file read
/// whole into memory, downloaded when remote, one file at a time, holding one table.
const BASE: Descriptor = Descriptor {
    name: "",
    title: "",
    extensions: &[],
    name_endings: &[],
    read: ReadMode::InMemory,
    compressed: Compressed::Refused,
    stream: None,
    http: RemoteRead::Downloaded,
    bucket_object: RemoteRead::Downloaded,
    bucket_prefix: None,
    many_files: false,
    lines: None,
    tables: None,
    summary: TEXT_ONLY,
    declares_types: false,
    conversion: None,
};

/// A delimited text format: scanned in place, decompressed once when compressed, and
/// followed as it grows.
const DELIMITED: Descriptor = Descriptor {
    read: ReadMode::Lazy,
    compressed: Compressed::Decompressed,
    lines: Some(Lines::Delimited(b',')),
    ..BASE
};

/// A text format that cannot be scanned where it is: read once, through its
/// compression, into files of its own.
const READ_INTO: Descriptor = Descriptor {
    read: ReadMode::Converted,
    compressed: Compressed::ReadThrough,
    ..BASE
};

/// A model file's tensor list: one row per tensor, from the header, which a remote
/// file serves by range.
const MODEL: Descriptor = Descriptor {
    http: RemoteRead::InPlace,
    bucket_object: RemoteRead::InPlace,
    bucket_prefix: Some(RemoteRead::InPlace),
    many_files: true,
    ..BASE
};

const PARQUET: Descriptor = Descriptor {
    name: "parquet",
    title: "Parquet",
    extensions: &["parquet"],
    read: ReadMode::Lazy,
    // Polars reads an object by its footer.
    bucket_object: RemoteRead::InPlace,
    bucket_prefix: Some(RemoteRead::InPlace),
    many_files: true,
    declares_types: true,
    summary: Summary::Tab("Parquet"),
    ..BASE
};

const CSV: Descriptor = Descriptor {
    name: "csv",
    title: "CSV",
    extensions: &["csv"],
    bucket_prefix: Some(RemoteRead::InPlace),
    many_files: true,
    ..DELIMITED
};

// TSV and PSV have a single-file reader and no multi-path one.
const TSV: Descriptor = Descriptor {
    name: "tsv",
    title: "TSV",
    extensions: &["tsv"],
    lines: Some(Lines::Delimited(b'\t')),
    ..DELIMITED
};

const PSV: Descriptor = Descriptor {
    name: "psv",
    title: "PSV",
    extensions: &["psv"],
    lines: Some(Lines::Delimited(b'|')),
    ..DELIMITED
};

const JSON: Descriptor = Descriptor {
    name: "json",
    title: "JSON",
    extensions: &["json"],
    many_files: true,
    ..BASE
};

const JSONL: Descriptor = Descriptor {
    name: "jsonl",
    title: "NDJSON",
    extensions: &["jsonl", "ndjson"],
    bucket_prefix: Some(RemoteRead::InPlace),
    many_files: true,
    lines: Some(Lines::Json),
    ..BASE
};

const ARROW: Descriptor = Descriptor {
    name: "arrow",
    title: "Arrow IPC",
    extensions: &["arrow", "arrows", "ipc", "feather"],
    read: ReadMode::Lazy,
    // A stream has no footer: it is converted to an IPC file, and downloaded first.
    stream: Some(ReadMode::Converted),
    bucket_object: RemoteRead::InPlace,
    bucket_prefix: Some(RemoteRead::InPlace),
    many_files: true,
    declares_types: true,
    summary: Summary::Tab("Arrow"),
    ..BASE
};

const AVRO: Descriptor = Descriptor {
    name: "avro",
    title: "Avro",
    extensions: &["avro"],
    many_files: true,
    declares_types: true,
    summary: Summary::Tab("Avro"),
    ..BASE
};

const ORC: Descriptor = Descriptor {
    name: "orc",
    title: "ORC",
    extensions: &["orc"],
    many_files: true,
    declares_types: true,
    summary: Summary::Tab("ORC"),
    ..BASE
};

// A workbook is sheets rather than rows, with nothing to concatenate.
const EXCEL: Descriptor = Descriptor {
    name: "excel",
    title: "Excel",
    extensions: &["xls", "xlsx", "xlsm", "xlsb"],
    tables: Some(Tables {
        opens: Some("its first table"),
        by_name: false,
        help: "a worksheet by name, or by 0-based index when no worksheet is so named",
    }),
    summary: Summary::Tab("Excel"),
    ..BASE
};

const SAFETENSORS: Descriptor = Descriptor {
    name: "safetensors",
    title: "SafeTensors",
    extensions: &["safetensors"],
    name_endings: &[".safetensors.index.json"],
    summary: Summary::Tab("Model"),
    ..MODEL
};

const GGUF: Descriptor = Descriptor {
    name: "gguf",
    title: "GGUF",
    extensions: &["gguf"],
    summary: Summary::Tab("Model"),
    ..MODEL
};

const NMEA: Descriptor = Descriptor {
    name: "nmea",
    title: "NMEA",
    extensions: &["nmea"],
    many_files: true,
    tables: Some(Tables {
        opens: Some("its fixes"),
        by_name: false,
        help: "fixes (default), GGA, RMC, VTG, GSA, GSV, GLL, ZDA or sentences",
    }),
    conversion: Some(Conversion {
        label: "Reading NMEA log",
        status: "Reading NMEA...",
    }),
    summary: Summary::Tab("GPS"),
    ..READ_INTO
};

const GPX: Descriptor = Descriptor {
    name: "gpx",
    title: "GPX",
    extensions: &["gpx"],
    many_files: true,
    conversion: Some(Conversion {
        label: "Reading GPX file",
        status: "Reading GPX...",
    }),
    summary: Summary::Tab("GPS"),
    ..READ_INTO
};

// Recordings, each with its own channels and rate, not parts of one table. Frames are
// read from the file where they are shown.
const AUDIO: Descriptor = Descriptor {
    name: "audio",
    title: "audio",
    extensions: &["wav", "wave", "bwf", "rf64", "aif", "aiff", "aifc"],
    read: ReadMode::Lazy,
    summary: Summary::Tab("Audio"),
    ..BASE
};

// Decoded whole, and refused over 64 MiB.
const MIDI: Descriptor = Descriptor {
    name: "midi",
    title: "MIDI",
    extensions: &["mid", "midi", "smf", "kar", "rmi"],
    many_files: true,
    summary: Summary::Tab("MIDI"),
    ..BASE
};

// Read in place, a page at a time, with sort and filters run in SQLite.
const SQLITE: Descriptor = Descriptor {
    name: "sqlite",
    title: "SQLite",
    extensions: &["db", "db3", "sqlite", "sqlite3"],
    read: ReadMode::Lazy,
    tables: Some(Tables {
        opens: None,
        by_name: true,
        help: "a table or view by name",
    }),
    summary: Summary::Tab("SQLite"),
    ..BASE
};

const VCD: Descriptor = Descriptor {
    name: "vcd",
    title: "VCD",
    extensions: &["vcd"],
    conversion: Some(Conversion {
        label: "Reading value change dump",
        status: "Reading VCD...",
    }),
    summary: Summary::Tab("VCD"),
    ..READ_INTO
};

// No extension of its own: known by its first bytes.
const FIX: Descriptor = Descriptor {
    name: "fix",
    title: "FIX",
    conversion: Some(Conversion {
        label: "Reading FIX log",
        status: "Reading FIX log...",
    }),
    summary: Summary::Tab("FIX"),
    ..READ_INTO
};

const SDF: Descriptor = Descriptor {
    name: "sdf",
    title: "SDF",
    extensions: &["sdf", "sd"],
    conversion: Some(Conversion {
        label: "Reading SDF records",
        status: "Reading SDF...",
    }),
    summary: Summary::Tab("SDF"),
    ..READ_INTO
};

// Decoded from a map of the file where it is shown; a compressed member of an archive
// is decompressed once to a file first.
const NUMPY: Descriptor = Descriptor {
    name: "numpy",
    title: "NumPy",
    extensions: &["npy", "npz"],
    read: ReadMode::Lazy,
    tables: Some(Tables {
        opens: None,
        by_name: false,
        help: "an array of an archive (.npz) by name",
    }),
    conversion: Some(Conversion {
        label: "Decompressing NumPy array",
        status: "Decompressing...",
    }),
    summary: Summary::Tab("NumPy"),
    ..BASE
};

// The symbol table is read into memory.
const ELF: Descriptor = Descriptor {
    name: "elf",
    title: "ELF",
    extensions: &["elf", "axf"],
    tables: Some(Tables {
        opens: Some("its symbols"),
        by_name: false,
        help: "symbols (default) or sections",
    }),
    summary: Summary::Tab("ELF"),
    ..BASE
};

// A flight log is indexed in one pass, and each table decoded from a map of the file
// where it is shown.
const ULOG: Descriptor = Descriptor {
    name: "ulog",
    title: "ULog",
    extensions: &["ulg"],
    read: ReadMode::Lazy,
    tables: Some(Tables {
        opens: None,
        by_name: false,
        help: "a topic",
    }),
    summary: Summary::Tab("ULog"),
    ..BASE
};

// `.bin`, which says nothing: known by its first bytes.
const DATAFLASH: Descriptor = Descriptor {
    name: "dataflash",
    title: "DataFlash",
    read: ReadMode::Lazy,
    tables: Some(Tables {
        opens: None,
        by_name: false,
        help: "a message type",
    }),
    summary: Summary::Tab("DataFlash"),
    ..BASE
};

// Known by its lines.
const CANDUMP: Descriptor = Descriptor {
    name: "candump",
    title: "candump",
    read: ReadMode::Lazy,
    tables: Some(Tables {
        opens: None,
        by_name: false,
        help: "frames (default), signals, or a message a dictionary names",
    }),
    summary: Summary::Tab("CAN"),
    ..BASE
};

// Text read as it stands: a row per line. Indexed in one pass and read from a map of
// the file where it is shown; what nothing else claims.
const TEXT: Descriptor = Descriptor {
    name: "text",
    title: "text",
    extensions: &["log", "txt"],
    read: ReadMode::Lazy,
    compressed: Compressed::Decompressed,
    many_files: true,
    lines: Some(Lines::Text),
    ..BASE
};

// `journalctl -o json`: NDJSON known by its first record's keys, with time, level
// and readable messages derived.
const JOURNAL: Descriptor = Descriptor {
    name: "journal",
    title: "systemd journal",
    many_files: true,
    lines: Some(Lines::Json),
    summary: Summary::Tab("Journal"),
    ..BASE
};

impl FileFormat {
    /// Every format, for the places that have to consider all of them, in the order
    /// `--format`'s help lists them.
    ///
    /// Written out, and so able to fall behind the enum. `every_format_is_listed`
    /// matches a variant exhaustively, so adding one stops that test compiling. What a
    /// format missing here would cost is bounded: `from_name` answers `None` for it,
    /// and every caller reads `None` as "not Parquet", which leaves counts off a
    /// directory rather than giving it another format's.
    pub const ALL: [Self; 27] = [
        Self::Parquet,
        Self::Csv,
        Self::Tsv,
        Self::Psv,
        Self::Json,
        Self::Jsonl,
        Self::Arrow,
        Self::Avro,
        Self::Orc,
        Self::Excel,
        Self::Safetensors,
        Self::Gguf,
        Self::Nmea,
        Self::Gpx,
        Self::Audio,
        Self::Midi,
        Self::Sqlite,
        Self::Vcd,
        Self::Fix,
        Self::Sdf,
        Self::Numpy,
        Self::Elf,
        Self::Ulog,
        Self::Dataflash,
        Self::Candump,
        Self::Text,
        Self::Journal,
    ];

    /// What text with nothing else to say is read as: a pipe, a followed file, a
    /// compressed file whose format is not named, when nothing in it says more.
    pub const TEXT: Self = Self::Text;

    /// The format's descriptor.
    pub const fn descriptor(self) -> &'static Descriptor {
        match self {
            Self::Parquet => &PARQUET,
            Self::Csv => &CSV,
            Self::Tsv => &TSV,
            Self::Psv => &PSV,
            Self::Json => &JSON,
            Self::Jsonl => &JSONL,
            Self::Arrow => &ARROW,
            Self::Avro => &AVRO,
            Self::Orc => &ORC,
            Self::Excel => &EXCEL,
            Self::Safetensors => &SAFETENSORS,
            Self::Gguf => &GGUF,
            Self::Nmea => &NMEA,
            Self::Gpx => &GPX,
            Self::Audio => &AUDIO,
            Self::Midi => &MIDI,
            Self::Sqlite => &SQLITE,
            Self::Vcd => &VCD,
            Self::Fix => &FIX,
            Self::Sdf => &SDF,
            Self::Numpy => &NUMPY,
            Self::Elf => &ELF,
            Self::Ulog => &ULOG,
            Self::Dataflash => &DATAFLASH,
            Self::Candump => &CANDUMP,
            Self::Text => &TEXT,
            Self::Journal => &JOURNAL,
        }
    }

    /// Detect file format from path extension. Returns None when extension is missing or unknown.
    ///
    /// A name ending a descriptor lists (`model.safetensors.index.json`) says its format
    /// before the extension does.
    pub fn from_path(path: &Path) -> Option<Self> {
        Self::from_name_ending(path).or_else(|| {
            path.extension()
                .and_then(|e| e.to_str())
                .and_then(Self::from_extension)
        })
    }

    /// The format a whole-name ending says (`model.safetensors.index.json`), before
    /// the extension does.
    pub fn from_name_ending(path: &Path) -> Option<Self> {
        let name = path.file_name()?.to_str()?.to_ascii_lowercase();
        Self::ALL.into_iter().find(|f| {
            f.descriptor()
                .name_endings
                .iter()
                .any(|end| name.ends_with(end))
        })
    }

    /// The format's name, as a row on the home screen says it: `12 parquet`, `3 csv`.
    ///
    /// The dataset cache stores it (`Holds`), so renaming one makes the records already
    /// on disk unreadable.
    pub fn name(self) -> &'static str {
        self.descriptor().name
    }

    /// What the format is called in a sentence.
    pub fn title(self) -> &'static str {
        self.descriptor().title
    }

    /// The format a [`FileFormat::name`] names, for a name that was stored rather than
    /// carried. The inverse of that method, and the only way back: a name is not an
    /// extension, so `from_extension` cannot read one.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|f| f.name() == name)
    }

    /// Whether many files of this format can be read as one table.
    ///
    /// Asked before a directory is offered as a dataset, so the home screen cannot
    /// promise an open the reader has no route for.
    pub fn reads_many_files(self) -> bool {
        self.descriptor().many_files
    }

    /// Whether a file of this format can hold several tables, each with a path inside
    /// it (`shop.db/orders`, `run.npz/weights`) that the home screen lists like a
    /// directory's files and `--table` names.
    pub fn holds_tables(self) -> bool {
        self.descriptor().tables.is_some()
    }

    /// Whether a file of this format holding several tables opens one when none is
    /// named. See [`Tables::opens`].
    pub fn opens_one_table(self) -> bool {
        self.descriptor()
            .tables
            .as_ref()
            .is_some_and(|t| t.opens.is_some())
    }

    /// The name of the format's tab of the Info panel, when it has one.
    pub fn summary_tab(self) -> Option<&'static str> {
        match self.descriptor().summary {
            Summary::Tab(tab) => Some(tab),
            Summary::None(_) => None,
        }
    }

    /// Whether `--table` picks one of the tables of a file of this format.
    pub fn takes_table(self) -> bool {
        self.descriptor().tables.is_some()
    }

    /// Whether a file of this format is read into files of its own before it is
    /// scanned: a GPS log, VCD dump, FIX log or SDF file.
    pub fn reads_into(self) -> bool {
        self.descriptor().compressed == Compressed::ReadThrough
    }

    /// How a file of this format is read when it is opened, as `stored` on disk.
    ///
    /// The one answer to "does this read only what it shows": the format table in
    /// `docs/formats/index.md`, the home screen's marker and the Info panel's `Read:`
    /// line all ask here. `None` when a file stored that way does not open: a compressed
    /// Parquet file, or an IPC stream of anything but Arrow.
    pub fn read_mode(self, stored: Stored) -> Option<ReadMode> {
        let d = self.descriptor();
        match stored {
            Stored::Plain => Some(d.read),
            Stored::Stream => d.stream,
            Stored::Compressed { in_memory } => match d.compressed {
                Compressed::Refused => None,
                Compressed::ReadThrough => Some(ReadMode::Converted),
                Compressed::Decompressed if in_memory => Some(ReadMode::InMemory),
                Compressed::Decompressed => Some(ReadMode::Converted),
            },
        }
    }

    /// How one object of this format in a bucket (S3, GCS, Azure), as `stored`, is
    /// read: in place with ranged reads, or downloaded first and then read as
    /// [`Self::read_mode`] says. Only a plain file is read in place.
    pub fn bucket_object(self, stored: Stored) -> RemoteRead {
        match stored {
            Stored::Plain => self.descriptor().bucket_object,
            _ => RemoteRead::Downloaded,
        }
    }

    /// How one HTTP(S) file of this format is read. A server that sends no ranges gets
    /// the download question.
    pub fn http_file(self) -> RemoteRead {
        self.descriptor().http
    }

    /// How a bucket prefix or glob of files of this format, as `stored`, is read as
    /// one table: in place, downloaded first, or `None` when it is not (browsed into
    /// and opened an object at a time). A prefix of streams is converted as it
    /// downloads.
    pub fn bucket_prefix(self, stored: Stored) -> Option<RemoteRead> {
        let d = self.descriptor();
        match stored {
            Stored::Plain => d.bucket_prefix,
            Stored::Stream => d.stream.map(|_| RemoteRead::Downloaded),
            Stored::Compressed { .. } => None,
        }
    }

    /// Whether a bucket prefix or glob of this format is scanned in place as one table.
    /// See [`Self::bucket_prefix`].
    pub fn reads_bucket_prefix(self) -> bool {
        self.bucket_prefix(Stored::Plain) == Some(RemoteRead::InPlace)
    }

    /// The column separator a delimited format is read with when `--delimiter` is not
    /// given. `None` for the formats that are not delimited text.
    pub fn separator(self) -> Option<u8> {
        match self.descriptor().lines {
            Some(Lines::Delimited(separator)) => Some(separator),
            _ => None,
        }
    }

    /// Whether `--follow` reads a file of this format as it grows: text read a line
    /// at a time.
    pub fn follows(self) -> bool {
        self.descriptor().lines.is_some()
    }

    /// Whether a file of this format is read a line a row, as it stands.
    pub fn is_lines(self) -> bool {
        self.descriptor().lines == Some(Lines::Text)
    }

    /// Whether a compressed file of this format is decompressed once, to a file or
    /// into memory, before it is read: delimited text and lines.
    pub fn decompressed_once(self) -> bool {
        self.descriptor().compressed == Compressed::Decompressed
    }

    /// What the open says while a file of this format is read into files of its own.
    /// A format with nothing of its own to say says it converts.
    pub fn conversion(self) -> Conversion {
        self.descriptor().conversion.unwrap_or(Conversion {
            label: "Converting",
            status: "Converting...",
        })
    }

    /// Parse format from extension string (e.g. "parquet", "csv").
    ///
    /// The one place an extension becomes a format. Everything that asks whether a name
    /// is data — the home screen, the search, `~` path input, the CLI and the cloud
    /// listings — asks here, so no route can offer a file another route cannot open.
    ///
    /// `.txt` and `.log` say text, read a line a row. A directory ranks text below
    /// every other format, so a README beside Parquet files does not decide what the
    /// directory is. A tabular `.txt` opens with `--format csv`.
    pub fn from_extension(ext: &str) -> Option<Self> {
        let ext = ext.to_ascii_lowercase();
        Self::ALL
            .into_iter()
            .find(|f| f.descriptor().extensions.contains(&ext.as_str()))
    }
}

/// `--format`'s help, listing every format by name.
pub fn format_help() -> String {
    let names: Vec<&str> = FileFormat::ALL.iter().map(|f| f.name()).collect();
    format!(
        "File format, when the extension does not say: {}; or a format spec: its name (`datui formats` lists them), its file (a path with a / or ending .toml), or its http(s), s3, gs or az URL (at most 1 MiB)",
        names.join(", ")
    )
}

/// `--table`'s help: what it takes for each format whose tables it picks.
pub fn table_help() -> String {
    let mut help = String::from("Table to open from a file that holds several.");
    for format in FileFormat::ALL {
        if let Some(tables) = &format.descriptor().tables {
            help.push_str(&format!(" {}: {}.", format.title(), tables.help));
        }
    }
    help.push_str(" Hugging Face cache and DatasetDict directories: a split (default train)");
    help
}

/// Why `--table` was refused for a file of `format` (`None`: not known), which holds
/// one table: the formats whose tables it picks. The reader names the file before it.
pub fn one_table(format: Option<FileFormat>) -> String {
    let what = format.map_or("This file".to_string(), |f| format!("A {} file", f.name()));
    let titles: Vec<&str> = FileFormat::ALL
        .into_iter()
        .filter(|f| f.takes_table())
        .map(FileFormat::title)
        .collect();
    format!(
        "{what} holds one table. --table picks one from {} files, or a Hugging Face dataset's split.",
        titles.join(", ")
    )
}

/// How an open reads a file. See [`FileFormat::read_mode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// Scanned where it is: only the rows shown, and what a query needs, are read.
    Lazy,
    /// Read through once into a temporary file, which is then scanned lazily.
    Converted,
    /// Read whole into memory before the table appears.
    InMemory,
}

impl ReadMode {
    /// The words for it, as the docs' table and the Info panel say them.
    pub fn label(self) -> &'static str {
        match self {
            Self::Lazy => "lazy",
            Self::Converted => "converted once",
            Self::InMemory => "in memory",
        }
    }

    /// The word a home screen row carries for it, short beside a name: `converts`,
    /// `in memory`; `None` for a lazy read, which most are.
    pub fn marker(self) -> Option<&'static str> {
        match self {
            Self::Lazy => None,
            Self::Converted => Some("converts"),
            Self::InMemory => Some("in memory"),
        }
    }
}

/// How a file sits on disk, where that changes how it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Stored {
    /// As its format's extension says.
    #[default]
    Plain,
    /// Under gzip, zstd, bzip2 or xz. `in_memory` is `[file_loading] decompress_in_memory`.
    Compressed { in_memory: bool },
    /// An Arrow IPC stream rather than an IPC file: no footer to find the rows by.
    Stream,
}

/// How a remote object is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteRead {
    /// With ranged reads, where it is.
    InPlace,
    /// Downloaded whole to a temporary file first.
    Downloaded,
}

impl RemoteRead {
    /// The words for it, as the docs' table says them.
    pub fn label(self) -> &'static str {
        match self {
            Self::InPlace => "in place",
            Self::Downloaded => "downloaded",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An extension or a name says one format, or the first to list it would always
    /// win and the other never be found by it.
    #[test]
    fn names_and_extensions_say_one_format() {
        let mut seen: Vec<&str> = Vec::new();
        for format in FileFormat::ALL {
            let d = format.descriptor();
            assert!(!d.title.is_empty(), "{format:?}");
            for ext in d.extensions {
                assert!(!seen.contains(ext), ".{ext} names two formats");
                assert_eq!(*ext, ext.to_ascii_lowercase());
                seen.push(ext);
                assert_eq!(FileFormat::from_extension(ext), Some(format));
            }
        }
        let names: Vec<&str> = FileFormat::ALL.map(FileFormat::name).to_vec();
        for name in &names {
            assert_eq!(names.iter().filter(|n| *n == name).count(), 1, "{name}");
        }
    }

    /// `--format` lists every format; `--table` names every format whose tables it
    /// picks, and refusing it names them too.
    #[test]
    fn help_and_refusals_come_from_the_descriptors() {
        let format = format_help();
        let table = table_help();
        let refused = one_table(Some(FileFormat::Csv));
        for f in FileFormat::ALL {
            assert!(format.contains(f.name()), "{}", f.name());
            let picked = format!("{}: ", f.title());
            assert_eq!(table.contains(&picked), f.takes_table(), "{}", f.name());
            if f.takes_table() {
                assert!(refused.contains(f.title()), "{refused}");
            }
        }
        assert!(
            refused.starts_with("A csv file holds one table"),
            "{refused}"
        );
        assert!(one_table(None).starts_with("This file holds one table"));
        assert!(FileFormat::Excel.takes_table(), "a sheet is a table");
        // A spec named in help would have to ship; none does.
        assert!(!format.contains("acme"), "{format}");
    }

    /// Every format read into files of its own says so in words of its own: none falls
    /// back to another's.
    #[test]
    fn conversions_say_what_they_read() {
        let mut seen: Vec<Conversion> = Vec::new();
        for f in FileFormat::ALL.into_iter().filter(|f| f.reads_into()) {
            let c = f.descriptor().conversion.expect("a conversion's words");
            assert!(c.status.ends_with("..."), "{}", c.status);
            assert!(
                !seen
                    .iter()
                    .any(|s| s.label == c.label || s.status == c.status),
                "{c:?}"
            );
            seen.push(c);
        }
    }
}
