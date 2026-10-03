//! The readers: what datui does with a file of each format.
//!
//! A format's descriptor ([`crate::FileFormat::descriptor`], in datui-cli) says what is
//! true of it without a file to read. Its [`Reader`] holds the code: the bytes that say
//! it, the scan that opens it, the conversion of a format read into files of its own,
//! the tables a file of it lists, the home preview, Copy as Python's Polars call and
//! the format a view of it exports to by default. What a file says besides its rows is
//! the scan's to report, as the Info panel tab of [`crate::members::Opened::detail`].
//!
//! Each format's reader lives beside its parser (`crate::sqlite::READER`), and those of
//! the formats Polars reads in [`polars`]. [`of`] maps every format to its reader,
//! exhaustively, so a format without one does not compile.
//!
//! The descriptor says whether a format has a tab of its own on the Info panel and
//! whether a file of it holds tables; the reader fills the tab, from what its scan
//! read or, for a format Polars opens, from what [`Reader::facts`] reads of the file's
//! footer when the panel first opens, and lists the tables.
//!
//! Adding a format is its variant and descriptor in datui-cli, its parser and reader
//! in a module of its own, and a line in [`of`].
//!
//! Outside a format's own module, the descriptors and this registry, a format is named
//! only where it changes what the app does, beyond what a descriptor or reader says:
//!
//! - Parquet: hive partitions and footers. A directory or glob of it is one scan with
//!   partition columns, a part file is Parquet by its directory's name, its rows are
//!   counted from footers, and a directory in an object store is read as Parquet only.
//! - Arrow IPC: streams have no footer and are converted to one file first
//!   (`Scan::Streams`, `Conversion::Streams`); Hugging Face caches and DatasetDicts are
//!   Arrow files with JSON metadata beside them, read a split at a time.
//! - JSON: Hugging Face metadata, and a model's config beside its weights, are not data
//!   left out.
//! - SafeTensors and GGUF: a directory of weights is the model, and a remote model is
//!   read by its headers ([`crate::remote_model`]).
//! - CSV: the reader a delimited spec reads through.
//! - Text: what text with nothing else to say is read as ([`FileFormat::TEXT`],
//!   [`crate::lines::guess`]); a name that says text is still asked its bytes, and a
//!   followed file's lines are counted by the watcher.
//! - Audio: a full quality run checks a recording's signal ([`crate::audio::recording`]).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use color_eyre::Result;
use color_eyre::eyre::eyre;

use crate::export_modal::ExportFormat;
use crate::members::Table;
use crate::scan::Scan;
use crate::segments::Converted;
use crate::text_formats::Detail;
use crate::unfinished::Writer;
use crate::{FileFormat, OpenOptions, ReadReport};

pub(crate) mod facts;
pub(crate) mod polars;

/// Lists the tables of a file of a format.
pub(crate) type ListTables = fn(&Path) -> Result<Vec<Table>>;

/// What the Info panel's worker reads of one local file of a format besides its size,
/// where the open left it to Polars: a Parquet footer.
pub(crate) type FactsFn = fn(&Path) -> Result<FormatFacts>;

/// A format's facts read. See [`Reader::facts`].
#[derive(Clone, Copy)]
pub(crate) struct Facts {
    pub read: FactsFn,
    /// Whether it reads a footer that gives the Schema tab's Compression column, which
    /// keeps its room while the read is out.
    pub footer: bool,
}

/// What a [`FactsFn`] read: the format's tab of the Info panel, and the footer the
/// Schema tab's Compression column is drawn from.
#[derive(Debug, Clone, Default)]
pub struct FormatFacts {
    pub detail: Option<Arc<Detail>>,
    pub footer: Option<crate::parquet_footer::Footer>,
}

/// The columns one table of a file of a format opens with, for the home screen's
/// preview: the one named, or the one the file opens when none is.
pub(crate) type TableSchema = fn(&Path, Option<&str>) -> Option<crate::discover::SchemaPreview>;

/// What a scan is given: the files to open as `format`, one unless the format reads
/// many as one table, and where to report what it found besides the frame.
pub(crate) struct ScanIn<'a> {
    pub format: FileFormat,
    pub paths: &'a [PathBuf],
    pub options: &'a OpenOptions,
    pub report: &'a mut ReadReport,
    pub formats: &'a crate::formats::Registry,
}

impl ScanIn<'_> {
    /// The file a format read one file at a time opens.
    pub fn path(&self) -> &Path {
        &self.paths[0]
    }
}

/// Opens files of a format: the frame, or what the load turns into one first.
pub(crate) type ScanFn = fn(ScanIn<'_>) -> Result<Scan>;

/// What a conversion is given: the files to read into files of their own, named
/// `display` to the user, written through `writer`, counting the bytes read in `read`.
pub(crate) struct ConvertIn<'a> {
    pub files: &'a [PathBuf],
    pub display: &'a Path,
    pub format: FileFormat,
    pub options: &'a OpenOptions,
    pub formats: &'a crate::formats::Registry,
    pub writer: &'a Writer,
    pub read: &'a AtomicU64,
}

/// What a conversion wrote, and what the file said besides its rows.
pub(crate) type ConvertOut = Result<(Converted, Option<Arc<Detail>>)>;

/// Reads files of a format into files of their own, which the dataset scans.
pub(crate) type ConvertFn = fn(&ConvertIn<'_>) -> ConvertOut;

/// What a scan of a prefix or glob in an object store is given: the URL as the user
/// named it, and as Polars lists it.
#[cfg(feature = "cloud")]
pub(crate) struct BucketIn<'a> {
    pub url: &'a str,
    pub path: ::polars::prelude::PlRefPath,
    pub cloud: ::polars::io::cloud::CloudOptions,
    pub glob: bool,
    pub options: &'a OpenOptions,
    pub format: FileFormat,
}

/// Scans a prefix or glob of a format in an object store as one table, in place.
#[cfg(feature = "cloud")]
pub(crate) type BucketScan = fn(BucketIn<'_>) -> Result<::polars::prelude::LazyFrame>;

/// The code behind one format.
pub(crate) struct Reader {
    /// Opens a file of it, or several of a format that reads many as one table.
    pub scan: ScanFn,
    /// Scans a prefix of it in an object store in place, for a format other than
    /// Parquet whose descriptor says a prefix is ([`FileFormat::reads_bucket_prefix`]).
    /// Parquet's own scan has hive partitioning, and a prefix of model files is read by
    /// its headers before a scan.
    #[cfg(feature = "cloud")]
    pub bucket_scan: Option<BucketScan>,
    /// Reads a file of it into files of its own: a format the scan answers with
    /// [`Scan::ReadInto`], or an archive's compressed member ([`Scan::Unpack`]).
    pub convert: Option<ConvertFn>,
    /// The bytes at the start of a file that say it is this format, if any do.
    pub signatures: &'static [Signature],
    /// The formats a file of this one is named as: a file whose name says one of them
    /// is still asked its first bytes for this (journal JSON in a `.json` file).
    pub refines: &'static [FileFormat],
    /// The tables a file of it lists on the home screen, read cheaply: a database's
    /// schema, an archive's directory. Only for a format whose descriptor says it holds
    /// tables that are listed.
    pub tables: Option<ListTables>,
    /// What the Info panel reads of one local file of it when it first opens, for a
    /// format whose scan leaves what the file says besides its rows to Polars. Its
    /// tab is offered from the open on, and filled when the read lands.
    pub facts: Option<Facts>,
    /// The columns of one of its tables, read cheaply for the home screen's preview,
    /// where a table's columns are not in its listing.
    pub table_schema: Option<TableSchema>,
    /// Whether a file named as it whose first bytes do not say it cannot open: a `.db`
    /// file that is not SQLite. The home screen lists such a file as no data.
    pub bytes_decide: bool,
    /// How the home screen's preview reads a file of it before it is opened, where its
    /// first rows are cheap.
    pub preview: Option<Preview>,
    /// How Copy as Python reads it with Polars, where Polars does.
    pub python: Option<crate::python_script::Python>,
    /// What a view of it is exported as unless the user picks: the format itself where
    /// datui writes it.
    pub export: Option<ExportFormat>,
}

/// How the home screen's preview reads a file's first rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Preview {
    /// A scan from the start, of a file no larger than the preview reads.
    Scan,
    /// Its first row group, of a file whose groups are no larger.
    RowGroup,
}

/// The reader of a format nothing is written for: no tables, no export default.
pub(crate) const BASE: Reader = Reader {
    scan: |input| {
        Err(eyre!(
            "datui has no reader for {} files.",
            input.format.name()
        ))
    },
    #[cfg(feature = "cloud")]
    bucket_scan: None,
    convert: None,
    signatures: &[],
    refines: &[],
    tables: None,
    facts: None,
    table_schema: None,
    bytes_decide: false,
    preview: None,
    python: None,
    export: None,
};

/// Opens `input`'s files as their format's reader does. An error opening one file
/// names it ([`crate::error_display::FileError`]); one of several names its own.
pub(crate) fn scan(input: ScanIn<'_>) -> Result<Scan> {
    let one = (input.paths.len() == 1).then(|| input.paths[0].clone());
    (of(input.format).scan)(input).map_err(|e| match one {
        Some(path) => crate::error_display::in_file(&path, e),
        None => e,
    })
}

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
        FileFormat::Text => &crate::lines::READER,
        FileFormat::Journal => &crate::journal::READER,
    }
}

/// Bytes read from the start of a file to tell its format by.
pub const HEAD: usize = 4096;

/// The bytes at the start of a file that say a format.
pub(crate) struct Signature {
    /// Whether `head`, a file's first bytes ([`HEAD`] of them, or the whole of a shorter
    /// file), say the format. `file` is the file they came from, where there is one:
    /// for bytes another format shares (an NPZ archive is a zip file) or that are
    /// confirmed further in (Parquet's footer).
    pub says: fn(&[u8], Option<&Path>) -> bool,
    /// How the bytes say it, which orders the signatures: magic numbers are asked
    /// first, then structure read from lengths, then text.
    pub kind: Kind,
    /// Where the bytes are believed.
    pub trusted: Trusted,
}

/// How a signature says its format. See [`Signature::kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Magic,
    Structure,
    Text,
}

/// Where a signature is believed. A weak one (`ORC`, three letters a text file may
/// start with) is believed in fewer places than a strong one.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Trusted {
    /// Data piped in, which has no name.
    pub pipe: bool,
    /// A file opened whose name says no format.
    pub open: Unnamed,
    /// A file a directory listing looks inside ([`crate::discover::worth_sniffing`]).
    /// Never ELF, which would list every executable, nor text, which is a parse.
    pub listing: bool,
    /// The bytes say a file of several tables, each a place inside it
    /// (`shop.db/orders`), whatever the file is called.
    pub tables: bool,
}

/// Which files whose names say no format a signature is believed for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unnamed {
    Never,
    /// Any: `.bin`, text (`.log`, `.txt`), or none.
    Any,
    /// Only a file with no extension at all, such as a part file.
    NoExtension,
}

/// Believed everywhere, but not as a file of tables.
pub(crate) const EVERYWHERE: Trusted = Trusted {
    pipe: true,
    open: Unnamed::Any,
    listing: true,
    tables: false,
};

/// Where a file's first bytes are asked to say its format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Asked {
    /// Data piped in.
    Pipe,
    /// A file being opened whose name says no format; `extension` is whether it has
    /// one at all.
    Open { extension: bool },
    /// A file a directory listing looks inside.
    Listing,
    /// Whether a file of any name holds several tables.
    Tables,
}

impl Asked {
    fn believes(self, format: FileFormat, trusted: Trusted) -> bool {
        match self {
            Asked::Pipe => trusted.pipe,
            Asked::Open { extension } => match trusted.open {
                Unnamed::Never => false,
                Unnamed::Any => true,
                Unnamed::NoExtension => !extension,
            },
            Asked::Listing => trusted.listing,
            Asked::Tables => trusted.tables && format.holds_tables(),
        }
    }
}

/// The format `head`, the first bytes of `file` (none for a pipe), says, as believed
/// where it is `asked`, of the formats `among` admits. The one sniffer: every format's
/// signatures, magic numbers first.
pub(crate) fn sniff(
    head: &[u8],
    file: Option<&Path>,
    asked: Asked,
    among: impl Fn(FileFormat) -> bool,
) -> Option<FileFormat> {
    [Kind::Magic, Kind::Structure, Kind::Text]
        .into_iter()
        .find_map(|kind| {
            FileFormat::ALL.into_iter().find(|&format| {
                among(format)
                    && of(format).signatures.iter().any(|sig| {
                        sig.kind == kind
                            && asked.believes(format, sig.trusted)
                            && (sig.says)(head, file)
                    })
            })
        })
}

/// The format a file named as `named` is by its first bytes, when they say one that
/// refines it ([`Reader::refines`]): journal JSON in a `.json` file.
pub(crate) fn refined(path: &Path, named: FileFormat) -> Option<FileFormat> {
    if !FileFormat::ALL
        .into_iter()
        .any(|f| of(f).refines.contains(&named))
    {
        return None;
    }
    let head = head_of(path)?;
    sniff(&head, Some(path), Asked::Open { extension: true }, |f| {
        of(f).refines.contains(&named)
    })
}

/// [`sniff`] for the file at `path`, by its first [`HEAD`] bytes.
pub(crate) fn sniff_file(path: &Path, asked: Asked) -> Option<FileFormat> {
    let head = head_of(path)?;
    sniff(&head, Some(path), asked, |_| true)
}

/// The format of a local file being opened whose name says none, by its first bytes;
/// `compression` is `--compression`. A format read through its compression (a GPS log,
/// a VCD dump) is named under it (`track.nmea.gz`) and known by its bytes inside it.
pub(crate) fn sniff_open(
    path: &Path,
    compression: Option<crate::CompressionFormat>,
) -> Option<FileFormat> {
    let read_through = |f: FileFormat| f.reads_into();
    let asked = Asked::Open {
        extension: path.extension().is_some(),
    };
    let is_file = path.is_file();
    if is_file
        && let Some(found) =
            head_of(path).and_then(|head| sniff(&head, Some(path), asked, |_| true))
    {
        return Some(found);
    }
    let compression = compression.or_else(|| crate::CompressionFormat::from_extension(path))?;
    if let Some(named) = path
        .file_stem()
        .and_then(|stem| FileFormat::from_path(Path::new(stem)))
        .filter(|f| read_through(*f))
    {
        return Some(named);
    }
    if !is_file {
        return None;
    }
    let head = crate::formats::head_of(path, Some(compression), HEAD as u64)?;
    sniff(&head, Some(path), asked, read_through)
}

/// The first [`HEAD`] bytes of the file at `path`, or all of a shorter one.
fn head_of(path: &Path) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut head = Vec::with_capacity(HEAD);
    std::fs::File::open(path)
        .ok()?
        .take(HEAD as u64)
        .read_to_end(&mut head)
        .ok()?;
    Some(head)
}

/// The scan of a format read into files of its own before it is scanned
/// (`Step::Convert`), as a compressed CSV is.
pub(crate) fn read_into(input: ScanIn<'_>) -> Result<Scan> {
    Ok(Scan::ReadInto {
        files: input.paths.to_vec(),
        format: input.format,
    })
}

/// Read `input`'s files into files of their own as their format's reader does, an
/// error named by the file the user opened.
pub(crate) fn convert(input: &ConvertIn<'_>) -> ConvertOut {
    match of(input.format).convert {
        Some(convert) => convert(input),
        None => Err(eyre!(
            "{} files are not read into files of their own.",
            input.format.name()
        )),
    }
    .map_err(|e| crate::error_display::in_file(input.display, e))
}

/// Why several files of a format that reads one at a time are refused.
pub(crate) fn many_files_refused() -> String {
    let (many, one): (Vec<FileFormat>, Vec<FileFormat>) = FileFormat::ALL
        .into_iter()
        .partition(|f| f.reads_many_files());
    let names = |formats: &[FileFormat]| {
        formats
            .iter()
            .map(|f| f.title())
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "Unsupported file type for multiple files: {} files are read as one table; open {} files one at a time.",
        names(&many),
        names(&one)
    )
}

/// The format a view read as `format` is exported as by default.
pub(crate) fn export_default(format: FileFormat) -> Option<ExportFormat> {
    of(format).export
}

/// Bad input opened as the load opens it, for each reader's error tests.
#[cfg(test)]
pub(crate) mod bad_input {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    use super::{ConvertIn, ScanIn};
    use crate::scan::Scan;
    use crate::{FileFormat, OpenOptions, ReadReport};

    /// What the user is told opening `bytes`, written to a file called `name` in
    /// `dir`, as `format`, through the scan, a conversion and the first rows. `None`
    /// when it opens.
    pub(crate) fn opening(
        dir: &Path,
        name: &str,
        bytes: &[u8],
        format: FileFormat,
        options: &OpenOptions,
    ) -> Option<String> {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        let said =
            |e: color_eyre::Report| crate::error_display::user_message_from_report(&e, Some(&path));
        let formats = crate::formats::Registry::of(Vec::new());
        let mut report = ReadReport::default();
        let paths = [path.clone()];
        let scan = super::scan(ScanIn {
            format,
            paths: &paths,
            options,
            report: &mut report,
            formats: &formats,
        });
        // The files a conversion wrote, kept until the rows are read.
        let mut written = Vec::new();
        let lf = match scan {
            Err(e) => return Some(said(e)),
            Ok(Scan::Frame(lf)) => *lf,
            Ok(Scan::ReadInto { files, format }) => {
                let unfinished = crate::unfinished::Unfinished::default();
                let writer = unfinished.writer(Arc::new(AtomicBool::new(false)));
                let read = AtomicU64::new(0);
                match super::convert(&ConvertIn {
                    files: &files,
                    display: &path,
                    format,
                    options,
                    formats: &formats,
                    writer: &writer,
                    read: &read,
                }) {
                    Err(e) => return Some(said(e)),
                    Ok((converted, _)) => {
                        written = converted.files;
                        converted.lf
                    }
                }
            }
            Ok(_) => return None,
        };
        let rows = lf.limit(100).collect();
        drop(written);
        rows.err().map(|e| said(color_eyre::Report::new(e)))
    }

    /// `message` is a reader error's shape: the file named in quotes first, its
    /// first line a sentence ended with a full stop, nothing Rust prints.
    pub(crate) fn assert_shape(message: &str, path: &Path) {
        let named = format!("\"{}\": ", path.display());
        assert!(message.starts_with(&named), "names the file: {message}");
        let first = message.lines().next().unwrap_or_default();
        assert!(first.ends_with('.'), "ends with a full stop: {message}");
        let what = &message[named.len()..];
        assert!(
            what.chars().next().is_some_and(|c| !c.is_lowercase()),
            "sentence case: {message}"
        );
        assert_eq!(
            what.matches(path.to_string_lossy().as_ref()).count(),
            0,
            "named once: {message}"
        );
        for rust in [
            "Some(",
            "None",
            "Error {",
            "Kind(",
            "PolarsError",
            "ComputeError",
            "os error",
        ] {
            assert!(!message.contains(rust), "no Rust ({rust}): {message}");
        }
    }

    /// Each of `bad`, a file name and its bytes, fails to open as `format` with an
    /// error of the one shape; `says` is a word each message holds.
    pub(crate) fn each_names_its_file(format: FileFormat, bad: &[(&str, &[u8], &str)]) {
        let dir = tempfile::tempdir().unwrap();
        for (name, bytes, says) in bad {
            let message = opening(dir.path(), name, bytes, format, &OpenOptions::default())
                .unwrap_or_else(|| panic!("{name} opens"));
            eprintln!("{message}");
            assert_shape(&message, &dir.path().join(name));
            assert!(message.contains(says), "{name}: {message}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn piped(head: &[u8]) -> Option<FileFormat> {
        sniff(head, None, Asked::Pipe, |_| true)
    }

    /// Text formats are known by their first bytes, and text no format claims is none.
    #[test]
    fn text_formats_by_their_first_bytes() {
        let said = [
            (
                &b"8=FIX.4.4\x019=5\x0135=0\x0110=000\x01\n"[..],
                FileFormat::Fix,
            ),
            (
                b"$timescale 1ns $end\n$scope module top $end\n",
                FileFormat::Vcd,
            ),
            (
                b"aspirin\n  RDKit\n\n  0  0  0  0  0  0  0  0  0  0999 V2000\nM  END\n$$$$\n",
                FileFormat::Sdf,
            ),
            (b"$GPGGA,1,2", FileFormat::Nmea),
            (b"<?xml version=\"1.0\"?>\n<gpx>", FileFormat::Gpx),
        ];
        for (head, format) in said {
            assert_eq!(piped(head), Some(format), "{format:?}");
        }
        assert_eq!(piped(b"a,b\n1,2\n"), None);
    }

    /// A file read through its compression is named under it and known by its bytes
    /// inside it; a name that says a format needs no look.
    #[test]
    fn a_compressed_file_by_its_name_or_what_it_holds() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.log.gz");
        let mut gz = flate2::write::GzEncoder::new(
            std::fs::File::create(&path).unwrap(),
            Default::default(),
        );
        gz.write_all(b"20260101-00:00:00 : 8=FIX.4.2|9=5|35=0|10=000|\n")
            .unwrap();
        gz.finish().unwrap();
        assert_eq!(sniff_open(&path, None), Some(FileFormat::Fix));
        for (name, format) in [
            ("lib.sdf.gz", Some(FileFormat::Sdf)),
            ("a.nmea.gz", Some(FileFormat::Nmea)),
            ("a.csv.gz", None),
        ] {
            assert_eq!(sniff_open(&dir.path().join(name), None), format, "{name}");
        }
    }

    /// Signatures are believed where they say: an executable is never listed, ORC's
    /// three letters are never piped, and only a file of tables is one.
    #[test]
    fn a_signature_is_believed_where_it_says() {
        let elf = b"\x7fELF\x02\x01\x01\0\0\0\0\0\0\0\0\0";
        assert_eq!(piped(elf), Some(FileFormat::Elf));
        assert_eq!(sniff(elf, None, Asked::Listing, |_| true), None);
        assert_eq!(
            sniff(elf, None, Asked::Tables, |_| true),
            Some(FileFormat::Elf)
        );
        assert_eq!(piped(b"ORC\x00"), None);
        assert_eq!(
            sniff(b"ORC\x00", None, Asked::Listing, |_| true),
            Some(FileFormat::Orc)
        );
        let npy = b"\x93NUMPY\x01\x00";
        assert_eq!(piped(npy), Some(FileFormat::Numpy));
        assert_eq!(sniff(npy, None, Asked::Tables, |_| true), None);
    }

    /// The copying page's reader table names every format by its title and the Polars
    /// call Copy as Python reads it with, or `df = ...` where it has none.
    #[test]
    fn the_copy_docs_name_each_format_s_reader() {
        let page = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/user-guide/copying.md");
        let text = std::fs::read_to_string(&page).expect("the copying page");
        let start = text.find("| Format | Reader |").expect("the reader table");
        let rows: Vec<(String, String)> = text[start..]
            .lines()
            .skip(2)
            .take_while(|l| l.starts_with('|'))
            .map(|l| {
                let (format, reader) = l.trim_matches('|').split_once(" | ").expect("two cells");
                (format.trim().to_string(), reader.trim().to_string())
            })
            .collect();
        let said: Vec<&str> = rows.iter().map(|(f, _)| f.as_str()).collect();
        let titles: Vec<&str> = FileFormat::ALL.iter().map(|f| f.title()).collect();
        assert_eq!(said, titles, "one row per format, in --format's order");
        for ((title, reader), format) in rows.iter().zip(FileFormat::ALL) {
            let expected = of(format)
                .python
                .as_ref()
                .map_or("`df = ...`".to_string(), |p| format!("`{}`", p.call));
            assert_eq!(*reader, expected, "{title}");
        }
    }

    /// A reader does what its descriptor says: a format whose tables are listed has a
    /// way to list them, and only such a format does; one read into files of its own
    /// converts; a prefix is scanned in place only where the descriptor says it is.
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
            // A format read into files of its own has a conversion to do it.
            if format.reads_into() {
                assert!(reader.convert.is_some(), "{}", format.name());
            }
            // A tab the facts fill is one the descriptor names.
            if reader.facts.is_some() {
                assert!(format.summary_tab().is_some(), "{}", format.name());
            }
            #[cfg(feature = "cloud")]
            if reader.bucket_scan.is_some() {
                assert!(format.reads_bucket_prefix(), "{}", format.name());
            }
        }
    }
}
