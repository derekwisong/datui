//! Reading a file with a spec, and the `datui formats` command.

use super::*;

/// The first `reach` bytes of `path`, through its decompressor when it has one.
pub fn head_of(
    path: &Path,
    compression: Option<crate::CompressionFormat>,
    reach: u64,
) -> Option<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path).ok()?;
    let reader: Box<dyn Read> = match compression {
        None => Box::new(file),
        Some(crate::CompressionFormat::Gzip) => Box::new(flate2::read::GzDecoder::new(file)),
        Some(crate::CompressionFormat::Zstd) => Box::new(zstd::Decoder::new(file).ok()?),
        Some(crate::CompressionFormat::Bzip2) => Box::new(bzip2::read::BzDecoder::new(file)),
        Some(crate::CompressionFormat::Xz) => Box::new(xz2::read::XzDecoder::new(file)),
    };
    let mut head = Vec::new();
    reader
        .take(reach.min(MAX_MATCH_READ))
        .read_to_end(&mut head)
        .ok()?;
    Some(head)
}

/// A file read through a spec, as the open carries it to the dataset.
pub struct Read {
    pub spec: Arc<Spec>,
    pub by: Chosen,
    /// The other specs that matched as well as `spec`, by the same rule.
    pub also: Vec<String>,
    /// Warnings from the read: trailing bytes, a short count.
    pub notes: Vec<String>,
    pub header: HeaderValues,
    pub records: Arc<dyn SpecRecords>,
}

impl std::fmt::Debug for Read {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Read")
            .field("spec", &self.spec.name)
            .field("by", &self.by)
            .field("also", &self.also)
            .field("rows", &self.records.rows())
            .finish()
    }
}

/// What a request for a format says, besides the path.
#[derive(Debug, Clone, Default)]
pub struct Asked {
    /// `--format FILE`.
    pub spec_file: Option<PathBuf>,
    /// `--format NAME`, or the spec picked in the view.
    pub spec_name: Option<String>,
    /// The spec `spec_file` names, read already: fetched, when it is remote.
    pub spec: Option<Arc<Spec>>,
    /// `--table NAME`: one variant of the spec's records, read alone; any other
    /// reader takes the name as its own table.
    pub variant: Option<String>,
    /// A built-in format from `--format`, which no spec overrides.
    pub builtin: bool,
    pub compression: Option<crate::CompressionFormat>,
    /// Several files are read as one: only a delimited spec reads them.
    pub text_only: bool,
}

/// Where a spec was chosen from, carried to a decompressed copy's read.
#[derive(Clone)]
pub struct Choice {
    pub spec: Arc<Spec>,
    pub by: Chosen,
    pub also: Vec<String>,
}

/// What [`route`] decided about one local path.
pub enum Route {
    /// Not a spec's: the path opens as it does without specs.
    Elsewhere,
    Read(Box<Read>),
    /// Compressed: decompress it, then read the copy with `choice.spec`.
    Decompress(Choice),
    /// A delimited spec's: read with the CSV reader, in the spec's dialect.
    Delimited(Choice),
}

/// The keys a delimited spec takes: its own, and the dialect the option registry
/// gives a spec key (`[csv]`'s keys and the layout flags'), so a spec reads as a
/// `[csv]` block.
pub(crate) fn delimited_spec_keys() -> Vec<&'static str> {
    use datui_cli::settings::{OPEN, SETTINGS};
    let mut keys = vec![
        "name",
        "description",
        "documentation",
        "kind",
        "match",
        "metadata_line",
        "columns",
    ];
    keys.extend(SETTINGS.iter().filter_map(|s| s.spec));
    keys.extend(OPEN.iter().filter_map(|o| o.spec));
    keys
}

/// Which specs may read `path` when nothing names one, or `None` when none may. What
/// the name already says is read as it says, compressed or not: a spec of records takes
/// only a name that says no format datui reads, and a delimited spec also one that says
/// delimited text.
pub(crate) fn unnamed_may(
    path: &Path,
    is_dir: bool,
    text_only: bool,
) -> Option<impl Fn(&Spec) -> bool> {
    let said = (!is_dir)
        .then(|| crate::discover::data_format(path))
        .flatten()
        // Text by its name (`.log`, `.txt`) says no more than no name does.
        .filter(|f| !f.is_lines());
    let parquet_key = crate::discover::is_parquet_key(&crate::discover::directory_and_name(path));
    let records_may = said.is_none() && !parquet_key && !text_only;
    let text_may =
        !is_dir && !parquet_key && said.is_none_or(|f| crate::FileFormat::separator(f).is_some());
    (records_may || text_may).then_some(move |s: &Spec| {
        if s.is_delimited() {
            text_may
        } else {
            records_may
        }
    })
}

/// The first `reach` bytes of `path` for comparing specs' magic, or `None` when its
/// bytes say a format datui reads already: a file with no extension may be Parquet,
/// Arrow, Avro or ORC by its bytes, which it stays.
pub(crate) fn spec_head(
    path: &Path,
    compression: Option<crate::CompressionFormat>,
    reach: u64,
) -> Option<Vec<u8>> {
    if compression.is_none() && crate::discover::sniff_format(path).is_some() {
        return None;
    }
    head_of(path, compression, reach)
}

/// Whether, and with which spec, `path` is read. In order: `--format FILE`, then
/// `--format NAME`, then a glob, then magic. A file whose name or bytes say it is a
/// format datui reads already keeps opening that way.
pub fn route(path: &Path, asked: &Asked, registry: &Registry) -> Result<Route, String> {
    let compression = asked.compression.or_else(|| {
        path.is_file()
            .then(|| crate::CompressionFormat::from_extension(path))
            .flatten()
    });
    let named = path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let explicit = if let Some(spec) = &asked.spec {
        Some(Choice {
            spec: spec.clone(),
            by: Chosen::SpecFile,
            also: Vec::new(),
        })
    } else if let Some(file) = &asked.spec_file {
        if crate::source::is_remote_url(file) {
            return Err(format!(
                "{}: a remote spec is fetched by the open, and was not",
                file.display()
            ));
        }
        let spec = Spec::load(file).map_err(|e| e.to_string())?;
        Some(Choice {
            spec: Arc::new(spec),
            by: Chosen::SpecFile,
            also: Vec::new(),
        })
    } else if let Some(name) = &asked.spec_name {
        let spec = registry.get(name).ok_or_else(|| {
            format!("no format named {name} on the search path; `datui formats` lists them")
        })?;
        Some(Choice {
            spec: spec.clone(),
            by: Chosen::Named,
            also: Vec::new(),
        })
    } else {
        None
    };
    if asked.text_only
        && let Some(choice) = &explicit
        && !choice.spec.is_delimited()
    {
        return Err(format!(
            "{} reads one file, or one directory of column files",
            choice.spec.name
        ));
    }
    let choice = match explicit {
        Some(choice) => choice,
        None => {
            if asked.builtin || registry.is_empty() {
                return Ok(Route::Elsewhere);
            }
            let is_dir = path.is_dir();
            let Some(wanted) = unnamed_may(path, is_dir, asked.text_only) else {
                return Ok(Route::Elsewhere);
            };
            // A glob names the file as it is stored uncompressed: `day.l2.zst` is an `*.l2`.
            let inner = match compression {
                Some(_) => path.with_extension(""),
                None => path.to_path_buf(),
            };
            let matched = registry.matching_among(&inner, is_dir, wanted, |reach| {
                spec_head(path, compression, reach)
            });
            let Some(matched) = matched else {
                return Ok(Route::Elsewhere);
            };
            let mut specs = matched.specs.into_iter();
            let spec = specs.next().expect("a match has a spec");
            Choice {
                spec,
                by: matched.by,
                also: specs.map(|s| s.name.clone()).collect(),
            }
        }
    };
    // The CSV reader reads a delimited spec's files, compressed or not.
    if choice.spec.is_delimited() {
        return Ok(Route::Delimited(choice));
    }
    let choice = match &asked.variant {
        Some(variant) => Choice {
            spec: Arc::new(choice.spec.with_variant(variant)?),
            ..choice
        },
        None => choice,
    };
    if compression.is_some() && path.is_file() {
        return Ok(Route::Decompress(choice));
    }
    read(path, &named, choice).map(|r| Route::Read(Box::new(r)))
}

/// Read `path` with the spec `choice` holds, naming it `named` in what it says.
pub fn read(path: &Path, named: &str, choice: Choice) -> Result<Read, String> {
    let opened = choice.spec.open(path, named)?;
    Ok(Read {
        spec: choice.spec,
        by: choice.by,
        also: choice.also,
        notes: opened.notes,
        header: opened.header,
        records: opened.records,
    })
}

impl Read {
    /// The dataset's notes about the read: which format, why, what else matched, the
    /// header's values, and the warnings.
    pub fn notes(&self) -> Vec<crate::notes::Note> {
        let note = |summary: String, scope: String| crate::notes::Note {
            summary,
            scope,
            read_as_text: None,
            passed_over: None,
        };
        let from = self
            .spec
            .path
            .as_ref()
            .map_or_else(|| "the spec".to_string(), |p| p.display().to_string());
        let mut notes = vec![note(
            format!(
                "read as {}, {}",
                self.spec.name,
                chosen_words(&self.spec, self.by)
            ),
            format!("from {from}"),
        )];
        if !self.also.is_empty() {
            notes.push(note(
                format!(
                    "{} also {} this file; press b to pick another",
                    self.also.join(", "),
                    if self.also.len() == 1 {
                        "matches"
                    } else {
                        "match"
                    }
                ),
                format!("by {}", self.by.words()),
            ));
        }
        if !self.header.values.is_empty() {
            let said: Vec<String> = self
                .header
                .values
                .iter()
                .map(|(name, value)| format!("{name} = {value}"))
                .collect();
            notes.push(note(
                format!("header: {}", said.join(", ")),
                "from the file's header".to_string(),
            ));
        }
        for warning in &self.notes {
            notes.push(note(
                warning.clone(),
                "from the file's length and the spec".to_string(),
            ));
        }
        notes
    }
}

/// A spec's match conditions as the command line prints them: its chips, or
/// `no match` when only a choice (`--format`, b, B) picks it.
pub(crate) fn match_words(spec: &Spec) -> String {
    let chips = spec.match_chips();
    if chips.is_empty() {
        FORMAT_ONLY.to_string()
    } else {
        chips_plain(&chips)
    }
}

/// What a spec with no `match` says in place of its conditions.
pub const FORMAT_ONLY: &str = "no match";

/// `datui formats`, or `datui formats check SPEC [FILE]`: what to print, and the exit
/// code (non-zero when the check finds an error).
pub fn command(
    action: Option<&crate::cli::FormatsAction>,
    args: &crate::cli::Args,
    config: &crate::config::AppConfig,
) -> (String, i32) {
    let path = search_path_for(config);
    let registry = Registry::load(&path);
    match action {
        None => (registry.listing(&path), 0),
        Some(crate::cli::FormatsAction::Check { spec, file }) => {
            let options = crate::OpenOptions::from_args_and_config(args, config);
            match check(spec, file.as_deref(), &registry, &options) {
                Ok(text) => (text, 0),
                Err(text) => (text, 1),
            }
        }
    }
}

/// Rows `formats check` prints from a file.
const CHECK_ROWS: usize = 10;

/// Check the spec `named` (a file, or a name on the search path) and, given `file`,
/// read its first rows.
pub(crate) fn check(
    named: &str,
    file: Option<&Path>,
    registry: &Registry,
    options: &crate::OpenOptions,
) -> Result<String, String> {
    let as_file = Path::new(named);
    if let Some(dict) = fix_dict_named(named, registry)? {
        return check_fix(&dict, file);
    }
    if let Some(dbc) = dbc_named(named, registry)? {
        return check_dbc(&dbc, file);
    }
    let spec = if as_file.is_file() {
        Arc::new(Spec::load(as_file).map_err(|e| format!("error: {e}\n"))?)
    } else if let Some(spec) = registry.get(named) {
        spec.clone()
    } else {
        let mut said = format!("error: no spec file or format named {named}\n");
        if let Some(e) = registry.errors.iter().find(|e| {
            e.path
                .as_ref()
                .is_some_and(|p| p.file_stem() == as_file.file_stem())
        }) {
            said.push_str(&format!("error: {e}\n"));
        }
        return Err(said);
    };
    let mut out = format!("{}: ok\n", spec.name);
    if let Some(from) = &spec.path {
        out.push_str(&format!("  from {}\n", from.display()));
    }
    out.push_str(&format!("  matches {}\n", match_words(&spec)));
    if spec.is_delimited() {
        return crate::delimited_spec::check(&spec, file, CHECK_ROWS, options)
            .map(|rest| out.clone() + &rest)
            .map_err(|rest| out.clone() + &rest);
    }
    let named_fields = spec
        .records
        .fields
        .iter()
        .filter(|f| f.name.is_some())
        .count();
    out.push_str(&format!("  {named_fields} record fields"));
    if let Some(width) = given_width(&spec.records.fields) {
        let size = match spec.records.size {
            Some(Amount::Given(size)) => size,
            _ => width,
        };
        if spec.layout == Layout::Rows
            && spec
                .records
                .size
                .as_ref()
                .is_none_or(|s| matches!(s, Amount::Given(_)))
        {
            out.push_str(&format!(", {size} bytes a record"));
        }
    }
    out.push('\n');
    let Some(file) = file else {
        return Ok(out);
    };
    let compression = crate::CompressionFormat::from_extension(file).filter(|_| file.is_file());
    let shown = file.file_name().map_or_else(
        || file.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let choice = Choice {
        spec: spec.clone(),
        by: Chosen::SpecFile,
        also: Vec::new(),
    };
    // A compressed file is read from a copy, as the open reads it.
    let copy;
    let readable = match compression {
        None => file,
        Some(compression) => {
            copy = decompressed_copy(file, compression)
                .map_err(|e| format!("{out}error: {shown}: {e}\n"))?;
            copy.path()
        }
    };
    let read = read(readable, &shown, choice).map_err(|e| format!("{out}error: {e}\n"))?;
    for note in &read.notes {
        out.push_str(&format!("warning: {note}\n"));
    }
    if !read.header.values.is_empty() {
        let said: Vec<String> = read
            .header
            .values
            .iter()
            .map(|(name, value)| format!("{name} = {value}"))
            .collect();
        out.push_str(&format!("header: {}\n", said.join(", ")));
    }
    out.push_str(&format!("{} records\n", read.records.rows()));
    let df = read
        .records
        .collect(CHECK_ROWS)
        .map_err(|e| format!("{out}error: {e}\n"))?;
    out.push_str(&text_table(&df));
    Ok(out)
}

/// The FIX dictionary `named` names: a dictionary file, or one on the search path.
fn fix_dict_named(
    named: &str,
    registry: &Registry,
) -> Result<Option<Arc<crate::fix::dict::Dictionary>>, String> {
    let as_file = Path::new(named);
    if as_file.is_file() {
        return match crate::fix::dict::Dictionary::load(as_file) {
            Ok(dict) => Ok(dict.map(Arc::new)),
            Err(e) => Err(format!("error: {e}\n")),
        };
    }
    Ok(registry.fix_dict(named).cloned())
}

/// The DBC file `named` names: a `.dbc` file, a `kind = "dbc"` TOML file, or one on the
/// search path by its name.
fn dbc_named(named: &str, registry: &Registry) -> Result<Option<Arc<crate::dbc::Dbc>>, String> {
    let as_file = Path::new(named);
    if as_file.is_file() {
        // Any other file would parse as an empty DBC: only these two kinds are asked.
        let dbc_like = as_file
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("dbc") || e.eq_ignore_ascii_case("toml"));
        if !dbc_like {
            return Ok(None);
        }
        return match crate::dbc::load(as_file) {
            Ok(dbc) => Ok(dbc.map(Arc::new)),
            Err(e) => Err(format!("error: {e}\n")),
        };
    }
    Ok(registry
        .dbc
        .iter()
        .find(|found| found.dbc.name == named)
        .map(|found| found.dbc.clone()))
}

/// `formats check` of a DBC file: its messages and signals, what it passed over and
/// the interface it applies to; with `file`, a candump log, how many of its frames it
/// names and which messages.
fn check_dbc(dbc: &Arc<crate::dbc::Dbc>, file: Option<&Path>) -> Result<String, String> {
    use std::io::Read;
    let mut out = format!("{}: ok\n", dbc.name);
    if let Some(from) = &dbc.path {
        out.push_str(&format!("  from {}\n", from.display()));
    }
    if let Some(interface) = &dbc.interface {
        out.push_str(&format!("  matches interface {interface}\n"));
    }
    let signals: usize = dbc.messages.iter().map(|m| m.signals.len()).sum();
    out.push_str(&format!(
        "  {}, {}\n",
        crate::text_formats::count(dbc.messages.len() as u64, "message", "messages"),
        crate::text_formats::count(signals as u64, "signal", "signals"),
    ));
    for note in &dbc.notes {
        out.push_str(&format!("warning: {note}\n"));
    }
    let Some(file) = file else {
        return Ok(out);
    };
    let failed =
        |out: &str, e: &dyn std::fmt::Display| format!("{out}error: {}: {e}\n", file.display());
    let read = std::sync::atomic::AtomicU64::new(0);
    let mut bytes = Vec::new();
    crate::text_formats::open_reader(file, &crate::OpenOptions::default(), &read)
        .and_then(|mut reader| reader.read_to_end(&mut bytes).map_err(Into::into))
        .map_err(|e| failed(&out, &e))?;
    let index = crate::candump::index(&bytes).map_err(|e| failed(&out, &e))?;
    let layers = crate::candump::Layers {
        dbcs: vec![dbc.clone()],
    };
    let listing = crate::candump::Listing::resolve(&index, layers);
    let frames = index.keys.len();
    out.push_str(&format!(
        "{}, {} of them named by {}\n",
        crate::text_formats::count(frames as u64, "frame", "frames"),
        frames - listing.unknown,
        dbc.name
    ));
    let named: Vec<String> = listing
        .messages
        .iter()
        .map(|(name, (_, rows))| format!("{name} ({})", rows.len()))
        .collect();
    if !named.is_empty() {
        out.push_str(&format!("messages in the log: {}\n", named.join(", ")));
    }
    Ok(out)
}

/// `formats check` of a FIX dictionary: what it names and matches; with `file`, how
/// many of the log's messages it applies to and the tags it names there.
fn check_fix(
    dict: &Arc<crate::fix::dict::Dictionary>,
    file: Option<&Path>,
) -> Result<String, String> {
    use std::io::Read;
    let mut out = format!("{}: ok\n", dict.name);
    if let Some(from) = &dict.path {
        out.push_str(&format!("  from {}\n", from.display()));
    }
    let summary = dict.matcher.summary();
    if !summary.is_empty() {
        out.push_str(&format!("  matches {summary}\n"));
    }
    let enums = dict.tags.values().filter(|t| !t.enums.is_empty()).count();
    out.push_str(&format!("  {} tags, {enums} with enums\n", dict.tags.len()));
    let Some(file) = file else {
        return Ok(out);
    };
    let read = std::sync::atomic::AtomicU64::new(0);
    let mut reader = crate::text_formats::open_reader(file, &crate::OpenOptions::default(), &read)
        .map_err(|e| format!("{out}error: {}: {e}\n", file.display()))?;
    let mut log = crate::fix::FixReader::new(crate::fix::dict::Layers::new(vec![dict.clone()]));
    let mut chunk = vec![0u8; 1 << 16];
    loop {
        let n = reader
            .read(&mut chunk)
            .map_err(|e| format!("{out}error: {}: {e}\n", file.display()))?;
        if n == 0 {
            break;
        }
        log.push(&chunk[..n]);
        let _ = log.take_batch();
    }
    let _ = log.finish();
    let stats = log.stats();
    out.push_str(&format!(
        "{} messages, {} of them matched by {}\n",
        stats.messages,
        stats.applied.get(1).copied().unwrap_or(0),
        dict.name
    ));
    let named: Vec<String> = log
        .tag_names()
        .into_iter()
        .filter(|(_, _, by)| *by == 1)
        .map(|(tag, name, _)| format!("{tag} {name}"))
        .collect();
    if !named.is_empty() {
        out.push_str(&format!("names in the log: {}\n", named.join(", ")));
    }
    Ok(out)
}

/// `df` as plain text: a row of names, then a row per record, columns aligned.
pub(crate) fn text_table(df: &polars::prelude::DataFrame) -> String {
    let mut rows: Vec<Vec<String>> = vec![
        df.get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect(),
    ];
    for i in 0..df.height() {
        rows.push(
            df.columns()
                .iter()
                .map(|c| match c.get(i) {
                    Ok(polars::prelude::AnyValue::String(s)) => s.to_string(),
                    Ok(polars::prelude::AnyValue::StringOwned(s)) => s.to_string(),
                    Ok(v) => v.to_string(),
                    Err(_) => String::new(),
                })
                .collect(),
        );
    }
    let widths: Vec<usize> = (0..rows[0].len())
        .map(|c| rows.iter().map(|r| r[c].chars().count()).max().unwrap_or(0))
        .collect();
    let mut out = String::new();
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        out.push_str(cells.join("  ").trim_end());
        out.push('\n');
    }
    out
}

/// `path` decompressed into a temporary file.
fn decompressed_copy(
    path: &Path,
    compression: crate::CompressionFormat,
) -> std::io::Result<tempfile::NamedTempFile> {
    use std::io::Read as _;
    let file = std::fs::File::open(path)?;
    let mut reader: Box<dyn std::io::Read> = match compression {
        crate::CompressionFormat::Gzip => Box::new(flate2::read::GzDecoder::new(file)),
        crate::CompressionFormat::Zstd => Box::new(zstd::Decoder::new(file)?),
        crate::CompressionFormat::Bzip2 => Box::new(bzip2::read::BzDecoder::new(file)),
        crate::CompressionFormat::Xz => Box::new(xz2::read::XzDecoder::new(file)),
    };
    let mut copy = tempfile::NamedTempFile::new()?;
    std::io::copy(&mut reader.by_ref(), copy.as_file_mut())?;
    Ok(copy)
}
