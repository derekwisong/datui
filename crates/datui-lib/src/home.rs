//! The home screen: datui's answer to "I want to look at my data", before you have
//! had to answer "where is it, exactly".
//!
//! # Roots
//!
//! Code lives in your working directory; the interesting datasets usually do not.
//! They are on a mount, a NAS, a scratch volume. So the home screen is built around
//! *roots* — places to look — gathered from three sources, none of which require
//! maintaining a catalogue:
//!
//! 1. **Configured** — `[data] directories`, a `PATH`-shaped list of places.
//! 2. **The working directory** — free, and right for local exports and fixtures.
//! 3. **Derived from recents** — if you opened `/mnt/data/sales/`, then `/mnt/data`
//!    is now somewhere datui knows to look.
//!
//! The third is what bridges "code here, data there" with no configuration at all:
//! you establish the association by using it once. It is derived state, so it costs
//! nothing to be wrong and nothing to throw away.

use crate::discover::{self, Entry, EntryKind};
use std::path::{Path, PathBuf};

/// Where a root came from. Shown subtly in the UI so the list is explicable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootOrigin {
    Cwd,
    Configured,
    Recent,
    /// Derived from the desktop's own recently-used list.
    Desktop,
}

impl RootOrigin {
    pub fn note(self) -> &'static str {
        match self {
            RootOrigin::Cwd => "current directory",
            RootOrigin::Configured => "configured",
            RootOrigin::Recent => "recent",
            RootOrigin::Desktop => "opened elsewhere",
        }
    }
}

/// Directories holding data files that the desktop has recorded you opening.
///
/// Reads `recently-used.xbel`, the freedesktop standard that file managers and GTK
/// applications write. It is here to solve one problem: a fresh install has no
/// recents of its own, so it has nowhere to point you.
///
/// **Only the directories are used, never the files.** That distinction is the whole
/// design. The list contains whatever you last opened anywhere on the machine, which
/// is frequently something you would not want appearing on a screen you demo — a
/// bank export, a password vault dump. Surfacing `~/Downloads` as a place to look is
/// useful; listing what is in it, unbidden, is not datui's business.
pub fn desktop_recent_dirs() -> Vec<PathBuf> {
    let Some(data_dir) = dirs::data_dir() else {
        return Vec::new();
    };
    let path = data_dir.join("recently-used.xbel");
    let Ok(contents) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    dirs_from_xbel(&contents)
}

/// Extract directories of data files from XBEL content.
///
/// Split out from the filesystem read so it can be tested directly. Scans for
/// `href="file://…"` rather than parsing XML: the attribute is all that is needed,
/// and a hand-rolled scan avoids taking an XML dependency for one file.
pub fn dirs_from_xbel(contents: &str) -> Vec<PathBuf> {
    const PREFIX: &str = "href=\"file://";
    let mut dirs: Vec<PathBuf> = Vec::new();

    for chunk in contents.split(PREFIX).skip(1) {
        let Some(end) = chunk.find('"') else { continue };
        let decoded = percent_decode(&chunk[..end]);
        let file = PathBuf::from(decoded);
        // Only files datui could actually open, and only ones still present.
        if !crate::discover::is_data_file(&file) || !file.is_file() {
            continue;
        }
        let Some(parent) = file.parent() else {
            continue;
        };
        if parent.as_os_str().is_empty() {
            continue;
        }
        let parent = parent.to_path_buf();
        if !dirs.contains(&parent) {
            dirs.push(parent);
        }
    }

    dirs
}

/// Decode `%20`-style escapes in a file URI.
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(byte) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Filesystem types that live over a network. Listing one can be slow, and it can
/// stop working entirely when the link or the server goes away — worth saying so next
/// to a root rather than leaving the user to wonder why a listing is empty or slow.
/// Directories promoted to roots because something in them was opened recently.
///
/// Every root costs a directory listing on every rebuild. Recents are capped at
/// fifty, so fifty scattered opens meant fifty listings — locally a stutter, on a
/// network share the difference between instant and unusable. The most recent eight
/// distinct directories cover where someone is actually working; older places stay
/// in `RECENT` as individual datasets and remain reachable by typing a path.
const MAX_RECENT_ROOTS: usize = 8;

/// Whether `path` is somewhere reading it could block: an object-store or HTTP URL,
/// or a directory on a network filesystem.
///
/// This is the predicate the home screen uses to decide what it may touch on the
/// interface thread. It answers from the string and the mount table alone, never by
/// reaching for the thing itself.
pub fn is_remote_path(path: &Path) -> bool {
    !matches!(
        crate::source::input_source(path),
        crate::source::InputSource::Local(_)
    ) || is_network_path(path)
}

/// Whether `path` sits on a network filesystem, according to the mount table.
///
/// Reads `/proc/self/mountinfo` and takes the longest mount point that is a prefix of
/// the path. Returns false wherever that file is unavailable or unparseable, so this
/// is a hint and never a gate.
pub fn is_network_path(path: &Path) -> bool {
    let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    network_fs_for_test(&mountinfo, path)
}

/// The mount-table logic, separated from reading `/proc` so it can be tested against
/// a fixture — the interesting cases (an NFS share shadowing an autofs entry at the
/// same path) are awkward to arrange on a real machine.
#[doc(hidden)]
pub fn network_fs_for_test(mountinfo: &str, path: &Path) -> bool {
    crate::locality::Mounts::parse(mountinfo).is_network(path)
}

/// A place datui will look, and whether it can currently be read.
#[derive(Debug, Clone)]
pub struct Root {
    pub path: PathBuf,
    pub origin: RootOrigin,
    /// True when the root is on a network filesystem.
    pub network: bool,
    /// False when the directory cannot be read — an unmounted NAS, a deleted
    /// scratch dir. Shown rather than hidden: "the mount is down" is information.
    pub available: bool,
}

/// A titled group of rows on the home screen.
#[derive(Debug, Clone)]
pub struct Section {
    pub title: String,
    /// Path shown beside the title, for root sections.
    pub subtitle: Option<String>,
    pub rows: Vec<Entry>,
    /// Set when a root could not be read, so the UI can say why it is empty.
    pub unavailable: bool,
}

/// What measuring a dataset yielded: rows, columns, and total size, each absent when
/// it cannot be known without reading the data.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Measured {
    pub rows: Option<usize>,
    pub cols: Option<usize>,
    pub size: Option<u64>,
    /// Column names, when the format gave them up for free.
    pub columns: Vec<String>,
}

/// How rows are ordered within each section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SortMode {
    /// Recency under Recent, name under a directory — what each section is naturally
    /// ordered by.
    #[default]
    Natural,
    /// Largest first: the question is "what is big in here".
    Size,
    /// Most recently changed first: the question is "what moved".
    Modified,
    /// Most rows first.
    Rows,
}

impl SortMode {
    /// What this mode is doing *here*.
    ///
    /// The default orders each section by whatever suits it — recency for a list of
    /// things you opened, name for a directory you are reading. Labelling that
    /// "natural" names the idea rather than the behaviour, and leaves the user to
    /// guess which of the two they are looking at. So the label follows the cursor.
    pub fn label_in(self, section_is_recency_ordered: bool) -> &'static str {
        match self {
            SortMode::Natural if section_is_recency_ordered => "recent",
            SortMode::Natural => "name",
            SortMode::Size => "size",
            SortMode::Modified => "modified",
            SortMode::Rows => "rows",
        }
    }

    pub fn next(self) -> Self {
        match self {
            SortMode::Natural => SortMode::Size,
            SortMode::Size => SortMode::Modified,
            SortMode::Modified => SortMode::Rows,
            SortMode::Rows => SortMode::Natural,
        }
    }
}

/// One line of the home screen. Headers are selectable so a section can be
/// collapsed and expanded from the keyboard.
#[derive(Debug, Clone, Copy)]
pub enum Row<'a> {
    Header {
        section: usize,
        /// Rows this section holds under the current filter.
        matches: usize,
        collapsed: bool,
    },
    Entry {
        section: usize,
        entry: &'a Entry,
    },
}

impl Row<'_> {
    pub fn section(&self) -> usize {
        match self {
            Row::Header { section, .. } | Row::Entry { section, .. } => *section,
        }
    }
}

/// Home screen state.
#[derive(Debug)]
pub struct HomeState {
    pub sections: Vec<Section>,
    /// Fuzzy filter over every row in every section.
    pub filter: String,
    /// Index into the flattened list of currently visible rows.
    pub selected: usize,
    pub scroll: usize,
    /// True while the user is typing a path directly.
    pub path_input_active: bool,
    pub path_input: String,
    /// Directory the user has descended into, if any. `None` means the root listing.
    pub browsing: Option<PathBuf>,
    /// Transient message (e.g. a path that does not exist).
    pub status: Option<String>,
    /// How a path is judged to be network-backed. Swappable so the "never touch a
    /// remote path on this thread" rule can be tested without a remote.
    pub network_check: fn(&Path) -> bool,
    /// Roots the current listing was built from, in order.
    pub root_paths: Vec<PathBuf>,
    /// Network roots whose listing has come back, keyed by path.
    pub probed: std::collections::HashMap<PathBuf, Vec<Entry>>,
    /// Network roots that did not answer.
    pub unreachable: std::collections::HashSet<PathBuf>,
    /// How rows are ordered inside each section.
    pub sort: SortMode,
    /// True while a listing is being built on a worker. The previous listing stays on
    /// screen meanwhile, so a refresh never blanks the view.
    pub listing_in_flight: bool,
    /// True while a measurement batch is out, so only one is in flight at a time.
    pub measure_in_flight: bool,
    /// Set while rows on screen are still unmeasured, so the main loop knows to draw
    /// another frame and measure the next batch.
    pub pending_enrich: bool,
    /// Row and column counts already read, keyed by path. Reading a Parquet footer
    /// is cheap; reading several hundred of them is not, so results are kept for the
    /// session and each dataset is measured once.
    pub enriched: std::collections::HashMap<PathBuf, Measured>,
    /// Titles of sections the user has collapsed. Keyed by title rather than index
    /// so the state survives a rebuild, which reorders and renumbers sections.
    /// Use [`HomeState::toggle_collapsed`] and [`HomeState::set_collapsed`] rather
    /// than touching this directly.
    pub collapsed: std::collections::HashSet<String>,
    /// Datasets found by walking below the working directory.
    pub search: SearchState,
}

/// The result of one recursive walk below the working directory.
///
/// Held apart from `sections` because it outlives them: a listing is rebuilt whenever
/// a probe answers or a measurement lands, and re-walking the tree each time would be
/// exactly the per-keystroke cost this feature exists to avoid.
#[derive(Debug, Clone, Default)]
pub struct SearchState {
    /// Where the walk started. `None` means no search has been asked for yet.
    pub root: Option<PathBuf>,
    /// Every dataset found so far, unfiltered. The filter runs over this in memory.
    pub results: Vec<Entry>,
    /// Directory entries examined, for the progress note.
    pub scanned: usize,
    /// A walk is out. Results may still be arriving.
    pub running: bool,
    /// The walk has finished, successfully or against a limit.
    pub done: bool,
    /// Why the walk stopped short, when it did.
    pub limited: Option<String>,
}

impl SearchState {
    /// Forget everything, because the place being searched has changed.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl Default for HomeState {
    fn default() -> Self {
        Self {
            sections: Vec::new(),
            filter: String::new(),
            selected: 0,
            scroll: 0,
            path_input_active: false,
            path_input: String::new(),
            browsing: None,
            status: None,
            network_check: is_remote_path,
            sort: SortMode::default(),
            listing_in_flight: false,
            measure_in_flight: false,
            root_paths: Vec::new(),
            probed: std::collections::HashMap::new(),
            unreachable: std::collections::HashSet::new(),
            pending_enrich: false,
            enriched: std::collections::HashMap::new(),
            collapsed: std::collections::HashSet::new(),
            search: SearchState::default(),
        }
    }
}

/// Everything [`build_listing`] needs, gathered on the interface thread from state it
/// already has, so the worker never reaches back into the app.
#[derive(Debug, Clone)]
pub struct ListingRequest {
    pub config_dirs: Vec<PathBuf>,
    pub recents: Vec<PathBuf>,
    pub desktop_dirs: Vec<PathBuf>,
    pub browsing: Option<PathBuf>,
    pub probed: std::collections::HashMap<PathBuf, Vec<Entry>>,
    pub unreachable: std::collections::HashSet<PathBuf>,
    pub network_check: fn(&Path) -> bool,
    /// What datui measured on a previous run. A row whose size and modification time
    /// still match is filled in from here, so the screen has counts and column names
    /// before anything has been read this time.
    pub known: std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
}

/// What a listing pass produced.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    pub sections: Vec<Section>,
    pub root_paths: Vec<PathBuf>,
}

/// Fold a measured probe into the record kept for a row.
pub fn measured_from(probe: &Entry, original: &Entry) -> Measured {
    Measured {
        rows: probe.rows,
        cols: probe.cols,
        size: probe.size.or(original.size),
        columns: probe.columns.clone(),
    }
}

/// A row a completed probe already produced for this exact path, if any.
fn probed_entry(
    probed: &std::collections::HashMap<PathBuf, Vec<Entry>>,
    path: &Path,
) -> Option<Entry> {
    probed.values().flatten().find(|e| e.path == path).cloned()
}

/// Build the home listing.
///
/// A free function taking everything it needs, so it can run on a worker thread. It
/// is the only place the home screen touches the filesystem, and it must never be
/// called from the thread that draws — a directory on a wedged mount, a FIFO, a
/// failing disk all block here, and none of them can be enumerated in advance.
pub fn build_listing(request: &ListingRequest) -> Listing {
    let ListingRequest {
        config_dirs,
        recents,
        desktop_dirs,
        browsing,
        probed,
        unreachable,
        network_check,
        known,
    } = request;
    let network_check = *network_check;
    // One read of the mount table for the whole listing. It is a kernel-generated
    // file, so consulting it cannot block on the filesystem it describes -- which is
    // the entire reason it is safe to ask about a share that has stopped answering.
    let mounts = crate::locality::Mounts::current();
    let mut sections: Vec<Section> = Vec::new();
    let mut root_paths: Vec<PathBuf> = Vec::new();

    // Descended into a directory: show only that.
    if let Some(dir) = browsing.clone() {
        let rows = discover::scan_dir(&dir);
        sections.push(Section {
            title: display_path(&dir),
            subtitle: None,
            rows,
            unavailable: false,
        });
        annotate(&mut sections, known, network_check, &mounts);
        return Listing {
            sections,
            root_paths,
        };
    }

    // Recents that still exist, most recent first.
    let recent_rows: Vec<Entry> = recents
        .iter()
        // `exists()` stats the path, so a remote entry is taken on trust and
        // dropped later only if its probe says it is gone.
        .filter(|p| network_check(p) || p.exists())
        // No display cap. The store already bounds this, the header states the
        // count, and the section folds — an invisible limit would just hide recents
        // with nothing to say it had.
        .map(|p| {
            // A probe of the containing root has already classified and measured
            // this; reuse it, so the same dataset does not read as `hive` under
            // its directory and `dir` under Recent.
            if let Some(known) = probed_entry(probed, p) {
                return known;
            }
            entry_for_path(p, network_check(p))
        })
        .collect();
    if !recent_rows.is_empty() {
        sections.push(Section {
            title: "Recent".to_string(),
            subtitle: None,
            rows: recent_rows,
            unavailable: false,
        });
    }

    // Desktop-derived places are collected rather than expanded — see below.
    let mut elsewhere: Vec<Entry> = Vec::new();

    let roots = HomeState::roots_with(config_dirs, recents, desktop_dirs, network_check);
    root_paths = roots.iter().map(|r| r.path.clone()).collect();
    for root in roots {
        // A place the desktop mentioned is listed as a directory to step into,
        // never expanded. Its contents are whatever you last opened anywhere on
        // the machine, which is regularly something you would not want appearing
        // on a screen you are sharing. Naming the place is useful; showing what
        // is in it, unasked, is not datui's business. Pressing Enter is the ask.
        if root.origin == RootOrigin::Desktop {
            if root.available {
                let mut entry = Entry::directory(&root.path);
                // Show the place, not just its leaf: "~/Downloads" says more
                // than "Downloads" when the section has no path of its own.
                entry.name = display_path(&root.path);
                elsewhere.push(entry);
            }
            continue;
        }

        // A derived root with nothing in it adds noise; keep configured and cwd
        // roots always, since the user named them or is standing in them.
        // A remote root is listed from whatever its background probe returned,
        // and left empty until then. Scanning it here is the thing that freezes
        // datui on a slow or absent network.
        // A local root is listed here; a remote one only reports what its background
        // probe already returned. Truncation is knowable for the local scan, which is
        // where a directory big enough to hit the cap realistically lives.
        let mut truncated = false;
        let rows = if root.network {
            probed.get(&root.path).cloned().unwrap_or_default()
        } else if root.available {
            let scan = discover::scan_dir_bounded(&root.path);
            truncated = scan.truncated;
            scan.entries
        } else {
            Vec::new()
        };
        // An empty derived root is noise and goes. One that cannot be *read* stays:
        // a network share that has stopped answering is the case the section
        // heading exists to report, and silently dropping it is the worst answer.
        let unreachable = root.network && unreachable.contains(&root.path);
        let waiting = root.network && !unreachable && !probed.contains_key(&root.path);
        if rows.is_empty() && root.origin == RootOrigin::Recent && root.available && !root.network {
            continue;
        }
        // A network root is worth flagging: it is the one that will be slow, and
        // the one that can stop answering.
        // Naming the filesystem rather than saying "network" costs one word and says
        // considerably more: nfs4, cifs and fuse.sshfs fail in different ways, and
        // none of them behaves like the tmpfs someone staged a dataset on.
        // Name the filesystem when the mount table agrees this is remote. When it
        // does not -- an unmounted automount, a path judged remote some other way --
        // fall back to the plain word, because losing the warning to gain a more
        // precise label is the wrong trade.
        let described = mounts.describe(&root.path);
        let fstype = if described.network() {
            described.fstype
        } else {
            "network".to_string()
        };
        let mut subtitle = if waiting {
            format!("{fstype} · checking · {}", root.origin.note())
        } else if root.network {
            format!("{fstype} · {}", root.origin.note())
        } else {
            root.origin.note().to_string()
        };
        // Say when the list is a prefix. A directory cut off at the cap otherwise
        // looks exactly like one that happens to hold that many things.
        if truncated {
            subtitle = format!("first {} · {}", discover::MAX_ENTRIES_PER_DIR, subtitle);
        }
        sections.push(Section {
            title: display_path(&root.path),
            subtitle: Some(subtitle),
            rows,
            unavailable: !root.available || unreachable,
        });
    }

    if !elsewhere.is_empty() {
        sections.push(Section {
            title: "Elsewhere".to_string(),
            subtitle: Some("opened elsewhere · press Enter to look".to_string()),
            rows: elsewhere,
            unavailable: false,
        });
    }

    // Fill in whatever was measured before and still matches. `scan_dir` already
    // stat'ed every row, so verifying the fingerprint costs nothing.
    //
    // The mount table is read once for the whole listing rather than per row.
    // Resolving a path against it is string work, and it is a kernel-generated file,
    // so nothing here can block on a filesystem that has stopped answering.
    annotate(&mut sections, known, network_check, &mounts);

    Listing {
        sections,
        root_paths,
    }
}

/// Apply a cached measurement to a row.
///
/// For a local row the fingerprint is checked: both size and modification time must
/// still agree, so a dataset that has changed invalidates itself. `scan_dir` already
/// stat'ed the row, so this costs nothing.
///
/// A remote row has no fingerprint to check, because checking it means a `stat` on a
/// path that may not answer. Its cached facts are used as-is. That is the right
/// trade: this is a cache of what a dataset looked like, the entry was written from a
/// real read, and a stale row count is a far better answer than an empty one for the
/// datasets that are hardest to reach and most worth remembering.
/// Fill every row in with what is already known about it, and with where it lives.
///
/// Called from each of `build_listing`'s exits. Having one function rather than a
/// loop at each return is the difference between adding a new exit and adding a new
/// exit whose rows silently lack half their facts.
fn annotate(
    sections: &mut [Section],
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    network_check: fn(&Path) -> bool,
    mounts: &crate::locality::Mounts,
) {
    for section in sections {
        for row in &mut section.rows {
            apply_known_facts(row, known, network_check(&row.path));
            row.cost.source = Some(mounts.describe(&row.path).fstype);
        }
    }
}

fn apply_known_facts(
    row: &mut Entry,
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    remote: bool,
) {
    let Some(facts) = known.get(&row.path) else {
        return;
    };

    if !remote {
        let same_bytes = row.size.map(|s| s == facts.size).unwrap_or(false)
            && row
                .modified
                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() == facts.mtime)
                .unwrap_or(false);
        if !same_bytes {
            return;
        }
    }

    row.rows = facts.rows;
    row.cols = facts.cols;
    if !facts.columns.is_empty() {
        row.columns = facts.columns.clone();
    }
    // The source is filled in from the live mount table afterwards, so what is
    // restored here is only what the file itself said about itself.
    let source = row.cost.source.take();
    row.cost = facts.cost.clone();
    row.cost.source = source;
    if remote {
        // A remote row was never stat'ed, so these are all it has.
        row.size = row.size.or(Some(facts.size));
        // What it was last seen to be, rather than what its name suggests. Guessing
        // here is how the same dataset ends up reading `hive` in one section and
        // something else in another.
        if row.kind == EntryKind::Unknown {
            if let Some(kind) = facts.kind {
                row.kind = kind;
            }
        }
    }
}

/// The record to keep for a row that has just been measured.
pub fn facts_for(entry: &Entry) -> Option<(PathBuf, crate::cache::DatasetFacts)> {
    let size = entry.size?;
    let mtime = entry
        .modified?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    if entry.rows.is_none() && entry.columns.is_empty() && entry.cost == Default::default() {
        return None; // Nothing learned worth keeping.
    }
    Some((
        entry.path.clone(),
        crate::cache::DatasetFacts {
            mtime,
            size,
            rows: entry.rows,
            cols: entry.cols,
            columns: entry.columns.clone(),
            kind: Some(entry.kind),
            // The source is where it is *now*, not where it was when measured: a
            // path can move between mounts, and a stale answer to "will this be
            // slow" is worse than no answer.
            cost: crate::discover::Cost {
                source: None,
                ..entry.cost.clone()
            },
        },
    ))
}

/// How well an entry answers the filter, by name or by column.
///
/// Searching column names is what turns a list of files into something you can ask a
/// question of: "which of these has a `customer_id`?" is the question a data person
/// actually has, and the answer is already in the Parquet footer datui read to get
/// the row count. A name match always outranks a column match, so typing a dataset's
/// name still finds the dataset.
///
/// Higher is better, as in fzf — see [`crate::fuzzy`] for why datui scores the way
/// that program does.
pub fn match_score(filter: &str, entry: &Entry) -> Option<i32> {
    if let Some(m) = crate::fuzzy::best_match(filter, &entry.name) {
        return Some(m.score);
    }
    if filter.is_empty() {
        return Some(0);
    }
    // Ranked below every name match, so column hits are an addition to what the
    // filter did rather than a dilution of it. Column matching is a substring test,
    // which has no score of its own worth comparing.
    matching_column(filter, entry).map(|_| -COLUMN_MATCH_PENALTY)
}

/// Distance by which a column match sits below any name match.
///
/// Larger than any score a name match can reach, so the two never interleave.
const COLUMN_MATCH_PENALTY: i32 = 1_000_000;

/// The first column of `entry` that contains `filter`, case-insensitively.
///
/// Substring rather than subsequence: a column name is short and specific, and a
/// fuzzy match over dozens of them matches nearly everything.
pub fn matching_column<'a>(filter: &str, entry: &'a Entry) -> Option<&'a str> {
    if filter.is_empty() {
        return None;
    }
    let needle = filter.to_lowercase();
    entry
        .columns
        .iter()
        .find(|c| c.to_lowercase().contains(&needle))
        .map(|c| c.as_str())
}

/// Character positions in `haystack` that `needle` matched, for highlighting.
///
/// Taken from the same alignment that produced the score, so the marks are always on
/// the characters that were actually scored.
pub fn fuzzy_positions(needle: &str, haystack: &str) -> Vec<usize> {
    crate::fuzzy::best_match(needle, haystack)
        .map(|m| m.positions)
        .unwrap_or_default()
}

/// Character positions of the first case-insensitive occurrence of `needle`.
///
/// Column matching is a substring test, not a subsequence one, so highlighting it has
/// to be too — otherwise the marks land on letters that had nothing to do with why
/// the row is on screen.
pub fn substring_positions(needle: &str, haystack: &str) -> Vec<usize> {
    if needle.is_empty() {
        return Vec::new();
    }
    let hay: Vec<char> = haystack.to_lowercase().chars().collect();
    let need: Vec<char> = needle.to_lowercase().chars().collect();
    if need.len() > hay.len() {
        return Vec::new();
    }
    for start in 0..=(hay.len() - need.len()) {
        if hay[start..start + need.len()] == need[..] {
            return (start..start + need.len()).collect();
        }
    }
    Vec::new()
}

/// Whether and how well `needle` matches `haystack`, higher being better.
///
/// A thin name over [`crate::fuzzy::best_match`]. Everything that ranks or highlights
/// goes through that one function, which is what keeps the two from drifting apart.
pub fn fuzzy_score(needle: &str, haystack: &str) -> Option<i32> {
    crate::fuzzy::best_match(needle, haystack).map(|m| m.score)
}

impl HomeState {
    /// Gather roots from config, cwd, and the directories of recent datasets.
    ///
    /// Order matters and is deliberate: configured places first (you said they
    /// matter), then directories implied by what you have actually opened, then the
    /// working directory. Duplicates collapse to their highest-priority origin.
    pub fn roots(
        config_dirs: &[PathBuf],
        recents: &[PathBuf],
        desktop_dirs: &[PathBuf],
    ) -> Vec<Root> {
        Self::roots_with(config_dirs, recents, desktop_dirs, is_remote_path)
    }

    /// As [`HomeState::roots`], with the network test injected.
    pub fn roots_with(
        config_dirs: &[PathBuf],
        recents: &[PathBuf],
        desktop_dirs: &[PathBuf],
        is_network: fn(&Path) -> bool,
    ) -> Vec<Root> {
        let mut roots: Vec<Root> = Vec::new();
        let mut seen: Vec<PathBuf> = Vec::new();

        let push =
            |path: PathBuf, origin: RootOrigin, roots: &mut Vec<Root>, seen: &mut Vec<PathBuf>| {
                // The network test reads only the mount table, so it is safe on a
                // path that would otherwise block.
                let network = is_network(&path);

                // Canonicalising and listing both touch the filesystem. On an
                // unreachable NFS share those block — for seconds on a `soft` mount,
                // and indefinitely and uninterruptibly on a `hard` one, which is the
                // default. Nothing on the interface thread may do that, so a remote
                // root is taken at face value and probed in the background instead.
                let key = if network {
                    path.clone()
                } else {
                    path.canonicalize().unwrap_or_else(|_| path.clone())
                };
                if seen.contains(&key) {
                    return;
                }
                seen.push(key);

                let available = if network {
                    true // unknown until probed; assumed present so it is listed
                } else {
                    std::fs::read_dir(&path).is_ok()
                };
                roots.push(Root {
                    path,
                    origin,
                    available,
                    network,
                });
            };

        let cwd = std::env::current_dir().ok();
        let cwd_key = cwd.as_ref().map(|c| {
            if is_network(c) {
                c.clone()
            } else {
                c.canonicalize().unwrap_or_else(|_| c.clone())
            }
        });

        // Where you are comes first. Standing in a directory is the strongest
        // statement of what you are working on right now — stronger than a directory
        // configured months ago, and far stronger than one listed only because
        // something in it was opened once. Ordering it last meant a recent root
        // holding sixty files buried the very place the user had just cd'd into.
        //
        // Claiming it here also keeps its own label when a recent dataset lives there
        // too: "current directory" tells you more than "recent" does.
        if let Some(cwd) = cwd.clone() {
            push(cwd, RootOrigin::Cwd, &mut roots, &mut seen);
        }

        for dir in config_dirs {
            push(dir.clone(), RootOrigin::Configured, &mut roots, &mut seen);
        }

        // A recent dataset implies its containing directory is a place worth showing.
        // `recents` is most-recent-first, so the newest distinct directories win the
        // budget and the rest fall off the end.
        let mut derived = 0usize;
        for recent in recents {
            if derived >= MAX_RECENT_ROOTS {
                break;
            }
            if let Some(parent) = recent.parent() {
                if parent.as_os_str().is_empty() {
                    continue;
                }
                let key = if is_network(parent) {
                    parent.to_path_buf()
                } else {
                    parent
                        .canonicalize()
                        .unwrap_or_else(|_| parent.to_path_buf())
                };
                if Some(&key) == cwd_key.as_ref() {
                    continue;
                }
                let before = roots.len();
                push(
                    parent.to_path_buf(),
                    RootOrigin::Recent,
                    &mut roots,
                    &mut seen,
                );
                // Only a directory that was actually added spends budget; fifty
                // recents from one directory still cost one root.
                if roots.len() > before {
                    derived += 1;
                }
            }
        }

        // Last, and weakest: places the desktop says you have opened data from. Only
        // useful before datui has recents of its own, so it should never outrank one.
        for dir in desktop_dirs {
            push(dir.clone(), RootOrigin::Desktop, &mut roots, &mut seen);
        }

        roots
    }

    /// Build the home listing.
    ///
    /// Recents lead, because for data on a mount the thing you want is almost always
    /// something you have opened before. Roots follow, each scanned one level deep.
    pub fn rebuild(&mut self, config_dirs: &[PathBuf], recents: &[PathBuf]) {
        self.rebuild_with(config_dirs, recents, &[])
    }

    /// As [`HomeState::rebuild`], plus directories derived from the desktop's own
    /// recently-used list.
    pub fn rebuild_with(
        &mut self,
        config_dirs: &[PathBuf],
        recents: &[PathBuf],
        desktop_dirs: &[PathBuf],
    ) {
        let request = ListingRequest {
            config_dirs: config_dirs.to_vec(),
            recents: recents.to_vec(),
            desktop_dirs: desktop_dirs.to_vec(),
            browsing: self.browsing.clone(),
            probed: self.probed.clone(),
            unreachable: self.unreachable.clone(),
            network_check: self.network_check,
            // The synchronous path is for tests and library callers; it consults no
            // cache, so what it produces is exactly what is on disk right now.
            known: Default::default(),
        };
        let listing = build_listing(&request);
        self.apply_listing(listing);
    }

    /// Install a listing built elsewhere, keeping the cursor on whatever it was on.
    pub fn apply_listing(&mut self, listing: Listing) {
        let previous = self.selected_entry().map(|e| e.path);
        self.sections = listing.sections;
        self.root_paths = listing.root_paths;
        // A rebuild replaces every section, and search results outlive rebuilds —
        // they came from a walk, not from this listing. Put them back.
        self.sync_search_section();

        // Keep the cursor on the same dataset across a refresh; landing back at the
        // top every time a background result arrives makes the screen unusable.
        if let Some(path) = previous {
            if let Some(idx) = self
                .visible()
                .iter()
                .position(|r| matches!(r, Row::Entry { entry, .. } if entry.path == path))
            {
                self.selected = idx;
                return;
            }
        }
        self.select_first_entry();
    }

    /// Rows currently passing the filter, flattened, as `(section index, row)`.
    /// Whether a section is collapsed.
    pub fn is_collapsed(&self, section: usize) -> bool {
        self.sections
            .get(section)
            .is_some_and(|s| self.collapsed.contains(&s.title))
    }

    /// Collapse or expand a section.
    pub fn toggle_collapsed(&mut self, section: usize) {
        let Some(title) = self.sections.get(section).map(|s| s.title.clone()) else {
            return;
        };
        if !self.collapsed.remove(&title) {
            self.collapsed.insert(title);
        }
    }

    pub fn set_collapsed(&mut self, section: usize, collapsed: bool) {
        let Some(title) = self.sections.get(section).map(|s| s.title.clone()) else {
            return;
        };
        if collapsed {
            self.collapsed.insert(title);
        } else {
            self.collapsed.remove(&title);
        }
    }

    /// Lines currently on screen: a header per non-empty section, followed by its
    /// matching rows unless it is collapsed.
    ///
    /// Results stay grouped even while filtering. Ranking them across sections would
    /// read better as a hit list, but it costs the one thing the grouping is for —
    /// seeing *where* a dataset lives — and a name on its own rarely says that.
    /// Title of the section holding recursive search results.
    ///
    /// A constant because collapse state is keyed by title, and because the renderer
    /// and the tests both need to name it.
    pub const SEARCH_SECTION: &'static str = "Found below";

    /// Put the current search results into `sections`, or take them out.
    ///
    /// Called after every rebuild and every batch of results. The section only exists
    /// while there is a filter: with none, every row matches, and twenty thousand
    /// matches is not a home screen.
    pub fn sync_search_section(&mut self) {
        self.sections.retain(|s| s.title != Self::SEARCH_SECTION);

        if self.filter.is_empty() || self.search.root.is_none() {
            return;
        }
        if self.search.results.is_empty() && !self.search.running {
            return;
        }

        // A dataset already on screen under the directory it lives in should not
        // appear a second time under the search. The search is for what you could
        // not otherwise see.
        let listed: std::collections::HashSet<&PathBuf> = self
            .sections
            .iter()
            .flat_map(|s| s.rows.iter().map(|r| &r.path))
            .collect();

        let rows: Vec<Entry> = self
            .search
            .results
            .iter()
            .filter(|e| !listed.contains(&e.path))
            .cloned()
            .collect();

        if rows.is_empty() && !self.search.running {
            return;
        }

        let root = self.search.root.clone().unwrap_or_default();
        let mut subtitle = display_path(&root);
        if self.search.running {
            subtitle = format!("{subtitle} · searching {} so far", self.search.scanned);
        } else if let Some(limit) = &self.search.limited {
            subtitle = format!("{subtitle} · {limit} · {} searched", self.search.scanned);
        } else {
            subtitle = format!("{subtitle} · {} searched", self.search.scanned);
        }

        self.sections.push(Section {
            title: Self::SEARCH_SECTION.to_string(),
            subtitle: Some(subtitle),
            rows,
            unavailable: false,
        });
    }

    /// Fold a batch of search results in, keeping the list free of duplicates.
    pub fn search_batch(&mut self, root: &Path, mut found: Vec<Entry>, scanned: usize) {
        // A batch from a walk the user has already moved on from is dropped: the
        // walk is abandoned rather than cancelled, so late results are normal.
        if self.search.root.as_deref() != Some(root) {
            return;
        }
        self.search.scanned = scanned;
        self.search.results.append(&mut found);
        self.sync_search_section();
    }

    /// Record that the walk under `root` has finished.
    pub fn search_finished(&mut self, root: &Path, scanned: usize, limited: Option<String>) {
        if self.search.root.as_deref() != Some(root) {
            return;
        }
        self.search.running = false;
        self.search.done = true;
        self.search.scanned = scanned;
        self.search.limited = limited;
        self.sync_search_section();
    }

    pub fn visible(&self) -> Vec<Row<'_>> {
        let mut out: Vec<Row<'_>> = Vec::new();
        for (si, section) in self.sections.iter().enumerate() {
            let mut matched: Vec<(&Entry, i32)> = section
                .rows
                .iter()
                .filter_map(|row| match_score(&self.filter, row).map(|s| (row, s)))
                .collect();

            // A section with nothing to show is dropped, unless it is standing in for
            // a root the user named or is currently in, where its absence would be
            // more confusing than an empty heading.
            let keep_empty = section.unavailable
                || matches!(
                    section.subtitle.as_deref(),
                    Some("configured") | Some("current directory")
                );
            if matched.is_empty() && !(keep_empty && self.filter.is_empty()) {
                continue;
            }

            // Within a section, rank by match quality; without a filter every score is
            // equal and the curated order is preserved. Ties go to the shorter name,
            // which is fzf's default tiebreak and the reason `sales` prefers
            // `sales.csv` over `sales_by_region_and_quarter.csv`.
            if !self.filter.is_empty() {
                matched.sort_by(|(a, sa), (b, sb)| {
                    sb.cmp(sa).then_with(|| a.name.len().cmp(&b.name.len()))
                });
            }

            // An explicit sort overrides both. Rows with nothing to sort by go last
            // rather than sorting as zero, so "biggest first" does not begin with a
            // page of datasets whose size is simply unknown.
            match self.sort {
                SortMode::Natural => {}
                SortMode::Size => {
                    matched.sort_by_key(|(e, _)| std::cmp::Reverse(e.size.unwrap_or(0)));
                }
                SortMode::Rows => {
                    matched.sort_by_key(|(e, _)| std::cmp::Reverse(e.rows.unwrap_or(0)));
                }
                SortMode::Modified => {
                    matched.sort_by_key(|(e, _)| {
                        std::cmp::Reverse(
                            e.modified
                                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                                .map(|d| d.as_secs())
                                .unwrap_or(0),
                        )
                    });
                }
            }

            let collapsed = self.collapsed.contains(&section.title);
            out.push(Row::Header {
                section: si,
                matches: matched.len(),
                collapsed,
            });
            if !collapsed {
                out.extend(
                    matched
                        .into_iter()
                        .map(|(entry, _)| Row::Entry { section: si, entry }),
                );
            }
        }
        out
    }

    /// The highlighted row, when it is a dataset rather than a section header.
    pub fn selected_entry(&self) -> Option<Entry> {
        match self.visible().get(self.selected) {
            Some(Row::Entry { entry, .. }) => Some((*entry).clone()),
            _ => None,
        }
    }

    /// The section the highlighted row belongs to.
    pub fn selected_section(&self) -> Option<usize> {
        self.visible().get(self.selected).map(|r| r.section())
    }

    /// Whether the highlighted row is a section header.
    pub fn selection_is_header(&self) -> bool {
        matches!(self.visible().get(self.selected), Some(Row::Header { .. }))
    }

    /// Remote roots that have neither answered nor been written off.
    ///
    /// The caller probes these off the interface thread; nothing here may touch them.
    pub fn pending_probes(&self) -> Vec<PathBuf> {
        let check = self.network_check;
        let mut out = Vec::new();
        for section in &self.sections {
            let Some(sub) = &section.subtitle else {
                continue;
            };
            if !sub.starts_with("network") {
                continue;
            }
            // The section title is a display path; recover the root it came from.
            for root in &self.root_paths {
                if display_path(root) == section.title
                    && check(root)
                    && !self.probed.contains_key(root)
                    && !self.unreachable.contains(root)
                    && !out.contains(root)
                {
                    out.push(root.clone());
                }
            }
        }
        out
    }

    /// Record what a probe found. An empty listing is still an answer.
    pub fn probe_ready(&mut self, root: PathBuf, rows: Vec<Entry>) {
        self.unreachable.remove(&root);
        self.probed.insert(root, rows);
    }

    /// Record that a probe could not read the root.
    pub fn probe_failed(&mut self, root: PathBuf) {
        self.probed.remove(&root);
        self.unreachable.insert(root);
    }

    /// Measure a batch of rows on the calling thread.
    ///
    /// For tests and library callers that know their paths are safe. **The application
    /// never calls this**: reading a footer opens a file, which blocks on a FIFO, a
    /// device node, a wedged mount or a failing disk. In the app the interface thread
    /// only ever decides *what* to measure, via [`HomeState::unmeasured_visible`], and
    /// a worker does the reading.
    pub fn measure_now(&mut self, limit: usize) -> bool {
        let wanted = self.unmeasured_visible(limit);
        let more = self.unmeasured_visible(limit + 1).len() > wanted.len();
        for entry in wanted {
            let mut probe = entry.clone();
            discover::enrich(&mut probe);
            self.enriched
                .insert(entry.path.clone(), measured_from(&probe, &entry));
        }
        self.apply_measurements();
        more
    }

    /// Rows on screen that have not been measured yet, up to `limit`.
    ///
    /// The interface thread decides *what* is worth measuring — it knows what is
    /// visible — and a worker does the reading.
    pub fn unmeasured_visible(&self, limit: usize) -> Vec<Entry> {
        let mut out = Vec::new();
        for row in self.visible() {
            let Row::Entry { entry, .. } = row else {
                continue;
            };
            if entry.rows.is_some() || self.enriched.contains_key(&entry.path) {
                continue;
            }
            // Remote rows are measured by their root's probe, which already reads that
            // filesystem. Measuring them here too would put a second thread on a share
            // that may never answer, and a wedged thread is never reclaimed.
            if (self.network_check)(&entry.path) {
                continue;
            }
            if !matches!(entry.kind, EntryKind::Directory | EntryKind::Unknown) {
                out.push(entry.clone());
            }
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Fold known measurements into the rows currently listed.
    pub fn apply_measurements(&mut self) {
        for section in &mut self.sections {
            for row in &mut section.rows {
                if let Some(m) = self.enriched.get(&row.path) {
                    row.rows = m.rows;
                    row.cols = m.cols;
                    if m.size.is_some() {
                        row.size = m.size;
                    }
                    if !m.columns.is_empty() {
                        row.columns = m.columns.clone();
                    }
                }
            }
        }
    }

    /// Put the cursor on the first dataset rather than the first header, so the
    /// preview pane has something to show without a keypress.
    pub fn select_first_entry(&mut self) {
        let rows = self.visible();
        self.selected = rows
            .iter()
            .position(|r| matches!(r, Row::Entry { .. }))
            .unwrap_or(0);
    }

    pub fn clamp_selection(&mut self) {
        let n = self.visible().len();
        if n == 0 {
            self.selected = 0;
        } else if self.selected >= n {
            self.selected = n - 1;
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
        let n = self.visible().len();
        if n == 0 {
            return;
        }
        let cur = self.selected as isize;
        let next = (cur + delta).rem_euclid(n as isize);
        self.selected = next as usize;
    }
}

/// Build an entry for a path that is already known (a recent), classifying it.
fn entry_for_path(path: &Path, remote: bool) -> Entry {
    // Classifying reads the directory, and stat'ing gives size and mtime. Both touch
    // the filesystem, so a remote entry is listed by name alone until its probe lands.
    let kind = if remote {
        // A name is all there is to go on without reading the path. An extension
        // settles it; anything else stays Unknown rather than being called a plain
        // directory, which would contradict the same dataset listed under its root as
        // `hive` once that root's probe lands.
        if discover::is_data_file(path) {
            EntryKind::File
        } else {
            EntryKind::Unknown
        }
    } else if path.is_dir() {
        discover::classify_directory(path)
    } else {
        EntryKind::File
    };
    let mut entry = Entry {
        path: path.to_path_buf(),
        kind,
        name: path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string_lossy().into_owned()),
        size: None,
        modified: None,
        rows: None,
        cols: None,
        columns: Vec::new(),
        cost: Default::default(),
    };
    if !remote {
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.is_file() {
                entry.size = Some(meta.len());
            }
            entry.modified = meta.modified().ok();
        }
    }
    entry
}

/// Abbreviate a path with `~` for display.
pub fn display_path(path: &Path) -> String {
    if let Some(home) = dirs::home_dir() {
        if let Ok(rest) = path.strip_prefix(&home) {
            if rest.as_os_str().is_empty() {
                return "~".to_string();
            }
            return format!("~/{}", rest.display());
        }
    }
    path.display().to_string()
}

/// Complete a partially typed path against the directory it names.
///
/// Returns the longest unambiguous extension of `typed`, and how many candidates
/// there were. Reading a directory can block, so this is only ever called from a
/// worker — never in response to a keystroke on the interface thread.
pub fn complete_path(typed: &str) -> (String, usize) {
    let expanded = expand_user_path(typed);
    let typed_ends_in_sep = typed.ends_with('/');

    let (dir, prefix) = if typed_ends_in_sep {
        (expanded.clone(), String::new())
    } else {
        match (expanded.parent(), expanded.file_name()) {
            (Some(parent), Some(name)) => {
                (parent.to_path_buf(), name.to_string_lossy().into_owned())
            }
            _ => (expanded.clone(), String::new()),
        }
    };

    let Ok(entries) = std::fs::read_dir(&dir) else {
        return (typed.to_string(), 0);
    };

    let mut names: Vec<String> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            // A leading dot is only offered when it was asked for; otherwise every
            // completion in a home directory is dotfiles.
            if name.starts_with('.') && !prefix.starts_with('.') {
                return None;
            }
            name.starts_with(&prefix).then_some(name)
        })
        .collect();
    if names.is_empty() {
        return (typed.to_string(), 0);
    }
    names.sort();

    // The longest prefix every candidate agrees on: completing further would be
    // guessing between them.
    let shared = names
        .iter()
        .skip(1)
        .fold(names[0].clone(), |acc, name| common_prefix(&acc, name));

    let mut completed = typed.to_string();
    completed.truncate(typed.len() - prefix.len());
    completed.push_str(&shared);

    // A single directory gets its separator, so the next Tab descends into it.
    if names.len() == 1 && dir.join(&shared).is_dir() && !completed.ends_with('/') {
        completed.push('/');
    }
    (completed, names.len())
}

fn common_prefix(a: &str, b: &str) -> String {
    a.chars()
        .zip(b.chars())
        .take_while(|(x, y)| x == y)
        .map(|(x, _)| x)
        .collect()
}

/// Expand `~` and `$VAR` in a path the user typed.
pub fn expand_user_path(raw: &str) -> PathBuf {
    crate::config::expand_config_path(raw)
}
