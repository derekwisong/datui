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
const NETWORK_FILESYSTEMS: &[&str] = &[
    "nfs",
    "nfs4",
    "cifs",
    "smb3",
    "smbfs",
    "afs",
    "9p",
    "ceph",
    "glusterfs",
    "fuse.sshfs",
    "fuse.rclone",
    "fuse.s3fs",
    "fuse.davfs",
    "davfs",
    "ftpfs",
    // An automount point that has not been triggered yet blocks on first access,
    // which is exactly what the marker is warning about. Once it triggers, the real
    // filesystem shadows it in the mount table and is judged on its own merits.
    "autofs",
];

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
    let mut best: Option<(usize, bool)> = None;

    for line in mountinfo.lines() {
        // Fields before the separator end with the mount point at index 4; the
        // filesystem type is the first field after it.
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let Some(mount_point) = before.split_whitespace().nth(4) else {
            continue;
        };
        let Some(fstype) = after.split_whitespace().next() else {
            continue;
        };
        if !path.starts_with(mount_point) {
            continue;
        }
        // Deepest mount wins, and among mounts at the same point the *last* one wins:
        // mountinfo lists them in mount order, so a later entry shadows an earlier one.
        // An NFS share automounted at a path appears after the autofs entry covering
        // the same path, and it is the NFS entry that describes what a read will do.
        let len = mount_point.len();
        if best.is_none_or(|(n, _)| len >= n) {
            best = Some((len, NETWORK_FILESYSTEMS.contains(&fstype)));
        }
    }

    best.map(|(_, network)| network).unwrap_or(false)
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
pub type Measured = (Option<usize>, Option<usize>, Option<u64>);

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
#[derive(Debug, Default)]
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
}

/// Case-insensitive subsequence match, the cheap half of fuzzy finding.
///
/// Returns a score where lower is better: the span of the match in the haystack,
/// so tighter and earlier matches sort first. `None` when the needle is not a
/// subsequence at all.
pub fn fuzzy_score(needle: &str, haystack: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    let hay: Vec<char> = haystack.to_lowercase().chars().collect();
    let mut first = None;
    let mut last = 0usize;
    let mut hi = 0usize;

    for nc in needle.to_lowercase().chars() {
        let mut found = false;
        while hi < hay.len() {
            if hay[hi] == nc {
                if first.is_none() {
                    first = Some(hi);
                }
                last = hi;
                hi += 1;
                found = true;
                break;
            }
            hi += 1;
        }
        if !found {
            return None;
        }
    }
    Some(last - first.unwrap_or(0) + first.unwrap_or(0) / 4)
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
        let mut roots: Vec<Root> = Vec::new();
        let mut seen: Vec<PathBuf> = Vec::new();

        let push =
            |path: PathBuf, origin: RootOrigin, roots: &mut Vec<Root>, seen: &mut Vec<PathBuf>| {
                let key = path.canonicalize().unwrap_or_else(|_| path.clone());
                if seen.contains(&key) {
                    return;
                }
                seen.push(key);
                let available = std::fs::read_dir(&path).is_ok();
                let network = is_network_path(&path);
                roots.push(Root {
                    path,
                    origin,
                    available,
                    network,
                });
            };

        for dir in config_dirs {
            push(dir.clone(), RootOrigin::Configured, &mut roots, &mut seen);
        }

        // The working directory is claimed up front so it keeps its own label even
        // when a recent dataset also lives there — "current directory" tells you more
        // than "recent" does. It is still listed last, because code is usually here
        // and data usually is not.
        let cwd = std::env::current_dir().ok();
        let cwd_key = cwd
            .as_ref()
            .map(|c| c.canonicalize().unwrap_or_else(|_| c.clone()));

        // A recent dataset implies its containing directory is a place worth showing.
        for recent in recents {
            if let Some(parent) = recent.parent() {
                if parent.as_os_str().is_empty() {
                    continue;
                }
                let key = parent
                    .canonicalize()
                    .unwrap_or_else(|_| parent.to_path_buf());
                if Some(&key) == cwd_key.as_ref() {
                    continue;
                }
                push(
                    parent.to_path_buf(),
                    RootOrigin::Recent,
                    &mut roots,
                    &mut seen,
                );
            }
        }

        if let Some(cwd) = cwd {
            push(cwd, RootOrigin::Cwd, &mut roots, &mut seen);
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
        self.sections.clear();

        // Descended into a directory: show only that.
        if let Some(dir) = self.browsing.clone() {
            let rows = discover::scan_dir(&dir);
            self.sections.push(Section {
                title: display_path(&dir),
                subtitle: None,
                rows,
                unavailable: false,
            });
            self.select_first_entry();
            return;
        }

        // Recents that still exist, most recent first.
        let recent_rows: Vec<Entry> = recents
            .iter()
            .filter(|p| p.exists())
            .take(15)
            .map(|p| entry_for_path(p))
            .collect();
        if !recent_rows.is_empty() {
            self.sections.push(Section {
                title: "Recent".to_string(),
                subtitle: None,
                rows: recent_rows,
                unavailable: false,
            });
        }

        // Desktop-derived places are collected rather than expanded — see below.
        let mut elsewhere: Vec<Entry> = Vec::new();

        for root in Self::roots(config_dirs, recents, desktop_dirs) {
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
            let rows = if root.available {
                discover::scan_dir(&root.path)
            } else {
                Vec::new()
            };
            // An empty derived root is noise and goes. One that cannot be *read* stays:
            // a network share that has stopped answering is the case the section
            // heading exists to report, and silently dropping it is the worst answer.
            if rows.is_empty() && root.origin == RootOrigin::Recent && root.available {
                continue;
            }
            // A network root is worth flagging: it is the one that will be slow, and
            // the one that can stop answering.
            let subtitle = if root.network {
                format!("network · {}", root.origin.note())
            } else {
                root.origin.note().to_string()
            };
            self.sections.push(Section {
                title: display_path(&root.path),
                subtitle: Some(subtitle),
                rows,
                unavailable: !root.available,
            });
        }

        if !elsewhere.is_empty() {
            self.sections.push(Section {
                title: "Elsewhere".to_string(),
                subtitle: Some("opened elsewhere · press Enter to look".to_string()),
                rows: elsewhere,
                unavailable: false,
            });
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
    pub fn visible(&self) -> Vec<Row<'_>> {
        let mut out: Vec<Row<'_>> = Vec::new();
        for (si, section) in self.sections.iter().enumerate() {
            let mut matched: Vec<(&Entry, usize)> = section
                .rows
                .iter()
                .filter_map(|row| fuzzy_score(&self.filter, &row.name).map(|s| (row, s)))
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
            // equal and the curated order is preserved.
            if !self.filter.is_empty() {
                matched.sort_by_key(|(_, score)| *score);
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

    /// Measure the rows about to be drawn, and only those.
    ///
    /// Enriching during `rebuild` meant every dataset under every root paid for a
    /// footer walk before the first frame — on a directory holding a dozen datasets
    /// that is hundreds of file reads, and the home screen simply does not appear
    /// for tens of seconds. Bounded by what fits on screen, it is a handful of reads,
    /// and the cache means scrolling back costs nothing.
    /// Returns true when rows remain unmeasured, so the caller can schedule another
    /// pass rather than paying for all of them before the first frame.
    pub fn enrich_visible(&mut self, limit: usize, budget: usize) -> bool {
        // Collect targets first: `visible()` borrows the sections immutably.
        let targets: Vec<(usize, PathBuf)> = self
            .visible()
            .iter()
            .filter_map(|r| match r {
                Row::Entry { section, entry } => Some((*section, entry.path.clone())),
                Row::Header { .. } => None,
            })
            .take(limit)
            .collect();

        let mut spent = 0usize;
        let mut remaining = false;
        for (section, path) in targets {
            let measured = match self.enriched.get(&path) {
                Some(cached) => *cached,
                None => {
                    // Measuring a hive dataset walks its files. A directory holding a
                    // dozen of them is hundreds of reads, so only a few are done per
                    // frame and the rest fill in over the next ones.
                    if spent >= budget {
                        remaining = true;
                        continue;
                    }
                    spent += 1;
                    let Some(row) = self
                        .sections
                        .get(section)
                        .and_then(|s| s.rows.iter().find(|r| r.path == path))
                    else {
                        continue;
                    };
                    let mut probe = row.clone();
                    discover::enrich(&mut probe);
                    let measured = (probe.rows, probe.cols, probe.size.or(row.size));
                    self.enriched.insert(path.clone(), measured);
                    measured
                }
            };
            if let Some(row) = self
                .sections
                .get_mut(section)
                .and_then(|s| s.rows.iter_mut().find(|r| r.path == path))
            {
                row.rows = measured.0;
                row.cols = measured.1;
                if measured.2.is_some() {
                    row.size = measured.2;
                }
            }
        }
        remaining
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
fn entry_for_path(path: &Path) -> Entry {
    let kind = if path.is_dir() {
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
    };
    if let Ok(meta) = std::fs::metadata(path) {
        if meta.is_file() {
            entry.size = Some(meta.len());
        }
        entry.modified = meta.modified().ok();
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

/// Expand `~` and `$VAR` in a path the user typed.
pub fn expand_user_path(raw: &str) -> PathBuf {
    crate::config::expand_config_path(raw)
}
