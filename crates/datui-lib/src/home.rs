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

/// A place datui will look, and whether it can currently be read.
#[derive(Debug, Clone)]
pub struct Root {
    pub path: PathBuf,
    pub origin: RootOrigin,
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
                roots.push(Root {
                    path,
                    origin,
                    available,
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
            let mut rows = discover::scan_dir(&dir);
            for row in rows.iter_mut() {
                discover::enrich(row);
            }
            self.sections.push(Section {
                title: display_path(&dir),
                subtitle: None,
                rows,
                unavailable: false,
            });
            self.clamp_selection();
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
            let mut rows = if root.available {
                discover::scan_dir(&root.path)
            } else {
                Vec::new()
            };
            for row in rows.iter_mut() {
                discover::enrich(row);
            }
            if rows.is_empty() && root.origin == RootOrigin::Recent {
                continue;
            }
            self.sections.push(Section {
                title: display_path(&root.path),
                subtitle: Some(root.origin.note().to_string()),
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

        self.clamp_selection();
    }

    /// Rows currently passing the filter, flattened, as `(section index, row)`.
    pub fn visible(&self) -> Vec<(usize, &Entry)> {
        let mut out: Vec<(usize, &Entry, usize)> = Vec::new();
        for (si, section) in self.sections.iter().enumerate() {
            for row in &section.rows {
                if let Some(score) = fuzzy_score(&self.filter, &row.name) {
                    out.push((si, row, score));
                }
            }
        }
        // With an active filter, rank across sections by match quality; without one,
        // keep the curated order (recents first, then roots).
        if !self.filter.is_empty() {
            out.sort_by_key(|(_, _, score)| *score);
            // Section headers are hidden while filtering, so a dataset that is both
            // recent and present in a listed directory would appear twice with
            // nothing to explain why. Keep the best-ranked occurrence.
            let mut seen: Vec<&std::path::Path> = Vec::new();
            out.retain(|(_, row, _)| {
                if seen.contains(&row.path.as_path()) {
                    false
                } else {
                    seen.push(row.path.as_path());
                    true
                }
            });
        }
        out.into_iter().map(|(si, row, _)| (si, row)).collect()
    }

    pub fn selected_entry(&self) -> Option<Entry> {
        self.visible().get(self.selected).map(|(_, e)| (*e).clone())
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
    discover::enrich(&mut entry);
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
