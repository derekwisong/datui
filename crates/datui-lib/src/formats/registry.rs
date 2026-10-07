//! The specs on the search path, and which one a file picks.

use super::*;

/// A spec found on the search path, and the copies of the same name it hides.
#[derive(Debug, Clone)]
pub struct Found {
    pub spec: Arc<Spec>,
    pub overrides: Vec<PathBuf>,
}

/// Every spec on the search path, first of each name first.
#[derive(Debug, Clone, Default)]
pub struct Registry {
    pub specs: Vec<Found>,
    /// FIX dictionaries: QuickFIX XML files and `kind = "fix"` TOML files.
    pub fix: Vec<FixFound>,
    /// DBC files for CAN logs: `.dbc` files and `kind = "dbc"` TOML files, in the order
    /// they are read.
    pub dbc: Vec<DbcFound>,
    /// Spec files that could not be read, each with why.
    pub errors: Vec<SpecError>,
}

/// A DBC file found on the search path.
#[derive(Debug, Clone)]
pub struct DbcFound {
    pub dbc: Arc<crate::dbc::Dbc>,
    /// The file it was found as: the `.dbc`, or the TOML that names it.
    pub path: PathBuf,
}

/// A FIX dictionary found on the search path, and the copies of the same name it hides.
#[derive(Debug, Clone)]
pub struct FixFound {
    pub dict: Arc<crate::fix::dict::Dictionary>,
    pub overrides: Vec<PathBuf>,
}

/// How a spec was chosen for a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chosen {
    /// `--format FILE`.
    SpecFile,
    /// `--format NAME`, or picked in the view.
    Named,
    Glob,
    Magic,
}

/// Why `spec` read a file, for the Notes tab: `matched by magic MKTD · version 1`
/// when its glob or its magic chose it, `chosen by --format FILE` otherwise.
pub fn chosen_words(spec: &Spec, by: Chosen) -> String {
    let chips = match by {
        Chosen::Glob | Chosen::Magic => spec.match_chips_chosen(by),
        Chosen::SpecFile | Chosen::Named => Vec::new(),
    };
    if chips.is_empty() {
        format!("chosen by {}", by.words())
    } else {
        format!("matched by {}", chips_plain(&chips))
    }
}

impl Chosen {
    pub fn words(self) -> &'static str {
        match self {
            Self::SpecFile => "--format FILE",
            Self::Named => "its name",
            Self::Glob => "its glob",
            Self::Magic => "its magic",
        }
    }
}

/// The specs a file matches, by the first rule that matched any.
#[derive(Debug, Clone)]
pub struct Matched {
    pub specs: Vec<Arc<Spec>>,
    pub by: Chosen,
}

/// The directories and files searched for specs, in order: the config directory's
/// `formats`, then `$DATUI_FORMATS_PATH`, then `[formats] path` from the config.
pub fn search_path(
    config_dir: Option<&Path>,
    env: Option<std::ffi::OsString>,
    configured: &[String],
) -> Vec<PathBuf> {
    let mut path = Vec::new();
    if let Some(dir) = config_dir {
        path.push(dir.join("formats"));
    }
    if let Some(env) = env {
        path.extend(std::env::split_paths(&env).filter(|p| !p.as_os_str().is_empty()));
    }
    path.extend(
        configured
            .iter()
            .filter(|p| !p.trim().is_empty())
            .map(|p| crate::config::expand_path(p)),
    );
    path
}

/// The search path `config` asks for: the config directory's `formats`, then
/// `$DATUI_FORMATS_PATH`, then its `[formats] path`.
pub fn search_path_for(config: &crate::config::AppConfig) -> Vec<PathBuf> {
    let config_dir = crate::config::ConfigManager::new(crate::APP_NAME)
        .ok()
        .map(|m| m.config_dir().to_path_buf());
    search_path(
        config_dir.as_deref(),
        std::env::var_os(PATH_VAR),
        &config.formats.path,
    )
}

impl Registry {
    /// Read every spec on `path`. A directory gives its `*.toml` files in name order; a
    /// file gives itself. What cannot be read is kept as an error, not fatal. A TOML
    /// file of `kind = "fix"`, or a QuickFIX XML file, is a FIX dictionary; a `.dbc`
    /// file, or a TOML file of `kind = "dbc"`, is a DBC file.
    pub fn load(path: &[PathBuf]) -> Self {
        let mut registry = Self::default();
        for entry in path {
            let files: Vec<PathBuf> = if entry.is_dir() {
                let Ok(listing) = std::fs::read_dir(entry) else {
                    continue;
                };
                let mut files: Vec<PathBuf> = listing
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| {
                        p.is_file()
                            && p.extension().is_some_and(|e| {
                                e.eq_ignore_ascii_case("toml")
                                    || e.eq_ignore_ascii_case("xml")
                                    || e.eq_ignore_ascii_case("dbc")
                            })
                    })
                    .collect();
                files.sort();
                files
            } else if entry.is_file() {
                vec![entry.clone()]
            } else {
                continue;
            };
            for file in files {
                // A DBC file, or a TOML file of `kind = "dbc"` that names one.
                let dbc_like = file.extension().is_some_and(|e| {
                    e.eq_ignore_ascii_case("dbc") || e.eq_ignore_ascii_case("toml")
                });
                if dbc_like {
                    match crate::dbc::load(&file) {
                        Ok(Some(dbc)) => {
                            registry.dbc.push(DbcFound {
                                dbc: Arc::new(dbc),
                                path: file,
                            });
                            continue;
                        }
                        Err(e) => {
                            registry.errors.push(e);
                            continue;
                        }
                        Ok(None) => {}
                    }
                }
                match crate::fix::dict::Dictionary::load(&file) {
                    Ok(Some(dict)) => {
                        registry.add_fix(dict, file);
                        continue;
                    }
                    Err(e) => {
                        registry.errors.push(e);
                        continue;
                    }
                    // An XML file that is not a FIX dictionary is not a spec either.
                    Ok(None)
                        if !file
                            .extension()
                            .is_some_and(|e| e.eq_ignore_ascii_case("toml")) =>
                    {
                        continue;
                    }
                    Ok(None) => {}
                }
                match Spec::load(&file) {
                    Ok(spec) => registry.add(spec, file),
                    Err(e) => registry.errors.push(e),
                }
            }
        }
        registry
    }

    fn add(&mut self, spec: Spec, file: PathBuf) {
        if let Some(found) = self.specs.iter_mut().find(|f| f.spec.name == spec.name) {
            found.overrides.push(file);
        } else {
            self.specs.push(Found {
                spec: Arc::new(spec),
                overrides: Vec::new(),
            });
        }
    }

    fn add_fix(&mut self, dict: crate::fix::dict::Dictionary, file: PathBuf) {
        if let Some(found) = self.fix.iter_mut().find(|f| f.dict.name == dict.name) {
            found.overrides.push(file);
        } else {
            self.fix.push(FixFound {
                dict: Arc::new(dict),
                overrides: Vec::new(),
            });
        }
    }

    /// The FIX dictionary named `name`.
    pub fn fix_dict(&self, name: &str) -> Option<&Arc<crate::fix::dict::Dictionary>> {
        self.fix
            .iter()
            .find(|f| f.dict.name == name)
            .map(|f| &f.dict)
    }

    /// The registry of `specs`, for tests and hosts that have their specs in hand.
    pub fn of(specs: Vec<Spec>) -> Self {
        let mut registry = Self::default();
        for spec in specs {
            let file = spec.path.clone().unwrap_or_default();
            registry.add(spec, file);
        }
        registry
    }

    pub fn get(&self, name: &str) -> Option<&Arc<Spec>> {
        self.specs
            .iter()
            .find(|f| f.spec.name == name)
            .map(|f| &f.spec)
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// The spec that reads the file `file`, by its glob or else by its magic as an open
    /// picks it, when it reads the file's records as several variants: the home screen
    /// lists them inside the file. Its first bytes are read only when no glob names it.
    pub fn variants_of(&self, file: &Path) -> Option<Arc<Spec>> {
        let globbed = self.by_glob(file, false);
        let spec = if globbed.is_empty() {
            let wanted = unnamed_may(file, false, false)?;
            let compression = crate::CompressionFormat::from_extension(file);
            if compression.is_some() || !self.specs.iter().any(|f| !f.spec.magic.is_empty()) {
                return None;
            }
            self.matching_among(file, false, wanted, |reach| spec_head(file, None, reach))?
                .specs
                .into_iter()
                .next()?
        } else {
            globbed.into_iter().find(|s| !s.is_delimited())?
        };
        spec.lists_variants().then_some(spec)
    }

    /// The spec that reads a local file a listing looked inside, as an open with
    /// nothing asked picks it: by glob, else by magic and `match.where`, compared
    /// against `head`, the bytes the listing already read from its front (all of it
    /// when `whole`). A spec whose match needs more than `head` holds is not asked.
    pub fn listed(&self, path: &Path, head: &[u8], whole: bool) -> Option<Arc<Spec>> {
        if self.is_empty() || crate::CompressionFormat::from_extension(path).is_some() {
            return None;
        }
        let wanted = unnamed_may(path, false, false)?;
        let held = head.len() as u64;
        let within = |s: &Spec| wanted(s) && (whole || s.match_reach() <= held);
        self.matching_among(path, false, within, |_| Some(head.to_vec()))?
            .specs
            .into_iter()
            .next()
    }

    /// The specs whose globs match `path`, a file or (for the columns layout) a
    /// directory, by name alone.
    pub fn by_glob(&self, path: &Path, is_dir: bool) -> Vec<Arc<Spec>> {
        self.specs
            .iter()
            .map(|f| &f.spec)
            .filter(|s| s.reads_directory() == is_dir && s.glob_matches(path))
            .cloned()
            .collect()
    }

    /// The specs `path` matches: by glob, else by magic. A spec with `match.where`
    /// matches only a file whose header holds those values. The front of the file is
    /// read through `head`, once, and only when a magic or a header is to be compared.
    pub fn matching(
        &self,
        path: &Path,
        is_dir: bool,
        head: impl FnOnce(u64) -> Option<Vec<u8>>,
    ) -> Option<Matched> {
        self.matching_among(path, is_dir, |_| true, head)
    }

    /// [`Self::matching`] among the specs `wanted` says may read `path`.
    pub fn matching_among(
        &self,
        path: &Path,
        is_dir: bool,
        wanted: impl Fn(&Spec) -> bool,
        head: impl FnOnce(u64) -> Option<Vec<u8>>,
    ) -> Option<Matched> {
        let mut globbed = self.by_glob(path, is_dir);
        globbed.retain(|s| wanted(s));
        let (candidates, by) = if !globbed.is_empty() {
            (globbed, Chosen::Glob)
        } else if is_dir {
            return None;
        } else {
            let magic: Vec<Arc<Spec>> = self
                .specs
                .iter()
                .map(|f| &f.spec)
                .filter(|s| !s.reads_directory() && !s.magic.is_empty() && wanted(s))
                .cloned()
                .collect();
            (magic, Chosen::Magic)
        };
        let reach = candidates
            .iter()
            .map(|s| match by {
                Chosen::Magic => s.match_reach(),
                _ if s.expect.is_empty() => 0,
                _ => s.match_reach(),
            })
            .max()
            .unwrap_or(0);
        let head = if reach > 0 && !is_dir {
            head(reach)
        } else {
            None
        };
        let specs: Vec<Arc<Spec>> = candidates
            .into_iter()
            .filter(|s| {
                let Some(head) = &head else {
                    // Nothing read: a glob match stands unless it asked about the header.
                    return by == Chosen::Glob && s.expect.is_empty();
                };
                (by != Chosen::Magic || s.magic_matches(head)) && s.header_matches(head)
            })
            .collect();
        (!specs.is_empty()).then_some(Matched { specs, by })
    }

    /// Text for `datui formats`: each spec, the file it came from, the copies it hides,
    /// and the spec files that could not be read.
    pub fn listing(&self, path: &[PathBuf]) -> String {
        let mut out = String::new();
        if self.specs.is_empty() {
            out.push_str("No format specs found.\n");
        }
        for found in &self.specs {
            let spec = &found.spec;
            out.push_str(&spec.name);
            let said = match_words(spec);
            if spec.is_delimited() {
                out.push_str(&format!("  (delimited; {said})"));
            } else {
                out.push_str(&format!("  ({said})"));
            }
            out.push('\n');
            if let Some(description) = &spec.description {
                out.push_str(&format!("  {description}\n"));
            }
            if let Some(file) = &spec.path {
                out.push_str(&format!("  {}\n", file.display()));
            }
            for hidden in &found.overrides {
                out.push_str(&format!("  overrides {}\n", hidden.display()));
            }
        }
        if !self.fix.is_empty() {
            out.push_str("\nDictionaries (FIX):\n");
        }
        for found in &self.fix {
            let dict = &found.dict;
            out.push_str(&dict.name);
            let summary = dict.matcher.summary();
            if !summary.is_empty() {
                out.push_str(&format!("  ({summary})"));
            }
            out.push('\n');
            if let Some(file) = &dict.path {
                out.push_str(&format!("  {}\n", file.display()));
            }
            for hidden in &found.overrides {
                out.push_str(&format!("  overrides {}\n", hidden.display()));
            }
        }
        if !self.dbc.is_empty() {
            out.push_str("\nDictionaries (DBC):\n");
        }
        for found in &self.dbc {
            let dbc = &found.dbc;
            out.push_str(&format!(
                "{}  ({}{})\n  {}\n",
                dbc.name,
                crate::text_formats::count(dbc.messages.len() as u64, "message", "messages"),
                dbc.interface
                    .as_ref()
                    .map(|i| format!(", interface {i}"))
                    .unwrap_or_default(),
                found.path.display()
            ));
        }
        if !self.errors.is_empty() {
            out.push_str("\nCould not read:\n");
            for e in &self.errors {
                out.push_str(&format!("  {e}\n"));
            }
        }
        out.push_str("\nSearched, in order:\n");
        for entry in path {
            out.push_str(&format!("  {}\n", entry.display()));
        }
        out
    }
}
