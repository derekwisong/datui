//! Opening a dataset: routing what was named, downloads, the scan and schema read
//! for each kind of source, the load's phases as jobs, and installing the result.

use crate::cli::{CompressionFormat, FileFormat};
#[cfg(feature = "cloud")]
use crate::cloud_hive;
use crate::feedback::Confirm;
use crate::jobs::{Answer, Job};
use crate::open_options::{OpenOptions, ReadReport, UnaskedDownload};
use crate::pivot_melt_modal::PivotMeltModal;
use crate::scan::Scan;
use crate::sort_filter_modal::SortFilterModal;
use crate::table::{DataTableState, OpenFacts};
#[cfg(feature = "cloud")]
use crate::wait_on_runtime;
use crate::{
    App, AppEvent, UNSUPPORTED, catalog, cli, dataset_files, discover, home, loading,
    quality_report, source,
};
use color_eyre::Result;
#[cfg(feature = "cloud")]
use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use polars::prelude::{LazyFrame, Schema, col};
#[cfg(feature = "cloud")]
use polars::prelude::{PlRefPath, ScanArgsParquet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Where the dataset on screen came from, and how it was opened.
pub struct OpenedSource {
    pub(crate) original_file_format: Option<crate::export_modal::ExportFormat>,
    pub(crate) original_file_delimiter: Option<u8>,
    /// The paths the dataset on screen was opened from, with the options it installed
    /// with: what `H` opens again with its header turned the other way.
    pub(crate) opened: Option<(Vec<PathBuf>, OpenOptions)>,
    /// Whether the open dataset was reached through the home screen. `q` pops
    /// the context: opened from home it returns there, launched straight onto
    /// a file it quits — the user's mental stack, not a mode.
    pub(crate) opened_from_home: bool,
    /// `--view NAME`, waiting for the dataset from the command line to land.
    /// Taken on the first install, so datasets opened later are not re-dressed.
    pub(crate) startup_view: Option<String>,
    /// The dataset whose downloaded shape is kept already. See
    /// [`Self::remember_a_downloads_shape`].
    pub(crate) shape_remembered: Option<u64>,
}

/// What a cloud open was pointed at: the URL as the user gave it, the prefix to list,
/// and the glob to keep, where they named one.
///
/// Together because they are one thought — where to look — and apart they put this
/// function's signature past the point where a reader can hold it.
#[cfg(feature = "cloud")]
struct CloudTarget<'a> {
    /// The URL as typed, which is where the bucket and scheme come from.
    full: &'a str,
    /// The literal prefix to list: the whole key, or the part of a glob before its star.
    key: String,
    /// The glob the user named, where they named one. The listing keeps only the keys
    /// it matches, so everything downstream sees a plain list of files.
    pattern: Option<&'a globset::GlobMatcher>,
}

/// Put hive partition columns first, ahead of the file's own columns. `drifts` keeps
/// the scan's hidden drift column, which the select would otherwise drop.
///
/// A free function rather than a method: rebuilding the scan to read a column as text
/// has to put the columns back the same way, and it happens on the table's state
/// rather than on the app.
pub(crate) fn hoist_partition_columns(
    lf: LazyFrame,
    schema: &Schema,
    partition_columns: &[String],
    drifts: bool,
) -> LazyFrame {
    if partition_columns.is_empty() {
        return lf;
    }
    let mut exprs: Vec<_> = partition_columns
        .iter()
        .map(|s| col(s.as_str()))
        .chain(
            schema
                .iter_names()
                .map(|s| s.to_string())
                .filter(|c| !partition_columns.contains(c))
                .map(|s| col(s.as_str())),
        )
        .collect();
    if drifts {
        exprs.push(col(crate::schema_union::DRIFT_COLUMN));
    }
    lf.select(exprs)
}

impl App {
    /// Whether an open is on its way and its dataset not installed yet: whatever table
    /// `data_table_state` holds meanwhile belongs to the dataset being replaced, so the
    /// main view shows the open's progress instead of it.
    pub(crate) fn awaiting_dataset(&self) -> bool {
        self.loading.awaiting_dataset()
    }

    /// What the loading screen and the footer say about the open in flight: its
    /// phase, the flat percentage beside it, the path it names and that path's size.
    pub(crate) fn load_shown(&self) -> Option<(&str, u16, Option<&Path>, u64)> {
        self.loading.current().map(|load| {
            let (phase, percent) = load.phase().label();
            (phase, percent, load.path(), load.size())
        })
    }

    /// What the load is doing, for whichever part of the screen is saying so.
    ///
    /// The footer count stands in for the phase while a pass is running: it says the
    /// same thing and says how far along it is. Both callers read it from
    /// [`Self::footers_this_frame`], one number taken once a frame, so they cannot say
    /// two different things about one wait.
    pub(crate) fn loading_phase<'a>(&self, phase: &'a str) -> std::borrow::Cow<'a, str> {
        match self.counting.footers_this_frame {
            Some((read, total)) => std::borrow::Cow::Owned(format!(
                "Reading footers: {} of {}",
                crate::numfmt::group_chrome(read),
                crate::numfmt::group_chrome(total)
            )),
            // A listing has no total to count towards, so it says how far it has got.
            None => match self.counting.listed_this_frame {
                Some(listed) => std::borrow::Cow::Owned(format!(
                    "Listing files: {}",
                    crate::numfmt::group_chrome(listed)
                )),
                None => std::borrow::Cow::Borrowed(phase),
            },
        }
    }

    /// An open is on its way: the loading screen takes over now, saying `phase`, and keys
    /// wait for it. Called before the event that carries the open out — by `run` before
    /// the first frame, and by a key before the `Open` it returns — because a frame is
    /// drawn between the two and would otherwise show the outgoing dataset.
    pub fn set_loading_phase(&mut self, phase: impl Into<String>, progress_percent: u16) {
        self.announce_open(false, phase.into(), progress_percent);
    }

    /// As [`Self::set_loading_phase`], for an open chosen on the home screen when
    /// `from_home`: that is where its failure is reported.
    pub(crate) fn announce_open(&mut self, from_home: bool, phase: String, percent: u16) {
        self.make_way_for_an_open();
        self.loading.announce(from_home, phase, percent);
    }

    /// Put the path on the loading screen, so a wait says what it is waiting for.
    pub(crate) fn name_what_is_loading(&mut self, path: PathBuf) {
        self.loading.name(path);
    }

    /// An open is being asked for: a load already doing work is put down for it, unless
    /// it has not started any (the look or the frame that leads to this open).
    pub(crate) fn make_way_for_an_open(&mut self) {
        if let Some(retired) = self.loading.make_way() {
            self.put_down_load(retired);
        }
    }

    /// An open has its request: make way for it, and stop what the dataset on screen
    /// was still reading for itself.
    pub(crate) fn begin_new_dataset(&mut self) {
        self.make_way_for_an_open();
        // A preview's dataset this open did not take is a page nobody is opening.
        self.home_app.previews.drop_prepared();
        self.reset_chart_state();
        self.jobs.advance();
        // The dataset's footer pass is no longer wanted, and unread, unpaid-for is better
        // than read and dropped. The open counts its own footers on a counter of its
        // own, which the dataset takes over if it installs.
        //
        // The meter needs no equivalent: it belongs to the dataset rather than to the
        // app, so a load that never reaches the screen never has one installed. See
        // `DataTableState::measurements`.
        self.counting.footer_progress.cancel();
    }

    /// Put down what the app keeps for a load the loader has retired: its jobs, whose
    /// answers are for a screen nobody is on, their lines on the footer, and the
    /// question about its download.
    pub(crate) fn put_down_load(&mut self, retired: loading::Retired) {
        let id = retired.id;
        let lines = self.jobs.quiet(|job| job.load() == Some(id));
        self.jobs.supersede(|job| job.load() == Some(id));
        if self
            .status_message
            .as_ref()
            .is_some_and(|status| lines.contains(status))
        {
            self.status_message = None;
        }
        if retired.asking {
            self.confirmation_modal.hide();
        }
    }

    /// Carry out what the open needs next.
    pub(crate) fn run_load_step(&mut self, step: loading::Step) -> Option<AppEvent> {
        use loading::Step;
        let load = self.loading.id();
        match step {
            Step::Nothing => None,
            Step::Crash(message) => Some(AppEvent::Crash(message)),
            Step::Failed(failed) => {
                self.load_failed(failed);
                None
            }
            Step::Tables(tables) => {
                self.land_on_tables(tables);
                None
            }
            Step::Hex(hex) => {
                self.land_on_hex(hex);
                None
            }
            Step::Install(loaded) => {
                // The view an open applies reads its own first rows, so the dataset's are
                // not read.
                if self.install_dataset(*loaded) {
                    return None;
                }
                #[cfg(test)]
                {
                    self.counting.first_rows_asked += 1;
                }
                if !self.spawn_async_collect(Self::LOADING_BUFFER) {
                    // Nothing to read: the buffer already serves the view.
                    if self.status_message.as_deref() == Some(Self::LOADING_BUFFER) {
                        self.status_message = None;
                    }
                    self.first_rows_settled();
                }
                None
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Ask(pending) => {
                // Nothing runs while the question is up: datui waits on a key, and a
                // spinner would read as progress. The loader holds the generation
                // meanwhile.
                self.confirmation_modal.show(
                    Self::download_confirmation_message(&pending, self.loading.download_note()),
                    Confirm::Download,
                );
                None
            }
            Step::AskRead(read) => {
                // As for a download: nothing runs, and the generation is held.
                self.loading.hold_while_asking(self.jobs.hold());
                self.confirmation_modal.show(
                    Self::in_memory_confirmation_message(&read),
                    Confirm::Download,
                );
                None
            }
            step => {
                let load = load.expect("a step that runs work belongs to the open in flight");
                self.spawn_load_phase(load, step);
                None
            }
        }
    }

    /// Keep the shape of a downloaded dataset under the URL it was opened from, once its
    /// rows are counted: nothing lists a web file, so this is the only way its recent,
    /// and its catalog row, can say `344 × 9` (#547 D12). Once per dataset.
    pub(crate) fn remember_a_downloads_shape(&mut self) {
        if self.source.shape_remembered == Some(self.dataset_generation) {
            return;
        }
        let Some(url) = self.path.clone().filter(|p| source::is_remote_url(p)) else {
            return;
        };
        let Some(state) = self.data_table_state.as_ref().filter(|s| s.fetched()) else {
            return;
        };
        let Some(rows) = state.num_rows_if_valid().filter(|_| !state.changes_rows()) else {
            return;
        };
        self.source.shape_remembered = Some(self.dataset_generation);
        let columns: Vec<String> = state
            .source_schema()
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        let facts = crate::cache::DatasetFacts {
            mtime: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
            size: 0,
            rows: Some(rows),
            cols: Some(columns.len()),
            cols_sampled: false,
            columns,
            kind: Some(discover::EntryKind::File),
            classified_by: discover::CLASSIFIER_VERSION,
            cost: Default::default(),
            holds: Default::default(),
        };
        // Off the UI thread: the index takes a lock other instances may hold.
        let cache = self.cache.clone();
        self.cache_writes
            .spawn(move || cache.record_dataset_facts(&[(url, facts)]));
    }

    /// Install the dataset an open read, and apply the view it opens with, if any.
    /// Returns whether that view is reading the first rows, which the caller then leaves
    /// to it.
    ///
    /// Only the loader hands one over, and only for the open in flight: an abandoned or
    /// replaced open's answer never gets this far.
    pub(crate) fn install_dataset(&mut self, loaded: loading::Loaded) -> bool {
        let loading::Loaded {
            state,
            path,
            options,
            debug_label,
            paths,
            recent,
            from_home,
            footers,
        } = loaded;
        let options = &options;
        self.counting.reset_for_dataset();
        // One per dataset that reaches the screen, rather than one per open started:
        // an open that fails leaves the last dataset up, and the pass still reading its
        // footers has to be able to finish into it.
        self.dataset_generation = self.dataset_generation.wrapping_add(1);
        self.quality.reset_for_dataset();
        // The findings narrowed to the last dataset's columns would hide this one's.
        self.analysis_modal.quality.findings = quality_report::FindingsView::default();
        self.analysis_modal.quality.evidence_read = None;
        // A query still running was over the dataset being replaced; its rollback
        // is that dataset's view. So was a view waiting on its pivot.
        self.prompt.query_running = None;
        self.jobs.supersede(|job| matches!(job, Job::ViewPivot(_)));
        // A sample being drawn was the last dataset's, and so were its paths.
        self.put_down_sample_draw();
        self.sample.paths.clear();
        // Whatever chart state survived belongs to the dataset being replaced.
        self.reset_chart_state();
        self.debug.schema_load = debug_label;
        // Home is now in the stack, so q pops back to it; never unset, since a
        // reread from the table (H) is not a new place.
        if from_home {
            self.source.opened_from_home = true;
        }
        // A frame handed over has no path to go back to.
        // Without the spec read: it holds the file's map, and a decompressed copy's map
        // keeps its disk space until the map goes, so it goes with the dataset.
        self.source.opened = paths.map(|paths| {
            let options = OpenOptions {
                format_read: None,
                sqlite: None,
                // Counted afresh by the next read.
                tail: None,
                prepared: None,
                ..options.clone()
            };
            (paths, options)
        });
        // Recorded once the dataset is installed: a file that fails to load is not one
        // anybody wants to get back to.
        if let Some(path) = recent {
            // Off the opening path. It takes a lock several instances may be contending
            // for -- opening a dataset must not queue behind another instance's
            // bookkeeping. Only the next home listing waits on it, on its worker.
            let cache = self.cache.clone();
            self.cache_writes.spawn(move || {
                cache.push_recent(&path);
            });
        }
        self.forget_the_rows_read();
        self.info.file_facts = None;
        let shown = home::catalogs(&self.app_config);
        self.info.codebook = path.as_deref().and_then(|p| home::codebook_for(&shown, p));
        self.info.catalog_entry = path
            .as_deref()
            .and_then(|p| home::catalog_entry_for(&shown, p));
        // The footers it still has to read are counted on the open's counter, which is
        // the dataset's now; the last dataset's pass, if any is left, stops.
        self.counting.footer_progress.cancel();
        self.counting.footer_progress = footers;
        self.data_table_state = Some(state);
        // A followed file's watcher starts with its dataset and stops with it.
        if options.follow
            && let Some(state) = self.data_table_state.as_mut()
        {
            match options.tail.as_deref() {
                Some(tail) => {
                    let follow = crate::follow::Follow::start(
                        tail.clone(),
                        self.app_config.read.follow_interval.duration(),
                        self.events.clone(),
                        options.spool.clone(),
                    );
                    state.start_following(if options.pipe {
                        follow.as_pipe()
                    } else {
                        follow
                    });
                    // Counted already, as the scan reads them: no count of its own.
                    state.follow_to(tail.rows(), false);
                }
                // A recording of something that cannot be read as it grows.
                None => self.flash_note(
                    "Only text and Arrow streams are followed: this shows what had arrived, and recording goes on"
                        .to_string(),
                ),
            }
        }
        // A count still waiting for the last dataset's rows to paint is not owed now.
        self.retire_a_count_the_rows_answered();
        self.path = path.clone();
        // Named for this file, so after its path is set.
        self.open_info_documentation();
        if let Some(ref p) = path {
            let read_as = self
                .data_table_state
                .as_ref()
                .and_then(DataTableState::read_as);
            self.source.original_file_format =
                Self::export_format_for(p, read_as.or(options.format));
            // CSV's delimiter: a comma unless the user named a separator. A `.tsv`
            // exports as TSV, whose preset is the tab; a tab in a `.csv` would reopen
            // as one column.
            self.source.original_file_delimiter = Some(options.separator_or(b','));
        } else {
            self.source.original_file_format = None;
            self.source.original_file_delimiter = None;
        }
        // A panel still up says what it says about the dataset on screen.
        if self.info_modal.active {
            self.read_file_facts();
            self.count_unfit();
        }
        // The dataset is on screen now; whatever it still has to learn about itself is
        // read behind it.
        self.start_pending_footers();
        self.start_indexing();
        // `#` for text and logs, unless the flag or the config said.
        if options.row_numbers_auto
            && let Some(state) = self.data_table_state.as_mut()
            && state.numbered_by_default()
        {
            state.set_row_numbers(true);
        }
        self.sort_filter_modal = SortFilterModal::new();
        self.pivot_melt_modal = PivotMeltModal::new();
        self.status_message = Some(Self::LOADING_BUFFER.to_string());

        // The dataset is installed and its schema known, so this is where a view
        // meets it. `--view` names one and applies to this first open alone;
        // `[views] auto_apply` dresses every open that has a matching view.
        // A fresh dataset starts with no view applied: the previous file's view
        // must not wear the check mark here, nor count as applied when edited.
        self.views.active_id = None;
        let (view, reason) = match self.source.startup_view.take() {
            Some(name) => match self.views.manager.get_view_by_name(&name).cloned() {
                Some(view) => (Some(view), None),
                None => {
                    self.error_modal.show(format!("No view named \"{name}\""));
                    (None, None)
                }
            },
            None if self.app_config.views.auto_apply => self
                .view_dataset()
                .zip(self.data_table_state.as_ref())
                .and_then(|(dataset, state)| {
                    self.views
                        .manager
                        .get_most_relevant(dataset, state.source_schema())
                })
                .map_or((None, None), |(view, reason)| (Some(view), Some(reason))),
            None => (None, None),
        };
        let Some(view) = view else {
            return false;
        };
        let applied = match reason {
            // Applied unasked, it says which view and why.
            Some(why) => self.apply_matched_view(&view, why),
            None => self.apply_view(&view),
        };
        match applied {
            // The view reads its own first rows, so the dataset's are never read.
            Ok(()) => true,
            Err(e) => {
                self.error_modal
                    .show(format!("Error applying view \"{}\": {e}", view.name));
                false
            }
        }
    }

    /// Enter the home screen, rebuilding it, abandoning any in-flight load.
    ///
    /// Returning home puts the cursor on whatever you currently have open, so the
    /// round trip out and back lands where you left rather than at the top.
    ///
    /// Abandoning puts the open in flight down at once ([`loading::Loader::retire`]):
    /// its jobs are superseded, so their answers are dropped on arrival and none can
    /// install a dataset or take the user off the screen they went to; its stop flag
    /// is raised, so a download stops and an in-flight cloud pass stops issuing paid
    /// reads within a wave. Work that is not the open's — an export, an analysis, the
    /// footer pass of the dataset already on screen — is deliberately left alone, so
    /// its progress indicator and its completion modal must survive this.
    pub fn abandon_load(&mut self) {
        let retired = self.loading.retire();
        if let Some(retired) = retired {
            self.put_down_load(retired);
        }
        // A chart being prepared for the dataset we are leaving would otherwise keep
        // the throbber up on the home screen, and its result could later land in a
        // different dataset with the same column names.
        self.reset_chart_state();
        // A look that is out belongs to the home screen being left, and the thread it is
        // on may never come back — a share that has gone away is the case it exists for.
        // Superseded, its answer touches nothing, and the keyboard does not wait for it.
        if self.jobs.supersede(|job| matches!(job, Job::Classify(_))) {
            self.home.status = None;
        }
        // And a collect that was waiting behind this load goes with it. Left standing,
        // it runs the moment the generation is free — reading the dataset the user
        // walked away from, at the home screen, with every key held.
        self.jobs.take_owed(Self::owed_rows);
        // Only an open's own wait is put down. An export holds keys too, and it keeps
        // running. The rows the open's last step is reading still land; nobody waits on
        // them.
        if retired.is_some() {
            self.busy = false;
            let quieted = self.jobs.quiet(Self::reading_rows);
            if self
                .status_message
                .as_ref()
                .is_some_and(|status| quieted.contains(status))
            {
                self.status_message = None;
            }
        }
        // Keys typed at the frozen screen were meant for the load, not for home:
        // replayed there they could open a dataset nobody asked for.
        self.screen_generation = self.screen_generation.wrapping_add(1);
    }

    /// Whether finding out what a path is could sit on a mount that never answers.
    ///
    /// Two halves. An object-store or HTTP URL names something no mount is responsible
    /// for — what is behind it is the scan's business, and stat'ing it only ever asks the
    /// working directory about a file called `s3:` — and an ordinary local path answers at
    /// once, so making the user wait a round trip for it would be a delay bought with
    /// nothing.
    ///
    /// What is left is a path on a mount the home screen calls a network one, which is
    /// the case `is_remote_path` exists to name and the only one worth a worker.
    pub(crate) fn looking_could_block(&self, path: &Path) -> bool {
        // `cloud://<id>` is a place, not a path: `input_source` calls the unknown scheme
        // local and `is_remote_path` calls it remote, so without this a worker would be
        // sent to stat it and come back with "No such path".
        !home::is_cloud_place(path)
            && matches!(source::input_source(path), source::InputSource::Local(_))
            && (self.home.network_check)(path)
    }

    /// Do with a path whatever its kind calls for: browse into it, say it is a lake
    /// table, or open it.
    ///
    /// `jump` is a path typed at `~` rather than a row already listed, which starts a new
    /// browse so Esc comes back from there to the listing.
    pub(crate) fn open_what_it_is(
        &mut self,
        path: PathBuf,
        kind: discover::EntryKind,
        jump: bool,
    ) -> Option<AppEvent> {
        let go_inside = |app: &mut Self, path: PathBuf| {
            if jump {
                app.home_jump_into(path);
            } else {
                app.home_browse_into(path);
            }
        };
        if kind == discover::EntryKind::Directory {
            go_inside(self, path);
            return None;
        }
        // No reader: a local file's bytes, in the hex view. A remote one is dimmed and
        // its details pane says why.
        if kind == discover::EntryKind::Other {
            if matches!(source::input_source(&path), source::InputSource::Local(_)) {
                self.open_hex(path, crate::hex_view::Origin::Home, true, None);
            }
            return None;
        }
        // A lake table's files are not its rows: the ones a delete or an update
        // tombstoned are still on disk, every rewritten version is here together, and
        // compaction leaves both sides in place. Going inside is what datui can honestly
        // do with one, and saying so is better than a silent wrong answer.
        if let Some(format) = kind.lake_name() {
            self.home.lake_here = Some((path.clone(), format));
            go_inside(self, path);
            return None;
        }
        let directory = matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        );
        // A directory typed at `~` is a place to go, as → makes it, whatever it holds:
        // its door is one row in, and naming a directory never starts a read of all of it.
        if directory && jump {
            go_inside(self, path);
            return None;
        }
        // A cloud directory that is a dataset opens as one: its URL as a prefix, which is
        // what makes the open a scan of every file under it.
        if directory && home::is_object_store_url(&path) {
            // A prefix, not a directory: the scan is what walks it.
            return Some(self.home_open_path(home::directory_dataset_url(&path), false));
        }
        // Said here, where the file was named, rather than after a download and a load
        // that could only end the same way. A row would be dimmed; a typed path has no
        // row, so the line says it.
        // A format spec may read it: by its glob, or by magic the open looks for.
        let a_spec_may_read = !self.formats.by_glob(&path, false).is_empty()
            || self.formats.specs.iter().any(|f| !f.spec.magic.is_empty());
        // A table inside a file of tables (`flight.ulg/sensor_accel.1`) has the file's
        // name in front, and a log found by its first bytes (`00000042.BIN`) a name
        // that says nothing.
        if kind == discover::EntryKind::File
            && discover::unreadable_by_name(&path)
            && !a_spec_may_read
            && crate::members::split(&path).is_none()
            && crate::members::holder(&path).is_none()
            && crate::members::split_variant(&path, &self.formats).is_none()
            && crate::hf_splits::split_place(&path).is_none()
        {
            self.home.status = Some(discover::NO_READER.to_string());
            return None;
        }
        // The preview read this file's first page through the open's own steps: the
        // open installs that dataset rather than reading it again.
        let prepared = (!directory)
            .then(|| self.home_app.previews.take_prepared(&path))
            .flatten();
        // A small file of the built-in catalog is fetched without a question: the row
        // already said what it is and what it weighs. A URL the user typed still asks.
        let unasked = self
            .home
            .catalogs
            .iter()
            .filter(|c| c.origin == catalog::Origin::Bundled)
            .flat_map(|c| c.datasets.iter())
            .find(|d| d.location == path)
            .filter(|_| {
                !jump && matches!(source::input_source(&path), source::InputSource::Http(_))
            })
            .map(|dataset| UnaskedDownload {
                limit: UnaskedDownload::LIMIT,
                listed: dataset.size,
            });
        match self.home_open_path(path, directory) {
            AppEvent::Open(paths, mut options) => {
                options.prepared = prepared.map(|p| Arc::new(Mutex::new(Some(p))));
                options.download_unasked = unasked;
                Some(AppEvent::Open(paths, options))
            }
            event => Some(event),
        }
    }

    /// What `datui <path>` does with a directory: the same rule as `Enter` on its row,
    /// so the highlighted row, the `~` prompt and the command line agree. A hive root or
    /// a directory whose files are one table opens as one table; any other directory
    /// opens the home screen browsed into it. The directory is looked into with
    /// [`home::look_into`], the home screen's own call. `--hive` still forces partition
    /// columns.
    ///
    /// Asks the filesystem whether a local path is a directory, so `run` calls it on a
    /// worker ([`AppEvent::OpenNamed`]). Returns the event that carries the open on:
    /// `LookThenOpenDirectory` or `Open`.
    pub fn route_named_paths(paths: Vec<PathBuf>, options: OpenOptions) -> AppEvent {
        Self::route_named_paths_with(paths, options, &crate::formats::Registry::default())
    }

    /// [`Self::route_named_paths`], with the format specs on the search path: a
    /// directory a spec reads as column files is opened, not looked at.
    pub fn route_named_paths_with(
        paths: Vec<PathBuf>,
        options: OpenOptions,
        formats: &crate::formats::Registry,
    ) -> AppEvent {
        if let Some(event) = Self::route_named_without_looking(&paths, &options) {
            return event;
        }
        // Several paths are a list of files to read together, and `--hive` is an answer
        // already given. Neither is a question about what one directory is.
        let single = (paths.len() == 1 && !options.hive).then(|| paths[0].clone());
        let Some(dir) = single.filter(|p| p.is_dir()) else {
            return AppEvent::Open(paths, options);
        };
        // A format spec named for it, or one whose glob names it, reads it as columns.
        if options.spec_file.is_some()
            || options.spec_name.is_some()
            || !formats.by_glob(&dir, true).is_empty()
        {
            return AppEvent::Open(paths, options);
        }
        // Looking at a directory reads its footers, or the front of a spread of its
        // files. For a directory of large Parquet that is seconds — 4.6 of them on a real
        // one — so it goes to a worker, and the answer comes back as an event like every
        // other read.
        AppEvent::LookThenOpenDirectory(dir, options)
    }

    /// The part of [`Self::route_named_paths`] that needs no filesystem: a cloud
    /// directory is looked at too, by one page of its listing — what is in it picks the
    /// reader, as it does for the `(all files)` row. Scanned blind, it was read as
    /// Parquet whatever it held. A glob, a file name or `--format` already says what to
    /// read.
    pub(crate) fn route_named_without_looking(
        paths: &[PathBuf],
        options: &OpenOptions,
    ) -> Option<AppEvent> {
        #[cfg(feature = "cloud")]
        if let [dir] = paths
            && !options.hive
            && home::is_object_store_url(dir)
            && options.format.is_none()
            && !dir.to_string_lossy().contains('*')
            && !home::names_a_file(dir)
        {
            return Some(AppEvent::LookThenOpenDirectory(
                dir.clone(),
                options.clone(),
            ));
        }
        let _ = (paths, options);
        None
    }

    /// The first named local path that is not there. A URL or a glob is left to the
    /// open, which says what it found, and standard input is no path.
    pub fn missing_named_path(
        paths: &[PathBuf],
        formats: &crate::formats::Registry,
    ) -> Option<PathBuf> {
        paths
            .iter()
            .find(|path| {
                !source::is_remote_url(path)
                    && !crate::stdin::is_stdin(path)
                    && !source::expands_as_glob(path)
                    && !path.exists()
                    && crate::members::split(path).is_none()
                    && crate::members::split_variant(path, formats).is_none()
            })
            .cloned()
    }

    /// Act on what the look at a directory named on the command line found.
    ///
    /// The other half of [`Self::route_named_paths`], which is
    /// where the reasoning for the rule itself is.
    pub(crate) fn open_the_directory_looked_at(
        &mut self,
        dir: PathBuf,
        kind: discover::EntryKind,
        holds: Option<&discover::Holds>,
        mut options: OpenOptions,
    ) -> Option<AppEvent> {
        #[cfg(feature = "cloud")]
        if home::is_object_store_url(&dir) {
            return self.open_the_cloud_directory_looked_at(dir, kind, holds, options);
        }
        let _ = holds;
        // No override for the user's reader settings here, and none needed: the look
        // read every file the way this open will, so `--no-header` and the skips have
        // already been accounted for by the rule rather than around it. Overriding
        // instead took three goes to get wrong in three different ways — it fired on
        // config values, it fired on directories with nothing readable in them, and it
        // fired on Parquet, which no CSV setting can affect.
        //
        // A lake table's files are not its rows, so the home screen is opened on it and
        // says why — the same sentence the row gives, because it is the same refusal.
        if let Some(format) = kind.lake_name() {
            self.enter_home();
            self.home.lake_here = Some((dir.clone(), format));
            self.home_jump_into(dir);
            return None;
        }
        // One table: read it. `hive` is what puts the open on the directory route, where
        // what the directory holds picks the reader.
        if matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        ) {
            options.hive = true;
            self.set_loading_phase("Scanning input", 10);
            self.name_what_is_loading(dir.clone());
            return Some(AppEvent::Open(vec![dir], options));
        }
        // A place to look inside. `datui .` is this, and so is a directory of separate
        // tables — where the `(all files)` row inside is the one keystroke that unions
        // them anyway.
        self.enter_home();
        self.home_jump_into(dir);
        None
    }

    /// As [`Self::open_the_directory_looked_at`], for a cloud directory: what `Enter`
    /// on its `(all files)` row does, or a browse into it when there is no data
    /// directly inside to read.
    #[cfg(feature = "cloud")]
    fn open_the_cloud_directory_looked_at(
        &mut self,
        dir: PathBuf,
        kind: discover::EntryKind,
        holds: Option<&discover::Holds>,
        options: OpenOptions,
    ) -> Option<AppEvent> {
        let open = |app: &mut Self, path: PathBuf, options: OpenOptions| {
            app.set_loading_phase("Scanning input", 10);
            app.name_what_is_loading(path.clone());
            Some(AppEvent::Open(vec![path], options))
        };
        // The listing was refused. The open says why, in the words of whatever
        // refused it, which is what happened before anything looked.
        let Some(holds) = holds else {
            return open(self, dir, options);
        };
        if let Some(format) = kind.lake_name() {
            self.enter_home();
            self.home.lake_here = Some((dir.clone(), format));
            self.home_jump_into(dir);
            return None;
        }
        let directory = home::directory_dataset_url(&dir);
        if matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        ) {
            let options = OpenOptions {
                hive: true,
                ..options
            };
            return open(self, directory, options);
        }
        if let Some((format, left_out)) = Self::cloud_prefix_format(holds) {
            let options = OpenOptions {
                hive: true,
                format: Some(format),
                left_out,
                ..options
            };
            return open(self, directory, options);
        }
        // Only directories, or nothing datui reads: somewhere to look inside, with
        // the reason when there is one.
        self.enter_home();
        self.home_jump_into(dir);
        self.home.status = Self::why_a_cloud_prefix_cannot_be_read(holds);
        None
    }

    /// What an open the home screen starts reads with: the config's read and CSV
    /// settings, as an open named on the command line has them under its flags.
    pub(crate) fn open_defaults(&self) -> OpenOptions {
        match crate::cli::parse_args(["datui"]) {
            Ok(args) => OpenOptions::from_args_and_config(&args, &self.app_config),
            Err(_) => OpenOptions::default(),
        }
    }

    /// `options` for the compressed delimited file `file`, in the dialect of the
    /// delimited spec it matches, or as they are when it matches none. The loader sends
    /// such a file straight to be decompressed, past the scan that matches the others.
    fn with_delimited_spec(
        file: &Path,
        mut options: OpenOptions,
        formats: &crate::formats::Registry,
    ) -> Result<OpenOptions> {
        if options.delimited.is_some() {
            return Ok(options);
        }
        let asked = crate::formats::Asked {
            spec_file: options.spec_file.clone(),
            spec: options.spec_fetched.clone(),
            spec_name: options.spec_name.clone(),
            compression: options.compression,
            ..Default::default()
        };
        let crate::formats::Route::Delimited(choice) =
            crate::formats::route(file, &asked, formats).map_err(|e| color_eyre::eyre::eyre!(e))?
        else {
            return Ok(options);
        };
        let Some(delimited) = choice.spec.delimited.clone() else {
            return Ok(options);
        };
        delimited.apply(&mut options);
        let chosen =
            crate::delimited_spec::DelimitedRead::chosen(choice.spec, choice.by, choice.also);
        let read = crate::delimited_spec::read_facts(&chosen, &[file.to_path_buf()], &options)?;
        options.delimited = Some(Arc::new(read));
        Ok(options)
    }

    /// Read a compressed CSV, TSV or PSV into a table state, split on its format's
    /// separator.
    ///
    /// This is the one input datui cannot scan lazily: the file has to be
    /// decompressed and parsed before anything can be shown, which for a large export
    /// is minutes. It takes no `&self` so it can run on a background thread.
    fn decompressed_delimited_state(
        path: &Path,
        options: &OpenOptions,
        writer: &crate::unfinished::Writer,
    ) -> Result<DataTableState> {
        let separator = options
            .format
            .and_then(FileFormat::separator)
            .unwrap_or(b',');
        DataTableState::from_read(
            crate::readers::csv::read_delimited(path, separator, options, writer)?,
            options,
        )
    }

    /// Polars' view of one source's S3 settings, for `scan_parquet`.
    #[cfg(feature = "cloud")]
    fn build_s3_cloud_options(settings: &crate::cloud_sources::S3Settings) -> CloudOptions {
        let settings = settings.clone();
        let virtual_hosted = (settings.endpoint.is_some() || settings.virtual_hosted.is_some())
            .then(|| settings.virtual_hosted_style().to_string());
        let configs: Vec<(AmazonS3ConfigKey, String)> = [
            (AmazonS3ConfigKey::Endpoint, settings.endpoint),
            (AmazonS3ConfigKey::AccessKeyId, settings.access_key_id),
            (
                AmazonS3ConfigKey::SecretAccessKey,
                settings.secret_access_key,
            ),
            (AmazonS3ConfigKey::Token, settings.session_token),
            (AmazonS3ConfigKey::Region, settings.region),
            (AmazonS3ConfigKey::VirtualHostedStyleRequest, virtual_hosted),
            (
                AmazonS3ConfigKey::SkipSignature,
                settings.skip_signature.then(|| "true".to_string()),
            ),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.map(|v| (key, v)))
        .chain([(
            AmazonS3ConfigKey::Client(crate::user_agent::CLIENT_KEY),
            crate::user_agent::get(),
        )])
        .collect();
        CloudOptions::default().with_aws(configs)
    }

    /// The bucket and key of an `s3://bucket/key` or `gs://bucket/key` URL. The key
    /// is empty for a bucket root.
    #[cfg(feature = "cloud")]
    pub(crate) fn cloud_bucket_and_key(url: &str) -> Result<(String, String)> {
        if let Some((_, container, key)) = source::azure_parts(url) {
            return Ok((container, key.trim_matches('/').to_string()));
        }
        crate::cloud_browse::split_bucket_url(url)
            .map(|(_, bucket, key)| (bucket, key))
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("URL must be s3://bucket/key or gs://bucket/key")
            })
    }

    /// The store Polars itself will scan `url` through, from its cache keyed on the
    /// bucket and `options`, so the footer read, the size probe and a download share
    /// one credential chain, TLS client and connection pool with the scan instead of
    /// each building a store of their own.
    #[cfg(feature = "cloud")]
    fn polars_object_store(
        url: &str,
        options: &CloudOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let url = url.to_string();
        let options = options.clone();
        wait_on_runtime(runtime, async move {
            let (_, store) = polars::io::cloud::build_object_store(
                PlRefPath::new(url.as_str()),
                Some(&options),
                false,
            )
            .await?;
            polars::prelude::PolarsResult::Ok(store.to_dyn_object_store().await.into_owned())
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("cancelled"))?
        .map_err(|e| color_eyre::eyre::eyre!("Object store config failed: {}", e))
    }

    /// Build an HTTP agent with a total time budget.
    ///
    /// ureq 3 moved timeouts off the request and onto agent configuration, so
    /// every request has to come from an agent to be bounded at all. Leaving a
    /// request unbounded would mean a remote that accepts a connection and then
    /// dribbles bytes forever hangs the whole TUI, and the user's only way out
    /// is to kill the process.
    ///
    /// `timeout_global` covers the entire exchange rather than individual
    /// socket operations, which is the property that matters here: a server
    /// that sends one byte every 29 seconds defeats a per-read timeout but not
    /// this one.
    #[cfg(feature = "http")]
    fn http_agent(total: std::time::Duration) -> ureq::Agent {
        crate::user_agent::ureq_config()
            .timeout_global(Some(total))
            .build()
            .into()
    }

    /// What a HEAD says an HTTP(S) file weighs: `None` when it does not say. An error
    /// only when the answer settles that the file cannot be had (a 404, no server); a
    /// server that refuses HEAD may still send the file.
    #[cfg(feature = "http")]
    pub(crate) fn fetch_remote_size_http(
        url: &str,
    ) -> std::result::Result<Option<u64>, crate::error_display::HttpGone> {
        let agent = Self::http_agent(std::time::Duration::from_secs(15));
        // ureq asks for gzip by default and strips Content-Length from a compressed
        // answer, so a server that compresses (GitHub Pages does) reports no size.
        // Identity asks for the file's own length, which is what lands on disk.
        match agent.head(url).header("Accept-Encoding", "identity").call() {
            Ok(r) => Ok(r
                .headers()
                .get("Content-Length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())),
            Err(e) => crate::error_display::http_gone(url, &e).map_or(Ok(None), Err),
        }
    }

    /// The size of one S3 or GCS object, from a HEAD through the shared store.
    #[cfg(feature = "cloud")]
    fn fetch_remote_size_cloud(
        url: &str,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Option<u64>> {
        use object_store::ObjectStoreExt;

        let (_bucket, key) = Self::cloud_bucket_and_key(url)?;
        if key.is_empty() {
            return Ok(None);
        }
        let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;
        let path = crate::cloud_browse::object_path(&key);
        let head = wait_on_runtime(runtime, async move { store.head(&path).await });
        Ok(head.and_then(|r| r.ok()).map(|meta| meta.size))
    }

    /// Download `url` to a temporary file. `stop` ends it early, while the server is
    /// sending or while it is silent, and any failure removes the file; see
    /// [`crate::download::read_to_temp`].
    ///
    /// Past `limit` bytes it stops with a [`crate::download::PastLimit`] error.
    #[cfg(feature = "http")]
    fn download_http_to_temp(
        url: &str,
        temp_dir: Option<&Path>,
        extension: Option<&str>,
        limit: Option<u64>,
        writer: &crate::unfinished::Writer,
    ) -> Result<crate::download::TempDownload> {
        use crate::download::StreamError;

        let url = url.to_string();
        let open = move || {
            let agent = Self::http_agent(std::time::Duration::from_secs(300));
            // ureq answers a 4xx or 5xx with an error, so every failure is said here.
            let response = agent
                .get(&url)
                .call()
                .map_err(|e| crate::error_display::http_message(&url, &e))?;
            // No length: ureq hands back a compressed answer decompressed, and the
            // Content-Length it came with is the wire's, not the file's.
            Ok((response.into_body().into_reader(), None))
        };
        crate::download::read_to_temp(temp_dir, extension, open, writer, limit).map_err(|error| {
            match error {
                StreamError::Open(message) => color_eyre::eyre::eyre!(message),
                StreamError::Read(e) => {
                    color_eyre::eyre::eyre!("Download failed partway. Check your connection: {e}")
                }
                StreamError::Short { expected, got } => color_eyre::eyre::eyre!(
                    "Download failed partway: it ended after {got} of {expected} bytes."
                ),
                StreamError::Write(report) => report,
                StreamError::Cut => color_eyre::eyre::eyre!("Download was cancelled."),
            }
        })
    }

    /// Stream one S3, GCS or Azure object to a temporary file, named for the user by
    /// its scheme in any error. A few chunks are in memory at a time; see
    /// [`crate::download`]. `writer`'s open stopping ends it early, and any failure
    /// removes the file.
    #[cfg(feature = "cloud")]
    fn download_cloud_to_temp(
        url: &str,
        cloud: &crate::config::CloudConfig,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        writer: &crate::unfinished::Writer,
    ) -> Result<crate::download::TempDownload> {
        use crate::download::StreamError;
        use object_store::ObjectStoreExt;

        let (label, example) = match source::input_source(Path::new(url)) {
            source::InputSource::Gcs(_) => ("GCS", "gs://bucket/path/file.csv"),
            source::InputSource::Azure(_) => (
                "Azure",
                "abfss://container@account.dfs.core.windows.net/path/file.csv",
            ),
            _ => ("S3", "s3://bucket/path/file.csv"),
        };
        let ext = source::download_suffix(url);
        let (_bucket, key) = Self::cloud_bucket_and_key(url)?;
        if key.is_empty() {
            return Err(crate::error_display::FileError::new(
                Path::new(url),
                format!("a {label} URL names an object here, such as {example}"),
            )
            .into());
        }
        let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;

        let path = crate::cloud_browse::object_path(&key);
        let open = async move {
            let got = store
                .get(&path)
                .await
                .map_err(|e| crate::error_display::store_message(&e))?;
            let len = got.range.end - got.range.start;
            Ok((got.into_stream(), Some(len)))
        };
        let failed = |what: String| -> color_eyre::Report {
            crate::error_display::FileError::new(Path::new(url), what).into()
        };
        crate::download::stream_to_temp(
            runtime,
            options.temp_dir.as_deref(),
            ext.as_deref(),
            open,
            writer,
        )
        .map_err(|error| match error {
            StreamError::Open(e) => failed(e),
            StreamError::Read(e) => failed(format!("the download stopped: {e}")),
            StreamError::Short { expected, got } => {
                failed(format!("it ended after {got} of {expected} bytes"))
            }
            StreamError::Write(report) => report,
            StreamError::Cut => failed("the download was cancelled".to_string()),
        })
    }

    /// Run the worker of an open's phase, as `load`'s job: its answer goes to the loader.
    ///
    /// Every phase runs off the event thread. The size probe is a HEAD request: fifteen
    /// seconds of timeout for HTTP, unbounded for S3 and GCS, and inline it froze the UI
    /// precisely where the user is most likely to want out. Scanning is where the
    /// wall-clock time goes — CSV schema inference, and hive directories with many files
    /// — and the schema read of a directory reads a footer from each file.
    /// The open's scan of `paths`, named `path`: what the frame is, or what has to
    /// happen before there is one. Run by the open's `Scan` phase, and by the home
    /// screen's preview, which hands what it builds to the open.
    pub(crate) fn scan_for_open(
        cloud: &crate::config::CloudConfig,
        formats: &crate::formats::Registry,
        paths: &[PathBuf],
        options: OpenOptions,
        path: Option<PathBuf>,
    ) -> std::result::Result<loading::LoadAnswer, String> {
        use loading::LoadAnswer;
        let bytes_of = |files: &[PathBuf]| -> u64 {
            files
                .iter()
                .filter_map(|f| std::fs::metadata(f).ok())
                .map(|m| m.len())
                .sum()
        };
        // What the read passed over rides back with the options it was asked
        // for, so the dataset can say what it left out. Seeded with what the
        // caller already knows and overwritten by what the read finds: a
        // directory on disk is the read's own answer, because it is the pass
        // that decides, while for a prefix in an object store Polars does the
        // listing and never sees the other formats — there the home screen's
        // listing is the only witness.
        let mut report = ReadReport {
            left_out: options.left_out.clone(),
            files_disagree: options.files_disagree,
            format: None,
            format_read: None,
            read_python: Vec::new(),
            sqlite: None,
            opened: None,
            splits: options.splits.clone(),
            delimited: None,
            table: None,
            guessed: false,
            read_notes: Vec::new(),
            typing: Default::default(),
        };
        // A followed file reads every row it can and counts the rest: a row
        // that does not fit the schema never stops the follow.
        let options = OpenOptions {
            ignore_errors: options.ignore_errors || options.follow,
            ..options
        };
        let named = |e: color_eyre::Report| {
            crate::error_display::user_message_from_report(&e, path.as_deref())
        };
        // An Arrow IPC stream followed is read by a scan of its own, not converted;
        // NDJSON followed is scanned rather than read whole, by its reader.
        let followed_stream = options.follow
            && crate::follow::followed_stream(
                &paths[0],
                Some(crate::follow::format_of(&paths[0], options.format)),
                &options,
            );
        let scan = if followed_stream {
            crate::follow::stream::scan(&paths[0])
                .map(Scan::from)
                .map_err(|e| color_eyre::eyre::eyre!(e))
        } else {
            Self::build_lazyframe_from_paths_with(cloud, paths, &options, &mut report, formats)
        }
        // Named as the dataset is: a download by its URL, not its temp file.
        .map_err(named)?;
        let format = scan.format(report.format.or(options.format));
        // Bounded to the complete records, and counted for the watcher. A
        // recording that cannot be followed is read as it stands, and goes on.
        let recording = options
            .spool
            .as_ref()
            .is_some_and(|handle| handle.spool().tee().is_some());
        let (scan, tail) = match scan {
            Scan::Frame(lf) if options.follow => {
                let format = crate::follow::format_of(&paths[0], format);
                let refused = (!followed_stream)
                    .then(|| crate::follow::refusal(Some(format), &options))
                    .flatten();
                match refused {
                    Some(_) if recording => (Scan::Frame(lf), None),
                    Some(refusal) => return Err(refusal),
                    None => {
                        let (lf, tail) =
                            crate::follow::bound_to_complete(*lf, &paths[0], format, &options)
                                .map_err(named)?;
                        (Scan::Frame(Box::new(lf)), Some(Arc::new(tail)))
                    }
                }
            }
            _ if options.follow && !recording => {
                return Err(crate::follow::refusal(format, &options)
                    .unwrap_or_else(|| "This file cannot be followed as it grows.".to_string()));
            }
            scan => (scan, None),
        };
        let read_mode = scan.read_mode(format, report.format_read.is_some(), &options);
        let mut options = OpenOptions {
            left_out: report.left_out,
            files_disagree: report.files_disagree,
            format,
            format_read: report.format_read,
            sqlite: report.sqlite,
            opened: report.opened,
            splits: report.splits,
            read_python: report.read_python,
            read_mode,
            tail,
            table: report.table.or_else(|| options.table.clone()),
            format_guessed: options.format_guessed || report.guessed,
            read_notes: report.read_notes,
            typing: report.typing,
            ..options
        };
        // The spec's dialect stays with the dataset, so a read again (`H`,
        // a decompressed copy) reads as this one did.
        if let Some(read) = report.delimited {
            read.delimited().apply(&mut options);
            options.delimited = Some(read);
        }
        Ok(match scan {
            Scan::Frame(lf) => LoadAnswer::Scanned { lf, path, options },
            Scan::Decompress { file, .. } => LoadAnswer::Compressed {
                file,
                path,
                options,
            },
            Scan::Streams(files) => LoadAnswer::Convert {
                what: loading::Conversion::Streams,
                bytes: bytes_of(&files),
                files,
                path,
                options,
            },
            Scan::DecompressSpec { file, choice } => LoadAnswer::CompressedRecords {
                file,
                path,
                choice,
                options,
            },
            Scan::ReadInto { files, format } => LoadAnswer::Convert {
                what: loading::Conversion::Text(format),
                bytes: bytes_of(&files),
                files,
                path,
                options,
            },
            Scan::Tables { file, tables, .. } => LoadAnswer::Tables { file, tables, path },
            Scan::Unpack {
                file,
                member,
                format,
            } => LoadAnswer::Convert {
                what: loading::Conversion::Text(format),
                bytes: bytes_of(std::slice::from_ref(&file)),
                files: vec![file],
                path,
                options: OpenOptions {
                    table: Some(member),
                    ..options
                },
            },
            Scan::Hex { file, asked } => LoadAnswer::Hex {
                file,
                asked,
                record_size: options.record_size,
            },
        })
    }

    /// The open's schema read of the scan's frame: the dataset, built with everything
    /// the open `made`. Run by the open's `ReadSchema` phase, and by the home screen's
    /// preview.
    pub(crate) fn read_schema_for_open(
        lf: LazyFrame,
        path: Option<PathBuf>,
        options: OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
        made: loading::Made,
    ) -> std::result::Result<loading::LoadAnswer, String> {
        use loading::LoadAnswer;
        let (state, facts, debug_label) =
            Self::build_schema_state(lf, path.as_deref(), &options, cloud, runtime, report)
                .map_err(|e| crate::error_display::user_message_from_report(&e, path.as_deref()))?;
        // Everything the open found, given to the dataset as it is built.
        let loading::Made {
            download,
            converted,
            notes,
            other_tables,
            detail,
        } = made;
        let mut open_notes = facts.open_notes;
        open_notes.extend(notes);
        let mut other_tables_found = facts.other_tables;
        other_tables_found.extend(other_tables);
        let state = state.with_open(OpenFacts {
            fetched: Self::fetched(download.as_ref(), path.as_deref()),
            download,
            converted,
            other_tables: other_tables_found,
            open_notes,
            detail: detail.or(facts.detail),
            ..facts
        });
        Ok(LoadAnswer::SchemaRead {
            state: Box::new(state),
            path,
            options,
            debug_label: Some(debug_label),
        })
    }

    fn spawn_load_phase(&mut self, load: loading::LoadId, step: loading::Step) {
        use loading::{LoadAnswer, Step};
        let job = Job::Load(load);
        match step {
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::ReadHeaders {
                url,
                format,
                options,
                writer,
            } => {
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                self.spawn_job(job, Some("Reading headers..."), move |_| {
                    let read = crate::remote_model::read(&url, format, &cloud, &runtime, &|| {
                        writer.stopped()
                    });
                    let crate::remote_model::Read { lf, summary, notes } = match read {
                        Ok(read) => read,
                        Err(crate::model_files::RangeError::NoRanges) => {
                            return Ok(Answer::Load(Box::new(LoadAnswer::NoRanges { options })));
                        }
                        // The URL in the message may carry a password or a signature.
                        Err(crate::model_files::RangeError::Failed(message)) => {
                            return Err(crate::logging::redact(&message, &[]));
                        }
                    };
                    let opened = Arc::new(crate::model_files::opened(&summary));
                    let options = OpenOptions {
                        format: Some(format),
                        opened: Some(opened.clone()),
                        ..options
                    };
                    // The table is the headers, in memory: nothing is left to scan.
                    let state = Self::schema_state_from_full_scan(
                        lf,
                        None,
                        &OpenOptions {
                            hive: false,
                            ..options.clone()
                        },
                    )
                    .map_err(|e| crate::error_display::user_message_from_report(&e, Some(&url)))?
                    .with_open(OpenFacts {
                        detail: opened.detail.clone(),
                        open_notes: notes,
                        read_as: Some(format),
                        ..Default::default()
                    });
                    Ok(Answer::Load(Box::new(LoadAnswer::SchemaRead {
                        state: Box::new(state),
                        path: Some(url),
                        options,
                        debug_label: Some("model headers (ranged)".to_string()),
                    })))
                });
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Probe(pending) => {
                #[cfg(feature = "cloud")]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                self.spawn_job(job, Some("Checking size..."), move |_| {
                    // Arrow in a store: its listing says which objects, and which of
                    // them are streams to download.
                    #[cfg(feature = "cloud")]
                    if let loading::PendingDownload::Arrow { url, .. } = &pending {
                        let (_, _, options) = pending.parts();
                        let (objects, options) = crate::cloud_arrow::list(
                            url, options, &cloud, &runtime,
                        )
                        .map_err(|e| crate::error_display::user_message_from_report(&e, None))?;
                        let size = crate::cloud_arrow::stream_bytes(&objects);
                        return Ok(Answer::Load(Box::new(LoadAnswer::Sized(
                            loading::PendingDownload::Arrow {
                                url: url.clone(),
                                objects,
                                size: Some(size),
                                options,
                            },
                        ))));
                    }
                    let size = match &pending {
                        #[cfg(feature = "http")]
                        loading::PendingDownload::Http { url, .. } => {
                            // A file that is not there, or a host that does not
                            // answer, ends the open here, not after a question
                            // about downloading it.
                            Self::fetch_remote_size_http(url).map_err(|gone| gone.message)?
                        }
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::S3 { url, .. }
                        | loading::PendingDownload::Gcs { url, .. }
                        | loading::PendingDownload::Azure { url, .. } => {
                            Self::fetch_remote_size_cloud(url, &cloud, &runtime).unwrap_or(None)
                        }
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::Arrow { size, .. } => *size,
                    };
                    Ok(Answer::Load(Box::new(LoadAnswer::Sized(
                        pending.with_size(size),
                    ))))
                });
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Download { pending, writer } => {
                // The load's stop flag is raised when it is abandoned or another open
                // replaces it, and when the app drops: the download stops at the next
                // chunk, or while the source is silent, and removes its file. Quitting
                // removes it even if the process ends first (`ExitSweep`).
                #[cfg(feature = "cloud")]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                let status = match &pending {
                    #[cfg(feature = "http")]
                    loading::PendingDownload::Http { .. } => "Downloading...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::S3 { .. } => "Downloading from S3...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::Gcs { .. } => "Downloading from GCS...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::Azure { .. } => "Downloading from Azure...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::Arrow { url, .. } => {
                        match source::input_source(Path::new(url)) {
                            source::InputSource::Gcs(_) => "Downloading from GCS...",
                            source::InputSource::Azure(_) => "Downloading from Azure...",
                            _ => "Downloading from S3...",
                        }
                    }
                };
                // How much, when the server said: a download nobody was asked about
                // says what it is fetching.
                let sized = pending
                    .parts()
                    .1
                    .filter(|_| status == "Downloading...")
                    .map(|size| format!("Downloading {}...", crate::numfmt::bytes(size)));
                let status = sized.as_deref().unwrap_or(status);
                self.spawn_job(job, Some(status), move |_| {
                    let (url, _, options) = pending.parts();
                    let fetched = match &pending {
                        #[cfg(feature = "http")]
                        loading::PendingDownload::Http { .. } => {
                            let ext = source::download_suffix(url);
                            // A download nobody was asked about stops at its limit, when
                            // the server did not say its size: a size it said bounds the
                            // transfer, and the bytes counted here are decompressed.
                            let limit = options
                                .download_unasked
                                .filter(|_| pending.parts().1.is_none())
                                .map(|unasked| unasked.limit);
                            Self::download_http_to_temp(
                                url,
                                options.temp_dir.as_deref(),
                                ext.as_deref(),
                                limit,
                                &writer,
                            )
                            .map(|file| (file, options.clone()))
                        }
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::S3 { .. }
                        | loading::PendingDownload::Gcs { .. }
                        | loading::PendingDownload::Azure { .. } => {
                            Self::download_cloud_to_temp(url, &cloud, options, &runtime, &writer)
                                .map(|file| (file, options.clone()))
                        }
                        // Its streams, converted as they arrive; its IPC files stay put.
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::Arrow { objects, .. } => {
                            crate::cloud_arrow::download(
                                objects, options, &cloud, &runtime, &writer,
                            )
                            .map(|(file, parts)| {
                                let options = OpenOptions {
                                    format: Some(FileFormat::Arrow),
                                    hive: false,
                                    arrow_parts: Some(Arc::new(parts)),
                                    ..options.clone()
                                };
                                (file, options)
                            })
                        }
                    };
                    let (download, options) = match fetched {
                        Err(e) if e.downcast_ref::<crate::download::PastLimit>().is_some() => {
                            return Ok(Answer::Load(Box::new(LoadAnswer::PastLimit(pending))));
                        }
                        fetched => fetched.map_err(|e| {
                            crate::error_display::user_message_from_report(&e, None)
                        })?,
                    };
                    Ok(Answer::Load(Box::new(LoadAnswer::Downloaded {
                        download,
                        options,
                    })))
                });
            }
            Step::Spool {
                options,
                writer,
                read,
            } => {
                // The read is a thread of its own, so a producer gone quiet does not hold
                // up the stop: Ctrl+O and quitting remove the partial file at once.
                let piped = self.pipes.stdin_reader.take();
                let stdout = self.pipes.stdout_pass.take();
                self.spawn_job(job, Some("Reading stdin..."), move |_| {
                    let open = move || -> crate::download::Opened<Box<dyn std::io::Read + Send>> {
                        Ok((piped.unwrap_or_else(|| Box::new(std::io::stdin())), None))
                    };
                    // Followed, the copy goes on behind the first rows; recorded, it
                    // goes to the file the user named.
                    // And read as it arrives when what it holds can be.
                    let (download, options) = if options.follow
                        || options.tee.is_some()
                        || crate::stdin::may_read_as_it_arrives(&options)
                    {
                        match crate::follow::spool(open, options, &writer, &read, stdout)? {
                            (crate::follow::Spooled::Temp(download), options) => {
                                (download, options)
                            }
                            (crate::follow::Spooled::Kept(file), options) => {
                                return Ok(Answer::Load(Box::new(LoadAnswer::Recorded {
                                    file,
                                    options,
                                })));
                            }
                        }
                    } else {
                        crate::stdin::spool(open, options, &writer, &read)?
                    };
                    Ok(Answer::Load(Box::new(LoadAnswer::Spooled {
                        download,
                        options,
                    })))
                });
            }
            Step::FetchSpec {
                url,
                options,
                writer,
            } => {
                #[cfg(any(feature = "http", feature = "cloud"))]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                self.spawn_job(job, Some("Reading spec..."), move |_| {
                    #[cfg(any(feature = "http", feature = "cloud"))]
                    let fetched = crate::remote_model::fetch_small(
                        &url,
                        crate::formats::MAX_SPEC_BYTES,
                        &cloud,
                        &runtime,
                        &|| writer.stopped(),
                    );
                    #[cfg(not(any(feature = "http", feature = "cloud")))]
                    let fetched: std::result::Result<Option<Vec<u8>>, String> = {
                        let _ = &writer;
                        Err(crate::error_display::file_message(
                            &url,
                            "this build reads no URLs",
                        ))
                    };
                    // The URL in the message may carry a password or a signature.
                    let bytes = fetched
                        .map_err(|message| crate::logging::redact(&message, &[]))?
                        .ok_or_else(|| {
                            crate::logging::redact(
                                &crate::error_display::file_message(
                                    &url,
                                    &format!(
                                        "a format spec is at most {}",
                                        crate::formats::MAX_SPEC_SAID
                                    ),
                                ),
                                &[],
                            )
                        })?;
                    let spec = crate::formats::Spec::from_bytes(&bytes, &url)
                        .map_err(|e| crate::logging::redact(&e.to_string(), &[]))?;
                    Ok(Answer::Load(Box::new(LoadAnswer::SpecFetched {
                        spec: Arc::new(spec),
                        options,
                    })))
                });
            }
            Step::DecompressRecords {
                file,
                path,
                choice,
                options,
                writer,
            } => {
                self.spawn_job(job, Some("Decompressing..."), move |_| {
                    let failed = |e: color_eyre::Report| {
                        crate::error_display::user_message_from_report(&e, Some(path.as_path()))
                    };
                    let compression = options
                        .compression
                        .or_else(|| CompressionFormat::from_extension(&file))
                        .ok_or_else(|| format!("{} is not compressed", path.display()))?;
                    let temp_dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
                    let copy = crate::readers::csv::decompress_to_copy(
                        &file,
                        compression,
                        &temp_dir,
                        &writer,
                    )
                    .map_err(failed)?;
                    Ok(Answer::Load(Box::new(LoadAnswer::DecompressedRecords {
                        copy,
                        path,
                        choice,
                        options,
                    })))
                });
            }
            Step::ReadRecords {
                copy,
                path,
                choice,
                options,
            } => {
                self.spawn_job(job, Some("Reading records..."), move |_| {
                    let named = path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    let read = crate::formats::read(&copy, &named, choice)?;
                    let lf = Arc::clone(&read.records).into_lazy().map_err(|e| {
                        crate::error_display::user_message_from_report(
                            &color_eyre::eyre::eyre!(e),
                            Some(path.as_path()),
                        )
                    })?;
                    Ok(Answer::Load(Box::new(LoadAnswer::Scanned {
                        lf: Box::new(lf),
                        path: Some(path),
                        options: OpenOptions {
                            format_read: Some(Arc::new(read)),
                            ..options
                        },
                    })))
                });
            }
            Step::Decompress {
                file,
                path,
                options,
                writer,
                download,
            } => {
                // Only delimited text and lines come this way, the format said by the
                // loader or the scan.
                let options = OpenOptions {
                    format: options.format.or(Some(FileFormat::TEXT)),
                    ..options
                };
                let formats = self.formats.clone();
                self.spawn_job(job, Some("Decompressing..."), move |_| {
                    let failed = |e: color_eyre::Report| {
                        crate::error_display::user_message_from_report(&e, Some(path.as_path()))
                    };
                    let options =
                        Self::with_delimited_spec(&file, options, &formats).map_err(failed)?;
                    let lines = options.delimited.is_none()
                        && options.format.is_some_and(FileFormat::is_lines);
                    let (state, opened) = if lines {
                        let (read, opened) =
                            crate::readers::csv::from_lines_decompressed(&file, &options, &writer)
                                .map_err(failed)?;
                        let state = DataTableState::from_read(read, &options).map_err(failed)?;
                        (state, Some(opened))
                    } else {
                        let state = Self::decompressed_delimited_state(&file, &options, &writer)
                            .map_err(failed)?;
                        (state, None)
                    };
                    let mut open_notes = options
                        .delimited
                        .as_ref()
                        .map(|read| read.notes())
                        .unwrap_or_default();
                    open_notes.extend(opened.iter().flat_map(|o| o.notes.iter().cloned()));
                    let state = state.with_open(OpenFacts {
                        fetched: Self::fetched(download.as_ref(), Some(&path)),
                        download,
                        open_notes,
                        records: opened.and_then(|o| o.window),
                        delimited: options.delimited.clone(),
                        read_as: options.format,
                        // The loader sends a compressed file here without a scan.
                        read_mode: options.format.and_then(|f| {
                            f.read_mode(crate::Stored::Compressed {
                                in_memory: options.decompress_in_memory,
                            })
                        }),
                        ..Default::default()
                    });
                    Ok(Answer::Load(Box::new(LoadAnswer::SchemaRead {
                        state: Box::new(state),
                        path: Some(path),
                        options,
                        debug_label: Some("decompressed delimited".to_string()),
                    })))
                });
            }
            Step::Convert {
                what,
                files,
                path,
                options,
                writer,
                read,
            } => {
                // The load's stop flag ends it at the next record batch or chunk,
                // removing its files; quitting removes them even if the process ends
                // first.
                let formats = self.formats.clone();
                self.spawn_job(job, Some(what.status()), move |_| {
                    let named = |e: color_eyre::Report| {
                        crate::error_display::user_message_from_report(&e, path.as_deref())
                    };
                    let converted = match what {
                        loading::Conversion::Streams => {
                            let converted = crate::ipc_stream::convert(
                                &files,
                                options.temp_dir.as_deref(),
                                &writer,
                                &read,
                            )
                            .map_err(named)?;
                            loading::Converted::Streams {
                                file: converted.file,
                                parts: converted.parts,
                            }
                        }
                        loading::Conversion::Text(format) => {
                            let display = path.clone().unwrap_or_else(|| files[0].clone());
                            let (converted, detail) =
                                crate::readers::convert(&crate::readers::ConvertIn {
                                    files: &files,
                                    display: &display,
                                    format,
                                    options: &options,
                                    formats: &formats,
                                    writer: &writer,
                                    read: &read,
                                })
                                .map_err(named)?;
                            loading::Converted::Frame {
                                files: converted.files,
                                lf: Box::new(converted.lf),
                                notes: converted.notes,
                                other_tables: converted.other_tables,
                                detail,
                            }
                        }
                    };
                    Ok(Answer::Load(Box::new(LoadAnswer::Converted {
                        converted,
                        path,
                        options,
                    })))
                });
            }
            Step::Scan {
                paths,
                options,
                display,
                status,
            } => {
                let cloud = self.app_config.cloud.clone();
                let formats = self.formats.clone();
                // A download is scanned from a temp path the user never typed and would not
                // recognise; the URL they did type is what names the dataset.
                let path = display.or_else(|| paths.first().cloned());
                self.home_app.reads.scans += 1;
                self.spawn_job(job, Some(status), move |_| {
                    Self::scan_for_open(&cloud, &formats, &paths, options, path)
                        .map(|answer| Answer::Load(Box::new(answer)))
                });
            }
            Step::ReadSchema {
                lf,
                path,
                options,
                progress,
                made,
            } => {
                self.debug.schema_load = None;
                let cloud = self.app_config.cloud.clone();
                let runtime = self.runtime.clone();
                let report = crate::measurements::OpenReport {
                    progress,
                    meter: Arc::new(crate::measurements::Meter::default()),
                    remembered: Some(self.cache.clone()),
                    writes: self.cache_writes.clone(),
                };
                self.spawn_job(job, Some("Reading schema..."), move |_| {
                    Self::read_schema_for_open(*lf, path, options, &cloud, &runtime, &report, made)
                        .map(|answer| Answer::Load(Box::new(answer)))
                });
            }
            Step::Nothing
            | Step::Crash(_)
            | Step::Install(_)
            | Step::Failed(_)
            | Step::Tables(_)
            | Step::Hex(_) => {
                unreachable!("not a phase with a worker")
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Ask(_) => unreachable!("not a phase with a worker"),
            Step::AskRead(_) => unreachable!("not a phase with a worker"),
        }
    }

    /// What the user is asked before files past `[read] memory_warning` are
    /// read whole into memory: `big.json: JSON reads 2.0 GiB into memory`.
    fn in_memory_confirmation_message(read: &loading::InMemory) -> String {
        let what = match read.files {
            1 => format!(
                "{}: {} reads",
                read.name
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| read.name.display().to_string()),
                read.format.title()
            ),
            n => format!("{n} {} files read", read.format.title()),
        };
        format!(
            "{what} {} into memory before the table appears.\n\nRead it?",
            crate::numfmt::bytes(read.bytes)
        )
    }

    /// What the user is being asked to agree to before a remote file is downloaded.
    #[cfg(any(feature = "http", feature = "cloud"))]
    fn download_confirmation_message(
        pending: &loading::PendingDownload,
        note: Option<&str>,
    ) -> String {
        let (url, size, options) = pending.parts();
        let size_str = size
            .map(crate::numfmt::bytes)
            .unwrap_or_else(|| "unknown".to_string());
        let dest_dir = options
            .temp_dir
            .as_deref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| std::env::temp_dir().display().to_string());
        let note = note.map(|note| format!("{note}\n\n")).unwrap_or_default();
        // Only a store's Arrow streams are downloaded, and converted as they arrive.
        let files = match pending.arrow_files() {
            Some((1, 0)) => "Arrow stream: converted as it downloads\n".to_string(),
            Some((streams, 0)) => {
                format!("Files: {streams} Arrow streams, converted as they download\n")
            }
            Some((streams, in_place)) => {
                let streams = match streams {
                    1 => "1 Arrow stream, converted as it downloads".to_string(),
                    n => format!("{n} Arrow streams, converted as they download"),
                };
                let in_place = match in_place {
                    1 => "1 IPC file read in place".to_string(),
                    n => format!("{n} IPC files read in place"),
                };
                format!("Files: {streams}; {in_place}\n")
            }
            None => String::new(),
        };
        format!(
            "{note}URL: {url}\n{files}File size: {size_str}\nDestination: {dest_dir} (temporary file)\n\nContinue with download?"
        )
    }
}

/// Scans and schema reads for each kind of source, used by the load's phases.
impl App {
    fn hoist_partition_columns(
        lf: LazyFrame,
        schema: &Schema,
        partition_columns: &[String],
        drifts: bool,
    ) -> LazyFrame {
        hoist_partition_columns(lf, schema, partition_columns, drifts)
    }

    /// Schema for a local directory of Parquet files: every column any of them has, from
    /// their footers, instead of `collect_schema()` over the whole set or one file's
    /// columns standing in for all.
    ///
    /// As a cloud prefix opens: past one wave of footers the two ends open the dataset
    /// and the rest are read behind it, joining when they land, and a directory whose
    /// listing has not changed since its footers were last all read opens from what
    /// they said then. On a network mount each footer is round trips, and a Hive tree
    /// is thousands of footers. `None` when the path is not that shape, or when nothing
    /// could be read — either way the caller falls back to the general scan, which
    /// reports the error properly if there is one.
    pub(crate) fn schema_state_from_local_hive(
        path: Option<&Path>,
        options: &OpenOptions,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        if !options.single_spine_schema {
            return None;
        }
        let p = path.filter(|p| p.is_dir() && options.hive)?;
        dataset_files::open(Arc::new(dataset_files::LocalFiles::new(p)), options, report)
    }

    /// The same one-file trick against an object store. This is the route that used to
    /// block the UI thread on a network round trip.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_hive(
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        // A prefix of Arrow files is read from its download (`cloud_arrow`).
        if !options.single_spine_schema || options.format == Some(FileFormat::Arrow) {
            return None;
        }
        // Unlike the local path this does not require --hive: a directory or glob URL
        // is already a hive scan by shape.
        let p = path.filter(|p| {
            let s = p.as_os_str().to_string_lossy();
            home::is_object_store_url(p) && (options.hive || source::is_prefix_or_glob(&s))
        })?;

        let (full, cloud_opts, store) = Self::cloud_store_for(p, cloud, runtime).ok()?;
        let (_bucket, key) = Self::cloud_bucket_and_key(&full).ok()?;
        Self::schema_state_from_cloud_hive_with(
            full, key, store, cloud_opts, options, runtime, report,
        )
    }

    /// The same, against a store already built.
    ///
    /// Split out so a test can hand it an in-memory store and cover the choice between
    /// the two routes below — including that each is given the counter it was called
    /// with, rather than one of its own.
    #[cfg(feature = "cloud")]
    pub(crate) fn schema_state_from_cloud_hive_with(
        full: String,
        key: String,
        store: Arc<dyn object_store::ObjectStore>,
        cloud_opts: CloudOptions,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        // Every file listed once, and the scan, the schema and the count all work from
        // that list — for a glob as much as for a prefix. datui expands the glob
        // itself: it lists the literal part of the key and matches the rest, so a glob
        // is an ordinary list of files by the time anything else sees it, and gets the
        // schema union, the row count, the notes and the measurements that a prefix
        // gets.
        //
        // The star cannot be handed to the object store. A listing prefix is a literal
        // string, so `data/*.parquet` matches nothing and the open falls through to a
        // whole-dataset scan with none of the above — which is what used to happen, for
        // every glob, silently (#228).
        let pattern = full.contains('*').then(|| {
            globset::GlobBuilder::new(&key)
                .literal_separator(true)
                .build()
                .map(|g| g.compile_matcher())
        });
        let pattern = match pattern {
            // A pattern datui cannot read is not one it should guess at.
            Some(Err(_)) => return None,
            Some(Ok(matcher)) => Some(matcher),
            None => None,
        };
        let listed = cloud_hive::prefix_of_glob(&key).to_string();
        Self::schema_state_from_cloud_files(
            CloudTarget {
                full: &full,
                key: listed,
                pattern: pattern.as_ref(),
            },
            store,
            cloud_opts,
            options,
            runtime,
            report,
        )
    }

    /// A cloud prefix of Parquet files as one dataset, from a single listing of it.
    ///
    /// The files are scanned by name, so Polars does not list the prefix again, and
    /// leniently (see `cloud_hive::lenient_scan`), since files written years apart
    /// differ. The state keeps the list, so the count reads footers rather than data
    /// and a buffer reads only the files holding its rows (see `RemoteFiles`).
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_files(
        target: CloudTarget<'_>,
        store: Arc<dyn object_store::ObjectStore>,
        cloud_opts: CloudOptions,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        let CloudTarget { full, key, pattern } = target;
        dataset_files::open(
            Arc::new(dataset_files::StoreFiles::new(
                full,
                key,
                pattern.cloned(),
                store,
                cloud_opts,
                runtime,
            )),
            options,
            report,
        )
    }

    /// What the home screen can say about one object opened from a bucket: its rows
    /// and columns from the footer the open read, under the URL it resolved to. The
    /// object's size is not known here — the footer is read from the tail — so the
    /// record carries none, and the row shows none. Its `mtime` is the time of the
    /// open: a remote record is never fingerprinted by it, and the index evicts its
    /// oldest `mtime` first, so a zero would make these the first to go.
    #[cfg(feature = "cloud")]
    pub(crate) fn record_cloud_object_facts(
        cache: Option<&crate::cache::CacheManager>,
        full: &str,
        footer: &cloud_hive::FileFooter,
    ) {
        let Some(cache) = cache else {
            return;
        };
        let columns: Vec<String> = footer
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        cache.record_dataset_facts(&[(
            PathBuf::from(full),
            crate::cache::DatasetFacts {
                mtime: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default(),
                size: 0,
                rows: Some(footer.rows()),
                cols: Some(columns.len()),
                cols_sampled: false,
                columns,
                kind: Some(discover::EntryKind::File),
                classified_by: discover::CLASSIFIER_VERSION,
                cost: discover::Cost {
                    row_groups: Some(footer.row_group_rows.len()),
                    ..Default::default()
                },
                holds: Default::default(),
            },
        )]);
    }

    /// General schema route: ask the frame itself. Slow for a wide hive dataset, which
    /// is the reason this whole phase belongs on a background thread.
    fn schema_state_from_full_scan(
        mut lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
    ) -> Result<DataTableState> {
        let schema = lf
            .collect_schema()
            .map_err(color_eyre::eyre::Report::from)?;
        let partition_columns =
            match path.filter(|p| options.hive && (p.is_dir() || source::expands_as_glob(p))) {
                Some(p) => crate::readers::hive::discover_hive_partition_columns(p)
                    .into_iter()
                    .filter(|c| schema.contains(c.as_str()))
                    .collect::<Vec<_>>(),
                None => Vec::new(),
            };
        let lf = Self::hoist_partition_columns(lf, &schema, &partition_columns, false);
        let part_cols = (!partition_columns.is_empty()).then_some(partition_columns);
        DataTableState::from_schema_and_lazyframe(schema, lf, options, part_cols)
    }

    /// Build the table state for a loaded frame, by the cheapest route that applies.
    ///
    /// Returns the state and a label naming the route it came from, for the debug
    /// overlay. Takes its config by value so all of it can run off the UI thread.
    fn build_schema_state(
        lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, OpenFacts, String)> {
        // The facts carry the meter of the route that actually built the dataset, so it
        // is installed with the dataset and nothing else can reach it. An open that
        // fails never gets here, which is what keeps the dataset still on screen
        // showing its own figures.
        let (state, mut facts, label) =
            Self::schema_state_by_route(lf, path, options, cloud, runtime, report)?;
        // What the open did, as against what it found. The one place both are known:
        // the scan has reported what it passed over, the caller has said whether this
        // is a lake table's plain files, and the state that will carry the notes is in
        // hand. See `DataTableState::open_notes` for why they are not the other notes.
        // A delimited file read with a header whose names are all numbers: its first
        // row of data, most likely, which `H` reads as data instead.
        let names_look_like_data = options.format.and_then(FileFormat::separator).is_some()
            && options.has_header != Some(false)
            && !crate::schema_union::names_are_names(
                &state
                    .schema()
                    .iter_names()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>(),
            );
        facts.open_notes = crate::notes::from_the_open(
            &options.left_out,
            options.read_as_plain_files_of,
            options.files_disagree,
            names_look_like_data,
        );
        // And the half of it that cannot be missed: the row count on screen is a true
        // count of the files and a wrong one of the table.
        facts.not_the_table = options.read_as_plain_files_of;
        if let Some(splits) = &options.splits {
            facts.other_tables = splits.others.clone();
            facts
                .open_notes
                .extend(crate::notes::map_caches(splits.caches));
        }
        if let Some(read) = &options.format_read {
            facts.open_notes.extend(read.notes());
            facts.format_read = Some(read.clone());
        }
        if let Some(opened) = &options.opened {
            facts.records = opened.window.clone();
            facts.detail = opened.detail.clone();
            facts.other_tables = opened.other_tables.clone();
            facts.open_notes.extend(opened.notes.iter().cloned());
            facts.units = opened.units.clone();
            facts.indexing = opened.indexing.clone();
            facts.numbering = opened.numbering.clone();
        }
        if let Some(sqlite) = &options.sqlite {
            facts.pushdown = Some(sqlite.pushdown.clone());
            facts.hold = sqlite.hold.lock().ok().and_then(|mut hold| hold.take());
            facts.other_tables = sqlite.other_tables.clone();
        }
        if let Some(read) = &options.delimited {
            facts.open_notes.extend(read.notes());
            facts.delimited = Some(read.clone());
        }
        facts.open_notes.extend(options.read_notes.iter().cloned());
        facts.typing = options.typing.clone();
        facts.read_mode = options.read_mode;
        facts.read_as = options.format;
        // The display path of a downloaded object is its URL too; only a scan that
        // really reads the object store in place buffers like one.
        // Arrow in a store reads its IPC files in place, and its streams from their
        // download (`cloud_arrow`).
        facts.remote_source = match &options.arrow_parts {
            Some(parts) => parts.iter().any(|part| {
                matches!(part, crate::ipc_stream::Part::InPlace(p) if source::is_remote_url(p))
            }),
            None => path.is_some_and(source::scans_in_place),
        };
        // The cheap footer-sum row count, for a local Parquet hive directory. Asked
        // here because a stat on a mount that has stopped answering hangs its thread.
        // A directory read as another format counts its rows by a scan: its footers
        // are not Parquet's.
        // Not for one read by file: its counter reads only the footers the open did not.
        if options.hive
            && facts.remote_files.is_none()
            && options.format.is_none_or(|f| f == FileFormat::Parquet)
            && let Some(dir) = path.filter(|p| !source::is_remote_url(p) && p.is_dir())
        {
            facts.parquet_count_dir = Some(dir.to_path_buf());
        }
        Ok((state, facts, label))
    }

    /// Scan a prefix in an object store with the reader its format calls for.
    ///
    /// Every cloud path went to `scan_parquet` whatever was under it, so a prefix of
    /// CSV came back "Could not read from S3. Check credentials and URL" — a false
    /// statement about the user's login, made about a directory datui could see the
    /// contents of. Polars' other scans take the same `CloudOptions` and do their own
    /// listing; nothing was passing them.
    ///
    /// Parquet keeps its own branch at each call site: it is the only one with hive
    /// partitioning, which is a Parquet-only capability in this reader, and it is the
    /// path every cloud dataset took before this existed.
    ///
    /// `None` when the format is not one of these, which sends the caller back to the
    /// Parquet scan it always made.
    #[cfg(feature = "cloud")]
    pub(crate) fn scan_cloud_prefix(
        url: &str,
        cloud_opts: CloudOptions,
        format: FileFormat,
        glob: bool,
        options: &OpenOptions,
    ) -> Option<Result<LazyFrame>> {
        // The formats the docs say a prefix reads in place. Parquet takes the caller's
        // own scan, and a prefix of model files is read by its headers before this.
        if !format.reads_bucket_prefix() {
            return None;
        }
        // A plain prefix is narrowed to the keys with an extension. A console's folder
        // marker comes back from the listing as `data` for `data/`, which Polars reads
        // as a file of a different kind from the rest and refuses the whole prefix.
        let pl_path = if url.ends_with('/') && !url.contains('*') {
            PlRefPath::new(format!("{url}**/*.*").as_str())
        } else {
            PlRefPath::new(url)
        };
        let scan = crate::readers::of(format).bucket_scan?;
        Some(scan(crate::readers::BucketIn {
            url,
            path: pl_path,
            cloud: cloud_opts,
            glob,
            options,
            format,
        }))
    }

    /// The format a prefix or glob in a store is read as, other than Parquet: what
    /// the listing said, else what a glob's names end in (`*.arrow`).
    #[cfg(feature = "cloud")]
    fn cloud_glob_format(url: &str, options: &OpenOptions) -> Option<FileFormat> {
        options
            .format
            .or_else(|| {
                url.contains('*')
                    .then(|| FileFormat::from_path(Path::new(url)))
                    .flatten()
            })
            .filter(|f| *f != FileFormat::Parquet)
    }

    /// The plain URL and Polars options for one object-store path, through the source
    /// it names or belongs to (`cloud_sources::resolve`).
    #[cfg(feature = "cloud")]
    fn resolve_cloud_url(
        path: &Path,
        cloud: &crate::config::CloudConfig,
    ) -> Result<(String, CloudOptions)> {
        let text = path.to_string_lossy();
        let resolved = crate::cloud_sources::resolve_for_open(&text, cloud)
            .map_err(|e| color_eyre::eyre::eyre!(e))?;
        use object_store::azure::AzureConfigKey;
        use polars::io::cloud::GoogleConfigKey;
        let gcs_agent = (
            GoogleConfigKey::Client(crate::user_agent::CLIENT_KEY),
            crate::user_agent::get(),
        );
        let options = match resolved.kind {
            crate::source::ProviderKind::S3 => Self::build_s3_cloud_options(&resolved.s3),
            crate::source::ProviderKind::Gcs
                if resolved.signing == crate::cloud_sources::Signing::Unsigned =>
            {
                CloudOptions::default()
                    .with_gcp([(GoogleConfigKey::SkipSignature, "true".into()), gcs_agent])
            }
            crate::source::ProviderKind::Gcs => match &resolved.gcloud {
                // The token comes from `gcloud` whenever Polars asks, so a long scan
                // outlives the one fetched here.
                Some((configuration, _)) => CloudOptions::default()
                    .with_gcp([gcs_agent])
                    .with_credential_provider(Some(crate::gcloud::polars_provider(configuration))),
                None => match &resolved.google_credentials {
                    Some(file) => CloudOptions::default().with_gcp([
                        (
                            GoogleConfigKey::ApplicationCredentials,
                            file.to_string_lossy().into_owned(),
                        ),
                        gcs_agent,
                    ]),
                    None => CloudOptions::default().with_gcp([gcs_agent]),
                },
            },
            crate::source::ProviderKind::Azure => {
                let (account, _, _) = source::azure_parts(&resolved.url)
                    .ok_or_else(|| color_eyre::eyre::eyre!("not an Azure URL"))?;
                let mut azure = crate::azure::polars_options(&account, &resolved.azure);
                azure.push((
                    AzureConfigKey::Client(crate::user_agent::CLIENT_KEY),
                    crate::user_agent::get(),
                ));
                CloudOptions::default().with_azure(azure)
            }
        };
        Ok((resolved.url, options))
    }

    /// The URL, Polars options and store for one object-store path. `cloud` is the
    /// effective config the `App` keeps (see `OpenOptions::effective_cloud`).
    #[cfg(feature = "cloud")]
    pub(crate) fn cloud_store_for(
        path: &Path,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<(String, CloudOptions, Arc<dyn object_store::ObjectStore>)> {
        let (full, cloud_opts) = Self::resolve_cloud_url(path, cloud)?;
        let store = Self::polars_object_store(&full, &cloud_opts, runtime)?;
        Ok((full, cloud_opts, store))
    }

    /// One Parquet object read in place: schema and row count from its footer, in one
    /// tail read through one store.
    ///
    /// Asking the frame for its schema fetched the footer through Polars, and Polars
    /// answers `len()` on a cloud scan by reading the first row group rather than the
    /// footer, so the background count that followed an open downloaded row group 0 a
    /// second time, alongside the buffer that was showing it. The footer has both
    /// answers for one 256 KiB range request; the schema is handed to the scan so
    /// Polars does not fetch it again, and the count is known before the first frame.
    /// A failure is returned, not swallowed: the caller falls back to asking the frame
    /// and puts the reason in the debug label.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_object(
        path: &Path,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, OpenFacts)> {
        let (full, cloud_opts, store) = Self::cloud_store_for(path, cloud, runtime)?;
        let (_bucket, key) = Self::cloud_bucket_and_key(&full)?;
        if key.is_empty() {
            return Err(color_eyre::eyre::eyre!("a bucket, not an object"));
        }
        let meter = report.meter.clone();
        let (footer, etag) = wait_on_runtime(runtime, async move {
            cloud_hive::footer_of_cloud_parquet(store, &key, &meter).await
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("cancelled"))??;
        let args = ScanArgsParquet {
            schema: Some(footer.schema.clone()),
            cloud_options: Some(cloud_opts),
            hive_options: polars::io::HiveOptions::default(),
            glob: false,
            ..Default::default()
        };
        let lf = LazyFrame::scan_parquet(PlRefPath::new(full.as_str()), args)?;
        let state =
            DataTableState::from_schema_and_lazyframe(footer.schema.clone(), lf, options, None)?;
        // The commonest cloud open, and the one the dataset index never heard about:
        // the prefix route records what it read, and this one read a footer too.
        Self::record_cloud_object_facts(report.remembered.as_ref(), &full, &footer);
        let column_bytes = crate::schema_union::column_bytes_per_row(&[Some(footer.clone())]);
        let facts = OpenFacts {
            remote_objects: vec![crate::local_copy::RemoteObject {
                url: full,
                size: footer.file_bytes as u64,
                etag,
            }],
            row_groups: vec![footer.row_group_rows],
            column_bytes,
            ..Default::default()
        };
        Ok((state, facts))
    }

    /// The schema routes, cheapest first: one local footer, one cloud footer (a hive
    /// prefix or a single object), then asking the frame.
    fn schema_state_by_route(
        lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, OpenFacts, String)> {
        #[cfg(not(feature = "cloud"))]
        let _ = (cloud, runtime);

        // A meter per attempt, and the winner's is the open's. The routes are tried in
        // order and the earlier ones measure before they discover they cannot finish —
        // the local hive route times its walk and its footer pass, then bails five
        // different ways. Sharing one meter would leave those figures on a dataset some
        // later route built, which is a row saying no files on a dataset that has them.
        //
        // Two guards, and the test holds them together rather than either alone: the
        // attempts take separate meters, and both full-scan arms hand back an empty
        // one. A directory whose only Parquet is a writer's own bookkeeping — a
        // `_delta_log` checkpoint — reaches the screen through the second of those, so
        // `test_a_route_that_gave_up_leaves_no_figures_on_the_dataset_that_opened`
        // fails when both are reverted and passes when either still stands. The
        // per-attempt meter alone is defensive: the route guards are mutually
        // exclusive enough that nothing reaches a later route through the first.
        let attempt = |report: &crate::measurements::OpenReport| crate::measurements::OpenReport {
            progress: report.progress.clone(),
            meter: Arc::new(crate::measurements::Meter::default()),
            remembered: report.remembered.clone(),
            writes: report.writes.clone(),
        };

        let local = attempt(report);
        if let Some((state, facts)) = Self::schema_state_from_local_hive(path, options, &local) {
            let facts = OpenFacts {
                measurements: local.meter,
                ..facts
            };
            return Ok((state, facts, "one-file (local)".to_string()));
        }
        // An open abandoned mid-listing is not one for the routes below to scan whole.
        if report.progress.is_cancelled() {
            return Err(color_eyre::eyre::eyre!("cancelled"));
        }
        #[cfg(feature = "cloud")]
        let cloud_hive_attempt = attempt(report);
        #[cfg(feature = "cloud")]
        if let Some((state, facts)) =
            Self::schema_state_from_cloud_hive(path, options, cloud, runtime, &cloud_hive_attempt)
        {
            let facts = OpenFacts {
                measurements: cloud_hive_attempt.meter,
                ..facts
            };
            return Ok((state, facts, "one-file (cloud)".to_string()));
        }
        #[cfg(feature = "cloud")]
        if let Some(p) = path.filter(|p| {
            source::scans_in_place(p)
                && !options.hive
                && !source::is_prefix_or_glob(&p.to_string_lossy())
        }) {
            let object = attempt(report);
            match Self::schema_state_from_cloud_object(p, options, cloud, runtime, &object) {
                Ok((state, facts)) => {
                    let facts = OpenFacts {
                        measurements: object.meter,
                        ..facts
                    };
                    return Ok((state, facts, "footer (cloud)".to_string()));
                }
                // Visible in the debug overlay, because the fallback costs a row group
                // for the count and that should not pass for the intended path.
                Err(e) => {
                    // A fresh meter, not the failed footer read's: the full scan
                    // measures nothing, and showing the attempt that did not work
                    // would describe a route the dataset did not come by.
                    return Self::schema_state_from_full_scan(lf, path, options).map(|state| {
                        (
                            state,
                            OpenFacts::default(),
                            format!("full scan (cloud footer: {e})"),
                        )
                    });
                }
            }
        }
        Self::schema_state_from_full_scan(lf, path, options)
            .map(|state| (state, OpenFacts::default(), "full scan".to_string()))
    }

    /// The files of one split, when `dir` is a Hugging Face `datasets` cache: its
    /// `dataset_info.json` or `state.json` beside Arrow files. What was chosen and left
    /// out goes in `report`. Any other directory reads every file.
    fn hugging_face_split(
        dir: &Path,
        format: FileFormat,
        files: Vec<PathBuf>,
        options: &OpenOptions,
        report: &mut ReadReport,
    ) -> Result<Vec<PathBuf>> {
        let metadata = || {
            ["dataset_info.json", "state.json"]
                .iter()
                .any(|name| dir.join(name).is_file())
        };
        if format != FileFormat::Arrow || !metadata() {
            return Ok(files);
        }
        let names: Vec<&str> = files
            .iter()
            .map(|f| f.file_name().and_then(|n| n.to_str()).unwrap_or_default())
            .collect();
        let (chosen, splits) = crate::hf_splits::choose(&names, options.table.as_deref())
            .map_err(|e| color_eyre::eyre::eyre!("{}: {e}", dir.display()))?;
        report.splits = Some(Arc::new(splits));
        Ok(chosen.into_iter().map(|i| files[i].clone()).collect())
    }

    /// Why `--table` was refused for `path`, a file of `format`, which holds one table.
    fn one_table(path: Option<&Path>, format: Option<FileFormat>) -> color_eyre::Report {
        match path {
            Some(path) => crate::error_display::FileError::new(path, cli::one_table(format)).into(),
            None => color_eyre::eyre::eyre!(cli::one_table(format)),
        }
    }

    /// A scan of `url` in an object store that Polars refused, with what to check.
    #[cfg(feature = "cloud")]
    fn cloud_scan_failed(url: &str, e: &polars::prelude::PolarsError) -> color_eyre::Report {
        let said = crate::error_display::user_message_from_polars(e);
        let (first, rest) = said.split_once('\n').unwrap_or((&said, ""));
        let first = first.trim_end().trim_end_matches('.');
        let what = format!("could not read it: {first}. Check the credentials and the URL.");
        let what = match rest {
            "" => what,
            rest => format!("{what}\n{rest}"),
        };
        crate::error_display::FileError::new(Path::new(url), what).into()
    }

    /// The inputs of an Arrow read as one table, in order: each IPC file scanned where
    /// it is, in a bucket or on disk, and each run of streams as its rows of
    /// `converted`, the IPC file they were converted to. Stacked as the files of a
    /// directory are ([`crate::readers::polars::union_of_files`]).
    fn scan_arrow_parts(
        cloud: &crate::config::CloudConfig,
        converted: Option<&PathBuf>,
        parts: &[crate::ipc_stream::Part],
    ) -> Result<LazyFrame> {
        use crate::ipc_stream::Part;
        #[cfg(not(feature = "cloud"))]
        let _ = cloud;
        let scan = |path: &Path| -> Result<LazyFrame> {
            #[cfg(feature = "cloud")]
            if source::is_remote_url(path) {
                let (url, cloud_options) = Self::resolve_cloud_url(path, cloud)?;
                let args = polars::prelude::UnifiedScanArgs {
                    cloud_options: Some(cloud_options),
                    ..Default::default()
                };
                return Ok(LazyFrame::scan_ipc(
                    PlRefPath::new(url.as_str()),
                    Default::default(),
                    args,
                )?);
            }
            // A converted stream sits in a temp directory the user names, `[` and all,
            // and a file read in place may be called `d[1].arrow` (#632).
            let args = polars::prelude::UnifiedScanArgs {
                glob: source::expands_as_glob(path),
                ..Default::default()
            };
            Ok(LazyFrame::scan_ipc(
                polars::prelude::PlRefPath::try_from_path(path)?,
                Default::default(),
                args,
            )?)
        };
        let streams = |offset: u64, rows: u64| -> Result<LazyFrame> {
            let file = converted
                .ok_or_else(|| color_eyre::eyre::eyre!("No converted Arrow file to read."))?;
            let lf = scan(file)?;
            // The whole file needs no slice, which would hide its row count.
            let whole = offset == 0
                && parts
                    .iter()
                    .all(|part| matches!(part, Part::Converted { .. }));
            Ok(if whole {
                lf
            } else {
                lf.slice(offset as i64, rows as polars::prelude::IdxSize)
            })
        };
        let mut frames = Vec::new();
        let mut run: Option<(u64, u64)> = None;
        for part in parts {
            match part {
                Part::Converted { offset, rows, .. } => {
                    run = Some(match run {
                        Some((start, n)) if start + n == *offset => (start, n + rows),
                        Some((start, n)) => {
                            frames.push(streams(start, n)?);
                            (*offset, *rows)
                        }
                        None => (*offset, *rows),
                    });
                }
                Part::InPlace(path) => {
                    if let Some((start, n)) = run.take() {
                        frames.push(streams(start, n)?);
                    }
                    frames.push(scan(path)?);
                }
            }
        }
        if let Some((start, n)) = run {
            frames.push(streams(start, n)?);
        }
        match frames.len() {
            0 => Err(color_eyre::eyre::eyre!("No Arrow files to read.")),
            1 => Ok(frames.remove(0)),
            _ => Ok(polars::prelude::concat(
                frames.as_slice(),
                crate::readers::polars::union_of_files(),
            )?),
        }
    }

    /// One split of a `save_to_disk` DatasetDict, `dir`, whose `dataset_dict.json` names
    /// `splits`: the subdirectory `--table` names, else the first offered, read as any
    /// directory is. The others are listed, as a cache directory's are.
    fn dataset_dict_split(
        dir: &Path,
        splits: &[String],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        let listed: Vec<&str> = splits.iter().map(String::as_str).collect();
        let mut picked = crate::hf_splits::pick(&listed, options.table.as_deref())
            .map_err(|e| color_eyre::eyre::eyre!("{}: {e}", dir.display()))?;
        let split = dir.join(picked.split.as_deref().unwrap_or_default());
        let inner = OpenOptions {
            table: None,
            splits: None,
            ..options.clone()
        };
        let scan = Self::build_local_lazyframe(&[split], &inner, report, formats)?;
        // The split's own directory names no splits; its `map()` files are still counted.
        picked.caches = report.splits.as_ref().map_or(0, |inner| inner.caches);
        report.splits = Some(Arc::new(picked));
        Ok(scan)
    }

    /// Build the LazyFrame for `paths`.
    ///
    /// Takes the cloud config by reference rather than reading `self`, so the same
    /// code can run on a background thread — scanning is where the wall-clock time
    /// goes for CSV (schema inference) and for hive directories with many files.
    /// Whether the files about to be read as one table do not all carry the same
    /// columns, for the note that says so.
    ///
    /// Only for the formats with no footer. A Parquet dataset's footers are read anyway
    /// and produce the exact version of this — which columns, in how many files, and
    /// where — so a second, vaguer note above those would be noise.
    ///
    /// A spread of the files rather than all of them, the same three
    /// [`crate::schema_union::sample_files`] reads for the label, and for the same
    /// reason: this runs on the way into a read the user is waiting for.
    fn files_disagree(
        files: &[PathBuf],
        options: &OpenOptions,
        found: FileFormat,
    ) -> crate::schema_union::Disagreement {
        // The format the read will use, not the one the names suggested: an explicit
        // `--format` outranks both, and judging a directory with a reader the open will
        // not use is a note about a read that never happened.
        let format = options.format.unwrap_or(found);
        if format == FileFormat::Parquet {
            return Default::default();
        }
        // Null values are the one setting the sample cannot mirror: `--null`
        // takes `COL=VAL` forms the reader resolves against the file it is opening, and
        // a sample that guessed would report a widening the table never did. They are
        // unset unless the user names them, so this stands down where it must and runs
        // everywhere else.
        if options.null_values.is_some() {
            return Default::default();
        }
        crate::schema_union::sample_files(files, format, &Self::read_as(options)).disagreement()
    }

    /// The reader settings a sample has to copy to describe what the open will do.
    ///
    /// Taken from the options the open is actually being made with, not guessed at and
    /// then bailed out of: `from_args_and_config` fills in `infer_schema_length` and
    /// `parse_strings` on every run with no flags at all, so a predicate over "did the
    /// user set anything" is true every time. That shipped once, and the notes about
    /// how a directory had been stacked never appeared outside the tests.
    pub(crate) fn read_as(options: &OpenOptions) -> crate::schema_union::ReadAs {
        crate::schema_union::ReadAs {
            delimiter: options.delimiter,
            has_header: options.has_header,
            skip_rows: options.skip_rows,
            skip_lines: options.skip_lines,
            infer_schema_length: options.infer_schema_length,
            ignore_errors: options.ignore_errors,
            try_parse_dates: options.csv_try_parse_dates(),
            comment_char: options.comment_char.clone(),
            header_rows: options.header_rows.clone(),
            header_join: options.header_join.clone(),
        }
    }

    /// A format spec reads a local file, or the downloaded copy of one remote object
    /// (`loading::remote_download`). What reaches here remote is a prefix or a glob,
    /// which would otherwise be scanned in place without the spec and say nothing.
    pub(crate) fn refuse_spec_in_place(path: &Path, options: &OpenOptions) -> Result<()> {
        if source::is_remote_url(path)
            && (options.spec_file.is_some() || options.spec_name.is_some())
        {
            return Err(crate::error_display::FileError::new(
                path,
                "a format spec reads one remote object at a time, not a prefix or a glob; name the object",
            )
            .into());
        }
        Ok(())
    }

    /// `found` is what the read has to say about itself, for the caller to put in the
    /// dataset's notes: which data files it passed over, and whether the files it did
    /// read carry the same columns. Written here rather than worked out by the caller
    /// because this is the pass that decides, and a second opinion formed from a second
    /// directory read is a second answer waiting to disagree.
    pub(crate) fn build_lazyframe_from_paths_with(
        cloud: &crate::config::CloudConfig,
        paths: &[PathBuf],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        // Arrow streams converted, or a bucket's Arrow listed: the load says where
        // each input's rows are.
        if let Some(parts) = &options.arrow_parts {
            if options.table.is_some() && options.splits.is_none() {
                // Named by an input the user knows, never the converted copy.
                let named = parts.first().map(|part| match part {
                    crate::ipc_stream::Part::InPlace(path) => path.as_path(),
                    crate::ipc_stream::Part::Converted { source, .. } => source.as_path(),
                });
                return Err(Self::one_table(named, Some(FileFormat::Arrow)));
            }
            return Self::scan_arrow_parts(cloud, paths.first(), parts).map(Scan::from);
        }
        // Only the cloud readers below take the settings.
        #[cfg(not(feature = "cloud"))]
        let _ = cloud;
        let path = &paths[0];
        Self::refuse_spec_in_place(path, options)?;
        match source::input_source(path) {
            source::InputSource::Http(_url) => {
                #[cfg(feature = "http")]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS load is handled in the event loop; this path should not be reached."
                    ));
                }
                #[cfg(not(feature = "http"))]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS URLs are not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::S3(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) =
                        Self::resolve_cloud_url(Path::new(&format!("s3://{url}")), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = Self::cloud_glob_format(&full, options)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf.map(Scan::from);
                    }
                    let pl_path = PlRefPath::new(full.as_str());
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args)
                        .map_err(|e| Self::cloud_scan_failed(&full, &e))?;
                    // The frame alone. Building a state here would ask Polars for the
                    // schema, which lists every file under a prefix, and the schema
                    // phase that follows lists them once more for itself.
                    return Ok(lf.into());
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "S3 is not supported in this build. Rebuild with default features and set AWS credentials (e.g. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)."
                    ));
                }
            }
            source::InputSource::Gcs(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) =
                        Self::resolve_cloud_url(Path::new(&format!("gs://{url}")), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = Self::cloud_glob_format(&full, options)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf.map(Scan::from);
                    }
                    let pl_path = PlRefPath::new(full.as_str());
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args)
                        .map_err(|e| Self::cloud_scan_failed(&full, &e))?;
                    return Ok(lf.into());
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "GCS (gs://) is not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::Azure(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) = Self::resolve_cloud_url(Path::new(&url), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = Self::cloud_glob_format(&full, options)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf.map(Scan::from);
                    }
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options: if is_glob {
                            polars::io::HiveOptions::new_enabled()
                        } else {
                            polars::io::HiveOptions::default()
                        },
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(PlRefPath::new(full.as_str()), args)
                        .map_err(|e| Self::cloud_scan_failed(&full, &e))?;
                    return Ok(lf.into());
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "Azure is not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::Local(_) => {}
        }
        Self::build_local_lazyframe(paths, options, report, formats)
    }

    /// The files a directory holds, read as `found`: through the delimited spec the
    /// first of them matches, when one does, else as the format says.
    fn read_directory_files(
        files: &[PathBuf],
        options: &OpenOptions,
        found: FileFormat,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        if options.delimited.is_none()
            && options.format.is_none()
            && found.separator().is_some()
            && let Some(first) = files.iter().find(|f| !crate::nul_tail::holds_nothing(f))
            && let Some(choice) = Self::delimited_spec_of(first, options, formats)?
        {
            let nested = OpenOptions {
                hive: false,
                format: Some(found),
                splits: report.splits.clone(),
                ..options.clone()
            };
            return Self::read_with_delimited_spec(files, &nested, report, formats, choice);
        }
        // A spec's read says how its files differ itself, from their own header lines.
        report.files_disagree = Self::files_disagree(files, options, found);
        let nested = OpenOptions {
            hive: false,
            format: Some(options.format.unwrap_or(found)),
            splits: report.splits.clone(),
            ..options.clone()
        };
        Self::build_local_lazyframe(files, &nested, report, formats)
    }

    /// The delimited spec whose glob or magic `file` matches, if one does.
    fn delimited_spec_of(
        file: &Path,
        options: &OpenOptions,
        formats: &crate::formats::Registry,
    ) -> Result<Option<crate::formats::Choice>> {
        let asked = crate::formats::Asked {
            compression: options.compression,
            text_only: true,
            ..Default::default()
        };
        match crate::formats::route(file, &asked, formats)
            .map_err(|e| color_eyre::eyre::eyre!(e))?
        {
            crate::formats::Route::Delimited(choice) => Ok(Some(choice)),
            _ => Ok(None),
        }
    }

    /// `paths` read with the CSV reader in the dialect of the delimited spec `choice`
    /// holds.
    fn read_with_delimited_spec(
        paths: &[PathBuf],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
        choice: crate::formats::Choice,
    ) -> Result<Scan> {
        let mut nested = options.clone();
        let Some(delimited) = choice.spec.delimited.clone() else {
            return Err(color_eyre::eyre::eyre!(
                "{} is not a delimited spec",
                choice.spec.name
            ));
        };
        delimited.apply(&mut nested);
        nested.delimited = Some(Arc::new(crate::delimited_spec::DelimitedRead::chosen(
            choice.spec,
            choice.by,
            choice.also,
        )));
        Self::build_local_lazyframe(paths, &nested, report, formats)
    }

    /// The local half of `build_lazyframe_from_paths_with`. A directory resolves to
    /// local files, so the recursion stays here and needs no cloud settings.
    pub(crate) fn build_local_lazyframe(
        paths: &[PathBuf],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        let path = &paths[0];

        // `--hex`: the file's bytes, whatever it holds.
        if options.hex
            && let [one] = paths
            && one.is_file()
        {
            return Ok(Scan::Hex {
                file: one.clone(),
                asked: true,
            });
        }

        // A glob of local files the first of which a delimited spec reads: read through
        // the spec, file by file, as a directory of them is. Polars' own scan of the
        // glob would read the spec's header lines as data.
        if let [pattern] = paths
            && !options.hive
            && options.delimited.is_none()
            && options.format.is_none()
            && source::expands_as_glob(pattern)
        {
            let files = crate::local_glob::expand(pattern);
            if let Some(first) = files.iter().find(|f| !crate::nul_tail::holds_nothing(f))
                && let Some(choice) = Self::delimited_spec_of(first, options, formats)?
            {
                let format = FileFormat::from_path(first).filter(|f| f.separator().is_some());
                let nested = OpenOptions {
                    format: format.or(Some(FileFormat::Csv)),
                    ..options.clone()
                };
                return Self::read_with_delimited_spec(&files, &nested, report, formats, choice);
            }
        }

        // A format spec: one asked for, or one whose glob or magic the path matches. A
        // path whose name or bytes already say what it is opens as it always has, but
        // for a delimited spec's text. Several files are matched by the first, and
        // only to a delimited spec.
        if !options.hive && options.delimited.is_none() {
            let asked = crate::formats::Asked {
                spec_file: options.spec_file.clone(),
                spec_name: options.spec_name.clone(),
                variant: options.table.clone(),
                spec: options.spec_fetched.clone(),
                builtin: options.format.is_some(),
                compression: options.compression,
                text_only: paths.len() > 1,
            };
            match crate::formats::route(path, &asked, formats)
                .map_err(|e| color_eyre::eyre::eyre!(e))?
            {
                crate::formats::Route::Elsewhere => {}
                crate::formats::Route::Delimited(choice) => {
                    return Self::read_with_delimited_spec(paths, options, report, formats, choice);
                }
                crate::formats::Route::Read(read) => {
                    let lf = Arc::clone(&read.records).into_lazy()?;
                    report.format_read = Some(Arc::new(*read));
                    return Ok(lf.into());
                }
                crate::formats::Route::Decompress(choice) => {
                    return Ok(Scan::DecompressSpec {
                        file: path.clone(),
                        choice,
                    });
                }
            }
        } else if options.hive && (options.spec_file.is_some() || options.spec_name.is_some()) {
            return Err(color_eyre::eyre::eyre!(
                "a format spec reads one file, or one directory of column files"
            ));
        }

        // The header lines of a delimited spec's first file: its units and metadata.
        if let Some(read) = &options.delimited
            && report.delimited.is_none()
            && path.is_file()
        {
            report.delimited = Some(Arc::new(crate::delimited_spec::read_facts(
                read, paths, options,
            )?));
        }

        // One path that is a directory, whether or not `--hive` said so: naming a
        // directory is the request to read it, and the dispatch below is what picks the
        // reader for what it holds. Behind `options.hive` alone, every route that
        // reached here with a directory and without the flag fell through to the
        // Parquet scan and answered `Unsupported file type`.
        if paths.len() == 1 && (options.hive || path.is_dir()) {
            // A file is a file whatever its name holds: `a*b.parquet` is not a glob.
            let is_single_file = path.is_file();
            if !is_single_file {
                // What the directory holds picks the reader. A directory used to go
                // straight to the Parquet scan whatever was in it, so a directory of
                // `.json.gz` was opened by seeking each file's last four bytes for a
                // `PAR1` that was never going to be there — the files were fine, the
                // reader was never asked to be the right one.
                if path.is_dir()
                    && let Some(splits) = crate::hf_splits::dataset_dict(path)
                {
                    return Self::dataset_dict_split(path, &splits, options, report, formats);
                }
                if path.is_dir() {
                    match crate::discover::directory_format(path) {
                        // Flat and Parquet: the scan below is already right for it.
                        crate::discover::DirectoryFormat::One(FileFormat::Parquet, _) => {}
                        // Partitions, or an empty directory. The files are a level down
                        // under `key=value` and only the hive scan walks a tree — but
                        // hive partitioning is a Parquet-only capability in the reader
                        // datui uses (`HiveOptions::new_disabled()` is hard-coded for
                        // CSV and NDJSON), so partitions of anything else cannot be
                        // read as one table here. Saying which files they are beats
                        // Parquet's complaint that they do not end with `PAR1`.
                        crate::discover::DirectoryFormat::Deeper => {
                            if let crate::discover::DirectoryFormat::One(found, files) =
                                crate::discover::hive_leaf_format(path)
                                && found != FileFormat::Parquet
                            {
                                // The extension rather than the format's own name: it
                                // is what is on the files the user can see.
                                let named = files
                                    .first()
                                    .and_then(|f| crate::discover::data_extension(f))
                                    .unwrap_or_else(|| format!("{found:?}").to_lowercase());
                                return Err(color_eyre::eyre::eyre!(
                                    "{} is partitioned into key=value directories of .{} \
                                     files. datui reads hive partitioning for Parquet \
                                     only — open one partition instead.",
                                    path.display(),
                                    named
                                ));
                            }
                        }
                        crate::discover::DirectoryFormat::One(found, files) => {
                            // Read as the files themselves, through the same readers a
                            // list of files typed on the command line goes through. An
                            // explicit `--format` is the user's own answer and outranks
                            // what the names say.
                            let format = options.format.unwrap_or(found);
                            let files =
                                Self::hugging_face_split(path, format, files, options, report)?;
                            return Self::read_directory_files(
                                &files, options, found, report, formats,
                            );
                        }
                        crate::discover::DirectoryFormat::Mixed {
                            format: found,
                            files,
                            passed_over,
                        } => {
                            // The commonest format is the table. A directory of a
                            // thousand CSVs and one stray JSON is a directory of CSVs,
                            // and refusing the whole of it over the stray was datui
                            // deciding that a directory it could read was not worth
                            // reading.
                            let format = options.format.unwrap_or(found);
                            let files =
                                Self::hugging_face_split(path, format, files, options, report)?;
                            let lf = Self::read_directory_files(
                                &files, options, found, report, formats,
                            )?;
                            // After the call, which reads a flat directory of one format
                            // and leaves nothing out of its own. A model's config and
                            // tokenizer JSON are not data the read passed over, and the
                            // weights are not the commonest format there, so neither is
                            // said.
                            if !matches!(found, FileFormat::Safetensors | FileFormat::Gguf) {
                                report.left_out = passed_over;
                            }
                            return Ok(lf);
                        }
                    }
                }
                let use_parquet_hive =
                    path.is_dir() || path.as_os_str().to_string_lossy().contains(".parquet");
                if use_parquet_hive {
                    // Only build the LazyFrame here; schema and partition discovery are the
                    // schema phase's ("Reading schema").
                    return crate::readers::hive::scan_parquet_hive(path).map(Scan::from);
                }
                return Err(color_eyre::eyre::eyre!(
                    "With --hive use a directory or a glob pattern for Parquet (e.g. path/to/dir or path/**/*.parquet)"
                ));
            }
        }

        // A file with no extension may still be Parquet: a part file in a directory named
        // `.parquet`. A regular file is only read when nothing else settled it. A name
        // that says text (`.log`, `.txt`) is read as lines unless its bytes say a format:
        // candump writes `.log`.
        let compressed = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path))
            .is_some();
        // Under a compression suffix, the name before it says delimited text or lines
        // (`x.tsv.gz`, `app.log.gz`).
        let named = FileFormat::from_path(path).or_else(|| {
            compressed
                .then(|| FileFormat::from_path(Path::new(path.file_stem()?)))
                .flatten()
                .filter(|f| f.decompressed_once())
        });
        let mut effective_format = options
            .format
            // A name that says a format another refines is asked its bytes for it:
            // journal JSON in a `.json` file.
            .or_else(|| {
                named.filter(|f| !f.is_lines()).map(|f| {
                    (!compressed)
                        .then(|| crate::readers::refined(path, f))
                        .flatten()
                        .unwrap_or(f)
                })
            })
            .or_else(|| {
                (path.extension().is_none()
                    && crate::discover::is_parquet_key(&path.to_string_lossy()))
                .then_some(FileFormat::Parquet)
            })
            // Any other file whose name says no format, by its first bytes: each
            // format's signature says where it is believed (`crate::readers`).
            .or_else(|| crate::readers::sniff_open(path, options.compression))
            .or(named);
        // Text no signature claims: JSON, CSV or TSV on evidence, lines otherwise. Bytes
        // that are not text are shown as they are.
        if effective_format.is_none()
            && let [file] = paths
            && file.is_file()
        {
            effective_format = crate::lines::guess_file(file, options.compression)
                .map(|f| crate::lines::as_asked(f, options));
            report.guessed = effective_format.is_some();
        }
        report.format = effective_format;

        // Refused rather than ignored: a file of one table opened with `--table` would
        // otherwise look like the table asked for.
        if options.table.is_some()
            && !effective_format.is_some_and(FileFormat::takes_table)
            && options.splits.is_none()
        {
            return Err(Self::one_table(Some(path), effective_format));
        }

        // One compressed CSV, TSV, PSV or text file, as a directory of one resolves to:
        // the load decompresses it (`Step::Decompress`) into a copy the dataset holds.
        // Read here, the copy went with the state dropped below and the frame scanned
        // nothing.
        if let [file] = paths
            && compressed
            && let Some(format) = effective_format.filter(|f| f.decompressed_once())
        {
            return Ok(Scan::Decompress {
                file: file.clone(),
                format,
            });
        }

        let Some(format) = effective_format else {
            if !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", path.display()),
                )
                .into());
            }
            // A local file nothing reads is shown as its bytes (#588).
            if paths.len() == 1 && path.is_file() {
                return Ok(Scan::Hex {
                    file: path.clone(),
                    asked: false,
                });
            }
            return Err(color_eyre::eyre::eyre!(match paths.len() {
                1 => UNSUPPORTED.to_string(),
                _ => crate::readers::many_files_refused(),
            }));
        };
        // The home screen asks `reads_many_files` before it offers a directory as one
        // dataset, and this is the same question, so it cannot offer one this refuses.
        if paths.len() > 1 && !format.reads_many_files() {
            if !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", path.display()),
                )
                .into());
            }
            return Err(color_eyre::eyre::eyre!(crate::readers::many_files_refused()));
        }
        let guessed;
        let options = if report.guessed {
            guessed = OpenOptions {
                format_guessed: true,
                ..options.clone()
            };
            &guessed
        } else {
            options
        };
        crate::readers::scan(crate::readers::ScanIn {
            format,
            paths,
            options,
            report,
            formats,
        })
    }
}
