//! What the status footer says: the App's state read into a [`Footer`].
//!
//! Kept beside `App` rather than in `render/`, because it reads the App's private
//! state: the open in flight, the export, the busy status message.

use crate::render::footer::{
    Footer, Position, ProgressCount, ProgressLine, QUERY_STAGE, Total, ViewState,
};
use crate::render::main_view::MainViewContent;
use crate::{App, ExportProgress, InputMode, InputType};

impl App {
    /// The footer for the screen `content` names; `progress_drawn` when its progress
    /// line has a row of its own.
    pub(crate) fn footer(&self, content: MainViewContent, progress_drawn: bool) -> Footer {
        let g = crate::glyphs::get();
        let spinner = g.spinner[self.throbber_frame as usize % g.spinner.len()];
        let mut footer = Footer {
            hints: crate::render::main_view::mode_hints(self, content),
            help: crate::render::main_view::help_key(self, content),
            spinner,
            message: self.flash.as_ref().map(|f| f.message.clone()),
            message_path: self.flash.as_ref().and_then(|f| f.path_from),
            ..Footer::default()
        };
        // A progress line says what the work is doing, with numbers; the status line
        // need not say it again.
        footer.work = if progress_drawn {
            None
        } else {
            self.work_message(content).or_else(|| {
                // Busy with nothing to say: the spinner alone.
                (self.is_busy() || self.chart_preparing() || self.value_counts_computing())
                    .then(String::new)
            })
        };
        if let Some(follow) = self.follow_mark() {
            footer.notes.extend(follow.notes());
        }
        match content {
            MainViewContent::Home => self.home_status(&mut footer),
            MainViewContent::Loading => {}
            MainViewContent::Datatable => {
                footer.dataset = self.dataset_label();
                // The builder covers the table: its position and view are not on
                // screen, so the status names where you are instead.
                if self.input_mode == InputMode::PivotMelt && self.pivot_melt_modal.active {
                    footer.stages.push("pivot & melt".to_string());
                } else {
                    self.table_status(&mut footer);
                }
            }
            MainViewContent::Analysis
            | MainViewContent::Chart
            | MainViewContent::ValueCounts
            | MainViewContent::Hex => {
                footer.dataset = self.dataset_label();
            }
        }
        footer
    }

    /// The table's state in pipeline order: query, reshape, filters, sort; the find
    /// in effect; and where the cursor is.
    fn table_status(&self, footer: &mut Footer) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        // The sample, in its place in the pipeline: over the source, or over the
        // query or filters it was drawn through.
        if let Some(sampled) = state.sampled() {
            let source = sampled.source();
            if sampled.through() {
                if !source.get_active_query().trim().is_empty()
                    || !source.get_active_sql_query().trim().is_empty()
                    || !source.get_active_fuzzy_query().trim().is_empty()
                {
                    footer.stages.push(QUERY_STAGE.to_string());
                } else {
                    footer.stages.push("filtered".to_string());
                }
            }
            footer.stages.push(sampled.label());
        }
        if !state.get_active_query().trim().is_empty()
            || !state.get_active_sql_query().trim().is_empty()
            || !state.get_active_fuzzy_query().trim().is_empty()
        {
            footer.stages.push(QUERY_STAGE.to_string());
        }
        if state.last_pivot_spec().is_some() {
            footer.stages.push("pivoted".to_string());
        } else if state.last_melt_spec().is_some() {
            footer.stages.push("melted".to_string());
        }
        if let Some(format) = state.not_the_table() {
            footer.stages.push(format!("not the {format} table"));
        }
        if let Some(read) = state.format_read()
            && !read.also.is_empty()
        {
            footer
                .stages
                .push(format!("{} formats match", read.also.len() + 1));
        }
        footer.view = self.view_state();
        // At the plain table a click on the query opens it at `:`, and on the filters
        // and sort the sidebar that lists them; over a dialog those keys type.
        if self.in_normal_table_view() {
            footer.query_key = Some(":");
            footer.view_key = Some("s");
        }
        // `#` is the view's place where the rows cannot say theirs in the source.
        if state.row_numbers_count_the_view() {
            footer.notes.push(("# counts the view".to_string(), false));
        }
        if let Some(mark) = self.find_mark() {
            footer.notes.push((mark, false));
        }
        // Nothing is counted while a load waits on the download confirmation, and a
        // spinning count there would read as progress.
        if self.awaiting_open_confirmation() {
            return;
        }
        let pending = self.row_count_pending();
        let unknown = !pending
            && !state.is_num_rows_valid()
            && self.counting.len_count_failed == Some(state.len_generation());
        // A pipe still being read, or a sample still being drawn: what is here is
        // a part of what is coming.
        let arriving = state
            .follow()
            .is_some_and(|follow| follow.is_pipe() && follow.live())
            || state.sampled().is_some_and(|sampled| sampled.drawing());
        let estimate = self.row_estimate();
        let total = if let Some(estimate) = estimate {
            // Until it is counted, which the progress line says while it is.
            Total::Estimated(estimate.rows as usize)
        } else if pending {
            Total::Pending
        } else if unknown {
            Total::Unknown
        } else if arriving {
            Total::Partial(state.num_rows())
        } else {
            Total::Known(state.num_rows())
        };
        let row = if state.num_rows() == 0 && !pending {
            0
        } else {
            state.cursor_row() + 1
        };
        footer.position = Some(Position {
            // Counted as the row numbers count, as `:` reads a row.
            row: if row == 0 {
                0
            } else {
                row - 1 + state.row_start_index()
            },
            total,
            column: state.columns_on_screen(),
        });
    }

    /// The filters and sort in effect, as the footer writes them.
    pub(crate) fn view_state(&self) -> ViewState {
        let Some(state) = self.data_table_state.as_ref() else {
            return ViewState::default();
        };
        let g = crate::glyphs::get();
        let filters = state
            .view_filters()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let text = f.describe();
                if i > 0 && f.logical_op == crate::filter_modal::LogicalOperator::Or {
                    format!("or {text}")
                } else {
                    text
                }
            })
            .collect();
        let columns = state.view_sort_columns();
        let sort = if !columns.is_empty() {
            Some(
                columns
                    .iter()
                    .zip(state.view_sort_descending())
                    .map(|(name, desc)| {
                        format!("{name} {}", if *desc { g.sort_desc } else { g.sort_asc })
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        } else if !state.view_sort_ascending() {
            Some("reversed".to_string())
        } else {
            None
        };
        ViewState {
            typed: state.retyped_columns(),
            filters,
            sort,
        }
    }

    /// The home screen: where the list is, how many rows the filter matched, and the
    /// order the rows are in.
    fn home_status(&self, footer: &mut Footer) {
        // The Documentation view: the dataset it documents.
        if let Some(doc) = self.info.documentation.doc.as_ref() {
            footer.dataset = Some(doc.title().to_string());
            footer.stages.push("documentation".to_string());
            return;
        }
        footer.dataset = self
            .home
            .browsing
            .as_deref()
            .map(|path| self.home.location_label(path));
        if !self.home.filter.is_empty() {
            footer.stages.push(match self.home.matched() {
                1 => "1 match".to_string(),
                n => format!("{} matches", crate::numfmt::group_chrome(n)),
            });
        }
        let in_recents = self
            .home
            .selected_section()
            .and_then(|i| self.home.sections.get(i))
            .is_some_and(|s| s.grouped_by_place);
        let waiting = self.home.listing_in_flight || self.home.awaiting_listing().is_some();
        if waiting && self.home.row_count() == 0 {
            footer.work = Some("Looking...".to_string());
        } else {
            footer
                .notes
                .push((format!("by {}", self.home.sort.label_in(in_recents)), false));
        }
    }

    /// The dataset on screen, as the footer names it: the file and the directory it
    /// is in (`weather/daily.parquet`), and the table of a file of tables.
    pub(crate) fn dataset_label(&self) -> Option<String> {
        self.data_table_state.as_ref()?;
        if self.reads_stdin() {
            return Some("stdin".to_string());
        }
        let path = self.path.as_deref()?;
        let name = path.file_name()?.to_string_lossy().to_string();
        let mut label = match path
            .parent()
            .and_then(|p| p.file_name())
            .map(|p| p.to_string_lossy())
        {
            Some(parent) if !parent.ends_with(':') => format!("{parent}/{name}"),
            _ => name,
        };
        // A table opened at its place (`shop.db/orders`) is named by the path already.
        if let Some(table) = self.view_table()
            && path
                .file_name()
                .is_none_or(|n| n.to_string_lossy() != table)
        {
            label = format!("{label}/{table}");
        }
        Some(label)
    }

    /// The dataset's name as a file stem, for a name an export suggests:
    /// `daily` for `weather/daily.csv.gz`, `shop_orders` for a table in `shop.db`.
    pub(crate) fn dataset_stem(&self) -> String {
        if self.reads_stdin() {
            return "stdin".to_string();
        }
        let Some(name) = self
            .path
            .as_deref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
        else {
            return "data".to_string();
        };
        // The compression's extension, then the format's.
        let mut stem = name.as_str();
        for _ in 0..2 {
            if let Some((rest, _)) = stem.rsplit_once('.')
                && !rest.is_empty()
            {
                stem = rest;
            }
        }
        let mut stem = stem.to_string();
        if let Some(table) = self.view_table()
            && table != name
        {
            stem = format!("{stem}_{table}");
        }
        stem.chars()
            .map(|c| if c == '/' || c == '\\' { '_' } else { c })
            .collect()
    }

    /// What the background work is doing, in words: the open in flight, an export,
    /// or the status message a job put up.
    fn work_message(&self, content: MainViewContent) -> Option<String> {
        // An export started over an open's first rows is the one the user waits on.
        let load = self
            .load_shown()
            .filter(|_| self.awaiting_dataset() || self.export_progress.is_none());
        let message = match (load, &self.export_progress) {
            // The load is paused on the user; the modal names the keys.
            (Some(_), _) if self.awaiting_open_confirmation() => None,
            (Some((phase, percent, ..)), _) => {
                let phase = self.loading_phase(phase);
                // A flat percentage beside a real count reads as the count's.
                let counting = self.counting.footers_this_frame.is_some()
                    || self.counting.listed_this_frame.is_some();
                if percent > 0 && !counting {
                    Some(format!("{phase}... ({percent}%)"))
                } else {
                    Some(format!("{phase}..."))
                }
            }
            (
                None,
                Some(ExportProgress {
                    current_phase,
                    written,
                    file_path,
                }),
            ) => {
                let filename = file_path.file_name().and_then(|f| f.to_str()).unwrap_or("");
                // The count last, so its changing width moves nothing.
                Some(match written {
                    Some(bytes) => format!(
                        "{current_phase}...  {filename}  {}",
                        crate::numfmt::bytes(*bytes)
                    ),
                    None => format!("{current_phase}...  {filename}"),
                })
            }
            (None, None) => {
                if self.fetch_too_young_to_mention() {
                    None
                } else if self.is_busy() {
                    self.status_message.clone()
                } else if self.chart_preparing() {
                    Some(self.chart_status())
                } else if content == MainViewContent::Datatable {
                    // Whatever is on the line, busy or not: an End waiting on a remote
                    // count parks without setting `busy`, and keys go on working. Only
                    // at the table, which is what these messages are about.
                    self.status_message.clone()
                } else {
                    None
                }
            }
        };
        message.map(|msg| {
            if self.input_dropped && self.is_busy() {
                format!("{msg}  input dropped while busy")
            } else {
                msg
            }
        })
    }

    /// The footer's progress line: a job with counts to show, and whether Esc stops
    /// it. A find reading the view counts its rows; a dataset still reading its
    /// footers counts files.
    pub(crate) fn footer_progress_line(&self, content: MainViewContent) -> Option<ProgressLine> {
        // Value counts reading the view: the rows read, and of a dataset of files, how
        // many of them the read has reached.
        if content == MainViewContent::ValueCounts
            && let Some(computing) = self.value_counts.computing.as_ref()
            && let Some(rows) = computing.watch.rows_seen()
        {
            let noun = if computing.exact { "rows" } else { "sampling" };
            let mut counts = vec![ProgressCount::of(noun, rows as u64, None)];
            if let Some((reached, files)) = computing.files_reached() {
                counts.push(ProgressCount::of(
                    "files",
                    reached as u64,
                    Some(files as u64),
                ));
            }
            return Some(ProgressLine {
                counts,
                // Esc leaves a sample being read, and stops a count of every row.
                stoppable: computing.exact,
            });
        }
        if content != MainViewContent::Datatable {
            return None;
        }
        let state = self.data_table_state.as_ref()?;
        if self.finding()
            && let Some(read) = self.prompt.find.read
        {
            return Some(ProgressLine {
                counts: vec![ProgressCount::of(
                    "rows",
                    read as u64,
                    state.num_rows_if_valid().map(|n| n as u64),
                )],
                stoppable: true,
            });
        }
        // A sample being drawn: the rows kept of those asked for, and the rows read.
        if let Some(draw) = self.sample_draw() {
            let mut counts = vec![ProgressCount::of(
                "kept",
                draw.rows.rows() as u64,
                matches!(
                    draw.sample.method,
                    crate::sampling::SampleMethod::Spread
                        | crate::sampling::SampleMethod::FirstRows
                )
                .then_some(draw.sample.rows as u64),
            )];
            if let Some(read) = draw.watch.rows_seen() {
                counts.push(ProgressCount::of("read", read as u64, None));
            }
            return Some(ProgressLine {
                counts,
                stoppable: true,
            });
        }
        // Lines indexed behind the first rows: how many so far, and how far through
        // the file.
        if let Some(lines) = state.indexing() {
            let (done, all) = lines.indexed_bytes();
            return Some(ProgressLine {
                counts: vec![
                    ProgressCount::of("lines", lines.rows() as u64, None),
                    ProgressCount::bytes("read", done, Some(all)),
                ],
                stoppable: false,
            });
        }
        // The exact count of a dataset of many files: footers read of how many.
        if let Some((read, total)) = self.footers_counted() {
            return Some(ProgressLine {
                counts: vec![ProgressCount::of("files", read as u64, Some(total as u64))],
                stoppable: true,
            });
        }
        if let Some((read, total)) = self
            .counting
            .footers_this_frame
            .filter(|_| self.dataset_is_still_reading_its_footers())
        {
            return Some(ProgressLine {
                counts: vec![ProgressCount::of(
                    "footers",
                    read as u64,
                    Some(total as u64),
                )],
                stoppable: false,
            });
        }
        None
    }

    /// Whether the footer offers the find's keys: a find is in effect, not reading
    /// and not being typed.
    pub(crate) fn find_hint_shown(&self) -> bool {
        self.find_shown() && !self.finding() && self.prompt.input_type != Some(InputType::Find)
    }

    /// Whether the footer offers the column's keys: the column cursor moved last.
    pub(crate) fn column_hints_shown(&self) -> bool {
        self.prompt.column_hints && self.input_mode == InputMode::Normal
    }
}
