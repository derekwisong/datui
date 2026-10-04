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
            && self.len_count_failed == Some(state.len_generation());
        let total = if pending {
            Total::Pending
        } else if unknown {
            Total::Unknown
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
    fn view_state(&self) -> ViewState {
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
        ViewState { filters, sort }
    }

    /// The home screen: where the list is, how many rows the filter matched, and the
    /// order the rows are in.
    fn home_status(&self, footer: &mut Footer) {
        // The Documentation view: the catalog and the dataset it documents.
        if let Some(entry) = self
            .documentation
            .entry
            .as_ref()
            .filter(|_| self.documentation.is_open())
        {
            footer.dataset = Some(entry.name.clone());
            footer.stages.push("documentation".to_string());
            return;
        }
        footer.dataset = self
            .home
            .browsing
            .as_deref()
            .map(|path| self.home.location_label(path));
        if !self.home.filter.is_empty() {
            let matches: usize = self
                .home
                .visible()
                .iter()
                .map(|row| match row {
                    crate::home::Row::Header { matches, .. } => *matches,
                    _ => 0,
                })
                .sum();
            footer.stages.push(match matches {
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
        if waiting && self.home.visible().is_empty() {
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
        if let Some(table) = self.view_table() {
            label = format!("{label}/{table}");
        }
        Some(label)
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
                let counting =
                    self.footers_this_frame.is_some() || self.listed_this_frame.is_some();
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
                        crate::discover::format_size(*bytes)
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
                    Some("Preparing chart...".to_string())
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
        if content != MainViewContent::Datatable {
            return None;
        }
        let state = self.data_table_state.as_ref()?;
        if self.finding()
            && let Some(read) = self.find.read
        {
            return Some(ProgressLine {
                counts: vec![ProgressCount {
                    noun: "rows",
                    done: read as u64,
                    total: state.num_rows_if_valid().map(|n| n as u64),
                }],
                stoppable: true,
            });
        }
        if let Some((read, total)) = self
            .footers_this_frame
            .filter(|_| self.dataset_is_still_reading_its_footers())
        {
            return Some(ProgressLine {
                counts: vec![ProgressCount {
                    noun: "footers",
                    done: read as u64,
                    total: Some(total as u64),
                }],
                stoppable: false,
            });
        }
        None
    }

    /// Whether the footer offers the find's keys: a find is in effect, not reading
    /// and not being typed.
    pub(crate) fn find_hint_shown(&self) -> bool {
        self.find_shown() && !self.finding() && self.input_type != Some(InputType::Find)
    }

    /// Whether the footer offers the column's keys: the column cursor moved last.
    pub(crate) fn column_hints_shown(&self) -> bool {
        self.column_hints && self.input_mode == InputMode::Normal
    }
}
