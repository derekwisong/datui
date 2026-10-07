//! The info panel's keys.

use crate::cli::FileFormat;
use crate::form::ListMove;
use crate::widgets::info::FileFacts;
use crate::widgets::info::InfoTab;
use crate::{App, AppEvent};
use crossterm::event::{KeyCode, KeyEvent};

/// What the Info panel shows of the dataset beyond its schema: file facts, codebook, catalog
/// entry and documentation.
#[derive(Default)]
pub struct InfoState {
    /// What Info's read found about the open file, with its `dataset_generation`.
    /// Read on a worker ([`crate::jobs::Job::FileFacts`]) when the panel opens and kept however it
    /// ended, so drawing or reopening never reads again.
    pub(crate) file_facts: Option<(u64, FileFacts)>,
    /// What the dataset's columns mean, when a catalog that lists it says.
    pub codebook: Option<std::sync::Arc<crate::codebook::Codebook>>,
    /// The catalog entry the open dataset is, or is inside, and its catalog's label:
    /// what Info's Documentation tab shows.
    pub catalog_entry: Option<(String, std::sync::Arc<crate::catalog::Dataset>)>,
    /// The Documentation view, full screen over home (Ctrl+E).
    pub documentation: crate::widgets::documentation::DocState,
    /// The same page for the open dataset, on Info's Documentation tab.
    pub info_documentation: crate::widgets::documentation::DocState,
    /// HEAD an HTTP(S) file under the cursor on home to show its size. Off under
    /// `cargo test` unless a test asks.
    pub head_web_rows: bool,
}

impl App {
    /// Keys in the info panel.
    pub(crate) fn info_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let schema_tab = self.info_modal.active_tab == InfoTab::Schema;
        let notes_tab = self.info_modal.active_tab == InfoTab::Notes;
        let documentation_tab = self.info_modal.active_tab == InfoTab::Documentation
            && self.info.info_documentation.is_open();
        // The Metadata and the file's own tab scroll their lists the same way.
        let detail_tab = matches!(
            self.info_modal.active_tab,
            InfoTab::Metadata | InfoTab::Format
        );
        // Excel's and SQLite's tabs list the file's tables with a cursor (Enter opens one):
        // how many.
        let tables: Option<usize> = detail_tab
            .then(|| self.data_table_state.as_ref()?.format_detail())
            .flatten()
            .filter(|d| self.info_modal.active_tab == InfoTab::Format && !d.tables.is_empty())
            .map(|d| d.list.len());
        let notes = self
            .data_table_state
            .as_ref()
            .map(|s| s.notes().len())
            .unwrap_or(0);
        let total_rows = self
            .data_table_state
            .as_ref()
            .map(|s| s.schema().len())
            .unwrap_or(0);
        let visible = self.info_modal.schema_visible_height;

        // Each tab's list moves its own cursor: schema and notes a row at a time,
        // documentation and tables by rows and pages, a detail list scrolled (the render
        // clamps).
        if let Some(step) = ListMove::from_key(event) {
            let one = matches!(step, ListMove::Up | ListMove::Down);
            let modal = &mut self.info_modal;
            if schema_tab && one {
                if step == ListMove::Down {
                    modal.schema_table_down(total_rows, visible);
                } else {
                    modal.schema_table_up(total_rows, visible);
                }
                return None;
            } else if documentation_tab && !matches!(step, ListMove::Home | ListMove::End) {
                let page = self.info.info_documentation.view_height;
                self.info.info_documentation.move_cursor(step.delta(page));
                return None;
            } else if notes_tab && one {
                modal.notes_move(step.delta(1), notes);
                return None;
            } else if let Some(tables) = tables {
                let page = modal.detail_visible;
                modal.detail_selected = step.apply(modal.detail_selected, tables, page);
                return None;
            } else if detail_tab {
                modal.detail_scroll_by(step.delta(modal.detail_visible));
                return None;
            }
        }

        match event.code {
            KeyCode::Esc | KeyCode::Char('i') if event.is_press() => {
                self.close_overlay();
            }
            // The rows counted exactly, where they are an estimate.
            KeyCode::Char('c') if event.is_press() && self.row_estimate().is_some() => {
                self.count_exactly();
            }
            // The file's bytes, in the hex view; Esc there comes back to the panel.
            KeyCode::Char('x') if event.is_press() => {
                if let Some(path) = self.hex_target() {
                    self.close_overlay();
                    self.open_hex(path, crate::hex_view::Origin::Info, false, None);
                }
            }
            // Delimited text: read the first row as data, or as names again; the panel closes
            // for the read.
            KeyCode::Char('H') if event.is_press() && schema_tab => {
                if self.header_toggle_offered() {
                    self.close_overlay();
                    return self.toggle_header();
                }
            }
            // Tabs switch from anywhere: the panel is a viewer, not a form, so Tab and ←→ step
            // tabs.
            KeyCode::Left | KeyCode::Char('h') | KeyCode::BackTab if event.is_press() => {
                let offered = self.info_tabs_on_offer();
                self.info_modal.switch_tab_prev(offered);
            }
            KeyCode::Right | KeyCode::Char('l') | KeyCode::Tab if event.is_press() => {
                let offered = self.info_tabs_on_offer();
                self.info_modal.switch_tab(offered);
            }
            KeyCode::Enter | KeyCode::Char(' ') if event.is_press() && documentation_tab => {
                self.info.info_documentation.toggle_legend();
            }
            KeyCode::Char('o') if event.is_press() && documentation_tab => {
                let link = self.info.info_documentation.link();
                self.ask_to_open_link(link);
            }
            KeyCode::Char('y') if event.is_press() && documentation_tab => {
                match self.info.info_documentation.copy_text() {
                    Some(text) => self.copy_documentation_text(text),
                    None => self.flash_note("Nothing to copy on this line".to_string()),
                }
            }
            KeyCode::Enter if event.is_press() && notes_tab => {
                self.read_the_selected_note_s_column_as_text();
            }
            // The column's type, as a spec's `type` would say it.
            KeyCode::Enter if event.is_press() && schema_tab => {
                let column = self.data_table_state.as_ref().and_then(|s| {
                    s.schema()
                        .get_at_index(self.info_modal.schema_selected_index)
                        .map(|(name, _)| name.to_string())
                });
                if let Some(column) = column {
                    self.open_retype(&column);
                }
            }
            KeyCode::Enter if event.is_press() && tables.is_some() => {
                return self.open_table_from_info();
            }
            _ => {}
        }
        None
    }

    /// Enter on the Excel or SQLite tab: open the worksheet or table under the cursor
    /// in place of this one, as `T` does.
    fn open_table_from_info(&mut self) -> Option<AppEvent> {
        let detail = self.data_table_state.as_ref()?.format_detail()?;
        let (name, _) = detail.list.get(self.info_modal.detail_selected)?;
        let name = name.clone();
        if !detail.tables.contains(&name) {
            self.flash_note("Not a table".to_string());
            return None;
        }
        if detail.table.as_ref() == Some(&name) {
            self.flash_note("Already open".to_string());
            return None;
        }
        self.close_overlay();
        self.switch_table(Some(name))
    }

    /// Take the offer on the selected note: read its column as text. `Ok(false)` means
    /// the note went stale under the cursor; a failure is the scan's, shown as any
    /// failed read.
    fn read_the_selected_note_s_column_as_text(&mut self) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let notes = state.notes();
        let Some(column) = notes
            .get(self.info_modal.notes_selected_index)
            .and_then(|note| note.read_as_text.clone())
        else {
            return;
        };
        // Rebuilt here, read off the UI thread. A failure is left showing on the state.
        if let Ok(true) = state.deferred(|s| s.read_column_as_text(&column)) {
            // The note is gone and the list shorter: keep the cursor as near as the list
            // allows rather than jumping to the top.
            let notes = state.notes().len();
            self.info_modal.notes_selected_index = self
                .info_modal
                .notes_selected_index
                .min(notes.saturating_sub(1));
            self.info_modal.notes_scroll_offset = 0;
            self.spawn_async_collect(Self::LOADING_BUFFER);
        }
    }

    /// Which of the Info panel's optional tabs the current dataset offers.
    fn info_tabs_on_offer(&self) -> crate::widgets::info::TabsOffered {
        let facts_tab = self.info_facts_tab();
        self.data_table_state
            .as_ref()
            .map(|state| crate::widgets::info::TabsOffered {
                documentation: self.info.info_documentation.is_open(),
                ..crate::widgets::info::TabsOffered::of(state, facts_tab)
            })
            .unwrap_or_default()
    }

    /// The format of the dataset on screen, as the open read it.
    pub(crate) fn opened_format(&self) -> Option<FileFormat> {
        self.source
            .opened
            .as_ref()
            .and_then(|(_, options)| options.format)
            .or_else(|| self.path.as_deref().and_then(FileFormat::from_path))
    }

    /// The Info tab the file facts fill: one local file (not a hive directory) whose
    /// reader has a facts read. See [`crate::widgets::info::InfoContext::facts_tab`].
    pub(crate) fn info_facts_tab(&self) -> Option<&'static str> {
        self.info_facts()
            .and_then(|(format, _)| format.summary_tab())
    }

    /// The format and facts read the Info worker made for the dataset on screen, once
    /// asked.
    pub(crate) fn info_facts(&self) -> Option<(FileFormat, crate::readers::Facts)> {
        match self.file_facts()? {
            // A directory, which has no footer of its own.
            FileFacts::Read {
                size: None,
                detail: None,
                ..
            } => None,
            _ => self.facts_of_open(),
        }
    }
}
