//! A paste: text the terminal brackets (bracketed paste), inserted as one edit into the
//! text that has focus, and into nothing else. It is never replayed as keys: a space
//! would press Space in a picker, a line break Enter, and each character would be its
//! own undo step and its own search.

use crate::app::form::Form;
use crate::widgets::text_input::{TextInput, one_line};
use crate::widgets::ui::PickerState;
use crate::{App, AppEvent, InputMode, InputType, Overlay};

/// What a paste goes into, where the app stands now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PasteTarget {
    /// The home screen's `~` prompt, or its filter.
    Home,
    /// The focused text ([`App::focused_text_mut`]).
    Field,
    /// Nothing takes text here (the table, a dialog's buttons): the paste is dropped,
    /// never read as keys.
    Nowhere,
}

/// The text that has focus, as [`App::text_field_focused`] names it.
pub(crate) enum FocusedText<'a> {
    /// A field's editor.
    Input(&'a mut TextInput),
    /// A picker's narrowing filter.
    Picker(&'a mut PickerState),
    /// A find typed into a plain string (the inspector's).
    Filter(&'a mut String),
}

impl FocusedText<'_> {
    /// Insert `text` as one edit; whether anything changed.
    fn insert(self, text: &str) -> bool {
        match self {
            FocusedText::Input(input) => input.insert_text(text),
            FocusedText::Picker(picker) => {
                let before = picker.filter.len();
                picker.type_text(text);
                picker.filter.len() != before
            }
            FocusedText::Filter(filter) => {
                let text = one_line(text);
                filter.push_str(&text);
                !text.is_empty()
            }
        }
    }
}

impl App {
    /// Where pasted text goes now.
    pub(crate) fn paste_target(&self) -> PasteTarget {
        // These take keys ahead of any field, and none of them types.
        if self.help.is_open()
            || self.confirmation_modal.active
            || self.error_modal.active
            || self.context_menu.is_some()
        {
            return PasteTarget::Nowhere;
        }
        if self.input_mode == InputMode::Home && self.overlay == Overlay::None {
            if self.info.documentation.is_open() {
                return PasteTarget::Nowhere;
            }
            return PasteTarget::Home;
        }
        if self.text_field_focused() {
            return PasteTarget::Field;
        }
        PasteTarget::Nowhere
    }

    /// The text that has focus, where [`App::text_field_focused`] says one does: what
    /// a typed character would edit.
    pub(crate) fn focused_text_mut(&mut self) -> Option<FocusedText<'_>> {
        use crate::app::modals::filter_modal::FilterEditStep;
        use FocusedText::{Filter, Input, Picker};
        if !self.text_field_focused() {
            return None;
        }
        match &mut self.overlay {
            Overlay::None => match self.prompt.input_type {
                Some(InputType::Query) => {
                    self.sync_query_focus();
                    Some(Input(self.query_input_mut()))
                }
                Some(InputType::Find) => Some(Input(&mut self.prompt.find.input)),
                None => None,
            },
            Overlay::Analysis => {
                let modal = &mut self.analysis_modal;
                if modal.sample_scope_typing() {
                    let form = modal.sample_form.as_mut()?;
                    let field = form.field;
                    form.input_mut(field).map(Input)
                } else if modal.quality_expected_typing() {
                    modal.quality.expected_form.as_mut()?.input_mut().map(Input)
                } else if modal.intent_typing() {
                    modal.quality.intent_form.as_mut()?.input_mut().map(Input)
                } else if modal.export_typing() {
                    Some(Input(&mut modal.quality.export.as_mut()?.path))
                } else {
                    None
                }
            }
            Overlay::View => self.view_modal.focused_input_mut().map(Input),
            Overlay::Export { .. } => match self.export_modal.focus {
                crate::ExportFocus::PathInput => Some(Input(&mut self.export_modal.path_input)),
                crate::ExportFocus::CsvDelimiter => {
                    Some(Input(&mut self.export_modal.csv_delimiter_input))
                }
                _ => None,
            },
            Overlay::Copy => self.copy_modal.shown_picker().map(|(p, _)| Picker(p)),
            Overlay::Inspect => Some(Filter(&mut self.inspector_modal.filter)),
            Overlay::GoToColumn => Some(Picker(&mut self.pickers.go_to_column)),
            Overlay::PickFormat => Some(Picker(&mut self.pickers.format_picker)),
            Overlay::PickTable => Some(Picker(&mut self.pickers.table_picker)),
            Overlay::Retype { .. } => Some(Picker(&mut self.column_forms.retype.as_mut()?.picker)),
            Overlay::Combine { .. } => {
                let modal = self.column_forms.combine.as_mut()?;
                if modal.picker.is_some() {
                    modal.shown_picker().map(|(p, _)| Picker(p))
                } else {
                    Some(Input(&mut modal.name))
                }
            }
            Overlay::Sample => {
                let form = self.sample.form.as_mut()?;
                let field = form.field;
                form.input_mut(field).map(Input)
            }
            Overlay::SortFilter => {
                let modal = &mut self.sort_filter_modal;
                if let Some(editor) = modal.filter.editor.as_mut() {
                    Some(match editor.step {
                        FilterEditStep::Column => Picker(&mut editor.column),
                        FilterEditStep::Operator => Picker(&mut editor.operator),
                        FilterEditStep::Value => Input(&mut editor.value),
                    })
                } else if let Some(picker) = modal.sort_picker.as_mut() {
                    Some(Picker(picker))
                } else {
                    Some(Input(&mut modal.sort.filter_input))
                }
            }
            Overlay::PivotMelt => {
                let modal = &mut self.pivot_melt_modal;
                if modal.picker.is_some() {
                    modal.shown_picker().map(|(p, _)| Picker(p))
                } else {
                    modal.focused_text_input_mut().map(Input)
                }
            }
            Overlay::ChartExport => {
                let modal = &mut self.chart.export_modal;
                let focus = modal.focus;
                modal.input_mut(focus).map(Input)
            }
            Overlay::Chart => self.chart.modal.shown_picker().map(|(p, _)| Picker(p)),
            Overlay::Hex => {
                let view = self.hex.view.as_mut()?;
                match view.picker.as_mut() {
                    Some(picker) => Some(Picker(picker)),
                    None => Some(Input(&mut view.input)),
                }
            }
            Overlay::Info | Overlay::ValueCounts => None,
        }
    }

    /// Take a paste as one edit into what has focus: at home the `~` prompt or the
    /// filter (one search, one listing), elsewhere [`App::focused_text_mut`]. Then
    /// what typing into it does too: a narrowed list, a cleared error.
    pub(crate) fn paste(&mut self, text: &str) -> Option<AppEvent> {
        match self.paste_target() {
            PasteTarget::Home => self.paste_at_home(one_line(text).trim()),
            PasteTarget::Field => {
                let text = self.as_the_field_takes(text);
                if let Some(focused) = self.focused_text_mut()
                    && focused.insert(&text)
                {
                    self.typed_into_focused();
                }
            }
            PasteTarget::Nowhere => {}
        }
        None
    }

    /// What of `text` the focused field takes: the chart's size takes digits alone.
    fn as_the_field_takes(&self, text: &str) -> String {
        use crate::chart::chart_export_modal::ChartExportFocus;
        let digits = self.overlay == Overlay::ChartExport
            && matches!(
                self.chart.export_modal.focus,
                ChartExportFocus::WidthInput | ChartExportFocus::HeightInput
            );
        if digits {
            text.chars().filter(char::is_ascii_digit).collect()
        } else {
            text.to_string()
        }
    }

    /// What typing a character into the focused text also does, done once after a
    /// paste changed it.
    fn typed_into_focused(&mut self) {
        match &self.overlay {
            Overlay::None => match self.prompt.input_type {
                Some(InputType::Query) => {
                    self.prompt.query_text_restored = false;
                    self.prompt.query_run_error = None;
                }
                Some(InputType::Find) => {
                    self.prompt.find.error = None;
                    self.refresh_live_matches();
                }
                None => {}
            },
            Overlay::Analysis => {
                let quality = &mut self.analysis_modal.quality;
                if let Some(form) = quality.expected_form.as_mut() {
                    form.error = None;
                }
                if let Some(form) = quality.intent_form.as_mut() {
                    form.error = None;
                }
                if let Some(form) = quality.export.as_mut() {
                    form.error = None;
                }
                if let Some(form) = self.analysis_modal.sample_form.as_mut() {
                    form.edited();
                }
            }
            Overlay::View => {
                if self.view_modal.form_focus == crate::widgets::view_modal::FormFocus::Name {
                    self.view_modal.name_error = None;
                }
            }
            Overlay::Export { .. } => {
                if self.export_modal.focus == crate::ExportFocus::PathInput {
                    self.export_modal.sync_format_to_path();
                    self.export_modal.path_error = None;
                }
            }
            Overlay::Inspect => self.refresh_inspector_list(),
            Overlay::Combine { .. } => {
                if let Some(modal) = self.column_forms.combine.as_mut() {
                    modal.problem = None;
                }
            }
            Overlay::Sample => {
                if let Some(form) = self.sample.form.as_mut() {
                    form.edited();
                }
            }
            Overlay::SortFilter => {
                let modal = &mut self.sort_filter_modal;
                if modal.filter.editor.is_none() && modal.sort_picker.is_none() {
                    // A narrowed list starts at its first match, where ↓ lands.
                    modal.sort.table_state.select(Some(0));
                }
            }
            Overlay::PivotMelt => self.pivot_melt_modal.attention = false,
            Overlay::ChartExport => {
                use crate::chart::chart_export_modal::ChartExportFocus;
                let modal = &mut self.chart.export_modal;
                match modal.focus {
                    ChartExportFocus::PathInput => modal.error = None,
                    ChartExportFocus::WidthInput | ChartExportFocus::HeightInput => {
                        modal.size_typed()
                    }
                    _ => {}
                }
            }
            Overlay::Hex => {
                if let Some(view) = self.hex.view.as_mut() {
                    view.prompt_error = None;
                }
            }
            Overlay::Copy
            | Overlay::GoToColumn
            | Overlay::PickFormat
            | Overlay::PickTable
            | Overlay::Retype { .. }
            | Overlay::Chart
            | Overlay::Info
            | Overlay::ValueCounts => {}
        }
    }

    /// As the home screen's character keys do, once for the whole text. On an empty
    /// filter a path starting with `~` opens the path prompt with it, as `~` typed
    /// there does.
    fn paste_at_home(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        self.home.status = None;
        self.home.apply_new_measurements();
        // A kept filter is selected: the paste replaces it, as a typed character does.
        if !self.home.path_input_active && std::mem::take(&mut self.home.filter_selected) {
            self.home.filter.clear();
            self.home.sync_search_section();
            self.home.select_first_entry();
        }
        if !self.home.path_input_active && self.home.filter.is_empty() && text.starts_with('~') {
            self.home.path_input_active = true;
            self.home.path_input.clear();
            self.home.path_listing = None;
            self.home.path_pick = None;
        }
        if self.home.path_input_active {
            self.home.path_input.push_str(text);
            self.list_the_typed_directory();
            self.home.pick_first_path();
            return;
        }
        self.home.filter.push_str(text);
        self.spawn_home_search();
        self.home.sync_search_section();
        self.home.select_first_entry();
        #[cfg(feature = "cloud")]
        self.narrow_cloud_listing();
    }
}
