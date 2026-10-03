//! State for the Views sidebar: the list of saved views scored against the
//! open file, and the save/edit form. (The code says "template" where the UI
//! says "view": the store format and config section keep the old name.)

use crate::template::{BrokenTemplate, MatchReason, Template};
use crate::widgets::text_input::TextInput;
use ratatui::widgets::TableState;

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum TemplateModalMode {
    #[default]
    List,
    Create,
    Edit,
}

/// One row of the list: a view scored against the open file, and the reason
/// its criteria fit, when they do.
pub struct ViewRow {
    pub template: Template,
    pub score: f64,
    pub reason: Option<MatchReason>,
}

/// The rows of the save/edit form, in Tab order. The five criteria rows walk
/// only while the Matching section is expanded.
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum FormFocus {
    #[default]
    Name,
    Description,
    Matching,
    ExactPath,
    RelativePath,
    PathPattern,
    FilenamePattern,
    SchemaMatch,
}

#[derive(Default)]
pub struct TemplateModal {
    pub active: bool,
    pub mode: TemplateModalMode,
    pub table_state: TableState,
    pub rows: Vec<ViewRow>,
    pub broken_templates: Vec<BrokenTemplate>, // Views that failed to parse
    // Save/edit form
    pub form_focus: FormFocus,
    /// Whether the Matching section's criteria rows are open for editing.
    /// Collapsed by default: the defaults stand, and the form is name-first.
    pub matching_expanded: bool,
    pub name_input: TextInput,
    pub description_input: TextInput,
    pub exact_path_input: TextInput,
    pub relative_path_input: TextInput,
    pub path_pattern_input: TextInput,
    pub filename_pattern_input: TextInput,
    pub schema_match_enabled: bool,
    /// The table of a file of tables the view is for, echoed under the criteria: set
    /// from the dataset on save, kept from the view on edit.
    pub table: Option<String>,
    pub editing_template_id: Option<String>, // None while creating
    pub show_help: bool,
    pub delete_confirm: bool,
    pub name_error: Option<String>,
    /// A refusal or note for the list's own status line, above the footer:
    /// said where the key was pressed, not in a modal. The next key clears it.
    pub status: Option<String>,
    pub history_limit: usize,
    /// Set by `i`: the selected view's score breakdown, (title, body).
    pub score_details: Option<(String, String)>,
}

impl TemplateModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn selected_template(&self) -> Option<&Template> {
        self.table_state
            .selected()
            .and_then(|i| self.rows.get(i))
            .map(|row| &row.template)
    }

    /// Move the list selection by one, wrapping. Broken rows are shown after
    /// the views but are not selectable.
    pub fn select_next(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let i = match self.table_state.selected() {
            Some(i) if i + 1 < self.rows.len() => i + 1,
            Some(_) => 0,
            None => 0,
        };
        self.table_state.select(Some(i));
    }

    pub fn select_prev(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let i = match self.table_state.selected() {
            Some(0) | None => self.rows.len() - 1,
            Some(i) => i - 1,
        };
        self.table_state.select(Some(i));
    }

    /// The form rows Tab walks right now, in order.
    fn focus_order(&self) -> &'static [FormFocus] {
        if self.matching_expanded {
            &[
                FormFocus::Name,
                FormFocus::Description,
                FormFocus::Matching,
                FormFocus::ExactPath,
                FormFocus::RelativePath,
                FormFocus::PathPattern,
                FormFocus::FilenamePattern,
                FormFocus::SchemaMatch,
            ]
        } else {
            &[FormFocus::Name, FormFocus::Description, FormFocus::Matching]
        }
    }

    pub fn next_focus(&mut self) {
        let order = self.focus_order();
        let at = order
            .iter()
            .position(|f| *f == self.form_focus)
            .unwrap_or(0);
        self.form_focus = order[(at + 1) % order.len()];
    }

    pub fn prev_focus(&mut self) {
        let order = self.focus_order();
        let at = order
            .iter()
            .position(|f| *f == self.form_focus)
            .unwrap_or(0);
        self.form_focus = order[(at + order.len() - 1) % order.len()];
    }

    /// Collapsing while focus sits on a hidden criteria row pulls it back to
    /// the section header, so focus never points at nothing.
    pub fn toggle_matching(&mut self) {
        self.matching_expanded = !self.matching_expanded;
        if !self.matching_expanded && !self.focus_order().contains(&self.form_focus) {
            self.form_focus = FormFocus::Matching;
        }
    }

    /// The text input the form focus sits on, for key forwarding.
    pub fn focused_input_mut(&mut self) -> Option<&mut TextInput> {
        match self.form_focus {
            FormFocus::Name => Some(&mut self.name_input),
            FormFocus::Description => Some(&mut self.description_input),
            FormFocus::ExactPath => Some(&mut self.exact_path_input),
            FormFocus::RelativePath => Some(&mut self.relative_path_input),
            FormFocus::PathPattern => Some(&mut self.path_pattern_input),
            FormFocus::FilenamePattern => Some(&mut self.filename_pattern_input),
            FormFocus::Matching | FormFocus::SchemaMatch => None,
        }
    }

    /// How many of the criteria are set, for the collapsed section's chip.
    pub fn criteria_count(&self) -> usize {
        [
            &self.exact_path_input,
            &self.relative_path_input,
            &self.path_pattern_input,
            &self.filename_pattern_input,
        ]
        .iter()
        .filter(|input| !input.value().trim().is_empty())
        .count()
            + usize::from(self.schema_match_enabled)
            + usize::from(self.table.is_some())
    }

    fn reset_form(&mut self, history_limit: usize, theme: &crate::config::Theme) {
        self.form_focus = FormFocus::Name;
        self.matching_expanded = false;
        self.name_error = None;
        self.history_limit = history_limit;
        self.name_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.description_input = TextInput::multiline()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.exact_path_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.relative_path_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.path_pattern_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.filename_pattern_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.schema_match_enabled = false;
        self.table = None;
    }

    pub fn enter_create_mode(&mut self, history_limit: usize, theme: &crate::config::Theme) {
        self.mode = TemplateModalMode::Create;
        self.editing_template_id = None;
        self.reset_form(history_limit, theme);
    }

    pub fn enter_edit_mode(
        &mut self,
        template: &Template,
        history_limit: usize,
        theme: &crate::config::Theme,
    ) {
        self.mode = TemplateModalMode::Edit;
        self.reset_form(history_limit, theme);
        self.editing_template_id = Some(template.id.clone());
        self.name_input.set_value(&template.name);
        self.description_input
            .set_value(template.description.clone().unwrap_or_default());
        self.exact_path_input.set_value(
            template
                .match_criteria
                .exact_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default(),
        );
        self.relative_path_input.set_value(
            template
                .match_criteria
                .relative_path
                .clone()
                .unwrap_or_default(),
        );
        self.path_pattern_input.set_value(
            template
                .match_criteria
                .path_pattern
                .clone()
                .unwrap_or_default(),
        );
        self.filename_pattern_input.set_value(
            template
                .match_criteria
                .filename_pattern
                .clone()
                .unwrap_or_default(),
        );
        self.schema_match_enabled = template.match_criteria.schema_columns.is_some();
        self.table = template.match_criteria.table.clone();
    }

    pub fn exit_form(&mut self) {
        self.mode = TemplateModalMode::List;
        self.editing_template_id = None;
        self.name_error = None;
    }

    /// Take the modal down wholesale, whatever mode it is in.
    pub fn close(&mut self) {
        self.exit_form();
        self.active = false;
        self.delete_confirm = false;
        self.score_details = None;
        self.show_help = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collapsed, Tab walks name → description → matching and wraps; the five
    /// criteria only join the walk once the section is expanded.
    #[test]
    fn tab_walks_the_criteria_only_when_expanded() {
        let mut modal = TemplateModal::new();
        modal.form_focus = FormFocus::Name;
        modal.next_focus();
        modal.next_focus();
        assert_eq!(modal.form_focus, FormFocus::Matching);
        modal.next_focus();
        assert_eq!(modal.form_focus, FormFocus::Name, "collapsed wraps early");

        modal.form_focus = FormFocus::Matching;
        modal.toggle_matching();
        modal.next_focus();
        assert_eq!(modal.form_focus, FormFocus::ExactPath);
        modal.prev_focus();
        modal.prev_focus();
        assert_eq!(modal.form_focus, FormFocus::Description);
    }

    /// Collapsing while focus is on a criteria row pulls focus back to the
    /// section header instead of leaving it on a hidden row.
    #[test]
    fn collapsing_rescues_focus_from_a_hidden_row() {
        let mut modal = TemplateModal::new();
        modal.matching_expanded = true;
        modal.form_focus = FormFocus::PathPattern;
        modal.toggle_matching();
        assert!(!modal.matching_expanded);
        assert_eq!(modal.form_focus, FormFocus::Matching);
    }

    #[test]
    fn criteria_count_counts_set_fields_and_the_schema_toggle() {
        let mut modal = TemplateModal::new();
        assert_eq!(modal.criteria_count(), 0);
        modal.exact_path_input.set_value("/data/x.csv");
        modal.filename_pattern_input.set_value("x_*.csv");
        modal.schema_match_enabled = true;
        assert_eq!(modal.criteria_count(), 3);
    }
}
