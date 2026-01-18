use crate::template::Template;
use ratatui::widgets::TableState;

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum TemplateModalMode {
    #[default]
    List,
    Create,
    Edit,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum TemplateFocus {
    #[default]
    TemplateList,
    CreateButton,
    EditButton,
    DeleteButton,
    HelpButton,
    SaveButton,
    CancelButton,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum CreateFocus {
    #[default]
    Name,
    Description,
    ExactPath,
    RelativePath,
    PathPattern,
    FilenamePattern,
    SchemaMatch,
    SaveButton,
    CancelButton,
}

#[derive(Default)]
pub struct TemplateModal {
    pub active: bool,
    pub mode: TemplateModalMode,
    pub focus: TemplateFocus,
    pub create_focus: CreateFocus,
    pub table_state: TableState,
    pub templates: Vec<(Template, f64)>, // Templates with relevance scores
    // Create/Edit mode fields
    pub create_name: String,
    pub create_name_cursor: usize,
    pub create_description: String,
    pub create_description_cursor: usize,
    pub create_exact_path: String,
    pub create_exact_path_cursor: usize,
    pub create_relative_path: String,
    pub create_relative_path_cursor: usize,
    pub create_path_pattern: String,
    pub create_path_pattern_cursor: usize,
    pub create_filename_pattern: String,
    pub create_filename_pattern_cursor: usize,
    pub create_schema_match_enabled: bool,
    pub editing_template_id: Option<String>, // ID of template being edited (None for create)
    pub show_help: bool,                     // Show help modal
    pub delete_confirm: bool,                // Show delete confirmation
    pub delete_confirm_focus: bool, // true = Delete button, false = Cancel button (default)
    pub name_error: Option<String>, // Error message for name validation
    pub description_scroll: usize,  // Scroll position for description field
    pub show_score_details: bool,   // Show score details popup
}

impl TemplateModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn selected_template(&self) -> Option<&Template> {
        self.table_state
            .selected()
            .and_then(|i| self.templates.get(i))
            .map(|(template, _)| template)
    }

    pub fn next_focus(&mut self) {
        if self.mode == TemplateModalMode::List {
            // In list mode, focus is always on the template list
            // No focus cycling needed
        } else {
            // In create mode, cycle through create focus areas
            self.create_focus = match self.create_focus {
                CreateFocus::Name => CreateFocus::Description,
                CreateFocus::Description => CreateFocus::ExactPath,
                CreateFocus::ExactPath => CreateFocus::RelativePath,
                CreateFocus::RelativePath => CreateFocus::PathPattern,
                CreateFocus::PathPattern => CreateFocus::FilenamePattern,
                CreateFocus::FilenamePattern => CreateFocus::SchemaMatch,
                CreateFocus::SchemaMatch => CreateFocus::SaveButton,
                CreateFocus::SaveButton => CreateFocus::CancelButton,
                CreateFocus::CancelButton => CreateFocus::Name,
            };
        }
    }

    pub fn prev_focus(&mut self) {
        if self.mode == TemplateModalMode::List {
            // In list mode, focus is always on the template list
            // No focus cycling needed
        } else {
            // Reverse cycle in create mode
            self.create_focus = match self.create_focus {
                CreateFocus::Name => CreateFocus::CancelButton,
                CreateFocus::Description => CreateFocus::Name,
                CreateFocus::ExactPath => CreateFocus::Description,
                CreateFocus::RelativePath => CreateFocus::ExactPath,
                CreateFocus::PathPattern => CreateFocus::RelativePath,
                CreateFocus::FilenamePattern => CreateFocus::PathPattern,
                CreateFocus::SchemaMatch => CreateFocus::FilenamePattern,
                CreateFocus::SaveButton => CreateFocus::SchemaMatch,
                CreateFocus::CancelButton => CreateFocus::SaveButton,
            };
        }
    }

    pub fn enter_create_mode(&mut self) {
        self.mode = TemplateModalMode::Create;
        self.create_focus = CreateFocus::Name;
        self.editing_template_id = None;
        self.name_error = None;
        // Reset create fields (will be populated by caller)
        self.create_name.clear();
        self.create_name_cursor = 0;
        self.create_description.clear();
        self.create_description_cursor = 0;
        self.create_exact_path.clear();
        self.create_exact_path_cursor = 0;
        self.create_relative_path.clear();
        self.create_relative_path_cursor = 0;
        self.create_path_pattern.clear();
        self.create_path_pattern_cursor = 0;
        self.create_filename_pattern.clear();
        self.create_filename_pattern_cursor = 0;
        self.create_schema_match_enabled = false;
    }

    pub fn enter_edit_mode(&mut self, template: &Template) {
        self.mode = TemplateModalMode::Edit;
        self.create_focus = CreateFocus::Name;
        self.editing_template_id = Some(template.id.clone());
        // Populate fields from template
        self.create_name = template.name.clone();
        self.create_name_cursor = self.create_name.len();
        self.create_description = template.description.clone().unwrap_or_default();
        self.create_description_cursor = self.create_description.len();
        self.create_exact_path = template
            .match_criteria
            .exact_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        self.create_exact_path_cursor = self.create_exact_path.len();
        self.create_relative_path = template
            .match_criteria
            .relative_path
            .clone()
            .unwrap_or_default();
        self.create_relative_path_cursor = self.create_relative_path.len();
        self.create_path_pattern = template
            .match_criteria
            .path_pattern
            .clone()
            .unwrap_or_default();
        self.create_path_pattern_cursor = self.create_path_pattern.len();
        self.create_filename_pattern = template
            .match_criteria
            .filename_pattern
            .clone()
            .unwrap_or_default();
        self.create_filename_pattern_cursor = self.create_filename_pattern.len();
        self.create_schema_match_enabled = template.match_criteria.schema_columns.is_some();
    }

    pub fn exit_create_mode(&mut self) {
        self.mode = TemplateModalMode::List;
        self.focus = TemplateFocus::TemplateList;
        self.editing_template_id = None;
    }
}
