use ratatui::widgets::TableState;

#[derive(Debug, Clone)]
pub struct SortColumn {
    pub name: String,
    pub sort_order: Option<usize>, // For sorting (which columns to sort by and in what order)
    pub display_order: usize,      // For column display order
    pub is_locked: bool,           // Whether this column is locked (and all columns before it)
    pub is_to_be_locked: bool, // Whether this column is to-be-locked (pending, shown as dim lock)
    pub is_visible: bool,      // Whether this column is visible in the table
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum SortFocus {
    #[default]
    Filter,
    ColumnList,
    Order,
    Apply,
    Cancel,
    Clear,
}

pub struct SortModal {
    pub active: bool,
    pub filter: String,
    pub filter_cursor: usize, // Cursor position in filter string
    pub columns: Vec<SortColumn>,
    pub table_state: TableState,
    pub ascending: bool,
    pub focus: SortFocus,
    pub has_unapplied_changes: bool,
}

impl Default for SortModal {
    fn default() -> Self {
        Self {
            active: false,
            filter: String::new(),
            filter_cursor: 0,
            columns: Vec::new(),
            table_state: TableState::default(),
            ascending: true,
            focus: SortFocus::default(),
            has_unapplied_changes: false,
        }
    }
}

impl SortModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filtered_columns(&self) -> Vec<(usize, &SortColumn)> {
        let mut filtered: Vec<_> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.name.to_lowercase().contains(&self.filter.to_lowercase()))
            .collect();
        // Sort by display_order to show columns in their current order
        filtered.sort_by_key(|(_, c)| c.display_order);
        filtered
    }

    pub fn get_column_order(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.columns.iter().filter(|c| c.is_visible).collect();
        cols.sort_by_key(|c| c.display_order);
        cols.into_iter().map(|c| c.name.clone()).collect()
    }

    pub fn get_locked_columns_count(&self) -> usize {
        // Count locked columns by checking display_order
        let mut locked_count = 0;
        for col in &self.columns {
            if col.is_locked {
                locked_count = locked_count.max(col.display_order + 1);
            }
        }
        locked_count
    }

    pub fn get_sorted_columns(&self) -> Vec<String> {
        let mut sorted: Vec<_> = self
            .columns
            .iter()
            .filter_map(|c| c.sort_order.map(|o| (o, c.name.clone())))
            .collect();
        sorted.sort_by_key(|(order, _)| *order);
        sorted.into_iter().map(|(_, name)| name).collect()
    }

    pub fn toggle_selection(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                if let Some(old_order) = self.columns[real_idx].sort_order {
                    self.columns[real_idx].sort_order = None;
                    for col in &mut self.columns {
                        if let Some(order) = col.sort_order {
                            if order > old_order {
                                col.sort_order = Some(order - 1);
                            }
                        }
                    }
                } else {
                    let max_order = self
                        .columns
                        .iter()
                        .filter_map(|c| c.sort_order)
                        .max()
                        .unwrap_or(0);
                    self.columns[real_idx].sort_order = Some(max_order + 1);
                }
                self.has_unapplied_changes = true;
            }
        }
    }

    // Move column up in display order (left)
    pub fn move_column_display_up(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                let current_display_order = self.columns[real_idx].display_order;
                let was_locked = self.columns[real_idx].is_locked;
                if current_display_order > 0 {
                    // Find the column with display_order one less (the column we're moving above)
                    let mut target_col_locked = false;
                    let mut target_col_to_be_locked = false;
                    for col in &self.columns {
                        if col.display_order == current_display_order - 1 {
                            target_col_locked = col.is_locked;
                            target_col_to_be_locked = col.is_to_be_locked;
                            break;
                        }
                    }

                    // Swap display orders
                    for col in &mut self.columns {
                        if col.display_order == current_display_order - 1 {
                            col.display_order = current_display_order;
                            break;
                        }
                    }
                    let new_display_order = current_display_order - 1;
                    let was_to_be_locked = self.columns[real_idx].is_to_be_locked;

                    // Find the last column in the lock/to-be-locked section BEFORE the move
                    // (needed to check if this is the last one)
                    let last_locked_or_to_be_order = self
                        .columns
                        .iter()
                        .filter(|c| c.is_locked || c.is_to_be_locked)
                        .map(|c| c.display_order)
                        .max()
                        .unwrap_or(0);

                    // Swap display orders
                    self.columns[real_idx].display_order = new_display_order;

                    // If moving an unlocked column into a locked region, inherit the lock status
                    if !was_locked && !was_to_be_locked {
                        if target_col_locked || target_col_to_be_locked {
                            // The column we moved above is locked or to-be-locked, so this column should match
                            self.columns[real_idx].is_locked = target_col_locked;
                            self.columns[real_idx].is_to_be_locked = target_col_to_be_locked;
                        }
                    } else {
                        // When moving a locked or to-be-locked column up, check if it's the last one in the lock/to-be-locked section
                        // Only clear to-be-locked if this is the last column in the lock/to-be-locked section
                        if (was_locked || was_to_be_locked)
                            && current_display_order == last_locked_or_to_be_order
                        {
                            // Clear to-be-locked for columns that are now at positions between new and old (exclusive of new, inclusive of old)
                            // After swap: the column that was at new_display_order is now at current_display_order
                            // We want to clear to-be-locked for columns at positions > new_display_order and <= current_display_order
                            // but don't remove real locks
                            for col in &mut self.columns {
                                if col.display_order > new_display_order
                                    && col.display_order <= current_display_order
                                    && col.is_to_be_locked
                                {
                                    col.is_to_be_locked = false;
                                }
                            }
                        }
                    }

                    self.has_unapplied_changes = true;
                    // Update selection to follow the moved item
                    if let Some(new_selected_idx) = self
                        .filtered_columns()
                        .iter()
                        .position(|&(idx, _)| idx == real_idx)
                    {
                        self.table_state.select(Some(new_selected_idx));
                    }
                }
            }
        }
    }

    // Move column down in display order (right)
    pub fn move_column_display_down(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                let max_display_order = self
                    .columns
                    .iter()
                    .map(|c| c.display_order)
                    .max()
                    .unwrap_or(0);
                let current_display_order = self.columns[real_idx].display_order;
                let was_locked = self.columns[real_idx].is_locked;
                if current_display_order < max_display_order {
                    // Find the column with display_order one more
                    for col in &mut self.columns {
                        if col.display_order == current_display_order + 1 {
                            col.display_order = current_display_order;
                            break;
                        }
                    }
                    let new_display_order = current_display_order + 1;
                    let was_to_be_locked = self.columns[real_idx].is_to_be_locked;

                    // Swap display orders first
                    self.columns[real_idx].display_order = new_display_order;

                    // If a locked or to-be-locked column is moved down, mark any unlocked columns it crosses as to-be-locked
                    // After swap: the column that was at new_display_order is now at current_display_order
                    // We need to mark columns that are now at positions from old position (inclusive) to new position (exclusive)
                    // Excluding the moved column itself (which is now at new_display_order)
                    if was_locked || was_to_be_locked {
                        for (idx, col) in self.columns.iter_mut().enumerate() {
                            // Mark columns that are now at positions from old position (inclusive) to new position (exclusive)
                            // Exclude the moved column itself
                            if idx != real_idx
                                && col.display_order >= current_display_order
                                && col.display_order < new_display_order
                                && !col.is_locked
                            {
                                col.is_to_be_locked = true;
                            }
                        }
                    }

                    self.has_unapplied_changes = true;
                    // Update selection to follow the moved item
                    if let Some(new_selected_idx) = self
                        .filtered_columns()
                        .iter()
                        .position(|&(idx, _)| idx == real_idx)
                    {
                        self.table_state.select(Some(new_selected_idx));
                    }
                }
            }
        }
    }

    // Toggle lock at this column (lock all columns up to and including this one)
    pub fn toggle_lock_at_column(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                let target_display_order = self.columns[real_idx].display_order;

                // Count how many columns are currently locked
                let current_locked_count = self.columns.iter().filter(|c| c.is_locked).count();

                // If clicking on a locked column or the first unlocked column, toggle lock boundary
                if target_display_order < current_locked_count {
                    // Unlock: set locked count to target_display_order
                    for col in &mut self.columns {
                        col.is_locked = col.display_order < target_display_order;
                        col.is_to_be_locked = false; // Clear to-be-locked when unlocking
                    }
                } else {
                    // Lock: set locked count to target_display_order + 1
                    for col in &mut self.columns {
                        col.is_locked = col.display_order <= target_display_order;
                        col.is_to_be_locked = false; // Clear to-be-locked when applying locks
                    }
                }
                self.has_unapplied_changes = true;
            }
        }
    }

    pub fn move_selection_up(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                if let Some(current_order) = self.columns[real_idx].sort_order {
                    if current_order > 1 {
                        for col in &mut self.columns {
                            if col.sort_order == Some(current_order - 1) {
                                col.sort_order = Some(current_order);
                                break;
                            }
                        }
                        self.columns[real_idx].sort_order = Some(current_order - 1);
                        self.has_unapplied_changes = true;
                        // Update selection to follow the moved item
                        if let Some(new_selected_idx) = self
                            .filtered_columns()
                            .iter()
                            .position(|&(idx, _)| idx == real_idx)
                        {
                            self.table_state.select(Some(new_selected_idx));
                        }
                    }
                }
            }
        }
    }

    pub fn move_selection_down(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                let max_order = self
                    .columns
                    .iter()
                    .filter_map(|c| c.sort_order)
                    .max()
                    .unwrap_or(0);
                if let Some(current_order) = self.columns[real_idx].sort_order {
                    if current_order < max_order {
                        for col in &mut self.columns {
                            if col.sort_order == Some(current_order + 1) {
                                col.sort_order = Some(current_order);
                                break;
                            }
                        }
                        self.columns[real_idx].sort_order = Some(current_order + 1);
                        // Update selection to follow the moved item
                        if let Some(new_selected_idx) = self
                            .filtered_columns()
                            .iter()
                            .position(|&(idx, _)| idx == real_idx)
                        {
                            self.table_state.select(Some(new_selected_idx));
                        }
                        self.has_unapplied_changes = true;
                    }
                }
            }
        }
    }

    pub fn next_focus(&mut self) {
        self.focus = match self.focus {
            SortFocus::Filter => SortFocus::ColumnList,
            SortFocus::ColumnList => SortFocus::Order,
            SortFocus::Order => SortFocus::Apply,
            SortFocus::Apply => SortFocus::Cancel,
            SortFocus::Cancel => SortFocus::Clear,
            SortFocus::Clear => SortFocus::Filter,
        };
    }

    pub fn prev_focus(&mut self) {
        self.focus = match self.focus {
            SortFocus::Filter => SortFocus::Clear,
            SortFocus::ColumnList => SortFocus::Filter,
            SortFocus::Order => SortFocus::ColumnList,
            SortFocus::Apply => SortFocus::Order,
            SortFocus::Cancel => SortFocus::Apply,
            SortFocus::Clear => SortFocus::Cancel,
        };
    }

    /// Advance focus within body only (Filter → ColumnList → Order). Returns true if we were on
    /// Order and caller should move to footer (Apply).
    pub fn next_body_focus(&mut self) -> bool {
        match self.focus {
            SortFocus::Order => return true,
            SortFocus::Filter => self.focus = SortFocus::ColumnList,
            SortFocus::ColumnList => self.focus = SortFocus::Order,
            SortFocus::Apply | SortFocus::Cancel | SortFocus::Clear => {}
        }
        false
    }

    /// Retreat focus within body only. Returns true if we were on Filter and caller should move to TabBar.
    pub fn prev_body_focus(&mut self) -> bool {
        match self.focus {
            SortFocus::Filter => return true,
            SortFocus::ColumnList => self.focus = SortFocus::Filter,
            SortFocus::Order => self.focus = SortFocus::ColumnList,
            SortFocus::Apply | SortFocus::Cancel | SortFocus::Clear => {}
        }
        false
    }

    pub fn clear_selection(&mut self) {
        // Reset all column state: clear sorting, unlock all, reset display order
        for (idx, col) in self.columns.iter_mut().enumerate() {
            col.sort_order = None;
            col.is_locked = false;
            col.is_to_be_locked = false;
            col.display_order = idx; // Reset to natural order (0, 1, 2, ...)
            col.is_visible = true; // Make all columns visible
        }
        self.has_unapplied_changes = true;
    }

    pub fn toggle_visibility(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;

                // Calculate max order before mutating
                let max_order = if self.columns[real_idx].is_visible {
                    0 // Will be recalculated if showing
                } else {
                    self.columns
                        .iter()
                        .filter(|c| c.is_visible)
                        .map(|c| c.display_order)
                        .max()
                        .unwrap_or(0)
                };

                let col = &mut self.columns[real_idx];

                if col.is_visible {
                    // Hiding: clear display order (set to a high value to push to end) and remove locked status
                    col.is_visible = false;
                    col.display_order = 9999; // High value to push hidden columns to end
                    col.is_locked = false; // Remove locked status when hiding
                    col.is_to_be_locked = false; // Remove to-be-locked status when hiding
                } else {
                    // Showing: assign next available display order (don't restore locked status)
                    col.is_visible = true;
                    col.display_order = max_order + 1;
                    // Don't restore locked status - it stays false
                }
                self.has_unapplied_changes = true;
            }
        }
    }

    pub fn jump_selection_to_order(&mut self, new_order: usize) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                let max_order = self
                    .columns
                    .iter()
                    .filter_map(|c| c.sort_order)
                    .max()
                    .unwrap_or(0);

                if new_order > 0 && new_order <= max_order + 1 {
                    let old_order = self.columns[real_idx].sort_order;
                    let selected_column_name = self.columns[real_idx].name.clone();

                    // Adjust existing orders
                    for col in &mut self.columns {
                        if col.name == selected_column_name {
                            continue; // Skip the selected column for now
                        }
                        if let Some(order) = col.sort_order {
                            if let Some(old) = old_order {
                                if new_order < old && order >= new_order && order < old {
                                    col.sort_order = Some(order + 1);
                                } else if new_order > old && order <= new_order && order > old {
                                    col.sort_order = Some(order - 1);
                                }
                            } else {
                                // If the selected column was not sorted before
                                if order >= new_order {
                                    col.sort_order = Some(order + 1);
                                }
                            }
                        }
                    }
                    self.columns[real_idx].sort_order = Some(new_order);

                    // Re-number to ensure continuous sequence if a gap was created or an item was removed
                    let mut current_sorted_cols: Vec<(&mut SortColumn, usize)> = self
                        .columns
                        .iter_mut()
                        .filter_map(|c| c.sort_order.map(|o| (c, o)))
                        .collect();
                    current_sorted_cols.sort_by_key(|(_, o)| *o);

                    for (i, (col, _)) in current_sorted_cols.into_iter().enumerate() {
                        col.sort_order = Some(i + 1);
                    }

                    // Update selection to follow the moved item
                    if let Some(new_selected_idx) = self
                        .filtered_columns()
                        .iter()
                        .position(|&(r_idx, _)| r_idx == real_idx)
                    {
                        self.table_state.select(Some(new_selected_idx));
                    }
                    self.has_unapplied_changes = true;
                } else if new_order == 0 {
                    // User wants to unset sort order
                    self.columns[real_idx].sort_order = None;
                    // Re-number to ensure continuous sequence
                    let mut current_sorted_cols: Vec<(&mut SortColumn, usize)> = self
                        .columns
                        .iter_mut()
                        .filter_map(|c| c.sort_order.map(|o| (c, o)))
                        .collect();
                    current_sorted_cols.sort_by_key(|(_, o)| *o);

                    for (i, (col, _)) in current_sorted_cols.into_iter().enumerate() {
                        col.sort_order = Some(i + 1);
                    }
                    // Selection should remain on the same column even if its sort order is removed
                    if let Some(new_selected_idx) = self
                        .filtered_columns()
                        .iter()
                        .position(|&(r_idx, _)| r_idx == real_idx)
                    {
                        self.table_state.select(Some(new_selected_idx));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_modal_new() {
        let modal = SortModal::new();
        assert!(!modal.active);
        assert_eq!(modal.filter, "");
        assert!(modal.columns.is_empty());
        assert!(modal.table_state.selected().is_none());
        assert!(modal.ascending);
        assert_eq!(modal.focus, SortFocus::Filter);
    }

    #[test]
    fn test_filtered_columns() {
        let mut modal = SortModal::new();
        modal.columns = vec![
            SortColumn {
                name: "Apple".to_string(),
                sort_order: None,
                display_order: 0,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "Banana".to_string(),
                sort_order: None,
                display_order: 1,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "Orange".to_string(),
                sort_order: None,
                display_order: 2,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
        ];
        modal.filter = "an".to_string();
        let filtered = modal.filtered_columns();
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].1.name, "Banana");
        assert_eq!(filtered[1].1.name, "Orange");
    }

    #[test]
    fn test_toggle_selection() {
        let mut modal = SortModal::new();
        modal.columns = vec![
            SortColumn {
                name: "A".to_string(),
                sort_order: None,
                display_order: 0,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "B".to_string(),
                sort_order: None,
                display_order: 1,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "C".to_string(),
                sort_order: None,
                display_order: 2,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
        ];
        modal.table_state.select(Some(1)); // Select "B"
        modal.toggle_selection();
        assert_eq!(modal.columns[0].sort_order, None);
        assert_eq!(modal.columns[1].sort_order, Some(1));
        assert_eq!(modal.columns[2].sort_order, None);

        modal.table_state.select(Some(0)); // Select "A"
        modal.toggle_selection();
        assert_eq!(modal.columns[0].sort_order, Some(2));
        assert_eq!(modal.columns[1].sort_order, Some(1));
        assert_eq!(modal.columns[2].sort_order, None);

        modal.table_state.select(Some(1)); // Deselect "B"
        modal.toggle_selection();
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert_eq!(modal.columns[1].sort_order, None);
        assert_eq!(modal.columns[2].sort_order, None);
    }

    #[test]
    fn test_get_sorted_columns() {
        let mut modal = SortModal::new();
        modal.columns = vec![
            SortColumn {
                name: "A".to_string(),
                sort_order: Some(2),
                display_order: 0,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "B".to_string(),
                sort_order: Some(1),
                display_order: 1,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "C".to_string(),
                sort_order: None,
                display_order: 2,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
        ];
        assert_eq!(modal.get_sorted_columns(), vec!["B", "A"]);
    }

    #[test]
    fn test_move_selection_up() {
        let mut modal = SortModal::new();
        modal.columns = vec![
            SortColumn {
                name: "A".to_string(),
                sort_order: Some(2),
                display_order: 0,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "B".to_string(),
                sort_order: Some(1),
                display_order: 1,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
        ];
        modal.table_state.select(Some(0)); // Select "A"
        modal.move_selection_up();
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert_eq!(modal.columns[1].sort_order, Some(2));
    }

    #[test]
    fn test_move_selection_down() {
        let mut modal = SortModal::new();
        modal.columns = vec![
            SortColumn {
                name: "A".to_string(),
                sort_order: Some(2),
                display_order: 0,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "B".to_string(),
                sort_order: Some(1),
                display_order: 1,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
        ];
        modal.table_state.select(Some(1)); // Select "B"
        modal.move_selection_down();
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert_eq!(modal.columns[1].sort_order, Some(2));
    }

    #[test]
    fn test_next_focus() {
        let mut modal = SortModal::new();
        assert_eq!(modal.focus, SortFocus::Filter);
        modal.next_focus();
        assert_eq!(modal.focus, SortFocus::ColumnList);
        modal.next_focus();
        assert_eq!(modal.focus, SortFocus::Order);
        modal.next_focus();
        assert_eq!(modal.focus, SortFocus::Apply);
        modal.next_focus();
        assert_eq!(modal.focus, SortFocus::Cancel);
        modal.next_focus();
        assert_eq!(modal.focus, SortFocus::Clear);
        modal.next_focus();
        assert_eq!(modal.focus, SortFocus::Filter);
    }

    #[test]
    fn test_prev_focus() {
        let mut modal = SortModal::new();
        assert_eq!(modal.focus, SortFocus::Filter);
        modal.prev_focus();
        assert_eq!(modal.focus, SortFocus::Clear);
        modal.prev_focus();
        assert_eq!(modal.focus, SortFocus::Cancel);
        modal.prev_focus();
        assert_eq!(modal.focus, SortFocus::Apply);
        modal.prev_focus();
        assert_eq!(modal.focus, SortFocus::Order);
        modal.prev_focus();
        assert_eq!(modal.focus, SortFocus::ColumnList);
        modal.prev_focus();
        assert_eq!(modal.focus, SortFocus::Filter);
    }

    #[test]
    fn test_clear_selection() {
        let mut modal = SortModal::new();
        modal.columns = vec![
            SortColumn {
                name: "A".to_string(),
                sort_order: Some(1),
                display_order: 0,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
            SortColumn {
                name: "B".to_string(),
                sort_order: Some(2),
                display_order: 1,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
            },
        ];
        modal.clear_selection();
        assert!(modal.columns[0].sort_order.is_none());
        assert!(modal.columns[1].sort_order.is_none());
    }
}
