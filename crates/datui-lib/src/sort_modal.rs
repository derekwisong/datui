use crate::widgets::column_widths::WidthChoice;
use crate::widgets::text_input::TextInput;
use ratatui::widgets::TableState;

#[derive(Debug, Clone)]
pub struct SortColumn {
    pub name: String,
    pub sort_order: Option<usize>, // For sorting (which columns to sort by and in what order)
    /// This column's own direction, meaningful while `sort_order` is set. Each column
    /// of a multi-sort runs its own way.
    pub sort_descending: bool,
    pub display_order: usize,  // For column display order
    pub is_locked: bool,       // Whether this column is locked (and all columns before it)
    pub is_to_be_locked: bool, // Whether this column is to-be-locked (pending, shown as dim lock)
    pub is_visible: bool,      // Whether this column is visible in the table
    /// How the column's width is chosen, as staged.
    pub width: WidthChoice,
    /// The width the table last drew the column at, where narrower and wider start.
    pub shown_width: Option<u16>,
}

pub struct SortModal {
    pub filter_input: TextInput,
    pub columns: Vec<SortColumn>,
    pub table_state: TableState,
    pub has_unapplied_changes: bool,
    pub history_limit: usize,
    /// Why the last key did nothing, for the sidebar's status line; the next key
    /// clears it.
    pub status: Option<String>,
    /// The column order last applied, hidden columns included. The table keeps only
    /// the visible order, so this is where a hidden column is listed, and returns
    /// to, when the sidebar reopens.
    pub applied_order: Vec<String>,
    /// How many leading columns of `applied_order` were frozen, hidden ones included:
    /// the table's count leaves out a hidden column that ended the span.
    pub applied_locked: usize,
    /// Rows the Columns list showed when last drawn: what PgUp and PgDn move.
    pub page_rows: usize,
    /// The Columns list as the find shows it, and a fingerprint of the find text and
    /// the columns it was worked out for: see [`Self::filtered_columns`].
    shown: std::sync::Mutex<Option<(u64, Vec<usize>)>>,
    /// Times the list was worked out, for a test that a key reuses it.
    #[cfg(test)]
    shown_builds: std::sync::atomic::AtomicUsize,
}

impl Default for SortModal {
    fn default() -> Self {
        Self {
            filter_input: TextInput::new(),
            columns: Vec::new(),
            table_state: TableState::default(),
            has_unapplied_changes: false,
            history_limit: 1000,
            status: None,
            applied_order: Vec::new(),
            applied_locked: 0,
            page_rows: 10,
            shown: Default::default(),
            #[cfg(test)]
            shown_builds: Default::default(),
        }
    }
}

impl SortModal {
    /// The columns the find text matches, in display order. Asked several times a key,
    /// so recomputed only when a fingerprint of the find text, names and order changes.
    pub fn filtered_columns(&self) -> Vec<(usize, &SortColumn)> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.filter_input.value().hash(&mut hasher);
        for c in &self.columns {
            (&c.name, c.display_order).hash(&mut hasher);
        }
        let key = hasher.finish();
        let mut shown = self.shown.lock().unwrap_or_else(|e| e.into_inner());
        if shown.as_ref().is_none_or(|(at, _)| *at != key) {
            let filter_text = self.filter_input.value().to_lowercase();
            let mut filtered: Vec<usize> = (0..self.columns.len())
                .filter(|&i| self.columns[i].name.to_lowercase().contains(&filter_text))
                .collect();
            filtered.sort_by_key(|&i| self.columns[i].display_order);
            *shown = Some((key, filtered));
            #[cfg(test)]
            self.shown_builds
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let rows = shown.as_ref().map_or(&[][..], |(_, rows)| rows);
        rows.iter().map(|&i| (i, &self.columns[i])).collect()
    }

    pub fn get_column_order(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.columns.iter().filter(|c| c.is_visible).collect();
        cols.sort_by_key(|c| c.display_order);
        cols.into_iter().map(|c| c.name.clone()).collect()
    }

    /// How many visible columns the table freezes: those at or before the last locked
    /// column in the order. A hidden column inside that span keeps its place but is
    /// not counted, since the table never draws it.
    pub fn get_locked_columns_count(&self) -> usize {
        let Some(last_locked) = self
            .columns
            .iter()
            .filter(|c| c.is_locked)
            .map(|c| c.display_order)
            .max()
        else {
            return 0;
        };
        self.columns
            .iter()
            .filter(|c| c.is_visible && c.display_order <= last_locked)
            .count()
    }

    /// How many leading columns of the full order are frozen, hidden ones included.
    pub fn get_locked_span(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.is_locked)
            .map(|c| c.display_order + 1)
            .max()
            .unwrap_or(0)
    }

    /// Every column of the order: all columns, hidden included, sorted by place.
    pub fn get_full_column_order(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.columns.iter().collect();
        cols.sort_by_key(|c| c.display_order);
        cols.into_iter().map(|c| c.name.clone()).collect()
    }

    pub fn get_sorted_columns(&self) -> Vec<String> {
        self.sorted_columns_and_directions().0
    }

    /// The staged sort: column names in sort order, and per column whether it runs
    /// descending.
    pub fn sorted_columns_and_directions(&self) -> (Vec<String>, Vec<bool>) {
        let mut sorted: Vec<_> = self
            .columns
            .iter()
            .filter_map(|c| c.sort_order.map(|o| (o, c.name.clone(), c.sort_descending)))
            .collect();
        sorted.sort_by_key(|(order, _, _)| *order);
        sorted
            .into_iter()
            .map(|(_, name, descending)| (name, descending))
            .unzip()
    }

    /// Space on a column: none → ascending → descending → none. Joining the sort
    /// appends the column at the end; leaving it renumbers the rest.
    pub fn cycle_sort(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                match (
                    self.columns[real_idx].sort_order,
                    self.columns[real_idx].sort_descending,
                ) {
                    (None, _) => {
                        let max_order = self
                            .columns
                            .iter()
                            .filter_map(|c| c.sort_order)
                            .max()
                            .unwrap_or(0);
                        self.columns[real_idx].sort_order = Some(max_order + 1);
                        self.columns[real_idx].sort_descending = false;
                    }
                    (Some(_), false) => {
                        self.columns[real_idx].sort_descending = true;
                    }
                    (Some(_), true) => self.unsort_index(real_idx),
                }
                self.has_unapplied_changes = true;
            }
        }
    }

    /// ← on a column: the cycle of Space backwards, none → descending → ascending →
    /// none.
    pub fn cycle_sort_back(&mut self) {
        let Some(idx) = self.table_state.selected() else {
            return;
        };
        let Some(&(real_idx, _)) = self.filtered_columns().get(idx) else {
            return;
        };
        let col = &self.columns[real_idx];
        match (col.sort_order, col.sort_descending) {
            (None, _) => {
                let max_order = self.columns.iter().filter_map(|c| c.sort_order).max();
                self.columns[real_idx].sort_order = Some(max_order.unwrap_or(0) + 1);
                self.columns[real_idx].sort_descending = true;
            }
            (Some(_), true) => self.columns[real_idx].sort_descending = false,
            (Some(_), false) => self.unsort_index(real_idx),
        }
        self.has_unapplied_changes = true;
    }

    /// The sort, as indices into `columns`, first key first.
    pub fn sort_entries(&self) -> Vec<usize> {
        let mut entries: Vec<(usize, usize)> = self
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.sort_order.map(|o| (o, i)))
            .collect();
        entries.sort_unstable();
        entries.into_iter().map(|(_, i)| i).collect()
    }

    /// Flip the direction of the sort's `entry`th key.
    pub fn flip_sort(&mut self, entry: usize) {
        if let Some(&i) = self.sort_entries().get(entry) {
            self.columns[i].sort_descending = !self.columns[i].sort_descending;
            self.has_unapplied_changes = true;
        }
    }

    /// Drop the sort's `entry`th key; the keys after it move up.
    pub fn remove_sort_entry(&mut self, entry: usize) {
        if let Some(&i) = self.sort_entries().get(entry) {
            self.unsort_index(i);
            self.has_unapplied_changes = true;
        }
    }

    /// Move the sort's `entry`th key one place earlier or later. Returns where it is
    /// now.
    pub fn move_sort_entry(&mut self, entry: usize, earlier: bool) -> usize {
        let entries = self.sort_entries();
        let to = if earlier {
            entry.checked_sub(1)
        } else {
            Some(entry + 1).filter(|to| *to < entries.len())
        };
        let (Some(to), Some(&from_i)) = (to, entries.get(entry)) else {
            return entry;
        };
        let to_i = entries[to];
        let a = self.columns[from_i].sort_order;
        self.columns[from_i].sort_order = self.columns[to_i].sort_order;
        self.columns[to_i].sort_order = a;
        self.has_unapplied_changes = true;
        to
    }

    /// Add the column named `name` as the sort's last key, ascending. Returns its
    /// place in the sort; `None` when no column has that name. A column already
    /// sorted keeps its place.
    pub fn add_sort(&mut self, name: &str) -> Option<usize> {
        let i = self.columns.iter().position(|c| c.name == name)?;
        if self.columns[i].sort_order.is_none() {
            let max_order = self.columns.iter().filter_map(|c| c.sort_order).max();
            self.columns[i].sort_order = Some(max_order.unwrap_or(0) + 1);
            self.columns[i].sort_descending = false;
            self.has_unapplied_changes = true;
        }
        self.sort_entries().iter().position(|&e| e == i)
    }

    /// Del on a column: drop it from the sort outright, wherever in the cycle
    /// it stands, and renumber the columns after it.
    pub fn remove_sort(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some((real_idx, _)) = filtered.get(idx) {
                let real_idx = *real_idx;
                if self.columns[real_idx].sort_order.is_some() {
                    self.unsort_index(real_idx);
                    self.has_unapplied_changes = true;
                }
            }
        }
    }

    fn unsort_index(&mut self, real_idx: usize) {
        let Some(old_order) = self.columns[real_idx].sort_order else {
            return;
        };
        self.columns[real_idx].sort_order = None;
        self.columns[real_idx].sort_descending = false;
        for col in &mut self.columns {
            if let Some(order) = col.sort_order
                && order > old_order
            {
                col.sort_order = Some(order - 1);
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
                            // Clear to-be-locked (never real locks) on the columns now between the new
                            // position (exclusive) and the old (inclusive).
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

                    // A locked or to-be-locked column moved down marks the unlocked columns it crossed
                    // (old position inclusive to new exclusive, not itself) as to-be-locked.
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
                if let Some(current_order) = self.columns[real_idx].sort_order
                    && current_order > 1
                {
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
                if let Some(current_order) = self.columns[real_idx].sort_order
                    && current_order < max_order
                {
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

    pub fn clear_selection(&mut self) {
        // Reset all column state: clear sorting, unlock all, reset display order
        for (idx, col) in self.columns.iter_mut().enumerate() {
            col.sort_order = None;
            col.sort_descending = false;
            col.is_locked = false;
            col.is_to_be_locked = false;
            col.display_order = idx; // Reset to natural order (0, 1, 2, ...)
            col.is_visible = true; // Make all columns visible
            col.width = WidthChoice::Auto;
        }
        self.has_unapplied_changes = true;
    }

    /// Change how the width of the column under the cursor is chosen, from what is
    /// staged: `<` and `>` step it, `f` fits it to the rows on screen, `w` returns
    /// it to automatic.
    pub fn change_width(&mut self, change: impl FnOnce(WidthChoice, Option<u16>) -> WidthChoice) {
        let Some(idx) = self.table_state.selected() else {
            return;
        };
        let Some(&(real_idx, _)) = self.filtered_columns().get(idx) else {
            return;
        };
        let col = &mut self.columns[real_idx];
        let width = change(col.width, col.shown_width);
        if width != col.width {
            col.width = width;
            self.has_unapplied_changes = true;
        }
    }

    /// Every column's width choice, as staged, for the table to apply.
    pub fn width_choices(&self) -> Vec<(String, WidthChoice)> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), c.width))
            .collect()
    }

    /// Hide or show the column under the cursor. Visibility only: the column keeps
    /// its place in the order and its lock, so showing it puts it back where it was.
    pub fn toggle_visibility(&mut self) {
        if let Some(idx) = self.table_state.selected() {
            let filtered = self.filtered_columns();
            if let Some(&(real_idx, _)) = filtered.get(idx) {
                let col = &mut self.columns[real_idx];
                col.is_visible = !col.is_visible;
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
                let old_order = self.columns[real_idx].sort_order;
                // A sorted column moves among the places already taken; an
                // unsorted one may also join at the end.
                let last = if old_order.is_some() {
                    max_order
                } else {
                    max_order + 1
                };

                if new_order > 0 && new_order <= last {
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
                    if old_order != Some(new_order) {
                        self.has_unapplied_changes = true;
                    }
                } else if new_order == 0 {
                    // User wants to unset sort order
                    if self.columns[real_idx].sort_order.take().is_some() {
                        self.has_unapplied_changes = true;
                    }
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
                } else {
                    // Past the end of the order: say so rather than doing nothing.
                    let range = if last == 1 {
                        "1".to_string()
                    } else {
                        format!("1-{last}")
                    };
                    self.status = Some(format!(
                        "Position {new_order} is past the end; use {range}."
                    ));
                }
            }
        }
    }
}

/// The sidebar's full order: `visible` as the table applies it, with each column of
/// `all` it leaves out (a hidden one) put back right after the column it followed in
/// `reference`, the order last applied, or in `all` when `reference` does not name
/// it. A hidden column with nothing before it goes first.
pub fn order_with_hidden(visible: &[String], all: &[String], reference: &[String]) -> Vec<String> {
    use std::collections::HashSet;
    let mut order = visible.to_vec();
    let mut placed: HashSet<&str> = visible.iter().map(String::as_str).collect();
    for name in all {
        if placed.contains(name.as_str()) {
            continue;
        }
        let earlier = match reference.iter().position(|r| r == name) {
            Some(i) => &reference[..i],
            None => &all[..all.iter().position(|a| a == name).unwrap_or(0)],
        };
        let at = earlier
            .iter()
            .rev()
            .find(|e| placed.contains(e.as_str()))
            .and_then(|e| order.iter().position(|o| o == e))
            .map_or(0, |p| p + 1);
        order.insert(at, name.clone());
        placed.insert(name.as_str());
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modal() -> SortModal {
        SortModal::default()
    }

    fn columns(names: &[&str]) -> Vec<SortColumn> {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| SortColumn {
                name: name.to_string(),
                sort_order: None,
                sort_descending: false,
                display_order: i,
                is_locked: false,
                is_to_be_locked: false,
                is_visible: true,
                width: WidthChoice::Auto,
                shown_width: Some(10),
            })
            .collect()
    }

    #[test]
    fn test_sort_modal_new() {
        let modal = modal();
        assert_eq!(modal.filter_input.value(), "");
        assert!(modal.columns.is_empty());
        assert!(modal.table_state.selected().is_none());
    }

    #[test]
    fn test_filtered_columns() {
        let mut modal = modal();
        modal.columns = columns(&["Apple", "Banana", "Orange"]);
        modal.filter_input.set_value("an");
        let filtered = modal.filtered_columns();
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].1.name, "Banana");
        assert_eq!(filtered[1].1.name, "Orange");
    }

    /// The list is worked out once per change of the find text, the names or the
    /// order, however often it is asked for.
    #[test]
    fn the_filtered_list_is_worked_out_once_per_change() {
        let builds = |m: &SortModal| m.shown_builds.load(std::sync::atomic::Ordering::Relaxed);
        let mut modal = modal();
        modal.columns = columns(&["Apple", "Banana", "Orange"]);
        for _ in 0..5 {
            modal.filtered_columns();
        }
        assert_eq!(builds(&modal), 1);
        modal.filter_input.set_value("an");
        modal.filtered_columns();
        modal.filtered_columns();
        assert_eq!(builds(&modal), 2);
        modal.columns[1].display_order = 9;
        let names: Vec<&str> = modal
            .filtered_columns()
            .iter()
            .map(|(_, c)| c.name.as_str())
            .collect();
        assert_eq!(names, ["Orange", "Banana"]);
        assert_eq!(builds(&modal), 3);
    }

    /// Space walks one column through none → ascending → descending → none,
    /// each column carrying its own direction.
    #[test]
    fn space_cycles_a_column_through_the_three_states() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B"]);
        modal.table_state.select(Some(0));

        modal.cycle_sort();
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert!(!modal.columns[0].sort_descending, "first press: ascending");

        modal.cycle_sort();
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert!(modal.columns[0].sort_descending, "second press: descending");

        modal.cycle_sort();
        assert_eq!(modal.columns[0].sort_order, None, "third press: out");
        assert!(!modal.columns[0].sort_descending);
    }

    /// A column leaving the sort renumbers the ones after it, whatever their
    /// directions, and the directions travel with their columns.
    #[test]
    fn leaving_the_sort_renumbers_and_keeps_directions() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B", "C"]);
        modal.table_state.select(Some(1)); // B ascending, order 1
        modal.cycle_sort();
        modal.table_state.select(Some(0)); // A order 2, then descending
        modal.cycle_sort();
        modal.cycle_sort();
        modal.table_state.select(Some(2)); // C order 3
        modal.cycle_sort();

        let (names, directions) = modal.sorted_columns_and_directions();
        assert_eq!(names, ["B", "A", "C"]);
        assert_eq!(directions, [false, true, false]);

        // B cycles out (asc → desc → none): A and C move up, directions intact.
        modal.table_state.select(Some(1));
        modal.cycle_sort();
        modal.cycle_sort();
        let (names, directions) = modal.sorted_columns_and_directions();
        assert_eq!(names, ["A", "C"]);
        assert_eq!(directions, [true, false]);
    }

    /// `0` takes a sorted column out and stages the change; on an unsorted
    /// column it changes nothing and stages nothing.
    #[test]
    fn zero_removes_a_column_and_stages_the_change() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B"]);
        modal.columns[0].sort_order = Some(1);
        modal.table_state.select(Some(1));
        modal.jump_selection_to_order(0);
        assert!(!modal.has_unapplied_changes, "B was not sorted");

        modal.table_state.select(Some(0));
        modal.jump_selection_to_order(0);
        assert_eq!(modal.columns[0].sort_order, None);
        assert!(modal.has_unapplied_changes);
    }

    /// A digit past the end of the order changes nothing and says which
    /// positions exist.
    #[test]
    fn a_digit_past_the_end_says_why() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B", "C"]);
        modal.columns[0].sort_order = Some(1);
        modal.table_state.select(Some(1));
        modal.jump_selection_to_order(5);
        assert_eq!(modal.columns[1].sort_order, None);
        assert!(!modal.has_unapplied_changes);
        assert_eq!(
            modal.status.as_deref(),
            Some("Position 5 is past the end; use 1-2.")
        );

        // The sorted column itself has only the positions already there, and
        // its own position is no change.
        modal.table_state.select(Some(0));
        modal.jump_selection_to_order(2);
        assert_eq!(
            modal.status.as_deref(),
            Some("Position 2 is past the end; use 1.")
        );
        modal.jump_selection_to_order(1);
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert!(!modal.has_unapplied_changes);
    }

    #[test]
    fn test_move_selection_up() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B"]);
        modal.columns[0].sort_order = Some(2);
        modal.columns[1].sort_order = Some(1);
        modal.table_state.select(Some(0)); // Select "A"
        modal.move_selection_up();
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert_eq!(modal.columns[1].sort_order, Some(2));
    }

    #[test]
    fn test_move_selection_down() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B"]);
        modal.columns[0].sort_order = Some(2);
        modal.columns[1].sort_order = Some(1);
        modal.table_state.select(Some(1)); // Select "B"
        modal.move_selection_down();
        assert_eq!(modal.columns[0].sort_order, Some(1));
        assert_eq!(modal.columns[1].sort_order, Some(2));
    }

    #[test]
    fn the_sort_list_flips_moves_and_drops_entries() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B", "C"]);
        assert_eq!(modal.add_sort("C"), Some(0));
        assert_eq!(modal.add_sort("A"), Some(1));
        assert_eq!(
            modal.add_sort("A"),
            Some(1),
            "a sorted column keeps its place"
        );
        assert_eq!(modal.get_sorted_columns(), ["C", "A"]);
        modal.flip_sort(1);
        assert_eq!(modal.sorted_columns_and_directions().1, [false, true]);
        assert_eq!(modal.move_sort_entry(1, true), 0);
        assert_eq!(modal.get_sorted_columns(), ["A", "C"]);
        assert_eq!(modal.move_sort_entry(0, true), 0, "the first stays first");
        modal.remove_sort_entry(0);
        assert_eq!(modal.get_sorted_columns(), ["C"]);
        assert_eq!(modal.columns[2].sort_order, Some(1), "renumbered");
    }

    #[test]
    fn the_sort_cycles_both_ways() {
        let mut modal = modal();
        modal.columns = columns(&["A"]);
        modal.table_state.select(Some(0));
        modal.cycle_sort_back();
        assert_eq!(modal.sorted_columns_and_directions().1, [true]);
        modal.cycle_sort_back();
        assert_eq!(modal.sorted_columns_and_directions().1, [false]);
        modal.cycle_sort_back();
        assert!(modal.get_sorted_columns().is_empty());
    }

    #[test]
    fn test_clear_selection() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B"]);
        modal.columns[0].sort_order = Some(1);
        modal.columns[0].sort_descending = true;
        modal.columns[1].sort_order = Some(2);
        modal.clear_selection();
        assert!(modal.columns[0].sort_order.is_none());
        assert!(!modal.columns[0].sort_descending);
        assert!(modal.columns[1].sort_order.is_none());
    }

    /// Narrower and wider step from the width drawn, fit and automatic stage as
    /// asked, and each change is staged rather than applied.
    #[test]
    fn width_changes_are_staged_on_the_column_under_the_cursor() {
        let mut modal = modal();
        modal.columns = columns(&["a", "b"]);
        modal.table_state.select(Some(1));
        modal.change_width(WidthChoice::wider);
        assert_eq!(modal.columns[1].width, WidthChoice::Manual(14));
        assert!(modal.has_unapplied_changes);
        modal.change_width(WidthChoice::narrower);
        modal.change_width(WidthChoice::narrower);
        assert_eq!(modal.columns[1].width, WidthChoice::Manual(6));
        modal.change_width(|_, _| WidthChoice::Fit);
        assert_eq!(modal.columns[1].width, WidthChoice::Fit);
        modal.change_width(|_, _| WidthChoice::Auto);
        assert_eq!(modal.columns[0].width, WidthChoice::Auto);
        assert_eq!(
            modal.width_choices(),
            vec![
                ("a".to_string(), WidthChoice::Auto),
                ("b".to_string(), WidthChoice::Auto)
            ]
        );
        modal.change_width(WidthChoice::wider);
        modal.clear_selection();
        assert_eq!(modal.columns[1].width, WidthChoice::Auto);
    }

    fn names(order: &[&str]) -> Vec<String> {
        order.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn hiding_and_showing_keeps_the_column_in_place() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B", "C", "D"]);
        modal.table_state.select(Some(1));
        modal.toggle_visibility();
        assert_eq!(modal.get_column_order(), names(&["A", "C", "D"]));
        // The list does not move under the cursor: the next row is still C.
        let listed: Vec<&str> = modal
            .filtered_columns()
            .iter()
            .map(|(_, c)| c.name.as_str())
            .collect();
        assert_eq!(listed, ["A", "B", "C", "D"]);
        modal.toggle_visibility();
        assert_eq!(modal.get_column_order(), names(&["A", "B", "C", "D"]));
    }

    #[test]
    fn a_hidden_column_keeps_its_lock_but_is_not_counted() {
        let mut modal = modal();
        modal.columns = columns(&["A", "B", "C", "D"]);
        modal.table_state.select(Some(2));
        modal.toggle_lock_at_column();
        assert_eq!(modal.get_locked_columns_count(), 3);
        modal.table_state.select(Some(1));
        modal.toggle_visibility();
        assert_eq!(modal.get_locked_columns_count(), 2, "A and C stay frozen");
        modal.toggle_visibility();
        assert_eq!(modal.get_locked_columns_count(), 3, "B is frozen again");
    }

    #[test]
    fn hidden_columns_go_back_after_the_column_they_followed() {
        let all = names(&["a", "b", "c", "d", "e"]);
        // Last applied as c, a, b, d, e with b and d hidden.
        let reference = names(&["c", "a", "b", "d", "e"]);
        assert_eq!(
            order_with_hidden(&names(&["c", "a", "e"]), &all, &reference),
            names(&["c", "a", "b", "d", "e"])
        );
        // Nothing applied from the sidebar: schema order places them.
        assert_eq!(
            order_with_hidden(&names(&["c", "e"]), &all, &[]),
            names(&["a", "b", "c", "d", "e"])
        );
    }
}
