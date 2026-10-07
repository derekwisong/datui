//! Moving through the table: the row cursor, the column cursor, sideways paging,
//! frozen columns and widths.

use super::*;

impl DataTableState {
    /// Returns true if a buffer collect is needed after the scroll.
    pub fn select_next(&mut self) -> bool {
        self.table_state.select_next();
        if let Some(selected) = self.table_state.selected()
            && selected >= self.visible_rows
            && self.visible_rows > 0
        {
            return self.slide_table(1);
        }
        false
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn page_down(&mut self) -> bool {
        self.slide_table(self.visible_rows as i64)
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn select_previous(&mut self) -> bool {
        if let Some(selected) = self.table_state.selected() {
            self.table_state.select_previous();
            if selected == 0 && self.view.start_row > 0 {
                return self.slide_table(-1);
            }
        } else {
            self.table_state.select(Some(0));
        }
        false
    }

    /// Returns true if a buffer collect is needed.
    pub fn scroll_to(&mut self, index: usize) -> bool {
        if self.view.start_row == index {
            return false;
        }
        self.view.start_row = index;
        true // caller must collect
    }

    /// Set scroll position for go-to-line (centered). Returns true if a collect is needed.
    pub fn scroll_to_row_centered(&mut self, row_index: usize) -> bool {
        if self.view.num_rows == 0 || self.visible_rows == 0 {
            return false;
        }
        let center_offset = self.visible_rows / 2;
        let mut start_row = row_index.saturating_sub(center_offset);
        let max_start = self.view.num_rows.saturating_sub(self.visible_rows);
        start_row = start_row.min(max_start);

        if self.view.start_row == start_row {
            let display_idx = row_index
                .saturating_sub(start_row)
                .min(self.visible_rows.saturating_sub(1));
            self.table_state.select(Some(display_idx));
            return false;
        }

        self.view.start_row = start_row;
        let display_idx = row_index
            .saturating_sub(start_row)
            .min(self.visible_rows.saturating_sub(1));
        self.table_state.select(Some(display_idx));
        true // caller must collect
    }

    /// Jump to the first page. Returns true if a collect is needed.
    pub fn scroll_to_start(&mut self) -> bool {
        self.table_state.select(Some(0));
        self.scroll_to(0)
    }

    /// Jump to the last page. Returns true if a collect is needed.
    pub fn scroll_to_end(&mut self) -> bool {
        if self.view.num_rows == 0 {
            self.view.start_row = 0;
            self.view.buffered_start_row = 0;
            self.view.buffered_end_row = 0;
            return false;
        }
        let end_start = self.view.num_rows.saturating_sub(self.visible_rows);
        if self.view.start_row == end_start {
            self.select_last_visible_row();
            return false;
        }
        self.view.start_row = end_start;
        self.select_last_visible_row();
        true // caller must collect
    }

    /// Set table selection to the last row in the current view (for use after scroll_to_end).
    fn select_last_visible_row(&mut self) {
        if self.view.num_rows == 0 {
            return;
        }
        let last_row_display_idx = (self.view.num_rows - 1).saturating_sub(self.view.start_row);
        let sel = last_row_display_idx.min(self.visible_rows.saturating_sub(1));
        self.table_state.select(Some(sel));
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn half_page_down(&mut self) -> bool {
        let half = (self.visible_rows / 2).max(1) as i64;
        self.slide_table(half)
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn half_page_up(&mut self) -> bool {
        if self.view.start_row == 0 {
            return false;
        }
        let half = (self.visible_rows / 2).max(1) as i64;
        self.slide_table(-half)
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn page_up(&mut self) -> bool {
        if self.view.start_row == 0 {
            return false;
        }
        self.slide_table(-(self.visible_rows as i64))
    }

    pub fn scroll_right(&mut self) {
        self.scroll_columns(ColumnMove::StepRight);
    }

    pub fn scroll_left(&mut self) {
        self.scroll_columns(ColumnMove::StepLeft);
    }

    /// Which shown columns the table last drew, and the cursor's: what the control
    /// bar's column position says.
    pub fn columns_on_screen(&self) -> Option<OnScreen> {
        self.on_screen
    }

    /// How many columns scroll: the shown ones right of those drawn frozen.
    fn scroll_count(&self) -> usize {
        self.view
            .column_order
            .len()
            .saturating_sub(self.frozen_shown())
    }

    /// Move the view sideways, leaving the cursor where it is unless the view leaves
    /// it behind on the left. Planned from the widths the columns were last drawn at;
    /// reads nothing. A page that needs a column not drawn yet waits for the next
    /// draw, which measures it from the rows on hand; a relative move typed behind it
    /// waits too and lands after it, in order, so no key is lost or planned on a guess.
    pub(super) fn scroll_columns(&mut self, mv: ColumnMove) {
        if matches!(
            mv,
            ColumnMove::First | ColumnMove::Last | ColumnMove::Reveal(_)
        ) {
            // Where these go does not depend on where the moves before them went.
            self.column_moves.clear();
        }
        if self.column_moves.is_empty()
            && let Some(start) = self.plan_known(mv)
        {
            self.apply_column_move(mv, start);
        } else {
            self.wait(WaitingMove::View(mv));
        }
    }

    /// Move the column cursor (`h` `l` `[` `]` `{` `}`), the view following only when
    /// the cursor would leave the screen. Reads nothing; a move that needs a column
    /// not drawn yet waits for the next draw, in order, as `Self::scroll_columns`
    /// says.
    pub fn move_cursor(&mut self, mv: CursorMove) {
        if matches!(mv, CursorMove::First | CursorMove::Last) {
            self.column_moves.clear();
        }
        if !self.column_moves.is_empty()
            || !self.land_cursor_move(mv, &mut |state: &mut Self, view| state.plan_known(view))
        {
            self.wait(WaitingMove::Cursor(mv));
        }
    }

    /// Put the cursor on the shown column `name` and show it as `g` does: left where
    /// it is when already whole on screen, else first after the frozen columns, or on
    /// the last page when it is there. A frozen column is on screen already.
    pub fn go_to_column(&mut self, name: &str) {
        let Some(at) = self.view.column_order.iter().position(|c| c == name) else {
            return;
        };
        self.column_moves.clear();
        self.place_cursor_at(at);
        if let Some(index) = at.checked_sub(self.frozen_shown()) {
            self.scroll_columns(ColumnMove::Reveal(index));
        }
    }

    /// Put the cursor on the shown column `name`, scrolling as little as it takes to
    /// show it.
    pub fn set_current_column(&mut self, name: &str) {
        let Some(at) = self.view.column_order.iter().position(|c| c == name) else {
            return;
        };
        self.column_moves.clear();
        self.place_cursor_at(at);
        self.follow_cursor(&mut |state: &mut Self, view| state.plan_known(view));
    }

    /// The column cursor's column: the one the per-column keys act on (value counts,
    /// copying a cell, the sidebar and inspector opening on it, a find in one column).
    /// The first shown column until the cursor moves; `None` with no columns shown.
    pub fn current_column(&self) -> Option<&str> {
        self.cursor_index()
            .map(|at| self.view.column_order[at].as_str())
    }

    /// The cursor's place among the shown columns, from 0, frozen ones first.
    pub fn current_column_index(&self) -> Option<usize> {
        self.cursor_index()
    }

    pub(crate) fn cursor_index(&self) -> Option<usize> {
        let last = self.view.column_order.len().checked_sub(1)?;
        Some(
            self.view
                .cursor_column
                .as_deref()
                .and_then(|name| self.view.column_order.iter().position(|c| c == name))
                .unwrap_or(self.view.cursor_at.min(last)),
        )
    }

    pub(super) fn place_cursor_at(&mut self, at: usize) {
        self.view.cursor_column = self.view.column_order.get(at).cloned();
        self.view.cursor_at = at;
    }

    /// After the shown columns changed: the cursor stays on its column by name, or,
    /// where that was hidden, takes the one now in its place; the next draw shows it.
    pub(super) fn settle_cursor(&mut self) {
        let at = self.cursor_index().unwrap_or(0);
        self.place_cursor_at(at);
        self.reveal_cursor = true;
    }

    /// Queue a move for the next draw, behind any already waiting.
    fn wait(&mut self, mv: WaitingMove) {
        if self.column_moves.len() < MAX_WAITING_MOVES {
            self.column_moves.push(mv);
        }
    }

    /// Scroll as little as it takes to show the cursor's column whole, with `plan`;
    /// a plan that needs a width not drawn yet waits for the next draw.
    fn follow_cursor(&mut self, plan: &mut impl FnMut(&mut Self, ColumnMove) -> Option<usize>) {
        let Some(index) = self
            .cursor_index()
            .and_then(|at| at.checked_sub(self.frozen_shown()))
        else {
            return;
        };
        let view = ColumnMove::Keep(index);
        match plan(self, view) {
            // On screen already: nothing moves, and the trail `[` retraces stays.
            Some(start) if start == self.termcol_index => {}
            Some(start) => self.apply_column_move(view, start),
            None => self.wait(WaitingMove::View(view)),
        }
    }

    /// Land a cursor move, the view planned with `plan`. Returns false, changing
    /// nothing, when a page cannot be planned yet: where it lands decides the cursor.
    fn land_cursor_move(
        &mut self,
        mv: CursorMove,
        plan: &mut impl FnMut(&mut Self, ColumnMove) -> Option<usize>,
    ) -> bool {
        let Some(cursor) = self.cursor_index() else {
            return true;
        };
        let last = self.view.column_order.len() - 1;
        let frozen = self.frozen_shown();
        match mv {
            CursorMove::Left | CursorMove::Right => {
                let at = if mv == CursorMove::Left {
                    cursor.saturating_sub(1)
                } else {
                    (cursor + 1).min(last)
                };
                self.place_cursor_at(at);
                self.follow_cursor(plan);
            }
            CursorMove::First | CursorMove::Last => {
                let (at, view) = if mv == CursorMove::First {
                    (0, ColumnMove::First)
                } else {
                    (last, ColumnMove::Last)
                };
                self.place_cursor_at(at);
                match plan(self, view) {
                    Some(start) => self.apply_column_move(view, start),
                    None => self.wait(WaitingMove::View(view)),
                }
            }
            CursorMove::PageLeft | CursorMove::PageRight => {
                let view = if mv == CursorMove::PageLeft {
                    ColumnMove::PageLeft
                } else {
                    ColumnMove::PageRight
                };
                let Some(start) = plan(self, view) else {
                    return false;
                };
                let from = self.termcol_index;
                self.apply_column_move(view, start);
                let at = if self.termcol_index != from {
                    // The new page, from its first column.
                    frozen + self.termcol_index
                } else if mv == CursorMove::PageRight {
                    // On the last page already: its last column.
                    last
                } else if cursor > frozen {
                    // On the first page: its first column, then the first of all.
                    frozen
                } else {
                    0
                };
                self.place_cursor_at(at.min(last));
            }
        }
        true
    }

    /// The scrolling columns, by name.
    pub(super) fn scrolling_names(&self) -> &[String] {
        &self.view.column_order[self.frozen_shown().min(self.view.column_order.len())..]
    }

    /// `[` straight after the `]` that came here goes back where that one started,
    /// whatever the widths say, so a page and back is the page left.
    fn retrace(&self, mv: ColumnMove) -> Option<usize> {
        let &(back, to) = self.page_trail.last()?;
        (mv == ColumnMove::PageLeft && to == self.termcol_index).then_some(back)
    }

    /// Where `mv` lands on the widths drawn in this view, or `None` when it needs one
    /// not drawn yet (or the room, before the first draw).
    fn plan_known(&self, mv: ColumnMove) -> Option<usize> {
        if let Some(back) = self.retrace(mv) {
            return Some(back);
        }
        let needs_widths = match mv {
            ColumnMove::StepLeft | ColumnMove::StepRight | ColumnMove::First => false,
            // Back to a column at or left of the first shown needs no width.
            ColumnMove::Keep(column) => column > self.termcol_index,
            _ => true,
        };
        let room = match self.scroll_room {
            Some(room) => room,
            None if needs_widths => return None,
            None => Room::default(),
        };
        let names = self.scrolling_names();
        crate::widgets::column_paging::plan(mv, self.termcol_index, names.len(), room, |i| {
            self.drawn_width(&names[i])
        })
    }

    /// Land `mv` at `start`, keeping the trail `[` retraces. A cursor the view leaves
    /// behind on the left comes along, to the first column shown.
    fn apply_column_move(&mut self, mv: ColumnMove, start: usize) {
        let from = self.termcol_index;
        let start = start.min(self.scroll_count().saturating_sub(1));
        match mv {
            ColumnMove::PageRight => {
                if start > from {
                    self.page_trail.push((from, start));
                }
            }
            ColumnMove::PageLeft if self.retrace(mv) == Some(start) => {
                self.page_trail.pop();
            }
            _ => self.page_trail.clear(),
        }
        self.scroll_columns_to(start);
        let frozen = self.frozen_shown();
        if let Some(cursor) = self.cursor_index()
            && cursor >= frozen
            && cursor < frozen + self.termcol_index
        {
            self.place_cursor_at(frozen + self.termcol_index);
        }
    }

    /// Forget sideways moves waiting on a draw and the trail `[` retraces: the
    /// columns they were counted over are gone.
    pub(super) fn clear_column_moves(&mut self) {
        self.column_moves.clear();
        self.page_trail.clear();
    }

    /// Start the scrolling columns at `start`, re-slicing the buffer held.
    fn scroll_columns_to(&mut self, start: usize) {
        let start = start.min(self.scroll_count().saturating_sub(1));
        if start != self.termcol_index {
            self.termcol_index = start;
            self.rescroll_columns();
        }
    }

    /// Record the scrolling side as the renderer lays it out, land the moves waiting
    /// on it, in order, and bring the cursor back on screen when it may have left,
    /// with `width`, which measures a column not drawn yet from the rows on hand.
    /// Called while drawing, before the scrolling columns are drawn; reads nothing,
    /// and measures only the columns a move crosses. With no rows on hand the moves
    /// wait for a draw that has them.
    pub(crate) fn land_column_moves(
        &mut self,
        room: Room,
        mut width: impl FnMut(&mut Self, &str) -> u16,
    ) {
        if self.scroll_room != Some(room) {
            // A resize, or a frozen column given back: the cursor may be off screen.
            self.reveal_cursor = true;
        }
        self.scroll_room = Some(room);
        if (self.column_moves.is_empty() && !self.reveal_cursor)
            || !self.buffer_on_hand()
            || self.defer_collect
        {
            return;
        }
        let mut plan = |state: &mut Self, mv: ColumnMove| -> Option<usize> {
            if let Some(back) = state.retrace(mv) {
                return Some(back);
            }
            let from = state.termcol_index;
            let count = state.scroll_count();
            Some(
                crate::widgets::column_paging::plan(mv, from, count, room, |i| {
                    let name = state.scrolling_names()[i].clone();
                    Some(width(state, &name))
                })
                .unwrap_or(from),
            )
        };
        for mv in std::mem::take(&mut self.column_moves) {
            match mv {
                WaitingMove::View(mv) => {
                    let start = plan(self, mv).unwrap_or(self.termcol_index);
                    self.apply_column_move(mv, start);
                }
                WaitingMove::Cursor(mv) => {
                    self.land_cursor_move(mv, &mut plan);
                }
            }
        }
        if std::mem::take(&mut self.reveal_cursor) {
            self.follow_cursor(&mut plan);
        }
    }

    /// Show the new column window, reading nothing.
    ///
    /// A sideways move changes which columns are on screen, not which rows, so it
    /// re-slices the buffer already held. It must not go through [`collect`], which
    /// counts the rows when the count is not yet known: `App::handle` calls
    /// `scroll_right` inline on the thread that draws and reads keys, and
    /// `key_acts_while_busy` lets Left and Right through while other work runs. On a
    /// staged-open cloud hive that count is a metadata read per object, and taken
    /// there it is a freeze no keystroke can interrupt.
    ///
    /// Nothing is drawn when there is no buffer to re-slice, or when what is held
    /// does not match the range it claims. [`collect`] reloaded the page in that
    /// second case; this does not, because `load_buffer` is a collect of that page
    /// and on a cloud hive that is row groups over the wire — the same freeze in a
    /// smaller size.
    ///
    /// The index still moves, so presses before the first buffer lands are spent on
    /// a view that cannot show them yet, and the first frame drawn is already scrolled
    /// to wherever they left it. That is the pre-existing behaviour: the old path
    /// redrew each press, but only by paying the wait this exists to avoid.
    ///
    /// [`collect`]: Self::collect
    fn rescroll_columns(&mut self) {
        if self.defer_collect || !self.buffer_on_hand() {
            return;
        }
        self.slice_buffer_into_display();
        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
    }

    pub fn headers(&self) -> Vec<String> {
        self.view.column_order.clone()
    }

    pub fn set_column_order(&mut self, order: Vec<String>) {
        self.view.column_order = order;
        self.clear_column_moves();
        // Fewer columns shown may leave the scroll past the last; keep one on screen.
        self.termcol_index = self
            .termcol_index
            .min(self.scroll_count().saturating_sub(1));
        self.drop_buffer();
        self.settle_cursor();
        self.collect();
    }

    pub fn set_locked_columns(&mut self, count: usize) {
        self.view.locked_columns_count = count.min(self.view.column_order.len());
        self.clear_column_moves();
        self.settle_cursor();
        self.termcol_index = self
            .termcol_index
            .min(self.scroll_count().saturating_sub(1));
        self.drop_buffer();
        self.collect();
    }

    pub fn locked_columns_count(&self) -> usize {
        self.view.locked_columns_count
    }

    /// How many columns are drawn frozen: the count asked for, or fewer while the
    /// last layout could not fit them all beside a usable scrolling column. The ones
    /// left out lead the scrolling columns, so every column stays reachable.
    pub fn frozen_shown(&self) -> usize {
        let (asked, shown) = self.view.frozen_fit;
        if asked == self.view.locked_columns_count {
            shown.min(asked)
        } else {
            self.view.locked_columns_count
        }
    }

    /// Take the layout's word for how many frozen columns fit, and re-slice the
    /// scrolling columns to start after them. Unscrolled, the frozen columns left out
    /// lead the scrolling ones; scrolled, the column the scroll started at stays
    /// first where it can, so a resize does not also move the view. Reads nothing;
    /// called while drawing, and only re-selects columns of the buffer already held.
    pub(crate) fn fit_frozen(&mut self, shown: usize) {
        let before = self.frozen_shown();
        let shown = shown.min(self.view.locked_columns_count);
        if shown == before {
            self.view.frozen_fit = (self.view.locked_columns_count, shown);
            return;
        }
        if self.defer_collect || !self.buffer_on_hand() {
            return;
        }
        self.view.frozen_fit = (self.view.locked_columns_count, shown);
        // The scrolling indices the trail was kept in shift with the frozen count.
        self.page_trail.clear();
        if self.termcol_index > 0 {
            let first = before + self.termcol_index;
            let last = self.view.column_order.len().saturating_sub(1);
            self.termcol_index = first.min(last).saturating_sub(shown);
        }
        self.slice_buffer_into_display();
    }

    /// The type a column has in the frame on screen: with its name, the identity its
    /// width is kept under.
    pub(crate) fn width_dtype(&self, name: &str) -> DataType {
        self.view
            .schema
            .get(name)
            .cloned()
            .unwrap_or(DataType::Null)
    }

    /// How a column's width is chosen.
    pub fn width_choice(&self, name: &str) -> WidthChoice {
        self.widths.choice(name, &self.width_dtype(name))
    }

    /// The width a column was last drawn at, if it has been drawn.
    pub fn shown_width(&self, name: &str) -> Option<u16> {
        self.widths.shown(name, &self.width_dtype(name))
    }

    /// The width the column takes on screen, the room it filled at the right edge
    /// included.
    pub fn on_screen_width(&self, name: &str) -> Option<u16> {
        self.widths.on_screen(name, &self.width_dtype(name))
    }

    /// The width a column draws at in this view, if it has been drawn since the
    /// widths were last relearned. What a sideways page is planned with.
    pub(crate) fn drawn_width(&self, name: &str) -> Option<u16> {
        self.widths.drawn(name, &self.width_dtype(name))
    }

    /// Set how each named column's width is chosen. Reads nothing: a fit is taken
    /// from the rows on screen when the table is next drawn.
    pub fn set_width_choices(&mut self, choices: impl IntoIterator<Item = (String, WidthChoice)>) {
        for (name, choice) in choices {
            let dtype = self.width_dtype(&name);
            self.widths.set_choice(&name, &dtype, choice);
        }
    }

    /// One column's rows on screen, from the buffer already held, as the table draws
    /// them. For fitting a column that may be scrolled out of view.
    pub(crate) fn page_column(&self, name: &str, offset: usize, len: usize) -> Option<DataFrame> {
        let column = self.view.buffered_df.as_ref()?.select([name]).ok()?;
        visible_slice(&column, offset, len)
    }
}
