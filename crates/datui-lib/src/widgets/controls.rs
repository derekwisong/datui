use crate::render::context::RenderContext;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Paragraph, Widget},
};

pub struct Controls {
    pub row_count: Option<usize>,
    /// True when `q` pops to the home screen instead of quitting; the bar says so.
    pub q_pops: bool,
    /// True when Enter drills into the row's group (a `by` view, a SQL GROUP BY);
    /// the first chip then names the drill rather than the inspector.
    pub enter_drills: bool,
    /// The dataset's full row count, when `row_count` is a filtered or queried subset
    /// of it. The bar then reads "417 of 1,000" instead of a bare number that
    /// hides the filter. Only ever a count something already resolved.
    pub total_row_count: Option<usize>,
    pub dimmed: bool,
    pub query_active: bool,
    pub custom_controls: Option<Vec<(&'static str, &'static str)>>,
    pub bg_color: Color,
    pub key_color: Color,   // Color for keybind hints (keys in toolbar)
    pub label_color: Color, // Color for action labels
    /// Text colour inside a key chip; the bar's background, so the chip reads as a cut-out.
    pub chip_text_color: Color,
    pub throbber_color: Color,
    pub use_unicode_throbber: bool, // When true, use 8-dot braille spinner (4 rows tall); else |/-\
    pub busy: bool,                 // When true, show throbber at far right
    pub throbber_frame: u8,         // Spinner frame (0..3 or 0..7 for unicode)
    /// When Some, replaces keybindings with spinner + message.
    pub status_message: Option<String>,
    /// A completion flash: one plain sentence in the gap between the chips and
    /// the row count. Drawn only in keybinding mode, so a busy status message
    /// always wins; appearing and expiring move nothing around it.
    pub flash: Option<String>,
    /// See `with_notes_pending`.
    pub notes_pending: bool,
    pub row_count_pending: bool, // When true, the exact count is still being determined: show a spinner in place of the (provisional, possibly inaccurate) number
    pub row_count_unknown: bool, // When true, the count could not be determined: show "?" instead of a misleading provisional number (takes effect only when not pending)
    /// Replaces the row count entirely, for views that are not showing a table.
    pub caption: Option<String>,
    /// Set to the lake format's name when the table on screen is a lake table's plain
    /// files rather than the table: `"Delta"`, `"Iceberg"`, `"Hudi"`.
    ///
    /// Drawn as a chip beside the row count, which is the number it is about — the
    /// files hold rows a delete tombstoned, versions an update replaced and both sides
    /// of a compaction, so the count is a true count of the files and a wrong one of
    /// the table. A note in the panel says the same at length; this is the half that
    /// cannot be missed, and the read is only defensible because both are there.
    pub not_the_table: Option<&'static str>,
    /// `"pivoted"` or `"melted"` while a reshape is standing between the data
    /// and the table on screen. Drawn as a chip beside the row count, which is
    /// the number it changed; `R` takes it away. Rule 6: a view mutated with
    /// nothing on screen saying so is unfinished.
    pub reshaped: Option<&'static str>,
    /// The column cursor's place among the shown columns. Drawn before the row
    /// count: `col 43 of 300`, or `col 43/300` on a narrow bar.
    pub columns: Option<crate::widgets::column_paging::OnScreen>,
    /// The find in effect: its pattern, and which match the cursor is on when that is
    /// known. Plain text beside the chips, since it is state, not an alarm.
    pub find: Option<String>,
    /// The chips the last render drew and where, for a click: see [`Self::drawn_chips`].
    drawn: std::cell::RefCell<Vec<(Rect, &'static str)>>,
    /// The table was read through a format spec: `b` picks another, and the bar
    /// offers it.
    pub format_key: bool,
    /// How many specs matched the file as well as the one it was read with. Drawn as
    /// a chip beside the row count: the file was read one of several ways.
    pub formats_tied: Option<usize>,
    /// The follow of the file on screen: a chip beside the count it moves.
    pub follow: Option<FollowMark>,
    pub warning_color: Color,
}

/// What the bar says about a followed file.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FollowMark {
    /// What `t` does on this screen, offered first among its keys.
    pub key: Option<&'static str>,
    /// The chip: `following · 3s ago`, `paused`.
    pub chip: String,
    /// Plain beside it: rows that came in below the cursor, or that wait to be shown.
    pub note: Option<String>,
    /// In the warning color: rows that did not fit the schema.
    pub warning: Option<String>,
}

impl Controls {
    /// The chips the bar was drawn with, each with where it landed. Empty before it
    /// is rendered, and while a flash has the row.
    pub fn drawn_chips(&self) -> Vec<(Rect, &'static str)> {
        self.drawn.take()
    }

    pub fn with_busy(mut self, busy: bool, throbber_frame: u8) -> Self {
        self.busy = busy;
        self.throbber_frame = throbber_frame;
        self
    }

    pub fn with_dimmed(mut self, dimmed: bool) -> Self {
        self.dimmed = dimmed;
        self
    }

    pub fn with_query_active(mut self, query_active: bool) -> Self {
        self.query_active = query_active;
        self
    }

    pub fn with_custom_controls(mut self, controls: Vec<(&'static str, &'static str)>) -> Self {
        self.custom_controls = Some(controls);
        self
    }

    pub fn with_colors(
        mut self,
        bg_color: Color,
        key_color: Color,
        label_color: Color,
        throbber_color: Color,
    ) -> Self {
        self.bg_color = bg_color;
        self.key_color = key_color;
        self.label_color = label_color;
        self.throbber_color = throbber_color;
        self
    }

    pub fn with_unicode_throbber(mut self, use_unicode: bool) -> Self {
        self.use_unicode_throbber = use_unicode;
        self
    }

    /// Give the Info key a quiet accent: datui has noticed something about the data
    /// and the panel has not been opened since. Never a count, never a color that
    /// reads as a warning — a note is an observation, not a fault.
    pub fn with_notes_pending(mut self, pending: bool) -> Self {
        self.notes_pending = pending;
        self
    }

    pub fn with_status_message(mut self, message: Option<String>) -> Self {
        self.status_message = message;
        self
    }

    pub fn with_flash(mut self, flash: Option<String>) -> Self {
        self.flash = flash;
        self
    }

    pub fn with_row_count_pending(mut self, pending: bool) -> Self {
        self.row_count_pending = pending;
        self
    }

    /// Replace the trailing row count with a caption of the view's own.
    ///
    /// "0 rows" is the table's counter; on a screen that is not showing a table it
    /// is at best meaningless and at worst looks like an empty dataset.
    pub fn with_caption(mut self, caption: Option<String>) -> Self {
        self.caption = caption;
        self
    }

    pub fn with_row_count_unknown(mut self, unknown: bool) -> Self {
        self.row_count_unknown = unknown;
        self
    }

    pub fn with_q_pops(mut self, q_pops: bool) -> Self {
        self.q_pops = q_pops;
        self
    }

    pub fn with_enter_drills(mut self, enter_drills: bool) -> Self {
        self.enter_drills = enter_drills;
        self
    }

    /// Set the full dataset count beside a filtered view's. See [`Self::total_row_count`].
    pub fn with_total_row_count(mut self, total: Option<usize>) -> Self {
        self.total_row_count = total;
        self
    }

    /// Say, beside the row count, that these are a lake table's files and not the
    /// table. See [`Self::not_the_table`].
    pub fn with_reshaped(mut self, reshaped: Option<&'static str>) -> Self {
        self.reshaped = reshaped;
        self
    }

    /// Say which columns are on screen. See [`Self::columns`].
    pub fn with_columns(
        mut self,
        columns: Option<crate::widgets::column_paging::OnScreen>,
    ) -> Self {
        self.columns = columns;
        self
    }

    /// Say which find is in effect. See [`Self::find`].
    pub fn with_find(mut self, find: Option<String>) -> Self {
        self.find = find;
        self
    }

    /// Offer `b`, and say how many other specs matched. See [`Self::format_key`].
    pub fn with_format(mut self, read: bool, tied: Option<usize>) -> Self {
        self.format_key = read;
        self.formats_tied = tied.filter(|n| *n > 0);
        self
    }

    pub fn with_not_the_table(mut self, format: Option<&'static str>) -> Self {
        self.not_the_table = format;
        self
    }

    pub fn with_follow(mut self, follow: Option<FollowMark>) -> Self {
        self.follow = follow;
        self
    }

    /// Create Controls from RenderContext (Phase 2+).
    /// This is the preferred way to create Controls with proper theming.
    pub fn from_context(row_count: usize, ctx: &RenderContext) -> Self {
        Self {
            caption: None,
            row_count: Some(row_count),
            q_pops: false,
            enter_drills: false,
            total_row_count: None,
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color: ctx.controls_bg,
            key_color: ctx.keybind_hints,
            label_color: ctx.keybind_labels,
            chip_text_color: ctx.text_inverse,
            throbber_color: ctx.throbber,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            flash: None,
            notes_pending: false,
            row_count_pending: false,
            reshaped: None,
            row_count_unknown: false,
            not_the_table: None,
            columns: None,
            find: None,
            drawn: Default::default(),
            format_key: false,
            formats_tied: None,
            follow: None,
            warning_color: ctx.warning,
        }
    }
}

impl Widget for &Controls {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let no_bg = self.bg_color == Color::Reset;
        if !no_bg {
            Block::default()
                .style(Style::default().bg(self.bg_color))
                .render(area, buf);
        }

        // Throbber frames come from the glyph sets, so they match the rest of the UI.
        let throbber_frames = if self.use_unicode_throbber {
            crate::glyphs::unicode().spinner
        } else {
            crate::glyphs::ascii().spinner
        };

        let throbber_ch = || -> String {
            if self.busy {
                throbber_frames[self.throbber_frame as usize % throbber_frames.len()].to_string()
            } else {
                " ".to_string()
            }
        };

        // Spinner frame independent of `busy` — used for the row-count placeholder while the
        // exact total is still being determined (that background count does not set `busy`).
        let spinner_ch =
            || -> &str { throbber_frames[self.throbber_frame as usize % throbber_frames.len()] };

        // Row-count text: while the count is pending a spinner stands in for the number, and if
        // the count couldn't be determined a "?" is shown — so the user never mistakes an
        // incomplete partial total for the final figure. A settled count that is a subset of a
        // known total says so ("417,321 of 1.2M"): the count on screen stays exact, the
        // universe abbreviates with the same formatter the home screen uses, and the exact
        // total is one `i` away in the Info panel.
        let rows_text = |count: usize| -> String {
            if self.row_count_pending {
                format!("{} rows", spinner_ch())
            } else if self.row_count_unknown {
                "? rows".to_string()
            } else if let Some(total) = self.total_row_count.filter(|&total| total != count) {
                format!(
                    "{} of {}",
                    crate::numfmt::group_chrome(count),
                    crate::discover::format_rows(total)
                )
            } else {
                format!("{} rows", crate::discover::format_rows(count))
            }
        };

        // The column cursor's place leads the count; a bar short of room takes the
        // compact form, so the keys keep theirs.
        let compact = area.width < 100;
        let columns_on = self.columns.filter(|_| self.caption.is_none());
        let with_gap = |label: String| {
            let gap = if compact { " " } else { "  " };
            format!("{label}{gap}{}{gap}", crate::glyphs::get().middot)
        };
        let columns = columns_on.map(|on| with_gap(on.label(compact)));
        let row_count_text = |count: usize| -> String {
            if let Some(caption) = &self.caption {
                return caption.clone();
            }
            let columns = columns.as_deref().unwrap_or("");
            format!("{columns}{}", rows_text(count))
        };
        // The room the position is given is its widest for this table, so moving the
        // cursor changes the number and never which chips fit beside it.
        let columns_room = columns_on
            .map(|on| {
                let widest = with_gap(on.widest_label(compact)).chars().count();
                widest.saturating_sub(columns.as_deref().map_or(0, |c| c.chars().count()))
            })
            .unwrap_or(0) as u16;

        let throbber_style = if no_bg {
            Style::default().fg(self.throbber_color)
        } else {
            Style::default().bg(self.bg_color).fg(self.throbber_color)
        };

        let (label_style, fill_style) = if no_bg {
            (Style::default().fg(self.label_color), Style::default())
        } else {
            let base = Style::default().bg(self.bg_color);
            (base.fg(self.label_color), base)
        };

        // The trailing chunk, in columns, or `None` when there is no row count to show.
        // A bare row count fits in twenty; a caption or a "417 of 1,000" may not, and
        // truncating either mid-word is worse than giving it the room it asked for —
        // so the reservation asks the same text the layout will later draw.
        //
        // Worked out once, above both modes, because the chip-fitting loop below has to
        // subtract exactly what its layout will later ask for — and because the status
        // layout used to hardcode twenty-one here and truncate the caption itself.
        let trailing = self
            .row_count
            .map(|count| (row_count_text(count).chars().count() as u16 + 1 + columns_room).max(20));

        // The chip that says the row count is not the table's. Immediately left of the
        // count, because the count is what it is about.
        let not_the_table = self
            .not_the_table
            .map(|format| format!(" not the {format} table "));
        let reshaped = self.reshaped.map(|verb| format!(" {verb} "));
        let tied = self
            .formats_tied
            .map(|others| format!(" {} formats match ", others + 1));
        let find = self.find.as_ref().map(|mark| format!("{mark} "));
        let follow_chip = self.follow.as_ref().map(|f| format!(" {} ", f.chip));
        let follow_note = self.follow.as_ref().and_then(|f| f.note.clone());
        let follow_warning = self.follow.as_ref().and_then(|f| f.warning.clone());
        let width_of = |text: &Option<String>| {
            text.as_ref()
                .map(|text| crate::glyphs::display_width(text) as u16 + 1)
                .unwrap_or(0)
        };
        let follow_width =
            width_of(&follow_chip) + width_of(&follow_note) + width_of(&follow_warning);
        let chip_width = follow_width
            + not_the_table
                .as_ref()
                .map(|text| text.chars().count() as u16)
                .unwrap_or(0)
            + reshaped
                .as_ref()
                .map(|text| text.chars().count() as u16 + 1)
                .unwrap_or(0)
            + find
                .as_ref()
                .map(|text| crate::glyphs::display_width(text) as u16 + 1)
                .unwrap_or(0)
            + tied
                .as_ref()
                .map(|text| text.chars().count() as u16 + 1)
                .unwrap_or(0);

        // The chip: the bar's accent behind the bar's own colour, the same cut-out a
        // key chip is, because it is the one thing here the eye must not slide past.
        let chip_style = Style::default()
            .bg(self.key_color)
            .fg(self.chip_text_color)
            .add_modifier(Modifier::BOLD);

        // What sits left of the count: the find in effect, plain, then the chips.
        let chip_line = || {
            let mut spans: Vec<ratatui::text::Span> = Vec::new();
            if let Some(text) = &find {
                spans.push(ratatui::text::Span::styled(text.clone(), label_style));
                spans.push(ratatui::text::Span::raw(" "));
            }
            if let Some(text) = &reshaped {
                spans.push(ratatui::text::Span::styled(text.clone(), chip_style));
                spans.push(ratatui::text::Span::raw(" "));
            }
            if let Some(text) = &tied {
                spans.push(ratatui::text::Span::styled(text.clone(), chip_style));
                spans.push(ratatui::text::Span::raw(" "));
            }
            if let Some(text) = &follow_note {
                spans.push(ratatui::text::Span::styled(text.clone(), label_style));
                spans.push(ratatui::text::Span::raw(" "));
            }
            if let Some(text) = &follow_warning {
                spans.push(ratatui::text::Span::styled(
                    text.clone(),
                    label_style.fg(self.warning_color),
                ));
                spans.push(ratatui::text::Span::raw(" "));
            }
            if let Some(text) = &follow_chip {
                spans.push(ratatui::text::Span::styled(text.clone(), chip_style));
                spans.push(ratatui::text::Span::raw(" "));
            }
            if let Some(text) = &not_the_table {
                spans.push(ratatui::text::Span::styled(text.clone(), chip_style));
            }
            ratatui::text::Line::from(spans)
        };

        // The keys this bar would offer, custom or default. Worked out above the
        // status mode too: the way-out subset stays on a busy bar.
        const DEFAULT_CONTROLS: [(&str, &str); 14] = [
            // First: Enter on a row is the primary action, how a first session
            // reads one record. It yields to Help at 60 columns by position alone.
            ("Enter", "Inspect"),
            ("/", "Query"),
            ("f", "Find"),
            ("i", "Info"),
            ("a", "Analysis"),
            ("c", "Chart"),
            ("s", "Sort & Filter"),
            ("p", "Pivot & Melt"),
            ("e", "Export"),
            ("y", "Copy"),
            ("^O", "Home"),
            ("g", "Column"),
            ("?", "Help"),
            ("q", "Quit"),
        ];
        let mut controls: Vec<(&str, &str)> = if let Some(ref custom) = self.custom_controls {
            custom.to_vec()
        } else {
            let mut defaults = DEFAULT_CONTROLS.to_vec();
            if self.q_pops {
                // The bar says which meaning q carries right now.
                defaults.last_mut().expect("q is the last chip").1 = "Home";
            }
            if self.enter_drills {
                // Where Enter drills, the bar names what Enter does; Space still
                // inspects, and help says so.
                defaults[0].1 = "Drill";
            }
            if self.find.is_some() {
                // Beside Find: how the find on the bar is taken back.
                let at = defaults
                    .iter()
                    .position(|(key, _)| *key == "f")
                    .map_or(defaults.len(), |i| i + 1);
                defaults.insert(at, ("Esc", "Clear Find"));
            }
            if self.format_key {
                // Beside Info, whose Notes say which spec read the file and why.
                let at = defaults
                    .iter()
                    .position(|(key, _)| *key == "i")
                    .map_or(defaults.len(), |i| i + 1);
                defaults.insert(at, ("b", "Format"));
            }
            defaults
        };
        // The follow is what changes on screen; its key comes first.
        if let Some(key) = self.follow.as_ref().and_then(|f| f.key) {
            controls.insert(0, ("t", key));
        }

        // Status message mode: [spinner 2ch] [message Fill] [escape chips] [chip]
        // [row count or caption]
        if let Some(ref msg) = self.status_message {
            // The way out never yields to a spinner: the keys that still act
            // while the app is busy stay on the bar, beside the status.
            let mut escapes = crate::widgets::ui::HintBar::with_styles(
                chip_style,
                label_style,
                Style::default().fg(self.key_color),
            );
            for &(key, label) in controls
                .iter()
                .filter(|(key, _)| matches!(*key, "Esc" | "^C" | "^Q" | "^O" | "q" | "Q" | "?"))
            {
                escapes = escapes.hint(key, label);
            }
            // A third of the bar at most: the message and the count are what a
            // busy bar is for, and the chips yield to them, way out last.
            let escapes_width = escapes.width_in(area.width / 3);

            let mut constraints = vec![
                Constraint::Length(2), // spinner
                Constraint::Fill(1),   // status message
                Constraint::Length(escapes_width),
            ];
            if chip_width > 0 {
                constraints.push(Constraint::Length(chip_width));
            }
            if let Some(width) = trailing {
                constraints.push(Constraint::Length(width));
            }

            let layout = Layout::new(Direction::Horizontal, constraints).split(area);

            // Spinner on the left
            Paragraph::new(format!("{} ", throbber_ch()))
                .style(throbber_style)
                .render(layout[0], buf);

            // Status message, cut with a mark rather than by the edge. The loading
            // phase can be "Reading footers: 1,203 of 6,541... (40%)", and a bare
            // Paragraph clipped that to "1,203 of 6" — a smaller number than the one
            // it is counting towards, which is worse than saying less.
            Paragraph::new(crate::render::loading_view::truncate(
                msg,
                layout[1].width as usize,
            ))
            .style(label_style)
            .render(layout[1], buf);

            escapes.render(layout[2], buf);
            self.drawn.replace(escapes.chips_in(layout[2]));

            let mut next = 3;
            if chip_width > 0 {
                Paragraph::new(chip_line()).render(layout[next], buf);
                next += 1;
            }
            // Row count (right-aligned, if available)
            if let Some(count) = self.row_count {
                Paragraph::new(row_count_text(count))
                    .style(label_style)
                    .right_aligned()
                    .render(layout[next], buf);
            }

            return;
        }

        // Completion flash mode: the same layout the busy status message uses,
        // minus the spinner — the message takes the chips' row for two seconds
        // and the trailing count holds still. The transition is the one the eye
        // already knows: spinner and phase, then the sentence, then the chips.
        if let Some(ref msg) = self.flash {
            let mut constraints = vec![
                Constraint::Length(2), // where the spinner would be
                Constraint::Fill(1),   // the sentence
            ];
            if chip_width > 0 {
                constraints.push(Constraint::Length(chip_width));
            }
            if let Some(width) = trailing {
                constraints.push(Constraint::Length(width));
            }
            let layout = Layout::new(Direction::Horizontal, constraints).split(area);
            Paragraph::new("").style(fill_style).render(layout[0], buf);
            Paragraph::new(crate::render::loading_view::truncate(
                msg,
                layout[1].width as usize,
            ))
            .style(label_style)
            .render(layout[1], buf);
            let mut next = 2;
            if chip_width > 0 {
                Paragraph::new(chip_line()).render(layout[next], buf);
                next += 1;
            }
            if let Some(count) = self.row_count {
                Paragraph::new(row_count_text(count))
                    .style(label_style)
                    .right_aligned()
                    .render(layout[next], buf);
            }
            return;
        }

        // Normal keybinding mode: the chips are a HintBar, the same renderer every
        // Surface footer uses.
        //
        // The accented label carries no background of its own: the bar's Block above
        // painted it already, and `notes_pending` only recolors the Info label.
        let mut bar = crate::widgets::ui::HintBar::with_styles(
            chip_style,
            label_style,
            Style::default().fg(self.key_color),
        );
        // The bar is cut from the right, but the way out yields last wherever
        // it sits: a chart bar that ends "Esc Back" must not lose exactly that
        // chip on a narrow terminal. Help yields second to last: nothing a
        // first session needs may live only behind a key the bar never shows.
        let n = controls.len() as i32;
        for (i, &(key, label)) in controls.iter().enumerate() {
            let way_out = matches!(key, "Esc" | "^C" | "^Q" | "q") || label == "Quit";
            let help = matches!(key, "?" | "F1");
            let weight = if way_out {
                n + 2
            } else if help {
                n + 1
            } else {
                n - i as i32
            };
            bar = bar.hint_weighted(key, label, weight);
        }
        if self.notes_pending {
            bar = bar.accent("i");
        }

        // Budgeting a flat twenty-one against a caption like "by recent  ·  128 datasets"
        // — twenty-seven — admits chips worth seven columns the solver then has to take
        // back out of the tail, and the tail is the last chip: at eighty-five columns the
        // bar read "type  Filte →  Inside".
        //
        // Plus one so the no-caption case reserves the twenty-one it always did. `Fill(1)`
        // is satisfied by nothing, so the extra column is a margin rather than a
        // requirement — it costs one chip at one width and keeps the common case as it
        // was.
        let right_reserved = trailing.map(|width| width + 1).unwrap_or(1) + chip_width;
        let chips_width = bar.width_in(area.width.saturating_sub(right_reserved));

        let mut constraints = vec![Constraint::Length(chips_width), Constraint::Fill(1)];
        if chip_width > 0 {
            constraints.push(Constraint::Length(chip_width));
        }
        if let Some(width) = trailing {
            constraints.push(Constraint::Length(width));
        }

        let layout = Layout::new(Direction::Horizontal, constraints).split(area);

        bar.render(layout[0], buf);
        self.drawn.replace(bar.chips_in(layout[0]));

        let mut next = 2;
        if chip_width > 0 {
            Paragraph::new(chip_line()).render(layout[next], buf);
            next += 1;
        }
        if let Some(count) = self.row_count {
            Paragraph::new(row_count_text(count))
                .style(label_style)
                .right_aligned()
                .render(layout[next], buf);
        }

        Paragraph::new("").style(fill_style).render(layout[1], buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The flash takes the chips' row the way the busy message does, and the
    /// trailing count holds still while it comes and goes.
    #[test]
    fn a_flash_reads_in_full_and_the_count_holds_still() {
        let quiet = with_row_count(1000);
        let flashed = with_row_count(1000).with_flash(Some("Exported to out.csv".to_string()));
        let width = 80u16;
        let before = render_to_string(&quiet, width);
        let during = render_to_string(&flashed, width);
        assert!(during.contains("Exported to out.csv"), "{during:?}");
        assert_eq!(
            before.find("1,000"),
            during.find("1,000"),
            "the count moved"
        );
        // A busy status message outranks the flash: work in progress is never
        // hidden behind a sentence about work already done.
        let busy = with_row_count(1000)
            .with_flash(Some("Exported to out.csv".to_string()))
            .with_status_message(Some("Collecting rows".to_string()));
        let bar = render_to_string(&busy, width);
        assert!(bar.contains("Collecting rows"), "{bar:?}");
        assert!(!bar.contains("Exported"), "{bar:?}");
    }

    /// A long caption does not cost the last chip the bar decided it had room for.
    ///
    /// The fitting loop reserved a flat twenty-one columns for the trailing chunk, while
    /// the layout asked for `caption + 1`. On the home screen the caption is routinely
    /// longer than that — "by recent  ·  128 datasets" is twenty-seven — so the loop
    /// admitted chips worth the difference and the solver took them back out of the
    /// tail, which is the chip the bar was least able to lose.
    #[test]
    fn a_long_caption_does_not_squeeze_the_last_chip() {
        let caption = "by recent  ·  128 datasets".to_string();
        let controls = with_row_count(0)
            .with_custom_controls(vec![
                ("Enter", "Open"),
                ("↑↓", "Move"),
                ("Esc", "Back"),
                ("type", "Filter"),
                ("→", "Inside"),
            ])
            .with_caption(Some(caption.clone()));

        // Chips are admitted in order, so a later one on screen means every earlier one
        // was admitted too — and each has to be there whole. Budgeting twenty-one against
        // a twenty-seven-column caption, width 85 rendered "type  Filte →  Inside".
        let labels = ["Open", "Move", "Back", "Filter", "Inside"];
        for width in 70..=120u16 {
            let bar = render_to_string(&controls, width);
            assert!(
                bar.contains(&caption),
                "the caption is never truncated (width {width}): {bar:?}"
            );
            let last = labels.iter().rposition(|label| bar.contains(label));
            if let Some(last) = last {
                for label in &labels[..last] {
                    assert!(
                        bar.contains(label),
                        "`{label}` was clipped to fit a chip admitted after it \
                         (width {width}): {bar:?}"
                    );
                }
            }
            // And the invariant above is not satisfied by simply losing the tail: past
            // the width where every chip fits, every chip is there.
            if width >= 92 {
                assert!(
                    bar.contains("Inside"),
                    "the last chip fits at {width} and is not shown: {bar:?}"
                );
            }
        }
    }

    /// A status message does not truncate the caption beside it either.
    ///
    /// The status layout hardcoded twenty-one columns for the trailing chunk while the
    /// caption asked for its own width, so it cut one mid-word — "by recent  ·  128 dat"
    /// — which is the thing the keybinding branch was fixed not to do.
    #[test]
    fn a_status_message_does_not_truncate_the_caption() {
        let caption = "by recent  ·  128 datasets".to_string();
        let controls = with_row_count(0)
            .with_caption(Some(caption.clone()))
            .with_status_message(Some("Counting rows to find the end…".to_string()));

        for width in 70..=140u16 {
            let bar = render_to_string(&controls, width);
            assert!(
                bar.contains(&caption),
                "the caption is whole at width {width}: {bar:?}"
            );
        }
    }

    /// The busy bar keeps the way out: the status message renders beside the
    /// escape chips instead of replacing every key.
    #[test]
    fn the_busy_bar_keeps_the_way_out() {
        let controls = with_row_count(0)
            .with_caption(Some("Loading".to_string()))
            .with_custom_controls(vec![("^O", "Home"), ("?", "Help"), ("q", "Quit")])
            .with_status_message(Some("Reading footers: 12 of 6,541...".to_string()));
        let bar = render_to_string(&controls, 100);
        assert!(bar.contains("Reading footers"), "{bar:?}");
        for piece in ["^O", "Home", "Help", "Quit"] {
            assert!(
                bar.contains(piece),
                "{piece} missing from busy bar: {bar:?}"
            );
        }

        // The default table bar while busy keeps its escapes too.
        let bar = render_to_string(
            &with_row_count(10).with_status_message(Some("Working...".to_string())),
            100,
        );
        assert!(bar.contains("Working"), "{bar:?}");
        assert!(bar.contains("Quit"), "the way out survives busy: {bar:?}");
        assert!(
            !bar.contains("Pivot"),
            "the feature chips do not crowd a busy bar: {bar:?}"
        );
    }

    /// A follow's chip sits beside the count it moves, its note and warning before it,
    /// and `t` leads the keys; on a narrow bar the keys yield, not the follow.
    #[test]
    fn a_follow_says_so_beside_the_count() {
        let mark = FollowMark {
            key: Some("Pause"),
            chip: "following · 3s ago".to_string(),
            note: Some("4 new below".to_string()),
            warning: Some("1 row does not fit".to_string()),
        };
        for width in [80u16, 160] {
            let bar = render_to_string(
                &with_row_count(1_234).with_follow(Some(mark.clone())),
                width,
            );
            let chip = bar.find("following · 3s ago").expect("the chip");
            let count = bar.find("1,234 rows").expect("the count");
            assert!(chip < count, "{bar:?}");
            assert!(bar.contains("4 new below"), "{bar:?}");
        }
        let wide = render_to_string(&with_row_count(1_234).with_follow(Some(mark)), 160);
        assert!(wide.contains("1 row does not fit"), "{wide:?}");
        assert!(
            wide.find("Pause") < wide.find("Inspect"),
            "t leads the keys: {wide:?}"
        );
    }

    /// The tests build the widget the way production does: from a context.
    fn with_row_count(row_count: usize) -> Controls {
        Controls::from_context(
            row_count,
            &crate::render::context::RenderContext::for_test(),
        )
    }

    fn render_to_string(controls: &Controls, width: u16) -> String {
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        controls.render(area, &mut buf);
        (0..width)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect::<String>()
    }

    /// The chip is in the bar, beside the count it is about, and it costs a key rather
    /// than the count.
    ///
    /// A note in the panel is a tab away. This is the half that cannot be missed, and
    /// reading a lake table's files at all is only defensible because both are there.
    #[test]
    fn a_lake_table_s_files_say_so_beside_the_row_count() {
        let bar = |format: Option<&'static str>| {
            render_to_string(&with_row_count(1_234).with_not_the_table(format), 80)
        };

        let plain = bar(None);
        let labelled = bar(Some("Delta"));
        assert!(plain.contains("1,234 rows"));
        assert!(
            !plain.contains("not the"),
            "nothing is said about an ordinary dataset"
        );
        assert!(labelled.contains("not the Delta table"), "got {labelled:?}");
        assert!(
            labelled.contains("1,234 rows"),
            "and the count it is about is still there: {labelled:?}"
        );
        assert!(
            labelled.find("not the Delta").unwrap() < labelled.find("1,234 rows").unwrap(),
            "immediately left of the count: {labelled:?}"
        );
        // Room for it comes out of the keys, which the bar drops from the tail as it
        // always has, rather than out of the count.
        assert!(
            labelled.contains("Inspect"),
            "the first keys are still offered: {labelled:?}"
        );
    }

    #[test]
    fn a_dataset_with_notes_gives_the_info_key_a_quiet_accent() {
        let area = Rect::new(0, 0, 80, 1);
        let paint = |pending: bool| {
            let mut buf = Buffer::empty(area);
            let mut controls = with_row_count(0).with_notes_pending(pending);
            controls.row_count = None;
            controls.render(area, &mut buf);
            buf
        };
        let plain = paint(false);
        let accented = paint(true);

        let text = |buf: &Buffer| {
            (0..area.width)
                .map(|x| buf[(x, 0)].symbol().to_string())
                .collect::<String>()
        };
        assert_eq!(
            text(&plain),
            text(&accented),
            "the accent says nothing extra; it is only a color"
        );
        assert!(
            text(&plain).contains("Info"),
            "the Info key is in the bar to begin with"
        );
        let changed: Vec<u16> = (0..area.width)
            .filter(|x| plain[(*x, 0)].fg != accented[(*x, 0)].fg)
            .collect();
        assert!(!changed.is_empty(), "the Info label takes the accent");

        // And only that chip: every other one is left alone. A label chunk is a
        // leading space, the word, then two cells of gap.
        let info_at = text(&plain).find("Info").unwrap() as u16;
        let chunk = (info_at - 1)..(info_at + "Info".len() as u16 + 2);
        for x in &changed {
            assert!(
                chunk.contains(x),
                "column {x} changed, which is outside the Info chip"
            );
        }
    }

    /// A filtered view says what it is a view of: "417 of 1,000".
    #[test]
    fn a_filtered_count_names_the_total_beside_it() {
        let out = render_to_string(&with_row_count(417).with_total_row_count(Some(1_000)), 80);
        assert!(out.contains("417 of 1,000"), "got: {out:?}");

        // The same total says nothing: nothing was filtered away.
        let out = render_to_string(&with_row_count(1_000).with_total_row_count(Some(1_000)), 80);
        assert!(out.contains("1,000 rows"), "got: {out:?}");
        assert!(!out.contains(" of "), "got: {out:?}");
    }

    /// The bar names the column cursor's place before the rows, in the short form on
    /// a narrow bar; the way out keeps its chip at 60 columns.
    #[test]
    fn the_cursor_column_leads_the_row_count() {
        use crate::widgets::column_paging::OnScreen;
        let on = Some(OnScreen {
            first: 41,
            last: 47,
            cursor: 43,
            total: 300,
        });
        let dot = crate::glyphs::get().middot;
        let wide = render_to_string(&with_row_count(200).with_columns(on), 120);
        assert!(
            wide.ends_with(&format!("col 43 of 300  {dot}  200 rows")),
            "got: {wide:?}"
        );
        let narrow = render_to_string(&with_row_count(200).with_columns(on), 60);
        assert!(
            narrow.ends_with(&format!("col 43/300 {dot} 200 rows")),
            "got: {narrow:?}"
        );
        assert!(narrow.contains("q  Quit"), "got: {narrow:?}");
        let none = render_to_string(&with_row_count(200), 80);
        assert!(!none.contains("col "), "got: {none:?}");

        // Moving the cursor changes the number, never which chips fit beside it.
        for width in [60, 80, 100, 120] {
            let at = |cursor| {
                let on = OnScreen {
                    first: 1,
                    last: 6,
                    cursor,
                    total: 300,
                };
                let bar = render_to_string(&with_row_count(200).with_columns(Some(on)), width);
                let cut = bar.find("col ").expect("the position is drawn");
                bar[..cut].trim_end().to_string()
            };
            assert_eq!(at(1), at(300), "at {width}");
            assert_eq!(at(1), at(43), "at {width}");
        }
    }

    /// The first chip names what Enter does: Inspect, or Drill where Enter drills.
    /// It leads from 80 columns up; at 60 it leads beside a short column position,
    /// and beside a wide one Help keeps its place and Inspect yields.
    #[test]
    fn the_enter_chip_leads_and_yields_to_help_at_60() {
        use crate::widgets::column_paging::OnScreen;
        for total in [14, 3_000] {
            let on = Some(OnScreen {
                first: 1,
                last: 5,
                cursor: 3,
                total,
            });
            for width in [60, 80, 120] {
                for (drills, chip, other) in [
                    (false, "Enter  Inspect", "Drill"),
                    (true, "Enter  Drill", "Inspect"),
                ] {
                    let bar = render_to_string(
                        &with_row_count(60)
                            .with_columns(on)
                            .with_enter_drills(drills),
                        width,
                    );
                    let at = format!("{total} at {width}: {bar:?}");
                    assert!(bar.contains("?  Help"), "{at}");
                    assert!(bar.contains("q  Quit"), "{at}");
                    assert!(!bar.contains(other), "{at}");
                    // `col 3/14` leaves Inspect room at 60; `col 3/3000` (reserved as
                    // `col 3000/3000`) leaves the shorter Drill chip room alone.
                    if width >= 80 || drills || total < 100 {
                        assert!(bar.trim_start().starts_with(chip), "{at}");
                    } else {
                        assert!(!bar.contains(chip), "{at}");
                    }
                }
            }
        }
    }

    /// The weighting that keeps Help is by key, not label: a custom bar with an
    /// Inspect chip cuts it from the right like any other.
    #[test]
    fn a_custom_inspect_chip_gets_no_weight() {
        let controls = with_row_count(0).with_custom_controls(vec![
            ("/", "Query"),
            ("x", "Something long"),
            ("z", "Inspect"),
            ("?", "Help"),
            ("q", "Quit"),
        ]);
        for width in 30..=70u16 {
            let bar = render_to_string(&controls, width);
            if bar.contains("Inspect") {
                assert!(bar.contains("Something long"), "at {width}: {bar:?}");
            }
        }
    }

    /// Pending and unknown keep their say: no "of" beside a spinner or a "?".
    #[test]
    fn the_total_defers_to_pending_and_unknown() {
        let out = render_to_string(
            &with_row_count(417)
                .with_total_row_count(Some(1_000))
                .with_row_count_pending(true)
                .with_busy(false, 1),
            80,
        );
        assert!(out.contains("/ rows"), "got: {out:?}");
        assert!(!out.contains(" of "), "got: {out:?}");

        let out = render_to_string(
            &with_row_count(417)
                .with_total_row_count(Some(1_000))
                .with_row_count_unknown(true),
            80,
        );
        assert!(out.contains("? rows"), "got: {out:?}");
        assert!(!out.contains(" of "), "got: {out:?}");
    }

    /// A pair too long for the flat twenty columns is given room, not clipped.
    #[test]
    fn a_long_count_pair_is_not_clipped() {
        let out = render_to_string(
            &with_row_count(999_417).with_total_row_count(Some(1_000_000)),
            80,
        );
        assert!(out.contains("999,417 of 1.0M"), "got: {out:?}");
    }

    /// A reshaped view says so beside the number the reshape changed.
    #[test]
    fn a_reshaped_view_carries_its_chip_beside_the_count() {
        let controls = with_row_count(7).with_reshaped(Some("pivoted"));
        let out = render_to_string(&controls, 80);
        assert!(out.contains(" pivoted "), "got: {out:?}");
        assert!(
            out.find(" pivoted ").unwrap() < out.find("7 rows").unwrap(),
            "the chip sits beside the count it is about: {out:?}"
        );
    }

    /// A table read through a spec offers `b` beside Info, and a file several specs
    /// matched says so beside the count.
    #[test]
    fn a_format_read_offers_b_and_a_tie_says_so() {
        let plain = render_to_string(&with_row_count(7), 120);
        assert!(!plain.contains("Format"), "got: {plain:?}");
        let read = render_to_string(&with_row_count(7).with_format(true, Some(0)), 120);
        assert!(
            read.find("Info").unwrap() < read.find("Format").unwrap(),
            "got: {read:?}"
        );
        assert!(!read.contains("formats match"), "got: {read:?}");
        let tied = render_to_string(&with_row_count(7).with_format(true, Some(1)), 120);
        assert!(
            tied.find(" 2 formats match ").unwrap() < tied.find("7 rows").unwrap(),
            "got: {tied:?}"
        );
    }

    #[test]
    fn shows_number_when_count_known() {
        let controls = with_row_count(1_234_567);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("1.2M rows"), "got: {out:?}");
    }

    #[test]
    fn shows_spinner_not_number_when_count_pending() {
        // ASCII throbber, frame 1 -> '/'
        let controls = with_row_count(42)
            .with_row_count_pending(true)
            .with_busy(false, 1);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("/ rows"), "expected spinner, got: {out:?}");
        // The provisional number must not leak into the display.
        assert!(!out.contains("42"), "provisional count leaked: {out:?}");
    }

    #[test]
    fn shows_question_mark_when_count_failed() {
        let controls = with_row_count(42).with_row_count_unknown(true);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("? rows"), "expected '?', got: {out:?}");
        assert!(!out.contains("42"), "provisional count leaked: {out:?}");
    }

    #[test]
    fn pending_takes_precedence_over_unknown() {
        let controls = with_row_count(42)
            .with_row_count_pending(true)
            .with_row_count_unknown(true)
            .with_busy(false, 1);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("/ rows"), "expected spinner, got: {out:?}");
        assert!(
            !out.contains("? rows"),
            "should not show '?' while pending: {out:?}"
        );
    }

    /// A status message too long for the bar is cut with a mark, not by the edge.
    ///
    /// The loading phase can be "Reading footers: 1,203 of 6,541... (40%)", and the bar
    /// renders into a fixed region beside the row count. Clipped bare it read
    /// "1,203 of 6" — a smaller number than the one it is counting towards, which is
    /// the one way this line could actively mislead.
    #[test]
    fn a_status_message_too_long_for_the_bar_is_cut_with_a_mark() {
        let ellipsis = crate::glyphs::get().ellipsis;
        let long = "Reading footers: 1,203 of 6,541... (40%)";
        for width in 40..=70u16 {
            let controls = with_row_count(99).with_status_message(Some(long.to_string()));
            let out = render_to_string(&controls, width);
            if out.contains("(40%)") {
                continue; // it fitted whole
            }
            assert!(
                out.contains(ellipsis),
                "at {width} the message is cut with nothing to say so: {out:?}"
            );
        }
    }

    #[test]
    fn pending_spinner_shown_in_status_message_mode() {
        let controls = with_row_count(99)
            .with_row_count_pending(true)
            .with_status_message(Some("Loading buffer...".to_string()))
            .with_busy(false, 0);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Loading buffer..."), "got: {out:?}");
        // Frame 0 ASCII -> '|'
        assert!(out.contains("| rows"), "expected spinner, got: {out:?}");
        assert!(!out.contains("99"), "provisional count leaked: {out:?}");
    }
}
