use crate::render::context::RenderContext;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Paragraph, Widget},
};

pub struct Controls {
    pub row_count: Option<usize>,
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
}

impl Default for Controls {
    fn default() -> Self {
        Self {
            caption: None,
            row_count: None,
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color: Color::Indexed(236), // Default for backward compatibility
            key_color: Color::Cyan,        // Keys in cyan
            label_color: Color::White,     // Labels in white
            chip_text_color: Color::Black,
            throbber_color: Color::Cyan,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            notes_pending: false,
            row_count_pending: false,
            row_count_unknown: false,
            not_the_table: None,
        }
    }
}

impl Controls {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_row_count(row_count: usize) -> Self {
        Self {
            caption: None,
            row_count: Some(row_count),
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color: Color::Indexed(236), // Default
            key_color: Color::Cyan,        // Keys in cyan
            label_color: Color::White,     // Labels in white
            chip_text_color: Color::Black,
            throbber_color: Color::Cyan,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            notes_pending: false,
            row_count_pending: false,
            row_count_unknown: false,
            not_the_table: None,
        }
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

    pub fn with_row_count_pending(mut self, pending: bool) -> Self {
        self.row_count_pending = pending;
        self
    }

    /// Replace the trailing row count with a caption of the view's own.
    ///
    /// "Rows: 0" is the table's counter; on a screen that is not showing a table it
    /// is at best meaningless and at worst looks like an empty dataset.
    pub fn with_caption(mut self, caption: Option<String>) -> Self {
        self.caption = caption;
        self
    }

    pub fn with_row_count_unknown(mut self, unknown: bool) -> Self {
        self.row_count_unknown = unknown;
        self
    }

    /// Say, beside the row count, that these are a lake table's files and not the
    /// table. See [`Self::not_the_table`].
    pub fn with_not_the_table(mut self, format: Option<&'static str>) -> Self {
        self.not_the_table = format;
        self
    }

    /// Create Controls from RenderContext (Phase 2+).
    /// This is the preferred way to create Controls with proper theming.
    pub fn from_context(row_count: usize, ctx: &RenderContext) -> Self {
        Self {
            caption: None,
            row_count: Some(row_count),
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
            notes_pending: false,
            row_count_pending: false,
            row_count_unknown: false,
            not_the_table: None,
        }
    }

    pub fn with_row_count_and_colors(
        row_count: usize,
        bg_color: Color,
        key_color: Color,
        label_color: Color,
        throbber_color: Color,
    ) -> Self {
        Self {
            caption: None,
            row_count: Some(row_count),
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color,
            key_color,
            label_color,
            chip_text_color: Color::Black,
            throbber_color,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            notes_pending: false,
            row_count_pending: false,
            row_count_unknown: false,
            not_the_table: None,
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

        // Throbber character for status mode (reused below).
        const THROBBER_ASCII: [char; 4] = ['|', '/', '-', '\\'];
        const THROBBER_BRAILLE_EIGHT: [char; 8] = ['⣷', '⣯', '⣟', '⡿', '⢿', '⣻', '⣽', '⣾'];

        let throbber_ch = || -> String {
            if self.busy {
                if self.use_unicode_throbber {
                    THROBBER_BRAILLE_EIGHT[self.throbber_frame as usize % 8].to_string()
                } else {
                    THROBBER_ASCII[self.throbber_frame as usize % 4].to_string()
                }
            } else {
                " ".to_string()
            }
        };

        // Spinner frame independent of `busy` — used for the row-count placeholder while the
        // exact total is still being determined (that background count does not set `busy`).
        let spinner_ch = || -> char {
            if self.use_unicode_throbber {
                THROBBER_BRAILLE_EIGHT[self.throbber_frame as usize % 8]
            } else {
                THROBBER_ASCII[self.throbber_frame as usize % 4]
            }
        };

        // Row-count text: while the count is pending a spinner stands in for the number, and if
        // the count couldn't be determined a "?" is shown — so the user never mistakes an
        // incomplete partial total for the final figure.
        let row_count_text = |count: usize| -> String {
            if let Some(caption) = &self.caption {
                return caption.clone();
            }
            if self.row_count_pending {
                format!("Rows: {}", spinner_ch())
            } else if self.row_count_unknown {
                "Rows: ?".to_string()
            } else {
                format!("Rows: {}", crate::numfmt::group_chrome(count))
            }
        };

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
        // A row count fits in twenty; a view's own caption may not, and truncating it
        // mid-word ("by recent · 78 dat") is worse than giving it the room it asked for.
        //
        // Worked out once, above both modes, because the chip-fitting loop below has to
        // subtract exactly what its layout will later ask for — and because the status
        // layout used to hardcode twenty-one here and truncate the caption itself.
        let trailing = self.row_count.map(|_| {
            self.caption
                .as_ref()
                .map(|c| c.chars().count() as u16 + 1)
                .unwrap_or(20)
                .max(20)
        });

        // The chip that says the row count is not the table's. Immediately left of the
        // count, because the count is what it is about.
        let not_the_table = self
            .not_the_table
            .map(|format| format!(" not the {format} table "));
        let chip_width = not_the_table
            .as_ref()
            .map(|text| text.chars().count() as u16)
            .unwrap_or(0);

        // The chip: the bar's accent behind the bar's own colour, the same cut-out a
        // key chip is, because it is the one thing here the eye must not slide past.
        let chip_style = Style::default()
            .bg(self.key_color)
            .fg(self.chip_text_color)
            .add_modifier(Modifier::BOLD);

        // Status message mode: [spinner 2ch] [message Fill] [chip] [row count or caption]
        if let Some(ref msg) = self.status_message {
            let mut constraints = vec![
                Constraint::Length(2), // spinner
                Constraint::Fill(1),   // status message
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

            let mut next = 2;
            if let Some(text) = &not_the_table {
                Paragraph::new(text.as_str())
                    .style(chip_style)
                    .render(layout[next], buf);
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

        // Normal keybinding mode (unchanged from original)
        const DEFAULT_CONTROLS: [(&str, &str); 9] = [
            ("/", "Query"),
            ("i", "Info"),
            ("a", "Analysis"),
            ("c", "Chart"),
            ("s", "Sort & Filter"),
            ("p", "Pivot & Melt"),
            ("e", "Export"),
            ("?", "Help"),
            ("q", "Quit"),
        ];

        let controls: Vec<(&str, &str)> = if let Some(ref custom) = self.custom_controls {
            custom.to_vec()
        } else {
            DEFAULT_CONTROLS.to_vec()
        };

        // A key chip is the key with one cell of padding on either side, then a space,
        // then the label, then two cells before the next chip.
        let pair_width = |(key, action): &(&str, &str)| -> u16 {
            (key.chars().count() as u16 + 2) + (action.chars().count() as u16 + 3)
        };

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
        let mut available = area.width.saturating_sub(right_reserved);

        let mut n_show = 0;
        for pair in controls.iter() {
            let need = pair_width(pair);
            if available >= need {
                available -= need;
                n_show += 1;
            } else {
                break;
            }
        }

        let mut constraints: Vec<Constraint> = controls
            .iter()
            .take(n_show)
            .flat_map(|(key, action)| {
                [
                    Constraint::Length(key.chars().count() as u16 + 2),
                    Constraint::Length(action.chars().count() as u16 + 3),
                ]
            })
            .collect();

        constraints.push(Constraint::Fill(1));
        if chip_width > 0 {
            constraints.push(Constraint::Length(chip_width));
        }
        if let Some(width) = trailing {
            constraints.push(Constraint::Length(width));
        }

        let layout = Layout::new(Direction::Horizontal, constraints).split(area);

        for (i, (key, action)) in controls.iter().take(n_show).enumerate() {
            let j = i * 2;
            Paragraph::new(format!(" {key} "))
                .style(chip_style)
                .render(layout[j], buf);
            let style = if self.notes_pending && *key == "i" {
                Style::default().fg(self.key_color)
            } else {
                label_style
            };
            Paragraph::new(format!(" {action}"))
                .style(style)
                .render(layout[j + 1], buf);
        }

        let fill_idx = n_show * 2;
        let mut next = fill_idx + 1;
        if let Some(text) = &not_the_table {
            Paragraph::new(text.as_str())
                .style(chip_style)
                .render(layout[next], buf);
            next += 1;
        }
        if let Some(count) = self.row_count {
            Paragraph::new(row_count_text(count))
                .style(label_style)
                .right_aligned()
                .render(layout[next], buf);
        }

        Paragraph::new("")
            .style(fill_style)
            .render(layout[fill_idx], buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let controls = Controls::with_row_count(0)
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
        let controls = Controls::with_row_count(0)
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
            render_to_string(
                &Controls::with_row_count(1_234).with_not_the_table(format),
                80,
            )
        };

        let plain = bar(None);
        let labelled = bar(Some("Delta"));
        assert!(plain.contains("Rows: 1,234"));
        assert!(
            !plain.contains("not the"),
            "nothing is said about an ordinary dataset"
        );
        assert!(labelled.contains("not the Delta table"), "got {labelled:?}");
        assert!(
            labelled.contains("Rows: 1,234"),
            "and the count it is about is still there: {labelled:?}"
        );
        assert!(
            labelled.find("not the Delta").unwrap() < labelled.find("Rows:").unwrap(),
            "immediately left of the count: {labelled:?}"
        );
        // Room for it comes out of the keys, which the bar drops from the tail as it
        // always has, rather than out of the count.
        assert!(
            labelled.contains("Query"),
            "the first keys are still offered: {labelled:?}"
        );
    }

    #[test]
    fn a_dataset_with_notes_gives_the_info_key_a_quiet_accent() {
        let area = Rect::new(0, 0, 80, 1);
        let paint = |pending: bool| {
            let mut buf = Buffer::empty(area);
            Controls::new()
                .with_notes_pending(pending)
                .render(area, &mut buf);
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

    #[test]
    fn shows_number_when_count_known() {
        let controls = Controls::with_row_count(1_234_567);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: 1,234,567"), "got: {out:?}");
    }

    #[test]
    fn shows_spinner_not_number_when_count_pending() {
        // ASCII throbber, frame 1 -> '/'
        let controls = Controls::with_row_count(42)
            .with_row_count_pending(true)
            .with_busy(false, 1);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: /"), "expected spinner, got: {out:?}");
        // The provisional number must not leak into the display.
        assert!(!out.contains("42"), "provisional count leaked: {out:?}");
    }

    #[test]
    fn shows_question_mark_when_count_failed() {
        let controls = Controls::with_row_count(42).with_row_count_unknown(true);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: ?"), "expected '?', got: {out:?}");
        assert!(!out.contains("42"), "provisional count leaked: {out:?}");
    }

    #[test]
    fn pending_takes_precedence_over_unknown() {
        let controls = Controls::with_row_count(42)
            .with_row_count_pending(true)
            .with_row_count_unknown(true)
            .with_busy(false, 1);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: /"), "expected spinner, got: {out:?}");
        assert!(
            !out.contains('?'),
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
            let controls = Controls::with_row_count(99).with_status_message(Some(long.to_string()));
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
        let controls = Controls::with_row_count(99)
            .with_row_count_pending(true)
            .with_status_message(Some("Loading buffer...".to_string()))
            .with_busy(false, 0);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Loading buffer..."), "got: {out:?}");
        // Frame 0 ASCII -> '|'
        assert!(out.contains("Rows: |"), "expected spinner, got: {out:?}");
        assert!(!out.contains("99"), "provisional count leaked: {out:?}");
    }
}
