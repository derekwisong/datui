//! The Sample form: which rows every analysis tool reads, and how they are picked.

use crate::render::context::RenderContext;
use crate::sample_modal::{SampleField, SampleForm};
use crate::sampling::SampleMethod;
use crate::widgets::ui::{FormRow, FormValue, HintBar, SectionRule, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget, Wrap};

/// How many source files the inventory under the scope row lists at once.
const FILES_SHOWN: usize = 5;

/// `cap` is the most rows the current tool keeps whatever the form asks for, so the
/// form can say so rather than let a larger number stand unexplained.
/// `focused` is whether the form has the cursor; an inline form waiting beside a
/// focused tool list is drawn without the rail, so one thing looks focused.
pub fn render(
    form: &SampleForm,
    focused: bool,
    files: &[String],
    cap: Option<usize>,
    area: Rect,
    buf: &mut Buffer,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let fields = form.fields();
    let on_scope = form.field == SampleField::Scope;
    let show_files = on_scope && !files.is_empty();
    let mut hints: Vec<(&str, &str)> = vec![
        ("Enter", if form.inline { "Run" } else { "Apply" }),
        (g.updown, "Row"),
    ];
    if !on_scope {
        hints.push((g.updown_lr, "Change"));
    }
    if show_files && files.len() > FILES_SHOWN {
        hints.push(("PgUp/PgDn", "Files"));
    }
    hints.push(("Esc", if form.inline { "Back" } else { "Cancel" }));
    let footer = HintBar::from_ctx(ctx).hints(&hints);

    let files_height = if show_files {
        2 + files.len().min(FILES_SHOWN) as u16
    } else {
        0
    };
    // Rows, a gap, the note (up to two lines), the error, the file inventory, the
    // footer, the frame.
    let lead = u16::from(form.inline) * 2;
    let height = lead + fields.len() as u16 + 1 + 2 + 1 + files_height + 1 + 2;
    // Inline, the form is the pane: it takes the pane's width, capped at a reading
    // measure, from the pane's top. Floating, it is a compact dialog in the middle.
    let frame = if form.inline {
        Rect {
            width: area.width.min(78),
            height: height.min(area.height),
            ..area
        }
    } else {
        let width = area.width.saturating_sub(4).clamp(40, 76);
        Rect {
            x: area.x + area.width.saturating_sub(width) / 2,
            y: area.y + area.height.saturating_sub(height) / 2,
            width: width.min(area.width),
            height: height.min(area.height),
        }
    };
    let inner = Surface::new("Sample")
        .footer(&footer)
        .render(frame, buf, ctx);

    let mut y = inner.y;
    if form.inline && inner.height > 0 {
        Paragraph::new("Which rows this tool reads. Enter runs it; s changes them later.")
            .style(Style::default().fg(ctx.text_primary))
            .render(Rect { height: 1, ..inner }, buf);
        y += 2;
    }
    let bottom = inner.y + inner.height;
    let line = |y: u16| Rect {
        y,
        height: 1,
        ..inner
    };
    let rows = crate::numfmt::group_chrome(form.draft.rows);
    let rows = if matches!(form.draft.method, SampleMethod::PerPartition { .. }) {
        format!("{rows} per value")
    } else {
        rows
    };
    let seed = form.draft.seed.to_string();
    for field in &fields {
        if y >= bottom {
            return;
        }
        let value = match field {
            SampleField::Scope => FormValue::Input(&form.scope_input),
            SampleField::Method => FormValue::Choice(match &form.draft.method {
                SampleMethod::Spread => "Spread",
                SampleMethod::PerPartition { .. } => "Per partition",
                SampleMethod::FirstRows => "First rows",
                SampleMethod::EveryRow => "Every row",
            }),
            SampleField::By => FormValue::Choice(match &form.draft.method {
                SampleMethod::PerPartition { column } => column.as_str(),
                _ => "",
            }),
            SampleField::Rows => FormValue::Choice(&rows),
            SampleField::Seed => FormValue::Choice(&seed),
        };
        FormRow {
            label: field.label(),
            value,
            focused: focused && form.field == *field,
            label_width: 12,
        }
        .render(line(y), buf, ctx);
        y += 1;
    }
    y += 1;

    // What the focused row means: the scope's grammar on the scope row, the method's
    // behavior elsewhere, and the tool's own limit when the size passes it.
    let mut note = if on_scope {
        "view, source, rows 1..5000, files 1,3, partition year=2021 (or 2019,2021 \
         or 2020..2022), time date=2024-01-01..2024-02-01"
            .to_string()
    } else {
        match &form.draft.method {
            SampleMethod::Spread => {
                "Random rows from across the whole scope; r draws another set.".to_string()
            }
            SampleMethod::PerPartition { column } => format!(
                "Up to the row count from each value of {column}, so small partitions count."
            ),
            SampleMethod::FirstRows => {
                "The first rows of the scope, in order: fastest, but only the head.".to_string()
            }
            SampleMethod::EveryRow => "No sampling: every row in scope is read.".to_string(),
        }
    };
    if let Some(cap) = cap.filter(|cap| {
        form.draft.method != SampleMethod::EveryRow && form.draft.rows > *cap && !on_scope
    }) {
        note.push_str(&format!(
            " This tool keeps at most {}.",
            crate::numfmt::group_chrome(cap)
        ));
    }
    if y + 2 <= bottom {
        Paragraph::new(note)
            .wrap(Wrap { trim: true })
            .style(Style::default().fg(ctx.dimmed))
            .render(
                Rect {
                    y,
                    height: 2,
                    ..inner
                },
                buf,
            );
    }
    y += 2;
    if let Some(error) = &form.error
        && y < bottom
    {
        Paragraph::new(Line::styled(
            error.as_str(),
            Style::default().fg(ctx.warning),
        ))
        .render(line(y), buf);
    }
    y += 1;

    if show_files && y + 1 < bottom {
        let chip = crate::numfmt::group_chrome(files.len());
        SectionRule {
            title: "Source files",
            chip: Some(&chip),
            focused: false,
        }
        .render(line(y), buf, ctx);
        y += 1;
        for (index, name) in files
            .iter()
            .enumerate()
            .skip(form.file_offset)
            .take(FILES_SHOWN)
        {
            if y >= bottom {
                break;
            }
            Paragraph::new(format!("{:>4}  {name}", index + 1))
                .style(Style::default().fg(ctx.text_primary))
                .render(line(y), buf);
            y += 1;
        }
    }
}
