//! Help overlay content loaded from `help-strings/*.txt` at compile time.
//! Edit the .txt files to change help content without touching Rust code.

macro_rules! include_help {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/help-strings/",
            $name,
            ".txt"
        ))
    };
}

pub fn main_view() -> &'static str {
    include_help!("main_view")
}

pub fn query() -> &'static str {
    include_help!("query")
}

pub fn go_to_line() -> &'static str {
    include_help!("go_to_line")
}

pub fn find() -> &'static str {
    include_help!("find")
}

pub fn go_to_column() -> &'static str {
    include_help!("go_to_column")
}

pub fn sort_filter() -> &'static str {
    include_help!("sort_filter")
}

pub fn pivot_melt() -> &'static str {
    include_help!("pivot_melt")
}

pub fn export() -> &'static str {
    include_help!("export")
}

pub fn copy() -> &'static str {
    include_help!("copy")
}

pub fn inspector() -> &'static str {
    include_help!("inspector")
}

pub fn info_panel() -> &'static str {
    include_help!("info_panel")
}

pub fn chart() -> &'static str {
    include_help!("chart")
}

pub fn home() -> &'static str {
    include_help!("home")
}

pub fn views() -> &'static str {
    include_help!("views")
}

pub fn analysis_distribution_detail() -> &'static str {
    include_help!("analysis_distribution_detail")
}

pub fn analysis_correlation_detail() -> &'static str {
    include_help!("analysis_correlation_detail")
}

pub fn analysis_distribution() -> &'static str {
    include_help!("analysis_distribution")
}

pub fn analysis_describe() -> &'static str {
    include_help!("analysis_describe")
}

pub fn analysis_correlation_matrix() -> &'static str {
    include_help!("analysis_correlation_matrix")
}

pub fn analysis_data_quality() -> &'static str {
    include_help!("analysis_data_quality")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The data table has no on-screen indicator for its display toggles, so
    /// the help overlay is the only place a user can discover them. Keep both
    /// listed.
    #[test]
    fn main_view_help_documents_display_toggles() {
        let help = main_view();
        assert!(help.contains("#:"), "row numbers toggle missing from help");
        assert!(
            help.contains("F:"),
            "number formatting toggle missing from help"
        );
        assert!(
            help.contains("number formatting"),
            "the F toggle needs a description a user can search for"
        );
    }

    /// Data Quality's help is short keyed tables: at 80 columns every line fits
    /// the overlay as written, with no line folded under itself, in UTF-8 and
    /// in ASCII.
    #[test]
    fn data_quality_help_fits_the_overlay_at_80_columns() {
        use ratatui::buffer::Buffer;
        use ratatui::layout::Rect;
        let help = analysis_data_quality();
        let ctx = crate::render::context::RenderContext::for_test();
        let area = Rect::new(0, 0, 80, 24);
        for text in [help.to_string(), crate::glyphs::instructions_in_ascii(help)] {
            let mut shown = Vec::new();
            for scroll in 0..text.lines().count() {
                let mut buf = Buffer::empty(area);
                let mut at = scroll;
                crate::render::overlays::render_help_overlay(
                    area,
                    &mut buf,
                    "Data Quality Help",
                    &text,
                    &mut at,
                    &ctx,
                );
                shown.extend((0..area.height).map(|y| {
                    (0..area.width)
                        .map(|x| buf[(x, y)].symbol().to_string())
                        .collect::<String>()
                }));
            }
            // What the overlay draws: ASCII twins when the locale is not UTF-8.
            let drawn = crate::glyphs::asciify_instructions(&text);
            for line in drawn.lines().filter(|line| !line.trim().is_empty()) {
                assert!(
                    shown.iter().any(|row| row.contains(line.trim_end())),
                    "folded or cut at 80 columns: {line:?}"
                );
            }
        }
        assert!(
            !help.to_lowercase().contains("budget"),
            "there is no compute budget any more"
        );
    }

    /// Every keyed row in a help section, as written (UTF-8), puts its
    /// description at the section's one column, at least two spaces past its
    /// key. A key followed by a single space reads as prose to the ASCII
    /// re-padding and sits out of line on screen, so it counts as a row too.
    #[test]
    fn every_help_section_shares_one_description_column() {
        use crate::glyphs::display_width;
        fn indent(line: &str) -> usize {
            line.len() - line.trim_start_matches(' ').len()
        }
        // Where a row's description starts: past the first run of two or
        // more spaces after the indent, with text after it.
        fn description(line: &str) -> Option<usize> {
            let lead = indent(line);
            let gap = lead + line[lead..].find("  ")?;
            let start = line.len() - line[gap..].trim_start_matches(' ').len();
            (start < line.len()).then_some(start)
        }
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/help-strings");
        let mut rows = 0;
        let mut misaligned = Vec::new();
        for entry in std::fs::read_dir(dir).expect("help-strings dir") {
            let path = entry.expect("dir entry").path();
            let name = path.file_name().expect("file name").to_string_lossy();
            let text = std::fs::read_to_string(&path).expect("help file");
            let lines: Vec<&str> = text.lines().collect();
            for section in lines.split(|line| line.trim().is_empty()) {
                let keyed: Vec<(&str, usize)> = section
                    .iter()
                    .filter_map(|line| description(line).map(|at| (*line, at)))
                    .collect();
                let Some(&(first, at)) = keyed.first() else {
                    continue;
                };
                let column = display_width(&first[..at]);
                let key_indent = indent(first);
                for &(line, at) in &keyed {
                    rows += 1;
                    if display_width(&line[..at]) != column {
                        misaligned.push(format!("{name}: {line:?} (section at {column})"));
                    }
                }
                for &line in section.iter().filter(|line| description(line).is_none()) {
                    // A key and one space, then its description. Prose that
                    // runs on after a colon goes on in lower case.
                    let Some(colon) = line.find(": ") else {
                        continue;
                    };
                    let prose = line[colon + 2..].starts_with(|c: char| c.is_lowercase());
                    if indent(line) == key_indent
                        && !prose
                        && display_width(&line[..colon]) + 3 > column
                    {
                        misaligned.push(format!("{name}: {line:?} (section at {column})"));
                    }
                }
            }
        }
        assert!(rows > 100, "the help files' keyed rows were checked");
        assert!(misaligned.is_empty(), "{}", misaligned.join("\n"));
    }
}
