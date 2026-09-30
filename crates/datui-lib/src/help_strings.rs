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
        assert!(help.contains("N:"), "row numbers toggle missing from help");
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
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
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
}
