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

pub fn editing() -> &'static str {
    include_help!("editing")
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

pub fn info_panel() -> &'static str {
    include_help!("info_panel")
}

pub fn chart() -> &'static str {
    include_help!("chart")
}

pub fn template() -> &'static str {
    include_help!("template")
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
