//! Re-export CLI definitions from the shared datui-cli crate.

pub use datui_cli::{
    Args, CacheAction, Command, CompressionFormat, ConfigAction, FileFormat, FormatChoice,
    FormatsAction, InferTypes, Lines, ReadMode, RemoteRead, Stored, ViewsAction, one_table,
    settings,
};

/// `argv` read as the command line is, for a host such as the Python binding that
/// spells its options as flags and `-c`. The error is clap's, on one line.
pub fn parse_args<I, T>(argv: I) -> Result<Args, String>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    use clap::Parser;
    Args::try_parse_from(argv).map_err(|e| {
        let text = e.to_string();
        text.lines()
            .next()
            .unwrap_or_default()
            .trim_start_matches("error: ")
            .to_string()
    })
}
