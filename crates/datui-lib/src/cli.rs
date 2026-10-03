//! Re-export CLI definitions from the shared datui-cli crate.

pub use datui_cli::{
    Args, Command, CompressionFormat, ConfigAction, FileFormat, FormatChoice, FormatsAction, Lines,
    ReadMode, RemoteRead, Stored, one_table,
};
