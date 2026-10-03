//! Re-export CLI definitions from the shared datui-cli crate.

pub use datui_cli::{
    Args, CacheAction, Command, CompressionFormat, ConfigAction, FileFormat, FormatChoice,
    FormatsAction, InferTypes, Lines, ReadMode, RemoteRead, Stored, ViewsAction, one_table,
};
