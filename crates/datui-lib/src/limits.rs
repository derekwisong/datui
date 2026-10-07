//! The caps of `[limits]`, read where a reader stops. Set once from the config at
//! startup; the defaults until then.

use std::sync::RwLock;

use crate::config::LimitsConfig;

static LIMITS: RwLock<LimitsConfig> = RwLock::new(LimitsConfig::DEFAULT);

/// The limits in force.
pub fn get() -> LimitsConfig {
    *LIMITS.read().unwrap_or_else(|e| e.into_inner())
}

/// Use `limits` from now on.
pub fn set(limits: LimitsConfig) {
    *LIMITS.write().unwrap_or_else(|e| e.into_inner()) = limits;
}

/// The end of a note that `left` were left out past `limit`, naming the `[limits]`
/// key that raises it: `12 records left out: past the first 64 · limits.x raises it`.
pub(crate) fn left_out(left: &str, limit: usize, key: &str) -> String {
    format!(
        "{left} left out: past the first {} {} limits.{key} raises it",
        crate::numfmt::group_chrome(limit),
        crate::glyphs::get().middot
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A note of what a cap left out names the key that raises it.
    #[test]
    fn a_left_out_note_names_its_key() {
        let note = left_out("3 records", 64 << 20, "indexed_records");
        assert!(note.starts_with("3 records left out: past the first 67,108,864"));
        assert!(note.ends_with("limits.indexed_records raises it"), "{note}");
    }

    /// `[limits]` in a config file sets the caps; what it leaves out stays the default.
    #[test]
    fn the_section_reads_from_toml() {
        let limits: LimitsConfig =
            toml::from_str("elf_symbols = 5\nmidi_bytes = \"1MiB\"").unwrap();
        assert_eq!(limits.elf_symbols, 5);
        assert_eq!(limits.midi_bytes.bytes(), 1 << 20);
        assert_eq!(
            limits.indexed_records,
            LimitsConfig::DEFAULT.indexed_records
        );
    }
}
