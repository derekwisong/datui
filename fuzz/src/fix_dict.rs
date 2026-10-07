//! FIX dictionaries: a QuickFIX XML data dictionary, read by a hand-written scanner of
//! tags and attributes, and the TOML form, on arbitrary text.
//!
//! Neither may panic; a dictionary that parses keeps its names within bounds and can
//! be layered under the built-in one.

use datui_lib::formats::fix::dict::{Dictionary, Layers, MAX_NAME, is_fix_toml};
use std::sync::Arc;

pub fn run(bytes: &[u8]) {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    let _ = is_fix_toml(text);
    let toml = Dictionary::parse_toml(text, None);
    let xml = Dictionary::parse_xml(text, "fuzz", None);
    for dict in [toml.ok(), xml.ok().flatten()].into_iter().flatten() {
        for def in dict.tags.values() {
            assert!(def.name.as_ref().is_none_or(|n| n.len() <= MAX_NAME));
            assert!(def.enums.values().all(|n| n.len() <= MAX_NAME));
        }
        let layers = Layers::new(vec![Arc::new(dict)]);
        let all = layers.applying(None, None, None);
        for tag in [8, 35, 5001] {
            let _ = layers.resolve(&all, tag);
        }
    }
}
