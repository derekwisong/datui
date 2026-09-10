//! Fuzzy matching, checked against the contract the highlighter relies on.
//!
//! `best_match` returns character positions that the home screen uses to highlight the
//! matched characters. It builds two parallel `Vec<char>` for the haystack, one as
//! written and one lowercased, and indexes both with the same offsets. Unicode
//! lowercasing can change the character count (`İ` becomes two characters), so the
//! parallel indexing is only sound because of an explicit length guard. This target
//! exists to keep that guard honest: every returned position must be a valid index
//! into the haystack's characters.
#![no_main]

use datui_lib::fuzzy::{best_match, is_match};
use libfuzzer_sys::fuzz_target;

/// Matching is quadratic in the haystack length by design (every start position is
/// tried). Bounding the inputs keeps a libFuzzer timeout meaningful as a hang report
/// rather than a report of the algorithm being what it is.
const MAX_LEN: usize = 1024;

fuzz_target!(|input: (&str, &str)| {
    let (needle, haystack) = input;
    if needle.len() > MAX_LEN || haystack.len() > MAX_LEN {
        return;
    }

    // Exercised for panics and hangs, but deliberately not asserted to agree with
    // `best_match`. The two lower case differently — `is_match` character by character,
    // `best_match` over whole strings — so they disagree on context-sensitive mappings
    // such as Greek final sigma (needle "ΑΣ" against haystack "ας"). That divergence is
    // reachable but harmless today: nothing outside this module calls `is_match`, so it
    // does not in fact gate anything. Asserting a contract no caller relies on would
    // just make the fuzzer report Unicode trivia.
    let _ = is_match(needle, haystack);

    let Some(m) = best_match(needle, haystack) else {
        return;
    };

    let hay: Vec<char> = haystack.chars().collect();
    let mut previous: Option<usize> = None;
    for &pos in &m.positions {
        assert!(
            pos < hay.len(),
            "position {pos} is out of bounds for a {} character haystack {haystack:?}",
            hay.len()
        );
        if let Some(p) = previous {
            assert!(
                pos > p,
                "positions must strictly ascend, got {p} then {pos} for needle {needle:?}, haystack {haystack:?}"
            );
        }
        previous = Some(pos);
        // Index it the way the highlighter does.
        let _ = hay[pos];
    }

    // One position per needle character, so highlighting covers the whole match.
    let expected = needle.to_lowercase().chars().count();
    assert_eq!(
        m.positions.len(),
        expected,
        "expected {expected} positions for needle {needle:?}, got {}",
        m.positions.len()
    );
});
