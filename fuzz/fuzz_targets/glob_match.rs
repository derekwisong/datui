//! The column-name glob matcher.
//!
//! `Glob` is a hand-rolled two-pointer wildcard matcher with backtracking on the last
//! `*`. Patterns come from the user's config, so both the pattern and the column name
//! are untrusted. The interesting failure here is not a panic but a hang: a
//! backtracking matcher that resets its pointers wrongly can fail to make progress.
#![no_main]

use datui_lib::numfmt::Glob;
use libfuzzer_sys::fuzz_target;

/// Short enough that libFuzzer's default timeout means the matcher failed to
/// terminate, not that the input was enormous.
const MAX_LEN: usize = 256;

fuzz_target!(|input: (&str, &str)| {
    let (pattern, name) = input;
    if pattern.len() > MAX_LEN || name.len() > MAX_LEN {
        return;
    }

    let glob = Glob::new(pattern);
    let matched = glob.matches(name);

    // A pattern with no wildcard is an equality test, and that is the fast path the
    // matcher takes, so it needs to agree with the general one.
    if !pattern.contains('*') && !pattern.contains('?') {
        assert_eq!(
            matched,
            pattern == name,
            "wildcard-free pattern {pattern:?} did not behave as equality against {name:?}"
        );
    }

    // A pattern is always matched by itself once its wildcards are gone, and `*`
    // matches everything.
    assert!(Glob::new("*").matches(name), "`*` failed to match {name:?}");
});
