//! The query language parser, run on arbitrary text.
//!
//! `parse_query` is a hand-written tokeniser and recursive-descent parser that runs on
//! whatever the user types into the query bar. It slices token vectors by index in
//! several places, so a malformed query must return `Err`, never panic.
#![no_main]

use datui_lib::query::{parse_query, sanitize_query_error};
use libfuzzer_sys::fuzz_target;

/// No human types a query longer than this, and past it the fuzzer spends all its
/// time on inputs that only exercise how deep the recursion goes.
const MAX_QUERY_LEN: usize = 4096;

fuzz_target!(|query: &str| {
    if query.len() > MAX_QUERY_LEN {
        return;
    }

    match parse_query(query) {
        // No invariant is asserted on a successful parse. The contract worth enforcing
        // here is that the parser terminates and reports failure as an error, and
        // guesses about the shape of a valid result turn out to be wrong: an early
        // version required output column names to be non-empty, and the fuzzer promptly
        // produced `select col[""]`, which asks for an empty name and gets one.
        Ok((cols, filter, by, names)) => {
            // Touch the results so the parse cannot be optimised away, and so the
            // expression trees are actually walked.
            let _ = cols.len() + by.len() + names.len() + usize::from(filter.is_some());
        }
        // Every error message reaches the user through this, so it has to survive
        // whatever the parser and Polars put into it.
        Err(msg) => {
            let _ = sanitize_query_error(&msg);
        }
    }
});
