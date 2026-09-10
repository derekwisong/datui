//! Number rendering, checked against its own width calculation.
//!
//! `NumberFormat` has two independent implementations of the same question. `width_*`
//! computes a display width arithmetically without building a string, and `write_*`
//! renders digits back-to-front into a fixed 64-byte stack buffer and returns the width
//! it produced. Callers size table columns from those widths, so a disagreement between
//! them corrupts the layout rather than failing visibly. The separator characters come
//! from user TOML, so they can be any `char`, including 4-byte ones.
#![no_main]

use arbitrary::Arbitrary;
use datui_lib::numfmt::{Grouping, NumberFormat};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
struct Input<'a> {
    grouping: GroupingChoice,
    group_sep: char,
    decimal_sep: char,
    floats: bool,
    float_precision: Option<u8>,
    ints: Vec<i64>,
    uints: Vec<u64>,
    reals: Vec<f64>,
    /// Fed straight to `regroup_decimal`, which documents itself as taking an
    /// already-rendered decimal string. Reaching it through `write_f64` only ever
    /// produces strings Rust's own float formatter emits, so it is exercised directly
    /// as well.
    rendered: Vec<&'a str>,
}

#[derive(Arbitrary, Debug)]
enum GroupingChoice {
    None,
    Thousands,
    Indian,
}

impl From<GroupingChoice> for Grouping {
    fn from(c: GroupingChoice) -> Self {
        match c {
            GroupingChoice::None => Grouping::None,
            GroupingChoice::Thousands => Grouping::Thousands,
            GroupingChoice::Indian => Grouping::Indian,
        }
    }
}

fuzz_target!(|input: Input| {
    let fmt = NumberFormat {
        grouping: input.grouping.into(),
        group_sep: input.group_sep,
        decimal_sep: input.decimal_sep,
        floats: input.floats,
        // Precision is a digit count. Anything past f64's precision is the config
        // layer's problem, not the renderer's, and huge values only make the fuzzer
        // build enormous strings.
        float_precision: input.float_precision.map(|p| p % 20),
    };

    let mut out = String::new();
    let mut scratch = String::new();

    for &v in &input.ints {
        out.clear();
        let written = fmt.write_i64(v, &mut out);
        assert_eq!(
            written,
            out.chars().count(),
            "write_i64 reported width {written} but appended {out:?} for {v} with {fmt:?}"
        );
        assert_eq!(
            written,
            fmt.width_i64(v),
            "width_i64 disagrees with write_i64 for {v} with {fmt:?}"
        );
    }

    for &v in &input.uints {
        out.clear();
        let written = fmt.write_u64(v, &mut out);
        assert_eq!(
            written,
            out.chars().count(),
            "write_u64 reported width {written} but appended {out:?} for {v} with {fmt:?}"
        );
        assert_eq!(
            written,
            fmt.width_u64(v),
            "width_u64 disagrees with write_u64 for {v} with {fmt:?}"
        );
    }

    for rendered in input.rendered.iter().take(64) {
        // Not a valid target for enormous inputs: the width is a count of characters,
        // and a string longer than a terminal is not what this renders.
        if rendered.len() > 4096 {
            continue;
        }
        out.clear();
        let written = fmt.regroup_decimal(rendered, &mut out);
        assert_eq!(
            written,
            out.chars().count(),
            "regroup_decimal reported width {written} but appended {out:?} for {rendered:?} with {fmt:?}"
        );
    }

    for &v in &input.reals {
        out.clear();
        let written = fmt.write_f64(v, &mut scratch, &mut out);
        assert_eq!(
            written,
            out.chars().count(),
            "write_f64 reported width {written} but appended {out:?} for {v} with {fmt:?}"
        );
    }
});
