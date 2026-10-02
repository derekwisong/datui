//! The SQL `GROUP BY` planner, run on arbitrary text.
//!
//! `sql_group::plan` reads every SQL statement the query prompt runs, to decide whether
//! Enter on a result row can drill into the rows behind it. It resolves keys by
//! ordinal, alias and expression and writes a statement of its own, so any text must
//! give a plan or `None`, never a panic.

use datui_lib::sql_group::plan;

/// No human types a statement longer than this.
const MAX_SQL_LEN: usize = 4096;

/// The columns of `df` while fuzzing, so keys resolve to columns as well as to
/// computed expressions.
const COLUMNS: &[&str] = &["dept", "salary", "ts", "name", "Dept"];

pub fn run(sql: &str) {
    if sql.len() > MAX_SQL_LEN {
        return;
    }
    // A plan needs the result's width to match the select list; try the widths a
    // statement a person writes has.
    for width in 0..8 {
        if let Some(plan) = plan(sql, COLUMNS, width) {
            assert!(plan.keys.iter().all(|k| k.result_index < width));
        }
    }
}
