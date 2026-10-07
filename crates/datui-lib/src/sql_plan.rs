//! Rewrites of a SQL statement's plan, so its rows page the way the table reads them:
//! a stable order for the pages of a sort, and subquery values counted once.

use std::sync::Arc;

use polars::prelude::*;

use crate::table::for_each_input;

/// The columns a SQL statement's plan orders its result by, leading ones first, and
/// whether each runs descending: down from the top through what keeps the order (a
/// LIMIT, a projection of plain columns) to the sort. Stops at the first key that
/// is an expression rather than a column; empty when no sort is on top.
pub(crate) fn ordered_by(plan: &polars::lazy::dsl::DslPlan) -> Vec<(String, bool)> {
    use polars::lazy::dsl::DslPlan;
    let mut node = plan;
    loop {
        node = match node {
            DslPlan::Slice { input, .. }
            | DslPlan::Filter { input, .. }
            | DslPlan::Cache { input, .. } => input,
            DslPlan::IR { dsl, .. } => dsl,
            DslPlan::Select { expr, input, .. }
                if expr.iter().all(|e| matches!(e, Expr::Column(_))) =>
            {
                input
            }
            DslPlan::Sort {
                by_column,
                sort_options,
                ..
            } => {
                let descending = &sort_options.descending;
                return by_column
                    .iter()
                    .map_while(|e| match e {
                        Expr::Column(name) => Some(name.to_string()),
                        _ => None,
                    })
                    .enumerate()
                    .map(|(i, name)| {
                        let down = descending
                            .get(i)
                            .or(descending.first())
                            .copied()
                            .unwrap_or(false);
                        (name, down)
                    })
                    .collect();
            }
            _ => return Vec::new(),
        };
    }
}

/// `plan` giving its rows in one order on every read. Each page is its own read of
/// the view, so a node free to return rows in any order lets pages repeat some rows
/// and skip others, a `LIMIT` keep different groups on each read, and a sort's ties
/// arrive in a different order each time. Every sort keeps tied rows in the order
/// they come, as `sort_options` does (Polars SQL sorts unstably and offers no
/// option), and every grouping, distinct, union and join keeps its input's order,
/// except a grouping sorted by all its keys (see [`sorts_by_group_keys`]). Only the
/// parts of the plan holding such a node are rewritten.
pub(crate) fn stable_order(plan: &mut polars::lazy::dsl::DslPlan) {
    order_stably(plan, false);
}

/// [`stable_order`], where `groups_sorted` says a sort above `plan` orders the rows
/// of the grouping it reads through `plan`.
fn order_stably(plan: &mut polars::lazy::dsl::DslPlan, groups_sorted: bool) {
    use polars::lazy::dsl::DslPlan;
    let unordered = |node: &DslPlan| match node {
        DslPlan::Sort { sort_options, .. } => !sort_options.maintain_order,
        DslPlan::GroupBy { maintain_order, .. } => !maintain_order,
        DslPlan::Distinct { options, .. } => !options.maintain_order,
        DslPlan::Union { args, .. } => !args.maintain_order,
        DslPlan::Join { options, .. } => options.args.maintain_order == MaintainOrderJoin::None,
        _ => false,
    };
    if !plan.into_iter().any(unordered) {
        return;
    }
    // Passed down the path sorts_by_group_keys walked to the grouping, and no other.
    let inputs_sorted = match plan {
        DslPlan::Sort { .. } => sorts_by_group_keys(plan),
        DslPlan::Select { .. } | DslPlan::IR { .. } => groups_sorted,
        _ => false,
    };
    match plan {
        DslPlan::Sort { sort_options, .. } => sort_options.maintain_order = true,
        DslPlan::GroupBy { maintain_order, .. } if !groups_sorted => *maintain_order = true,
        DslPlan::Distinct { options, .. } => options.maintain_order = true,
        DslPlan::Union { args, .. } => args.maintain_order = true,
        DslPlan::Join { options, .. } => {
            Arc::make_mut(options).args.maintain_order = MaintainOrderJoin::LeftRight;
        }
        _ => {}
    }
    if let DslPlan::IR { dsl, .. } = plan {
        // A plan asked for its schema is wrapped as IR, which would run as converted:
        // rewrite the plan it came from, and leave the IR behind.
        let mut inner = Arc::unwrap_or_clone(dsl.clone());
        order_stably(&mut inner, inputs_sorted);
        *plan = inner;
        return;
    }
    for_each_input(plan, &mut |input| order_stably(input, inputs_sorted));
}

/// Whether `sort` sorts the rows of a grouping by every one of its keys, so the
/// order the groups arrive in never shows and keeping it is wasted time (#523).
/// Keys are unique per group, so such a sort has no ties, wherever it puts NULLs: a
/// NULL key is one group, and NaN and -0.0 group as the sort compares them. The
/// groups must reach the sort through projections that only pass or rename
/// columns: a filter or a computed column could depend on the order they arrive
/// in, as `ROW_NUMBER() OVER ()` does. A key counts only as a plain column of the
/// sort, which is what polars-sql makes of an alias, an ordinal or a key's own
/// name. It evaluates any other expression against the grouped columns, which
/// already hold the keys: `ORDER BY x % 4 * 2` over `GROUP BY x % 4 * 2` sorts by
/// the key's `% 4 * 2`, and ties keys that differ.
fn sorts_by_group_keys(sort: &polars::lazy::dsl::DslPlan) -> bool {
    use polars::lazy::dsl::DslPlan;
    let DslPlan::Sort {
        input, by_column, ..
    } = sort
    else {
        return false;
    };
    // The sort's columns, under the names they have at each node on the way down.
    let mut names: Vec<PlSmallStr> = by_column
        .iter()
        .filter_map(|e| match e {
            Expr::Column(name) => Some(name.clone()),
            _ => None,
        })
        .collect();
    let mut node: &DslPlan = input;
    loop {
        match node {
            DslPlan::Select { input, expr, .. } => {
                // (output name, input name) of each column.
                let Some(renames) = expr
                    .iter()
                    .map(|e| match e {
                        Expr::Column(c) => Some((c, c)),
                        Expr::Alias(inner, alias) => match &**inner {
                            Expr::Column(c) => Some((alias, c)),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    return false;
                };
                names = names
                    .iter()
                    .filter_map(|name| {
                        renames
                            .iter()
                            .find(|(out, _)| *out == name)
                            .map(|(_, source)| (*source).clone())
                    })
                    .collect();
                node = input;
            }
            DslPlan::IR { dsl, .. } => node = dsl,
            DslPlan::GroupBy {
                keys,
                options,
                apply: None,
                ..
            } if **options == GroupbyOptions::default() => {
                return keys.iter().all(|key| {
                    let meta = key.clone().meta();
                    !meta.has_multiple_outputs()
                        && meta.output_name().is_ok_and(|key| names.contains(&key))
                });
            }
            _ => return false,
        }
    }
}

/// `plan` with an `IN (SELECT …)` subquery's values counted once instead of once per
/// row. polars-sql adds the values as a one-row list column and filters on
/// `col.first().list.len()` and `col.first().list.contains(NULL)`. The streaming
/// engine repeats that `first()` for every row of a batch and the list kernels copy
/// the list into each: rows times values, 26 GB for a page of a 100k-row table where
/// a third of the rows match (#509). Asked of the values exploded, the questions read
/// the one list; an empty list explodes to no rows, so both answers are unchanged.
/// Only those questions are rewritten: a user's own `ARRAY_LENGTH(FIRST(l))` differs
/// once exploded when the first list is NULL.
pub(crate) fn count_subquery_values_once(plan: &mut polars::lazy::dsl::DslPlan) {
    use polars::lazy::dsl::{DslPlan, FunctionExpr, ListFunction};
    fn ask_once(e: Expr, names: &[PlSmallStr]) -> Expr {
        if !asks_of_subquery_values(&e, names) {
            return e;
        }
        let Expr::Function {
            mut input,
            function: FunctionExpr::ListExpr(function),
        } = e
        else {
            return e;
        };
        let values = input.swap_remove(0).explode(ExplodeOptions {
            empty_as_null: false,
            keep_nulls: true,
        });
        match function {
            ListFunction::Length => values.len(),
            _ => values.null_count().gt(lit(0)),
        }
    }
    if !plan.into_iter().any(asks_per_row) {
        return;
    }
    match plan {
        // A plan asked for its schema is wrapped as IR, which would run as converted:
        // rewrite the plan it came from, and leave the IR behind.
        DslPlan::IR { dsl, .. } => {
            let mut inner = Arc::unwrap_or_clone(dsl.clone());
            count_subquery_values_once(&mut inner);
            *plan = inner;
            return;
        }
        DslPlan::Filter { input, predicate } => {
            let names = subquery_value_columns(input);
            *predicate = predicate.clone().map_expr(|e| ask_once(e, &names));
        }
        _ => {}
    }
    for_each_input(plan, &mut count_subquery_values_once);
}

/// The columns of `schema`, `plan`'s columns, that carry what polars-sql added to
/// hold `IN` subqueries' values (see [`subquery_value_columns`]). A WHERE's projection
/// drops them, but a QUALIFY keeps them in its result, a list of every value on every
/// row, and a statement reading its result as a table carries them on (#519), under
/// a join's suffix when both sides hold one. Matched by the name polars-sql gave
/// them, which is unique to the process, so no column of the user's is taken for one.
pub(crate) fn leftover_subquery_value_columns(
    plan: &mut polars::lazy::dsl::DslPlan,
    schema: &Schema,
) -> Vec<PlSmallStr> {
    use polars::lazy::dsl::DslPlan;
    fn find(plan: &mut DslPlan, values: &mut Vec<PlSmallStr>, suffixes: &mut Vec<PlSmallStr>) {
        match plan {
            DslPlan::IR { dsl, .. } => {
                let mut inner = Arc::unwrap_or_clone(dsl.clone());
                find(&mut inner, values, suffixes);
                return;
            }
            DslPlan::Join { options, .. } => suffixes.push(options.args.suffix().clone()),
            _ => {}
        }
        values.extend(subquery_value_columns(plan));
        for_each_input(plan, &mut |input| find(input, values, suffixes));
    }
    fn carries(name: &str, values: &[PlSmallStr], suffixes: &[PlSmallStr]) -> bool {
        values.iter().any(|v| v == name)
            || suffixes.iter().any(|s| {
                name.strip_suffix(s.as_str())
                    .is_some_and(|rest| carries(rest, values, suffixes))
            })
    }
    let (mut values, mut suffixes) = (Vec::new(), Vec::new());
    find(plan, &mut values, &mut suffixes);
    if values.is_empty() {
        return Vec::new();
    }
    suffixes.retain(|s| !s.is_empty());
    schema
        .iter_names()
        .filter(|name| carries(name, &values, &suffixes))
        .cloned()
        .collect()
}

/// Whether `node` filters on a question of an `IN` subquery's values that the
/// streaming engine answers once per row.
pub(crate) fn asks_per_row(node: &polars::lazy::dsl::DslPlan) -> bool {
    let polars::lazy::dsl::DslPlan::Filter { input, predicate } = node else {
        return false;
    };
    let names = subquery_value_columns(input);
    !names.is_empty()
        && predicate
            .into_iter()
            .any(|e| asks_of_subquery_values(e, &names))
}

/// The columns polars-sql adds beside `plan` to hold `IN` subqueries' values: each
/// subquery is selected as one aliased list and concatenated horizontally, broadcast
/// to the frame's rows (`SQLContext::process_subqueries`).
fn subquery_value_columns(plan: &polars::lazy::dsl::DslPlan) -> Vec<PlSmallStr> {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::HConcat { inputs, options } if options.broadcast_unit_length => inputs
            .iter()
            .skip(1)
            .filter_map(|input| match input {
                DslPlan::Select { expr, .. } => match expr.as_slice() {
                    [Expr::Alias(_, name)] => Some(name.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        DslPlan::IR { dsl, .. } => subquery_value_columns(dsl),
        _ => Vec::new(),
    }
}

/// Whether `e` is polars-sql asking how many values an `IN` subquery returned, or
/// whether one is NULL: `list.len` or `list.contains(NULL)` of `col(name).first()`,
/// where `name` is one of `names`, the columns holding the values.
fn asks_of_subquery_values(e: &Expr, names: &[PlSmallStr]) -> bool {
    use polars::lazy::dsl::{FunctionExpr, ListFunction};
    let values = |e: &Expr| {
        matches!(e, Expr::Agg(AggExpr::First(c))
            if matches!(&**c, Expr::Column(name) if names.contains(name)))
    };
    match e {
        Expr::Function {
            input,
            function: FunctionExpr::ListExpr(ListFunction::Length),
        } => matches!(input.as_slice(), [set] if values(set)),
        Expr::Function {
            input,
            function: FunctionExpr::ListExpr(ListFunction::Contains { nulls_equal: true }),
        } => {
            matches!(input.as_slice(), [set, Expr::Literal(item)] if values(set) && item.is_null())
        }
        _ => false,
    }
}
