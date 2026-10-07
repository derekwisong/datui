//! What a SQL `GROUP BY` result was grouped from, read from the statement itself, so
//! Enter on a row of the result can show the rows behind it.
//!
//! Only a plain grouping of the one table qualifies: `SELECT keys…, aggregates… FROM df
//! [WHERE …] GROUP BY keys [HAVING …] [ORDER BY …] [LIMIT …]`, each key selected. Joins,
//! subqueries, CTEs, set operations, window functions, `DISTINCT ON`, `GROUP BY ALL` and
//! keys that are not in the select list give no plan, so drill-down stays off rather
//! than guess.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Distinct, Expr, FunctionArguments, GroupByExpr, Ident, ObjectNamePart, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor, WildcardAdditionalOptions,
    visit_expressions_mut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserOptions};

/// How a grouped statement relates its result to its source rows.
#[derive(Debug, PartialEq)]
pub struct GroupPlan {
    /// A statement over the same table and `WHERE` that returns every source row, each
    /// computed key added as a column named by [`KeySource::Computed`].
    pub source_sql: String,
    pub keys: Vec<PlanKey>,
    /// Whether the statement says which groups come back in which order: an ORDER BY,
    /// or a LIMIT that picks some.
    pub ordered: bool,
}

/// One grouping key: where it sits in the result and how the source computes it.
#[derive(Debug, PartialEq)]
pub struct PlanKey {
    /// Position of the key's column in the result.
    pub result_index: usize,
    pub source: KeySource,
}

#[derive(Debug, PartialEq)]
pub enum KeySource {
    /// A column of the source as it stands.
    Column(String),
    /// An expression, added to the source rows under this name.
    Computed(String),
}

/// Prefix of the columns `source_sql` adds for computed keys; the drill drops them.
const KEY_PREFIX: &str = "__datui_group_key_";

/// The plan for `sql`, run against a table whose columns are `columns` and giving a
/// result `result_width` columns wide, or `None` when the statement is not a grouping
/// whose rows can be traced back reliably.
pub fn plan(sql: &str, columns: &[&str], result_width: usize) -> Option<GroupPlan> {
    // Parsed as polars-sql parses it, so the two read the same statement.
    let statements = Parser::new(&GenericDialect)
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .try_with_sql(sql)
        .ok()?
        .parse_statements()
        .ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    if !plain_query(query) || !Plain::check(query) {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !plain_select(select) {
        return None;
    }
    let GroupByExpr::Expressions(group_by, modifiers) = &select.group_by else {
        return None;
    };
    if group_by.is_empty() || !modifiers.is_empty() {
        return None;
    }
    let [from] = select.from.as_slice() else {
        return None;
    };
    let TableFactor::Table { name, alias, .. } = &from.relation else {
        return None;
    };
    // Every select item is one result column, in order; a wildcard or a
    // multi-column alias breaks that.
    let items: Vec<(&Expr, Option<&Ident>)> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(e) => Some((e, None)),
            SelectItem::ExprWithAlias { expr, alias } => Some((expr, Some(alias))),
            _ => None,
        })
        .collect::<Option<_>>()?;
    if items.len() != result_width {
        return None;
    }
    // `df.dept` and `t.dept` name the column `dept`.
    let qualifiers: Vec<&str> = name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .into_iter()
        .chain(alias.as_ref().map(|a| &a.name))
        .map(|ident| ident.value.as_str())
        .collect();
    let normalize = |e: &Expr| normalized(e, &qualifiers);

    let mut keys = Vec::with_capacity(group_by.len());
    let mut computed = Vec::new();
    for key in group_by {
        let (index, expr) = resolve_key(key, &items, columns, &normalize)?;
        let source = match normalize(expr) {
            Expr::Identifier(ident) if columns.contains(&ident.value.as_str()) => {
                KeySource::Column(ident.value)
            }
            _ => {
                let name = format!("{KEY_PREFIX}{}", computed.len());
                computed.push(format!("{expr} AS \"{name}\""));
                KeySource::Computed(name)
            }
        };
        keys.push(PlanKey {
            result_index: index,
            source,
        });
    }

    let mut source_sql = String::from("SELECT *");
    for column in &computed {
        source_sql.push_str(", ");
        source_sql.push_str(column);
    }
    source_sql.push_str(&format!(" FROM {from}"));
    if let Some(selection) = &select.selection {
        source_sql.push_str(&format!(" WHERE {selection}"));
    }
    let ordered = query.order_by.is_some() || query.limit_clause.is_some() || query.fetch.is_some();
    Some(GroupPlan {
        source_sql,
        keys,
        ordered,
    })
}

/// The columns of `sql`'s result, named in `result`, that are columns of the table it
/// reads (named in `columns`) unchanged, renamed or not: each result name with the
/// column's. Only a plain statement's select list is read: `*`, a column, or a column
/// `AS` a name. Anything else, or a statement that is not plain, gives none.
pub fn passed_through(sql: &str, columns: &[&str], result: &[&str]) -> Vec<(String, String)> {
    let Some(statements) = Parser::new(&GenericDialect)
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .try_with_sql(sql)
        .and_then(|mut parser| parser.parse_statements())
        .ok()
    else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    if !plain_query(query) || !Plain::check(query) {
        return Vec::new();
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };
    if !plain_select(select) {
        return Vec::new();
    }
    let [from] = select.from.as_slice() else {
        return Vec::new();
    };
    let TableFactor::Table { name, alias, .. } = &from.relation else {
        return Vec::new();
    };
    let qualifiers: Vec<&str> = name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .into_iter()
        .chain(alias.as_ref().map(|a| &a.name))
        .map(|ident| ident.value.as_str())
        .collect();
    let column = |e: &Expr| match normalized(e, &qualifiers) {
        Expr::Identifier(ident) if columns.contains(&ident.value.as_str()) => Some(ident.value),
        _ => None,
    };
    let plain = |options: &WildcardAdditionalOptions| {
        options.opt_ilike.is_none()
            && options.opt_exclude.is_none()
            && options.opt_except.is_none()
            && options.opt_replace.is_none()
            && options.opt_rename.is_none()
    };
    // A name given twice is an error in Polars, so a computed column never shares a
    // name with one kept here.
    let mut kept: Vec<(String, String)> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => kept.extend(column(e).map(|c| (c.clone(), c))),
            SelectItem::ExprWithAlias { expr, alias } => {
                kept.extend(column(expr).map(|c| (alias.value.clone(), c)));
            }
            SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options)
                if plain(options) =>
            {
                kept.extend(columns.iter().map(|c| (c.to_string(), c.to_string())));
            }
            _ => {}
        }
    }
    kept.retain(|(shown, _)| result.contains(&shown.as_str()));
    kept
}

/// The select item a `GROUP BY` entry names, as Polars resolves it: an ordinal, then a
/// select alias that is not also a source column, then the same expression selected.
fn resolve_key<'a>(
    key: &'a Expr,
    items: &[(&'a Expr, Option<&Ident>)],
    columns: &[&str],
    normalize: &impl Fn(&Expr) -> Expr,
) -> Option<(usize, &'a Expr)> {
    if let Expr::Value(value) = key {
        let ordinal: usize = value.to_string().parse().ok()?;
        let (expr, _) = items.get(ordinal.checked_sub(1)?)?;
        return Some((ordinal - 1, expr));
    }
    if let Expr::Identifier(ident) = key
        && !columns.contains(&ident.value.as_str())
        && let Some(index) = items
            .iter()
            .position(|(_, alias)| alias.is_some_and(|a| a.value == ident.value))
    {
        return Some((index, items[index].0));
    }
    let wanted = normalize(key);
    let index = items.iter().position(|(e, _)| normalize(e) == wanted)?;
    Some((index, key))
}

/// `e` spelled one way for comparing keys, keeping only what polars-sql reads: no
/// parentheses, no quotes around names (`"dept"` is `dept`, and case always counts),
/// no table name in front of a column, and function names in lowercase.
fn normalized(e: &Expr, qualifiers: &[&str]) -> Expr {
    let mut e = e.clone();
    let _ = visit_expressions_mut(&mut e, |e| {
        match e {
            Expr::Nested(inner) => *e = inner.as_ref().clone(),
            Expr::Identifier(ident) => ident.quote_style = None,
            Expr::CompoundIdentifier(parts) => {
                for part in parts.iter_mut() {
                    part.quote_style = None;
                }
                if let [table, column] = parts.as_slice()
                    && qualifiers.contains(&table.value.as_str())
                {
                    *e = Expr::Identifier(column.clone());
                }
            }
            Expr::Function(f) => {
                for part in f.name.0.iter_mut() {
                    if let ObjectNamePart::Identifier(ident) = part {
                        ident.value = ident.value.to_lowercase();
                        ident.quote_style = None;
                    }
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    e
}

/// No clause beyond ORDER BY and LIMIT around the one SELECT.
fn plain_query(query: &Query) -> bool {
    query.with.is_none()
        && matches!(query.body.as_ref(), SetExpr::Select(_))
        && query.locks.is_empty()
        && query.for_clause.is_none()
        && query.settings.is_none()
        && query.format_clause.is_none()
        && query.pipe_operators.is_empty()
}

/// One table, no join, and nothing that reshapes rows before or beside the grouping.
fn plain_select(select: &Select) -> bool {
    let one_table = match select.from.as_slice() {
        [from] => {
            from.joins.is_empty()
                && matches!(
                    &from.relation,
                    TableFactor::Table {
                        alias,
                        args: None,
                        sample: None,
                        version: None,
                        with_ordinality: false,
                        json_path: None,
                        ..
                    } if alias.as_ref().is_none_or(|a| a.columns.is_empty())
                )
        }
        _ => false,
    };
    one_table
        && matches!(
            select.distinct,
            None | Some(Distinct::Distinct | Distinct::All)
        )
        && select.top.is_none()
        && select.exclude.is_none()
        && select.into.is_none()
        && select.lateral_views.is_empty()
        && select.prewhere.is_none()
        && select.connect_by.is_empty()
        && select.cluster_by.is_empty()
        && select.distribute_by.is_empty()
        && select.sort_by.is_empty()
        && select.named_window.is_empty()
        && select.qualify.is_none()
        && select.value_table_mode.is_none()
}

/// Walks the whole statement for what the checks on its clauses cannot see: a second
/// query anywhere (a subquery), a window function, or `UNNEST`, which changes the rows
/// before they are grouped.
#[derive(Default)]
struct Plain {
    queries: usize,
}

impl Plain {
    fn check(query: &Query) -> bool {
        let mut plain = Plain::default();
        query.visit(&mut plain).is_continue() && plain.queries == 1
    }
}

impl Visitor for Plain {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.queries += 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                ControlFlow::Break(())
            }
            Expr::Function(f)
                if f.over.is_some()
                    || matches!(f.args, FunctionArguments::Subquery(_))
                    || f.name.to_string().eq_ignore_ascii_case("unnest") =>
            {
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests;
