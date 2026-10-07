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
mod tests {
    use super::*;

    const COLUMNS: &[&str] = &["dept", "salary", "ts", "name"];

    fn keys(sql: &str, width: usize) -> Option<Vec<(usize, KeySource)>> {
        plan(sql, COLUMNS, width).map(|p| {
            p.keys
                .into_iter()
                .map(|k| (k.result_index, k.source))
                .collect()
        })
    }

    fn column(name: &str) -> KeySource {
        KeySource::Column(name.to_string())
    }

    fn computed(i: usize) -> KeySource {
        KeySource::Computed(format!("{KEY_PREFIX}{i}"))
    }

    #[test]
    fn plain_grouping_keeps_where_and_drops_the_rest() {
        let plan = plan(
            "SELECT dept, AVG(salary) AS avg_salary FROM df WHERE salary > 100000 \
             GROUP BY dept HAVING COUNT(*) > 1 ORDER BY avg_salary DESC LIMIT 3",
            COLUMNS,
            2,
        )
        .expect("a plan");
        assert_eq!(plan.source_sql, "SELECT * FROM df WHERE salary > 100000");
        assert_eq!(
            plan.keys,
            vec![PlanKey {
                result_index: 0,
                source: column("dept")
            }]
        );
    }

    #[test]
    fn keys_resolve_by_alias_ordinal_and_expression() {
        // Renamed in the result: the source column is still the key.
        assert_eq!(
            keys("SELECT COUNT(*) AS n, dept AS d FROM df GROUP BY dept", 2),
            Some(vec![(1, column("dept"))])
        );
        assert_eq!(
            keys("SELECT dept, COUNT(*) FROM df GROUP BY 1", 2),
            Some(vec![(0, column("dept"))])
        );
        // A select alias for a computed key.
        let p = plan(
            "SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n FROM df GROUP BY h",
            COLUMNS,
            2,
        )
        .expect("a plan");
        assert_eq!(p.keys[0].source, computed(0));
        assert_eq!(
            p.source_sql,
            format!("SELECT *, EXTRACT(HOUR FROM ts) AS \"{KEY_PREFIX}0\" FROM df")
        );
        // The same expression, repeated.
        assert_eq!(
            keys(
                "SELECT dept, EXTRACT(HOUR FROM ts) AS h, SUM(salary) FROM df \
                 GROUP BY dept, EXTRACT(HOUR FROM ts)",
                3
            ),
            Some(vec![(0, column("dept")), (1, computed(0))])
        );
        // A table alias in front of a column.
        assert_eq!(
            keys("SELECT t.dept, COUNT(*) FROM df AS t GROUP BY dept", 2),
            Some(vec![(0, column("dept"))])
        );
    }

    /// Quotes, parentheses, a table name and a function name's case do not change what
    /// polars-sql reads, so they do not stop a key from matching its select item.
    #[test]
    fn keys_match_however_they_are_spelled() {
        assert_eq!(
            keys("SELECT \"dept\", COUNT(*) FROM df GROUP BY dept", 2),
            Some(vec![(0, column("dept"))])
        );
        assert_eq!(
            keys(
                "SELECT t.dept, COUNT(*) FROM df t GROUP BY \"t\".\"dept\"",
                2
            ),
            Some(vec![(0, column("dept"))])
        );
        assert_eq!(
            keys(
                "SELECT upper(dept) AS u, COUNT(*) FROM df GROUP BY UPPER((df.\"dept\"))",
                2
            ),
            Some(vec![(0, computed(0))])
        );
        // Case is part of a name, quoted or not: `Dept` is another column.
        assert_eq!(keys("SELECT Dept, COUNT(*) FROM df GROUP BY dept", 2), None);
    }

    #[test]
    fn a_source_column_wins_over_an_alias_of_the_same_name() {
        // Polars groups by the column `salary`, not the alias.
        assert_eq!(
            keys(
                "SELECT salary, dept AS salary2, COUNT(*) FROM df GROUP BY salary, dept",
                3
            ),
            Some(vec![(0, column("salary")), (1, column("dept"))])
        );
    }

    #[test]
    fn shapes_without_a_reliable_source_give_no_plan() {
        for sql in [
            "SELECT dept, salary FROM df",
            "SELECT AVG(salary) FROM df GROUP BY dept",
            "SELECT * FROM df GROUP BY dept",
            "SELECT a.dept, COUNT(*) FROM df a JOIN df b ON a.name = b.name GROUP BY a.dept",
            "SELECT dept, COUNT(*) FROM df, df AS b GROUP BY dept",
            "SELECT dept, COUNT(*) FROM (SELECT * FROM df) GROUP BY dept",
            "WITH t AS (SELECT * FROM df) SELECT dept, COUNT(*) FROM t GROUP BY dept",
            "SELECT dept, COUNT(*) FROM df WHERE salary > (SELECT AVG(salary) FROM df) \
             GROUP BY dept",
            "SELECT dept, COUNT(*) FROM df WHERE dept IN (SELECT dept FROM df) GROUP BY dept",
            "SELECT dept, COUNT(*) FROM df GROUP BY dept UNION SELECT dept, 1 FROM df",
            "SELECT dept, RANK() OVER (ORDER BY COUNT(*)) FROM df GROUP BY dept",
            "SELECT DISTINCT ON (dept) dept, COUNT(*) FROM df GROUP BY dept",
            "SELECT dept, COUNT(*) FROM df GROUP BY ALL",
            "SELECT dept, COUNT(*) FROM df GROUP BY 3",
            "SELECT UPPER(dept), COUNT(*) FROM df GROUP BY LOWER(dept)",
            "SELECT dept, COUNT(*) FROM df GROUP BY dept; SELECT 1",
            "SELECT dept, UNNEST(name), COUNT(*) FROM df GROUP BY dept",
            "not sql at all",
        ] {
            assert_eq!(plan(sql, COLUMNS, 2), None, "{sql}");
        }
        // The result is not one column per select item.
        assert_eq!(
            plan("SELECT dept, COUNT(*) FROM df GROUP BY dept", COLUMNS, 3),
            None
        );
    }

    #[test]
    fn passed_through_names_the_columns_a_statement_keeps() {
        let columns = ["a", "b", "c d"];
        let kept = |sql: &str, result: &[&str]| super::passed_through(sql, &columns, result);
        let pairs = |p: &[(&str, &str)]| -> Vec<(String, String)> {
            p.iter()
                .map(|(s, f)| (s.to_string(), f.to_string()))
                .collect()
        };
        assert_eq!(
            kept("SELECT * FROM df WHERE a > 1", &["a", "b", "c d"]),
            pairs(&[("a", "a"), ("b", "b"), ("c d", "c d")])
        );
        assert_eq!(
            kept(
                r#"SELECT b * 2 AS b, df.a AS x, "c d", COUNT(*) AS a FROM df GROUP BY 1, 2, 3"#,
                &["b", "x", "c d", "a"]
            ),
            pairs(&[("x", "a"), ("c d", "c d")])
        );
        assert!(kept("SELECT * EXCLUDE (a) FROM df", &["b", "c d"]).is_empty());
        assert!(kept("SELECT a FROM df JOIN df AS e ON true", &["a"]).is_empty());
        assert!(kept("not sql", &["a"]).is_empty());
    }
}
