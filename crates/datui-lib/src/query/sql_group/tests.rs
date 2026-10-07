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
