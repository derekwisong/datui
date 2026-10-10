use super::*;

#[test]

fn test_tokenize_simple() {
    let query = "select a, b where a > 10";

    let tokens = tokenize(query).unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Select,
            Token::Identifier("a".to_string()),
            Token::Comma,
            Token::Identifier("b".to_string()),
            Token::Where,
            Token::Identifier("a".to_string()),
            Token::Op(">".to_string()),
            Token::Number(10.0),
        ]
    );
}

#[test]

fn test_tokenize_operators() {
    let query = "a != b, c >= d, e <= f, g <> h";

    let tokens = tokenize(query).unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Identifier("a".to_string()),
            Token::Op("!=".to_string()),
            Token::Identifier("b".to_string()),
            Token::Comma,
            Token::Identifier("c".to_string()),
            Token::Op(">=".to_string()),
            Token::Identifier("d".to_string()),
            Token::Comma,
            Token::Identifier("e".to_string()),
            Token::Op("<=".to_string()),
            Token::Identifier("f".to_string()),
            Token::Comma,
            Token::Identifier("g".to_string()),
            Token::Op("<>".to_string()),
            Token::Identifier("h".to_string()),
        ]
    );
}

#[test]

fn test_parse_simple_expr() {
    let tokens = tokenize("a + 1").unwrap();

    let expr = parse_expr(&tokens).unwrap();

    assert_eq!(expr, col("a").add(lit(1.0)));
}

#[test]

fn test_parse_complex_expr() {
    let tokens = tokenize("(a + 1) * 2").unwrap();

    let expr = parse_expr(&tokens).unwrap();

    assert_eq!(expr, (col("a").add(lit(1.0))).mul(lit(2.0)));
}

#[test]

fn test_parse_not_function() {
    let query = "select a where not[a = b]";

    let filter = only(parse_query(query).unwrap().filters);

    assert_eq!(filter, Some(col("a").eq(col("b")).not()));
}

#[test]

fn test_parse_not_equivalent_to_neq() {
    let query1 = "select a where a != b";

    let query2 = "select a where not[a = b]";

    let query3 = "select a where not a = b";

    let filter1 = only(parse_query(query1).unwrap().filters);

    let filter2 = only(parse_query(query2).unwrap().filters);

    let filter3 = only(parse_query(query3).unwrap().filters);

    // All should produce equivalent expressions

    assert_eq!(filter1, Some(col("a").neq(col("b"))));

    assert_eq!(filter2, Some(col("a").eq(col("b")).not()));

    assert_eq!(filter3, Some(col("a").eq(col("b")).not()));
}

#[test]

fn test_parse_avg_without_brackets() {
    let query = "select avg 5+a by category";

    let cols = parse_query(query).unwrap().cols;

    assert_eq!(cols.len(), 1);

    // Should parse as avg[(5+a)]
}

#[test]

fn test_parse_string_literal() {
    let query = "select a, b:\"foo\"";

    let cols = parse_query(query).unwrap().cols;

    assert_eq!(cols.len(), 2);

    // First column is a, second is b with literal "foo"

    assert_eq!(cols[0], col("a"));

    assert_eq!(cols[1], lit("foo").alias("b"));
}

#[test]

fn test_parse_string_in_where() {
    let query = "select a where name=\"george\", age > 7";

    let filters = parse_query(query).unwrap().filters;

    // name = "george", then age > 7
    assert_eq!(
        filters,
        vec![col("name").eq(lit("george")), col("age").gt(lit(7.0))]
    );
}

#[test]

fn test_parse_col_syntax() {
    let query = "select col[\"first name\"]";

    let cols = parse_query(query).unwrap().cols;

    assert_eq!(cols.len(), 1);

    assert_eq!(cols[0], col("first name"));
}

#[test]

fn test_parse_col_syntax_with_alias() {
    let query = "select a, b:col[\"first name\"]";

    let cols = parse_query(query).unwrap().cols;

    assert_eq!(cols.len(), 2);

    assert_eq!(cols[0], col("a"));

    assert_eq!(cols[1], col("first name").alias("b"));
}

#[test]

fn test_parse_col_syntax_with_string_literal() {
    let query = "select col[\"first name\"]:\"derek\", foo where foo > 7";

    let ParsedQuery { cols, filters, .. } = parse_query(query).unwrap();
    let filter = only(filters);

    assert_eq!(cols.len(), 2);

    assert_eq!(cols[0], lit("derek").alias("first name"));

    assert_eq!(cols[1], col("foo"));

    assert!(filter.is_some());
}

#[test]

fn test_parse_string_escape_sequences() {
    let query = "select a where name=\"george\\\"s name\"";

    let filter = only(parse_query(query).unwrap().filters);

    // Should parse escaped quote correctly

    assert!(filter.is_some());
}

#[test]

fn test_parse_query_simple_where() {
    let query = "select a where a > 10";

    let filter = only(parse_query(query).unwrap().filters);

    assert_eq!(filter, Some(col("a").gt(lit(10.0))));
}

#[test]
fn test_parse_query_unary_minus_in_where() {
    // Minus next to literal with operator on other side: -0.5+discount → (-0.5)+discount
    let query = "select sum total-1 by product where 0<-0.5+discount";
    let ParsedQuery { cols, filters, .. } = parse_query(query).unwrap();
    let filter = only(filters);
    assert_eq!(cols.len(), 1);
    assert!(filter.is_some());
    // Filter: 0 < (-0.5) + discount
    let expected = lit(0.0).lt(lit(0).sub(lit(0.5)).add(col("discount")));
    assert_eq!(filter, Some(expected));
}

#[test]
fn test_parse_query_negative_literal_where() {
    let query = "select where 0<-0.1+discount";
    let filter = only(parse_query(query).unwrap().filters);
    let expected = lit(0.0).lt(lit(0).sub(lit(0.1)).add(col("discount")));
    assert_eq!(filter, Some(expected));
}

#[test]
fn test_parse_unary_plus_minus_expr() {
    let tokens = tokenize("-0.5").unwrap();
    let expr = parse_expr(&tokens).unwrap();
    assert_eq!(expr, lit(0).sub(lit(0.5)));
    let tokens = tokenize("+x").unwrap();
    let expr = parse_expr(&tokens).unwrap();
    assert_eq!(expr, col("x"));
}

#[test]

fn test_parse_query_alias() {
    let query = "select my_col:a + 1";

    let cols = parse_query(query).unwrap().cols;

    assert_eq!(cols, vec![col("a").add(lit(1.0)).alias("my_col")]);
}

#[test]

fn test_parse_query_where_commas_are_successive_conditions() {
    // Strict q: `|` is Greater, right to left like any operator, so this is
    // a > (10 | (a < 5)), not (a > 10) or (a < 5). The comma separates conditions.
    let query = "select a where a > 10 | a < 5, b = 2";

    let filters = parse_query(query).unwrap().filters;

    let greater = polars::lazy::dsl::max_horizontal([lit(10.0), col("a").lt(lit(5.0))]).unwrap();
    assert_eq!(filters, vec![col("a").gt(greater), col("b").eq(lit(2.0))]);
}

#[test]

fn test_parse_query_neq() {
    let query = "select a where a != 10";

    let filter = only(parse_query(query).unwrap().filters);

    assert_eq!(filter, Some(col("a").neq(lit(10.0))));
}

#[test]

fn test_parse_query_gte() {
    let query = "select a where a >= 10";

    let filter = only(parse_query(query).unwrap().filters);

    assert_eq!(filter, Some(col("a").gt_eq(lit(10.0))));
}

#[test]

fn test_parse_query_lte() {
    let query = "select a where a <= 10";

    let filter = only(parse_query(query).unwrap().filters);

    assert_eq!(filter, Some(col("a").lt_eq(lit(10.0))));
}

#[test]

fn test_empty_query() {
    let query = "select";

    let ParsedQuery { cols, filters, .. } = parse_query(query).unwrap();
    let filter = only(filters);

    assert!(cols.is_empty());

    assert!(filter.is_none());
}

#[test]

fn test_select_all_implicit() {
    let query = "select where a > 1";

    let ParsedQuery { cols, filters, .. } = parse_query(query).unwrap();
    let filter = only(filters);

    assert!(cols.is_empty());

    assert_eq!(filter, Some(col("a").gt(lit(1.0))));
}

#[test]
fn test_invalid_queries() {
    for query in ["a > 10", "select (a + 1", "select a where a ? 10"] {
        assert!(parse_query(query).is_err(), "{query}");
    }
}

#[test]
fn test_parse_right_to_left_operator_precedence() {
    // Test that operators are evaluated right-to-left
    // c>c%n should be parsed as c > (c % n), not (c > c) % n
    let query = "select t, v where c>c%n";

    let filter = only(parse_query(query).unwrap().filters);

    // Should parse as c > (c % n)
    let expected = col("c").gt(col("c").true_div(col("n")));
    assert_eq!(filter, Some(expected));
}

// --- Date/datetime accessor tests ---

#[test]
fn test_tokenize_dot_accessor() {
    let tokens = tokenize("foo.date").unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Identifier("foo".to_string()),
            Token::Dot,
            Token::Identifier("date".to_string()),
        ]
    );
}

#[test]
fn test_parse_simple_date_accessor() {
    let tokens = tokenize("timestamp.date").unwrap();
    let expr = parse_expr(&tokens).unwrap();
    assert_eq!(expr, col("timestamp").dt().date().alias("timestamp_date"));
}

#[test]
fn test_parse_col_with_date_accessor() {
    let tokens = tokenize("col[\"Created At\"].year").unwrap();
    let expr = parse_expr(&tokens).unwrap();
    assert_eq!(expr, col("Created At").dt().year().alias("Created At_year"));
}

#[test]
fn test_parse_chained_accessors() {
    let tokens = tokenize("dt_col.date.year").unwrap();
    let expr = parse_expr(&tokens).unwrap();
    assert_eq!(
        expr,
        col("dt_col")
            .dt()
            .date()
            .dt()
            .year()
            .alias("dt_col_date_year")
    );
}

#[test]
fn test_parse_query_select_with_date_accessor() {
    let query = "select event_date: timestamp.date";
    let cols = parse_query(query).unwrap().cols;
    assert_eq!(cols.len(), 1);
    assert_eq!(
        cols[0],
        col("timestamp")
            .dt()
            .date()
            .alias("timestamp_date")
            .alias("event_date")
    );
}

#[test]
fn test_parse_query_select_col_with_accessor() {
    let query = "select col[\"Event Time\"].date, col[\"Event Time\"].year";
    let cols = parse_query(query).unwrap().cols;
    assert_eq!(cols.len(), 2);
    assert_eq!(
        cols[0],
        col("Event Time").dt().date().alias("Event Time_date")
    );
    assert_eq!(
        cols[1],
        col("Event Time").dt().year().alias("Event Time_year")
    );
}

#[test]
fn test_parse_query_where_with_date_accessor() {
    let query = "select where created_at.month = 12";
    let filter = only(parse_query(query).unwrap().filters);
    assert_eq!(
        filter,
        Some(
            col("created_at")
                .dt()
                .month()
                .alias("created_at_month")
                .eq(lit(12.0))
        )
    );
}

#[test]
fn test_parse_query_where_dow() {
    let query = "select where event_ts.dow = 1";
    let filter = only(parse_query(query).unwrap().filters);
    assert_eq!(
        filter,
        Some(
            col("event_ts")
                .dt()
                .weekday()
                .alias("event_ts_dow")
                .eq(lit(1.0))
        )
    );
}

#[test]
fn test_parse_all_accessors() {
    let accessors = [
        "date",
        "time",
        "year",
        "month",
        "week",
        "day",
        "dow",
        "month_start",
        "month_end",
    ];
    for accessor in accessors {
        let query = format!("select x.{}", accessor);
        let result = parse_query(&query);
        assert!(
            result.is_ok(),
            "Accessor '{}' should parse: {:?}",
            accessor,
            result.err()
        );
    }
}

#[test]
fn test_parse_unknown_accessor() {
    let query = "select x.nosuchaccessor";
    let result = parse_query(query);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Unknown accessor"));
    assert!(err.contains("nosuchaccessor"));
}

#[test]
fn test_parse_date_literal() {
    let tokens = tokenize("2021.01.01").unwrap();
    assert_eq!(tokens, vec![Token::DateLiteral("2021-01-01".to_string())]);
}

#[test]
fn test_parse_query_where_date_literal() {
    let query = "select where dt_col.date > 2021.01.01";
    let filter = only(parse_query(query).unwrap().filters);
    assert!(filter.is_some());
    // Verify the filter parses without error (date literal 2021.01.01 -> ISO 2021-01-01)
}

/// A decimal is a number, not a date literal, with or without a leading digit.
#[test]
fn test_tokenize_decimal_numbers() {
    for (text, number) in [(".5", 0.5), ("2.5", 2.5)] {
        assert_eq!(
            tokenize(text).unwrap(),
            vec![Token::Number(number)],
            "{text}"
        );
    }
}

#[test]
fn test_sanitize_duplicate_column_error() {
    let polars_msg = "duplicate: projections contained duplicate output name 'timestamp'. It's possible that multiple expressions are returning the same default column name. If this is the case, try renaming the columns with `.alias(\"new_name\")` to avoid duplicate column names.";
    let sanitized = sanitize_query_error(polars_msg);
    assert!(sanitized.contains("Duplicate column name"));
    assert!(sanitized.contains("timestamp"));
    assert!(sanitized.contains("my_date: timestamp.date"));
    assert!(!sanitized.contains(".alias("));
}

#[test]
fn test_parse_timestamp_literal() {
    let tokens = tokenize("2021.01.15T14:30:00.123456").unwrap();
    assert!(matches!(tokens[0], Token::TimestampLiteral { .. }));
}

#[test]
fn test_parse_null_and_not_null() {
    let f1 = only(parse_query("select where null col1").unwrap().filters);
    assert!(f1.is_some());
    let f2 = only(parse_query("select where not null col1").unwrap().filters);
    assert!(f2.is_some());
}

#[test]
fn test_parse_coalesce() {
    let cols = parse_query("select a: coln^cola^colb").unwrap().cols;
    assert_eq!(cols.len(), 1);
    // coalesce(coln, coalesce(cola, colb)) - parsing succeeds
}

#[test]
fn test_parse_first_last_aggregation() {
    let cols = parse_query("select first[value], last[value] by group")
        .unwrap()
        .cols;
    assert_eq!(cols.len(), 2);
}

#[test]
fn test_parse_string_accessors() {
    let filter = only(
        parse_query("select where city_name.ends_with[\"lanta\"]")
            .unwrap()
            .filters,
    );
    assert!(filter.is_some());
    let cols = parse_query("select name.len, name.upper").unwrap().cols;
    assert_eq!(cols.len(), 2);
}

#[test]
fn test_parse_format_accessor() {
    let tokens = tokenize("dt_col.format[\"%Y-%m\"]").unwrap();
    let expr = parse_expr(&tokens).unwrap();
    // dt_col.format["%Y-%m"] parses to dt.to_string - verify we got an expr
    assert!(!format!("{:?}", expr).is_empty());
}

#[test]
fn test_parse_by_with_date_accessor() {
    let query = "select order_date, count: count id by order_date.year";
    let ParsedQuery {
        cols,
        group_by: group_by_cols,
        ..
    } = parse_query(query).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(group_by_cols.len(), 1);
    assert_eq!(
        group_by_cols[0],
        col("order_date").dt().year().alias("order_date_year")
    );
}

#[test]
fn test_unaliased_aggregates_of_same_column_coexist() {
    let query = "select avg salary, max salary by department";
    let ParsedQuery {
        cols,
        group_by: group_by_cols,
        ..
    } = parse_query(query).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], col("salary").mean().alias("avg_salary"));
    assert_eq!(cols[1], col("salary").max().alias("max_salary"));
    assert_eq!(group_by_cols, vec![col("department")]);
}

#[test]
fn test_unaliased_aggregate_bracketed_and_bare_name_alike() {
    let bracketed = parse_query("select avg[salary] by department")
        .unwrap()
        .cols;
    let bare = parse_query("select avg salary by department").unwrap().cols;
    assert_eq!(bracketed, bare);
    assert_eq!(bracketed[0], col("salary").mean().alias("avg_salary"));
}

#[test]
fn test_unaliased_aggregate_col_syntax_auto_alias() {
    let cols = parse_query("select sum[col[\"unit price\"]] by region")
        .unwrap()
        .cols;
    assert_eq!(cols[0], col("unit price").sum().alias("sum_unit price"));
}

#[test]
fn test_bare_count_names_itself() {
    let cols = parse_query("select count[x] by g").unwrap().cols;
    assert_eq!(cols[0], col("x").count().alias("count_x"));
}

#[test]
fn test_explicit_alias_overrides_aggregate_auto_alias() {
    let cols = parse_query("select total:sum[price] by region")
        .unwrap()
        .cols;
    // The outer alias is applied last, so the result column is named "total".
    assert_eq!(
        cols[0],
        col("price").sum().alias("sum_price").alias("total")
    );
}

#[test]
fn test_aggregate_of_expression_keeps_default_name() {
    // No single source column, so there is nothing to build a {fn}_{column} name from.
    let cols = parse_query("select sum[price*qty] by region").unwrap().cols;
    assert_eq!(cols[0], (col("price").mul(col("qty"))).sum());
}

#[test]
fn test_docs_grouping_example_collects_with_auto_aliases() {
    // The example from docs/user-guide/querying-data.md must run as written.
    let query = "select avg salary, max salary, count name by department";
    let ParsedQuery {
        cols,
        group_by: group_by_cols,
        ..
    } = parse_query(query).unwrap();
    let df = df!(
        "department" => &["eng", "eng", "ops"],
        "salary" => &[100.0f64, 200.0, 300.0],
        "name" => &["a", "b", "c"],
    )
    .unwrap();
    let out = df
        .lazy()
        .group_by(group_by_cols)
        .agg(cols)
        .collect()
        .unwrap();
    let names: Vec<String> = out
        .get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect();
    assert_eq!(
        names,
        ["department", "avg_salary", "max_salary", "count_name"]
    );
}

#[test]
fn test_slash_divides_like_percent() {
    let slash = parse_expr(&tokenize("a/b").unwrap()).unwrap();
    let percent = parse_expr(&tokenize("a%b").unwrap()).unwrap();
    assert_eq!(slash, percent);
    assert_eq!(slash, col("a").true_div(col("b")));
}

#[test]
fn test_slash_right_to_left() {
    // Right-to-left like every other operator: 1/c+a is 1/(c+a).
    let expr = parse_expr(&tokenize("1/c+a").unwrap()).unwrap();
    assert_eq!(expr, lit(1.0).true_div(col("c").add(col("a"))));
}

#[test]
fn test_slash_in_where_clause() {
    // Same shape as the existing % test: c>c/n is c > (c/n).
    let filter = only(parse_query("select t, v where c>c/n").unwrap().filters);
    assert_eq!(filter, Some(col("c").gt(col("c").true_div(col("n")))));
}

#[test]
fn test_by_after_where_errors_with_clause_order() {
    // The parser used to drop `by dept` on the floor and filter as if it
    // were never typed.
    let err = parse_query("select name, salary where x > 1 by dept").unwrap_err();
    assert!(
        err.contains("Unexpected 'by' after the where clause"),
        "{err}"
    );
    assert!(
        err.contains("select [by group] [where conditions]"),
        "{err}"
    );
}

#[test]
fn test_by_after_where_without_condition_operator() {
    let err = parse_query("select where flag by dept").unwrap_err();
    assert!(
        err.contains("Unexpected 'by' after the where clause"),
        "{err}"
    );
}

#[test]
fn test_by_inside_parens_in_where_errors_as_stray_token() {
    // Nested in parentheses it is not a clause boundary, so the expression
    // parser reports it instead.
    let err = parse_query("select a where (x by g)").unwrap_err();
    assert!(
        err.contains("Unexpected 'by' after the expression"),
        "{err}"
    );
}

#[test]
fn test_trailing_garbage_after_where_errors() {
    let err = parse_query("select a where a > 1 2").unwrap_err();
    assert!(err.contains("Unexpected '2' after the expression"), "{err}");

    let err = parse_query("select a where null col1 foo").unwrap_err();
    assert!(
        err.contains("Unexpected 'foo' after the expression"),
        "{err}"
    );
}

#[test]
fn test_trailing_garbage_in_select_errors() {
    let err = parse_query("select a b").unwrap_err();
    assert!(err.contains("Unexpected 'b' after the expression"), "{err}");

    let err = parse_query("select (a, b)").unwrap_err();
    assert!(err.contains("joins lists in q"), "{err}");
}

#[test]
fn test_duplicate_clauses_error() {
    let err = parse_query("select a where x > 1 where y > 2").unwrap_err();
    assert!(err.contains("Unexpected second 'where'"), "{err}");
    assert!(err.contains("','"), "{err}");

    let err = parse_query("select a by g by h").unwrap_err();
    assert!(err.contains("Unexpected second 'by'"), "{err}");
}

#[test]
fn test_operators_that_repeat_an_operand_are_bounded() {
    // Found by the `parse_query` fuzz target: each `wavg` repeats its operands, so a
    // chain of them grew the expression threefold per link until memory ran out.
    let chain = format!("select {}x", "w wavg ".repeat(30));
    let err = parse_query(&chain).unwrap_err();
    assert!(err.contains("Expression is too large"), "{err}");

    let xbar = format!("select {}x{}", "(1 xbar ".repeat(40), ")".repeat(40));
    assert!(parse_query(&xbar).is_err());

    let inner = format!(
        "select {}x{}",
        "(".repeat(20),
        " in [1, 2, 3, 4])".repeat(20)
    );
    assert!(parse_query(&inner).is_err());

    assert!(parse_query("select w wavg x wavg y by g").is_ok());
}

#[test]
fn test_deeply_nested_expression_is_rejected_not_crashed() {
    // Found by the `parse_query` fuzz target: the parser is recursive descent, so a
    // long enough chain of unary operators or parentheses recursed until the stack
    // ran out and the process died. These must come back as errors.
    let unary = format!("select {}x", "-".repeat(5_000));
    assert!(
        parse_query(&unary).is_err(),
        "deep unary chain should error"
    );

    let parens = format!("select {}x{}", "(".repeat(5_000), ")".repeat(5_000));
    assert!(parse_query(&parens).is_err(), "deep nesting should error");

    // The counter has to come back down, or the first deep query would poison every
    // later one on the same thread.
    assert!(
        parse_query("select a + b * c").is_ok(),
        "an ordinary query must still parse after a rejected one"
    );
}

// --- q additions (#367) ---

/// Run a query over `df` the way `DataTableState::query` does.
fn eval(query: &str, df: &DataFrame) -> DataFrame {
    let ParsedQuery {
        cols,
        filters,
        group_by: by,
        distinct,
        ..
    } = parse_query_over(query, Some(df.schema().as_ref())).unwrap();
    let mut lf = df.clone().lazy();
    for f in filters {
        lf = lf.filter(f);
    }
    if !by.is_empty() {
        let keys = by.len();
        lf = lf.group_by(by).agg(cols);
        let schema = lf.collect_schema().unwrap();
        let sort: Vec<Expr> = schema
            .iter_names()
            .take(keys)
            .map(|n| col(n.as_str()))
            .collect();
        lf = lf.sort_by_exprs(sort, SortMultipleOptions::default());
    } else if !cols.is_empty() {
        lf = lf.select(cols);
    }
    if distinct {
        lf = lf.unique_stable(None, UniqueKeepStrategy::First);
    }
    lf.collect().unwrap()
}

/// One column of the result as display strings, nulls as "null".
fn values(df: &DataFrame, name: &str) -> Vec<String> {
    df.column(name)
        .unwrap()
        .as_materialized_series()
        .iter()
        .map(|v| match v {
            AnyValue::String(s) => s.to_string(),
            AnyValue::StringOwned(s) => s.to_string(),
            v => v.to_string(),
        })
        .collect()
}

/// The one where condition, if any; fails on several.
fn only(filters: Vec<Expr>) -> Option<Expr> {
    assert!(filters.len() <= 1, "{filters:?}");
    filters.into_iter().next()
}

fn parse_err(query: &str) -> String {
    parse_query(query).unwrap_err()
}

#[test]
fn test_time_part_accessors_parse() {
    let expr = parse_expr(&tokenize("ts.hour").unwrap()).unwrap();
    assert_eq!(expr, col("ts").dt().hour().alias("ts_hour"));
    let expr = parse_expr(&tokenize("ts.doy").unwrap()).unwrap();
    assert_eq!(expr, col("ts").dt().ordinal_day().alias("ts_doy"));
    for accessor in ["hour", "minute", "second", "quarter", "doy"] {
        let q = format!("select x.{}", accessor);
        assert!(parse_query(&q).is_ok(), "{q}");
    }
}

#[test]
fn test_time_part_accessors_evaluate() {
    let df = df!("ts" => &["2024-03-15 13:45:30", "2024-12-31 00:00:05"])
        .unwrap()
        .lazy()
        .select([col("ts")
            .str()
            .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))])
        .collect()
        .unwrap();
    let out = eval(
        "select ts.hour, ts.minute, ts.second, ts.quarter, ts.doy",
        &df,
    );
    assert_eq!(values(&out, "ts_hour"), ["13", "0"]);
    assert_eq!(values(&out, "ts_minute"), ["45", "0"]);
    assert_eq!(values(&out, "ts_second"), ["30", "5"]);
    assert_eq!(values(&out, "ts_quarter"), ["1", "4"]);
    assert_eq!(values(&out, "ts_doy"), ["75", "366"]);
}

#[test]
fn test_hour_groups_trips() {
    // The taxi example: trips by pickup hour.
    let df =
        df!("pickup" => &["2025-01-01 08:10:00", "2025-01-01 08:50:00", "2025-01-01 17:00:00"])
            .unwrap()
            .lazy()
            .with_column(col("pickup").str().to_datetime(
                None,
                None,
                StrptimeOptions::default(),
                lit("raise"),
            ))
            .collect()
            .unwrap();
    let out = eval("select trips: count pickup by pickup.hour", &df);
    assert_eq!(values(&out, "pickup_hour"), ["8", "17"]);
    assert_eq!(values(&out, "trips"), ["2", "1"]);
}

/// A timestamp literal reads as a clock in the zone of the column it meets, as
/// the table shows that column; Polars refuses a zoned/naive comparison otherwise.
#[test]
fn a_timestamp_literal_takes_the_zone_of_its_column() {
    let zoned = |zone: &str| {
        df!("t" => &["2013-01-15 14:00:00", "2013-01-15 15:00:00"])
            .unwrap()
            .lazy()
            .with_column(col("t").str().to_datetime(
                Some(TimeUnit::Microseconds),
                TimeZone::opt_try_new(Some(zone)).unwrap(),
                StrptimeOptions::default(),
                lit("raise"),
            ))
            .collect()
            .unwrap()
    };
    for zone in ["UTC", "America/New_York"] {
        let df = zoned(zone);
        for (query, rows) in [
            ("select where t > 2013.01.15T14:30:00.123456", 1),
            ("select where 2013.01.15T14:30:00 < t", 1),
            ("select where t = 2013.01.15T15:00:00", 1),
            (
                "select where t >= 2013.01.15T14:00:00, t < 2013.01.16T00:00:00",
                2,
            ),
            ("select where t > 2013.01.15", 2),
        ] {
            assert_eq!(eval(query, &df).height(), rows, "{zone}: {query}");
        }
        let out = eval("select later: t ^ 2013.01.15T00:00:00", &df);
        assert_eq!(out.height(), 2, "{zone}");
    }
    // A column with no zone is untouched.
    let naive = df!("t" => &["2013-01-15 14:00:00"])
        .unwrap()
        .lazy()
        .with_column(col("t").str().to_datetime(
            None,
            None,
            StrptimeOptions::default(),
            lit("raise"),
        ))
        .collect()
        .unwrap();
    assert_eq!(
        eval("select where t < 2013.01.15T14:30:00", &naive).height(),
        1
    );

    // "Copy as Python" says the same.
    let schema = zoned("America/New_York").schema().clone();
    let mut nodes = parse_nodes("select where t > 2013.01.15T14:30:00").unwrap();
    nodes.resolve_time_zones(&schema);
    let python = nodes.python_filters().concat();
    assert!(
        python.contains("time_zone=\"America/New_York\", ambiguous=\"earliest\""),
        "{python}"
    );
}

/// One row of each temporal type, plus text, for the quoted-text errors.
fn temporal_frame() -> DataFrame {
    df!("d" => &["2024-01-01"], "s" => &["2024.01.01"])
        .unwrap()
        .lazy()
        .with_columns([
            lit("2024-01-01T05:00:00")
                .str()
                .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
                .alias("ts"),
            lit("2024-01-01T05:00:00")
                .str()
                .to_datetime(None, None, StrptimeOptions::default(), lit("raise"))
                .dt()
                .time()
                .alias("t"),
            lit(5i64)
                .cast(DataType::Duration(TimeUnit::Milliseconds))
                .alias("dur"),
            col("d").str().to_date(StrptimeOptions::default()),
        ])
        .collect()
        .unwrap()
}

/// The error parsing `query` over `df`.
fn parse_error_over(query: &str, df: &DataFrame) -> String {
    parse_query_over(query, Some(df.schema().as_ref()))
        .err()
        .unwrap_or_else(|| panic!("{query} should fail"))
}

#[test]
fn test_quoted_text_against_temporal_column_is_a_q_error() {
    let df = temporal_frame();
    let cases = [
        (
            "select where d = \"2024.01.01\"",
            "d is a date; \"2024.01.01\" is a string. A date is 2024.01.01",
        ),
        (
            "select where d < \"Jan 1\"",
            "d is a date; \"Jan 1\" is a string. A date is 2024.01.01",
        ),
        (
            "select where d = \"2024-01-01\"",
            "d is a date; \"2024-01-01\" is a string. A date is 2024.01.01",
        ),
        (
            "select where ts = \"2024.01.01T05:00:00\"",
            "ts is a timestamp; \"2024.01.01T05:00:00\" is a string. A timestamp is 2024.01.01T05:00:00",
        ),
        (
            "select where ts < \"2023.06.30T23:59:59.5\"",
            "ts is a timestamp; \"2023.06.30T23:59:59.5\" is a string. A timestamp is 2023.06.30T23:59:59.5",
        ),
        (
            "select where ts < \"2024.01.01\"",
            "ts is a timestamp; \"2024.01.01\" is a string. A timestamp is 2024.01.01T05:00:00",
        ),
        (
            "select where t = \"05:00:00\"",
            "t is a time; \"05:00:00\" is a string. A time has no literal; compare t.hour, t.minute or t.second with a number",
        ),
        (
            "select where t < \"05:00:00\"",
            "t is a time; \"05:00:00\" is a string. A time has no literal; compare t.hour, t.minute or t.second with a number",
        ),
        (
            "select where dur = \"5s\"",
            "dur is a duration; \"5s\" is a string. A duration has no literal",
        ),
        (
            "select where \"5s\" >= dur",
            "dur is a duration; \"5s\" is a string. A duration has no literal",
        ),
        (
            "select where d in [\"2024.01.01\", \"2024.01.02\"]",
            "d is a date; \"2024.01.01\" is a string. A date is 2024.01.01",
        ),
        (
            "select x: d != \"x\"",
            "d is a date; \"x\" is a string. A date is 2024.01.01",
        ),
    ];
    for (query, want) in cases {
        assert_eq!(parse_error_over(query, &df), want, "{query}");
    }

    // A name that is not a bare word is named as it is typed.
    let mut renamed = df.clone();
    renamed.rename("d", "start date".into()).unwrap();
    assert_eq!(
        parse_error_over("select where col[\"start date\"] = \"x\"", &renamed),
        "col[\"start date\"] is a date; \"x\" is a string. A date is 2024.01.01"
    );

    // The remedies run.
    assert_eq!(eval("select where d = 2024.01.01", &df).height(), 1);
    assert_eq!(eval("select where d in [2024.01.01]", &df).height(), 1);
    assert_eq!(
        eval("select where ts = 2024.01.01T05:00:00", &df).height(),
        1
    );
    assert_eq!(eval("select where t.hour = 5", &df).height(), 1);
}

#[test]
fn test_quoted_text_against_text_column_still_compares() {
    let df = temporal_frame();
    assert_eq!(eval("select where s = \"2024.01.01\"", &df).height(), 1);
    assert_eq!(eval("select where s < \"2025\"", &df).height(), 1);
    assert_eq!(eval("select where s in [\"2024.01.01\"]", &df).height(), 1);
    // `like` and string functions are text by name; left to themselves.
    assert!(parse_query_over("select where d like \"2024*\"", Some(df.schema().as_ref())).is_ok());
    // Without a schema nothing is known, so nothing is refused here.
    assert!(parse_query_over("select where d = \"2024.01.01\"", None).is_ok());
}

#[test]
fn test_to_date_and_to_datetime_parse_strings() {
    let df = df!(
        "DATE" => &["20240101", "20241231", "junk"],
        "Date" => &["Sat Sep 12 2020", "Tue Jan 12 2021(P)", "Sun Sep 13 2020"],
        "stamp" => &["2024-01-02 03:04", "2024-05-06 07:08", "nope"],
    )
    .unwrap();
    let out = eval(
        "select day: DATE.to_date[\"%Y%m%d\"], d: Date.replace[\"(P)\", \"\"].to_date[\"%a %b %d %Y\"], t: stamp.to_datetime[\"%Y-%m-%d %H:%M\"]",
        &df,
    );
    // A value that does not match the format is null, not an error.
    assert_eq!(values(&out, "day"), ["2024-01-01", "2024-12-31", "null"]);
    assert_eq!(
        values(&out, "d"),
        ["2020-09-12", "2021-01-12", "2020-09-13"]
    );
    assert_eq!(
        values(&out, "t"),
        ["2024-01-02 03:04:00", "2024-05-06 07:08:00", "null"]
    );
}

#[test]
fn test_to_date_parses_an_integer_column() {
    // NOAA's DATE is 20240101; read from CSV it is an integer.
    let df = df!("DATE" => &[20240101i64, 20240229]).unwrap();
    let out = eval("select d: DATE.to_date[\"%Y%m%d\"]", &df);
    assert_eq!(values(&out, "d"), ["2024-01-01", "2024-02-29"]);
}

#[test]
fn test_casts() {
    let df = df!(
        "s" => &["3", "4.5", "x"],
        "f" => &[1.9f64, -1.9, 3.0],
    )
    .unwrap();
    let out = eval("select a: s.int, b: s.float, c: f.int, d: f.str", &df);
    assert_eq!(values(&out, "a"), ["3", "null", "null"]);
    assert_eq!(values(&out, "b"), ["3.0", "4.5", "null"]);
    assert_eq!(values(&out, "c"), ["1", "-1", "3"]);
    assert_eq!(values(&out, "d"), ["1.9", "-1.9", "3.0"]);
    assert_eq!(out.column("a").unwrap().dtype(), &DataType::Int64);
    assert_eq!(out.column("b").unwrap().dtype(), &DataType::Float64);
    assert_eq!(out.column("d").unwrap().dtype(), &DataType::String);
}

#[test]
fn test_string_pieces() {
    let df = df!("FT" => &["0–3", "12–1", "  2–2  "]).unwrap();
    let out = eval(
        "select home: FT.part[\"–\", 0].int, away: FT.part[\"–\", -1].int, none: FT.part[\"–\", 5], head: FT.slice[0, 2], tail: FT.slice[-2], s: FT.strip, r: FT.replace[\"–\", \"-\"]",
        &df,
    );
    assert_eq!(values(&out, "home"), ["0", "12", "null"]);
    assert_eq!(values(&out, "away"), ["3", "1", "null"]);
    assert_eq!(values(&out, "none"), ["null", "null", "null"]);
    assert_eq!(values(&out, "head"), ["0–", "12", "  "]);
    assert_eq!(values(&out, "tail"), ["–3", "–1", "  "]);
    assert_eq!(values(&out, "s"), ["0–3", "12–1", "2–2"]);
    assert_eq!(values(&out, "r"), ["0-3", "12-1", "  2-2  "]);
}

#[test]
fn test_string_pieces_auto_alias() {
    let cols = parse_query("select FT.part[\"-\", 0], FT.strip")
        .unwrap()
        .cols;
    let names: Vec<String> = cols
        .iter()
        .map(|e| e.clone().meta().output_name().unwrap().to_string())
        .collect();
    assert_eq!(names, ["FT_part_-_0", "FT_strip"]);
}

#[test]
fn test_in_parses_to_equalities() {
    let filter = only(
        parse_query("select where name in [\"a\", \"b\"]")
            .unwrap()
            .filters,
    );
    assert_eq!(
        filter,
        Some(col("name").eq(lit("a")).or(col("name").eq(lit("b"))))
    );
}

#[test]
fn test_in_filters() {
    let df = df!(
        "name" => &["Emma", "Jennifer", "Olivia", "Mary"],
        "n" => &[1i32, 2, 3, 4],
    )
    .unwrap();
    let out = eval(
        "select name where name in [\"Emma\", \"Jennifer\", \"Olivia\"]",
        &df,
    );
    assert_eq!(values(&out, "name"), ["Emma", "Jennifer", "Olivia"]);
    let out = eval("select n where n in [2, 4.0, -1]", &df);
    assert_eq!(values(&out, "n"), ["2", "4"]);
    let out = eval("select name where not name in [\"Mary\"]", &df);
    assert_eq!(values(&out, "name"), ["Emma", "Jennifer", "Olivia"]);
    // Commas inside the list are not where-clause ANDs.
    let out = eval("select name where name in [\"Emma\", \"Mary\"], n > 1", &df);
    assert_eq!(values(&out, "name"), ["Mary"]);
}

#[test]
fn test_in_long_list_nests_shallowly() {
    let items: Vec<String> = (0..2000).map(|i| i.to_string()).collect();
    let q = format!("select where x in [{}]", items.join(", "));
    let df = df!("x" => &[5i64, 1999, 2000]).unwrap();
    assert_eq!(values(&eval(&q, &df), "x"), ["5", "1999"]);
}

#[test]
fn test_in_a_list_past_the_node_cap_on_a_column() {
    // A pasted list of ids: the column is not what multiplies.
    let items: Vec<String> = (0..12_000).map(|i| i.to_string()).collect();
    let q = format!("select where x in [{}]", items.join(", "));
    let df = df!("x" => &[5i64, 11_999, 12_000]).unwrap();
    assert_eq!(values(&eval(&q, &df), "x"), ["5", "11999"]);
}

#[test]
fn test_in_errors() {
    let err = parse_err("select where x in 1");
    assert!(err.contains("in takes a list"), "{err}");
    let err = parse_err("select where x in []");
    assert!(err.contains("in needs a list of values"), "{err}");
    let err = parse_err("select where x in [1,, 2]");
    assert!(err.contains("in needs a list of values"), "{err}");
    let err = parse_err("select where x in [1] = y");
    assert!(err.contains("in takes a list"), "{err}");
    let err = parse_err("select where x in [1] + [2]");
    assert!(err.contains("in takes a list"), "{err}");
}

#[test]
fn test_like_matches_whole_value() {
    let df = df!("item" => &["Crispy Chicken", "Chicken", "Fish", "a.b", "axb"]).unwrap();
    let out = eval("select item where item like \"*Chicken*\"", &df);
    assert_eq!(values(&out, "item"), ["Crispy Chicken", "Chicken"]);
    // Anchored: a prefix pattern does not match mid-string.
    let out = eval("select item where item like \"Chick*\"", &df);
    assert_eq!(values(&out, "item"), ["Chicken"]);
    let out = eval("select item where item like \"F?sh\"", &df);
    assert_eq!(values(&out, "item"), ["Fish"]);
    // Regex characters are literal.
    let out = eval("select item where item like \"a.b\"", &df);
    assert_eq!(values(&out, "item"), ["a.b"]);
}

#[test]
fn test_like_errors() {
    let err = parse_err("select where item like Chicken");
    assert!(err.contains("like takes a quoted pattern"), "{err}");
}

#[test]
fn test_like_regex() {
    assert_eq!(like_regex("*a?.b*"), "(?s)^.*a.\\.b.*$");
}

/// One column of each integer width and signedness, `a_*` the larger and `b_*`
/// the smaller, so a difference never wraps an unsigned type.
fn integer_widths() -> (DataFrame, Vec<&'static str>) {
    let names = ["i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64"];
    let types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    let mut columns = Vec::new();
    for (name, dtype) in names.iter().zip(types) {
        for (side, vals) in [("a", [10i64, 20, 30]), ("b", [1, 2, 3])] {
            let c = Column::new(format!("{side}_{name}").into(), vals);
            columns.push(c.cast(&dtype).unwrap());
        }
    }
    let df = DataFrame::new_infer_height(columns).unwrap();
    (df, names.to_vec())
}

fn as_f64(df: &DataFrame, name: &str) -> Vec<f64> {
    df.column(name)
        .unwrap()
        .cast(&DataType::Float64)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .collect()
}

#[test]
fn mixed_integer_widths_do_arithmetic() {
    let (df, names) = integer_widths();
    // a = 10, 20, 30 and b = 1, 2, 3 in every width.
    let ops: [(&str, [f64; 3]); 5] = [
        ("+", [11.0, 22.0, 33.0]),
        ("-", [9.0, 18.0, 27.0]),
        ("*", [10.0, 40.0, 90.0]),
        ("/", [10.0, 10.0, 10.0]),
        ("%", [10.0, 10.0, 10.0]),
    ];
    let mut parts = Vec::new();
    let mut expected = Vec::new();
    for (i, (op, want)) in ops.iter().enumerate() {
        for l in &names {
            for r in &names {
                let alias = format!("r{i}_{l}_{r}");
                parts.push(format!("{alias}: a_{l} {op} b_{r}"));
                expected.push((alias, *want));
            }
        }
    }
    for l in &names {
        for r in &names {
            let alias = format!("m_{l}_{r}");
            parts.push(format!("{alias}: a_{l} mod b_{r}"));
            expected.push((alias, [0.0, 0.0, 0.0]));
        }
    }
    let out = eval(&format!("select {}", parts.join(", ")), &df);
    for (alias, want) in expected {
        assert_eq!(as_f64(&out, &alias), want, "{alias}");
    }
}

#[test]
fn mixed_integer_widths_with_literals() {
    let (df, names) = integer_widths();
    let mut parts = Vec::new();
    let mut expected = Vec::new();
    for name in &names {
        for (tag, expr, want) in [
            ("p", format!("b_{name} + 7"), [8.0, 9.0, 10.0]),
            ("s", format!("a_{name} - 7"), [3.0, 13.0, 23.0]),
            ("l", format!("7 - b_{name}"), [6.0, 5.0, 4.0]),
            ("m", format!("b_{name} * 2.5"), [2.5, 5.0, 7.5]),
            ("d", format!("a_{name} / 2"), [5.0, 10.0, 15.0]),
            ("q", format!("60 / b_{name}"), [60.0, 30.0, 20.0]),
            ("r", format!("a_{name} mod 7"), [3.0, 6.0, 2.0]),
        ] {
            let alias = format!("{tag}_{name}");
            parts.push(format!("{alias}: {expr}"));
            expected.push((alias, want));
        }
    }
    let out = eval(&format!("select {}", parts.join(", ")), &df);
    for (alias, want) in expected {
        assert_eq!(as_f64(&out, &alias), want, "{alias}");
    }
}

#[test]
fn mixed_integer_widths_filter_and_group() {
    let (df, _) = integer_widths();
    let out = eval("select a_u8 where 10 = a_i64 / b_u8, 8 < b_u16 + 7", &df);
    assert_eq!(values(&out, "a_u8"), ["20", "30"]);
    let out = eval(
        "select t: sum a_u8 * b_i16, n: max a_i64 - b_u32 by k: b_u16 mod 2",
        &df,
    );
    assert_eq!(as_f64(&out, "k"), [0.0, 1.0]);
    assert_eq!(as_f64(&out, "t"), [40.0, 100.0]);
    assert_eq!(as_f64(&out, "n"), [18.0, 27.0]);
}

#[cfg(feature = "sql")]
#[test]
fn mixed_integer_widths_in_sql() {
    let (df, _) = integer_widths();
    let mut ctx = polars_sql::SQLContext::new();
    ctx.register("df", df.lazy());
    let out = ctx
        .execute(
            "SELECT a_i64 / b_u8 AS d, a_u16 % b_u8 AS m, b_u8 + 7 AS p, \
             a_i8 * b_u64 AS x FROM df WHERE a_u8 - b_i16 > 9",
        )
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(as_f64(&out, "d"), [10.0, 10.0]);
    assert_eq!(as_f64(&out, "m"), [0.0, 0.0]);
    assert_eq!(as_f64(&out, "p"), [9.0, 10.0]);
    assert_eq!(as_f64(&out, "x"), [40.0, 90.0]);
}

#[test]
fn test_xbar_buckets() {
    let df = df!(
        "fare" => &[-1.0f64, 0.0, 4.99, 5.0, 12.5],
        "n" => &[-1i64, 0, 4, 5, 12],
    )
    .unwrap();
    let out = eval("select f: 5 xbar fare, i: 5 xbar n, h: 0.5 xbar fare", &df);
    assert_eq!(values(&out, "f"), ["-5.0", "0.0", "0.0", "5.0", "10.0"]);
    // A whole-number bucket keeps an integer column integral.
    assert_eq!(values(&out, "i"), ["-5", "0", "0", "5", "10"]);
    assert_eq!(out.column("i").unwrap().dtype(), &DataType::Int64);
    assert_eq!(values(&out, "h"), ["-1.0", "0.0", "4.5", "5.0", "12.5"]);
}

#[test]
fn test_xbar_groups() {
    let df = df!("fare" => &[1.0f64, 3.0, 7.0, 12.0, 14.0]).unwrap();
    let out = eval("select trips: count fare by b: 5 xbar fare", &df);
    assert_eq!(values(&out, "b"), ["0.0", "5.0", "10.0"]);
    assert_eq!(values(&out, "trips"), ["2", "1", "2"]);
}

#[test]
fn test_xbar_errors() {
    let err = parse_err("select 0 xbar fare");
    assert!(err.contains("positive bucket size"), "{err}");
    let err = parse_err("select -5 xbar fare");
    assert!(err.contains("positive bucket size"), "{err}");
}

#[test]
fn test_mod() {
    let df = df!("n" => &[-7i64, 7, 9], "f" => &[7.5f64, -0.5, 2.0]).unwrap();
    let out = eval("select a: n mod 3, b: f mod 2, c: -7 mod 3", &df);
    // Floored, as in q: the result takes the sign of the divisor.
    assert_eq!(values(&out, "a"), ["2", "1", "0"]);
    assert_eq!(values(&out, "b"), ["1.5", "1.5", "0.0"]);
    assert_eq!(values(&out, "c"), ["2", "2", "2"]);
    // A negative whole divisor is an integer too.
    let out = eval("select a: n mod -3", &df);
    assert_eq!(values(&out, "a"), ["-1", "-2", "0"]);
    assert_eq!(out.column("a").unwrap().dtype(), &DataType::Int64);
}

#[test]
fn test_word_operators_right_to_left() {
    let parse = |s: &str| parse_expr(&tokenize(s).unwrap()).unwrap();
    // a = b mod 2 is a = (b mod 2).
    assert_eq!(parse("a = b mod 2"), col("a").eq(col("b").rem(lit(2i64))));
    // 5 xbar x + 1 buckets x + 1; 2 * 5 xbar x doubles the bucket.
    assert_eq!(
        parse("5 xbar x + 1"),
        col("x").add(lit(1.0)).floor_div(lit(5i64)).mul(lit(5i64))
    );
    assert_eq!(
        parse("2 * 5 xbar x"),
        lit(2.0).mul(col("x").floor_div(lit(5i64)).mul(lit(5i64)))
    );
    // flag = name in [...] compares flag with the membership test.
    assert_eq!(
        parse("flag = name in [\"a\"]"),
        col("flag").eq(col("name").eq(lit("a")))
    );
    assert_eq!(parse("x mod 2 in [1]"), col("x").rem(lit(2.0).eq(lit(1.0))));
    // A symbol operator to the left of like takes the whole like as its right side.
    assert_eq!(
        parse("ok = name like \"a*\""),
        col("ok").eq(col("name")
            .cast(DataType::String)
            .str()
            .contains(lit("(?s)^a.*$"), true))
    );
}

#[test]
fn test_word_operators_right_to_left_evaluate() {
    let df = df!("x" => &[3i64, 4, 9]).unwrap();
    // 1 + x mod 4 is 1 + (x mod 4), not (1 + x) mod 4.
    // A whole number added to an integer is one, as in q.
    let out = eval("select a: 1 + x mod 4", &df);
    assert_eq!(values(&out, "a"), ["4", "1", "2"]);
    let out = eval("select a: (1 + x) mod 4", &df);
    assert_eq!(values(&out, "a"), ["0", "1", "2"]);
    // x mod 2 in [1] would be x mod (2 in [1]); parentheses test the remainder.
    let out = eval("select x where (x mod 2) in [1]", &df);
    assert_eq!(values(&out, "x"), ["3", "9"]);
}

#[test]
fn test_word_operators_are_still_column_names() {
    let cols = parse_query("select in, mod, like + xbar").unwrap().cols;
    assert_eq!(cols[0], col("in"));
    assert_eq!(cols[1], col("mod"));
    assert_eq!(cols[2], col("like").add(col("xbar")));
    let err = parse_err("select x.in");
    assert!(err.contains("Unknown accessor: 'in'"), "{err}");
}

#[test]
fn test_new_aggregates() {
    let cols = parse_query("select nunique ID, var x, dev x by g")
        .unwrap()
        .cols;
    assert_eq!(cols[0], col("ID").n_unique().alias("nunique_ID"));
    assert_eq!(cols[1], col("x").var(1).alias("var_x"));
    assert_eq!(cols[2], col("x").std(1).alias("dev_x"));

    let df = df!(
        "g" => &["a", "a", "a", "b"],
        "ID" => &["s1", "s1", "s2", "s3"],
        "x" => &[1.0f64, 2.0, 3.0, 5.0],
    )
    .unwrap();
    let out = eval("select nunique ID, var x, dev[x] by g", &df);
    assert_eq!(values(&out, "nunique_ID"), ["2", "1"]);
    assert_eq!(values(&out, "var_x"), ["1.0", "null"]);
    assert_eq!(values(&out, "dev_x"), ["1.0", "null"]);
}

#[test]
fn test_wavg() {
    let df = df!(
        "g" => &["a", "a", "a", "b"],
        "w" => &[Some(1i64), Some(3), Some(5), Some(2)],
        "x" => &[Some(10.0f64), Some(20.0), None, Some(4.0)],
    )
    .unwrap();
    let out = eval("select w wavg x by g", &df);
    // The null value's weight stays out of the total: (10 + 60) / 4.
    assert_eq!(values(&out, "wavg_x"), ["17.5", "4.0"]);
    // A group with no complete pair has no average.
    let out = eval("select w wavg x where null x", &df);
    assert_eq!(values(&out, "wavg_x"), ["null"]);
    for query in ["select wavg[x]", "select wavg x"] {
        let err = parse_err(query);
        assert!(
            err.contains("wavg goes between weights and values"),
            "{err}"
        );
    }
}

#[test]
fn test_round_and_math_functions() {
    let df = df!("x" => &[2.25f64, -2.5, 4.0]).unwrap();
    let out = eval(
        "select r: x.round, r1: x.round[1], s: sqrt x, l: log[x], e: exp 0 * x",
        &df,
    );
    assert_eq!(values(&out, "r"), ["2.0", "-3.0", "4.0"]);
    assert_eq!(values(&out, "r1"), ["2.3", "-2.5", "4.0"]);
    assert_eq!(values(&out, "s"), ["1.5", "NaN", "2.0"]);
    assert!(values(&out, "l")[2].starts_with("1.386"));
    assert_eq!(values(&out, "e"), ["1.0", "1.0", "1.0"]);
    // An aggregate rounds after it is computed.
    let df = df!("g" => &["a", "a"], "d" => &[1.0f64, 2.34]).unwrap();
    let out = eval("select m: (avg d).round[1] by g", &df);
    assert_eq!(values(&out, "m"), ["1.7"]);
}

#[test]
fn test_select_distinct() {
    let ParsedQuery { cols, distinct, .. } =
        parse_query("select distinct carrier, origin").unwrap();
    assert!(distinct);
    assert_eq!(cols, vec![col("carrier"), col("origin")]);
    let distinct = parse_query("select carrier").unwrap().distinct;
    assert!(!distinct);

    let df = df!(
        "carrier" => &["UA", "UA", "AA", "UA"],
        "origin" => &["EWR", "EWR", "JFK", "LGA"],
        "n" => &[1i32, 2, 3, 4],
    )
    .unwrap();
    let out = eval("select distinct carrier, origin", &df);
    assert_eq!(values(&out, "carrier"), ["UA", "AA", "UA"]);
    assert_eq!(values(&out, "origin"), ["EWR", "JFK", "LGA"]);
    let out = eval("select distinct carrier where n > 1", &df);
    assert_eq!(values(&out, "carrier"), ["UA", "AA"]);
    // A column named distinct is col["distinct"].
    let ParsedQuery { cols, distinct, .. } = parse_query("select col[\"distinct\"]").unwrap();
    assert!(!distinct);
    assert_eq!(cols, vec![col("distinct")]);
    // So is a bare `distinct` that is plainly a column or an alias.
    let ParsedQuery { cols, distinct, .. } = parse_query("select distinct, n").unwrap();
    assert!(!distinct);
    assert_eq!(cols, vec![col("distinct"), col("n")]);
    let ParsedQuery { cols, distinct, .. } = parse_query("select distinct: n").unwrap();
    assert!(!distinct);
    assert_eq!(cols, vec![col("n").alias("distinct")]);
}

#[test]
fn test_from_df_is_optional() {
    // `from df` sits where q puts it: after the select list and by, before where.
    for (with, without) in [
        (
            "select mean dep_delay by hour from df where origin = \"JFK\"",
            "select mean dep_delay by hour where origin = \"JFK\"",
        ),
        ("select from df where x > 1", "select where x > 1"),
        ("select from df", "select"),
        ("select a, b from df", "select a, b"),
        ("select distinct a from df", "select distinct a"),
        ("select n: count a by g from df", "select n: count a by g"),
    ] {
        assert_eq!(
            format!("{:?}", parse_query(with).unwrap()),
            format!("{:?}", parse_query(without).unwrap()),
            "{with}"
        );
    }
}

#[test]
fn test_from_names_only_df() {
    for query in [
        "select from trades",
        "select a by g from trades where a > 1",
        "select from data.csv",
    ] {
        assert_eq!(
            parse_query(query).unwrap_err(),
            "q reads the table on screen, named df: … from df …",
            "{query}"
        );
    }
    let err = parse_query("select a where a > 1 from df").unwrap_err();
    assert!(err.contains("after the where clause"), "{err}");
    let err = parse_query("select a from df by g").unwrap_err();
    assert!(err.contains("'by' after 'from df'"), "{err}");
}

#[test]
fn test_from_column_names_and_values() {
    // A column named `from` still reads as one wherever a table name cannot follow.
    let cols = |q: &str| parse_query(q).unwrap().cols;
    assert_eq!(cols("select from"), vec![col("from")]);
    assert_eq!(cols("select from, to"), vec![col("from"), col("to")]);
    assert_eq!(cols("select from from df"), vec![col("from")]);
    assert_eq!(cols("select from + 1"), vec![col("from") + lit(1.0)]);
    assert_eq!(cols("select from.year"), cols("select col[\"from\"].year"));
    assert_eq!(cols("select max from"), cols("select max col[\"from\"]"));
    let ParsedQuery { group_by, .. } = parse_query("select n: count a by from").unwrap();
    assert_eq!(group_by, vec![col("from")]);
    assert_eq!(
        only(parse_query("select where from = \"df\"").unwrap().filters),
        Some(col("from").eq(lit("df")))
    );
    assert_eq!(
        only(parse_query("select where from in [1, 2]").unwrap().filters),
        only(
            parse_query("select where col[\"from\"] in [1, 2]")
                .unwrap()
                .filters
        )
    );
    // Names that contain the word, and values that are it.
    assert_eq!(
        cols("select from_city, datefrom from df"),
        vec![col("from_city"), col("datefrom")]
    );
    assert_eq!(
        only(
            parse_query("select from df where city = \"from df\"")
                .unwrap()
                .filters
        ),
        Some(col("city").eq(lit("from df")))
    );
    // A column named df is still a column.
    assert_eq!(cols("select df from df"), vec![col("df")]);
    // Run against data: from df changes nothing.
    let df = df!("from" => &[1i64, 2, 3], "df" => &["x", "y", "z"]).unwrap();
    let out = eval("select from, df from df where from > 1", &df);
    assert_eq!(values(&out, "df"), ["y", "z"]);
}

#[test]
fn test_accessor_argument_count_errors() {
    for (query, expected) in [
        (
            "select x.part[\",\"]",
            "part takes 2 arguments, e.g. .part[\"-\", 0]; got 1",
        ),
        ("select x.slice", "slice takes 1 to 2 arguments"),
        ("select x.replace[\"a\"]", "replace takes 2 arguments"),
        ("select x.round[1, 2]", "round takes 0 to 1 arguments"),
        (
            "select x.to_date[\"%Y\", \"%m\"]",
            "to_date takes 0 to 1 arguments",
        ),
        ("select x.hour[1]", "hour takes no arguments"),
        ("select x.strip[\" \"]", "strip takes no arguments"),
        ("select x.int[1]", "int takes no arguments"),
        ("select x.format", "format takes 1 argument"),
    ] {
        let err = parse_err(query);
        assert!(err.contains(expected), "{query}: {err}");
    }
}

#[test]
fn test_accessor_argument_type_errors() {
    for (query, expected) in [
        (
            "select x.part[0, \",\"]",
            "part: argument 1 must be quoted text",
        ),
        (
            "select x.part[\",\", \"a\"]",
            "part: argument 2 must be a whole number",
        ),
        (
            "select x.part[\",\", 1.5]",
            "part: argument 2 must be a whole number",
        ),
        ("select x.round[-1]", "round: decimals cannot be negative"),
        (
            "select x.slice[0, -1]",
            "slice: the length cannot be negative",
        ),
        ("select x.slice[a + 1]", "slice takes literal arguments"),
        ("select x.part[\",\"", "Unmatched bracket after .part"),
    ] {
        let err = parse_err(query);
        assert!(err.contains(expected), "{query}: {err}");
    }
}

#[test]
fn test_unknown_accessor_lists_new_names() {
    let err = parse_err("select x.nosuch");
    for name in [
        "hour",
        "minute",
        "second",
        "quarter",
        "doy",
        "to_date",
        "to_datetime",
        "part",
        "slice",
        "replace",
        "strip",
        "round",
        "int",
        "float",
        "str",
    ] {
        assert!(err.contains(name), "{name} missing from: {err}");
    }
}

#[test]
fn test_nested_functions_parse_in_linear_time() {
    // Each argument used to be parsed as an aggregate and then again as a scalar
    // function, doubling the work at every level of nesting.
    let bare = format!("select {}x", "abs ".repeat(40));
    assert!(parse_query(&bare).is_ok());
    let bracketed = format!("select {}x{}", "sqrt[".repeat(30), "]".repeat(30));
    assert!(parse_query(&bracketed).is_ok());
}

/// One expression's Python code.
fn py(expr: &str) -> String {
    parse_node(&tokenize(expr).unwrap()).unwrap().python()
}

#[test]
fn expressions_read_as_python_polars() {
    assert_eq!(py("a"), "pl.col(\"a\")");
    assert_eq!(py("col[\"first name\"]"), "pl.col(\"first name\")");
    // Python's `&` binds tighter than its comparisons, so operands that are
    // operations are parenthesized.
    assert_eq!(py("a > 1"), "pl.col(\"a\") > 1.0");
    assert_eq!(
        py("a + b * c"),
        "pl.col(\"a\") + (pl.col(\"b\") * pl.col(\"c\"))"
    );
    assert_eq!(py("-x"), "pl.lit(0) - pl.col(\"x\")");
    assert_eq!(py("x mod 3"), "pl.col(\"x\") % 3");
    assert_eq!(py("5 xbar fare"), "(pl.col(\"fare\") // 5) * 5");
    assert_eq!(py("a ^ 0"), "pl.coalesce(pl.col(\"a\"), pl.lit(0.0))");
    assert_eq!(
        py("name in [\"Emma\", \"Olivia\"]"),
        "(pl.col(\"name\") == \"Emma\") | (pl.col(\"name\") == \"Olivia\")"
    );
    assert_eq!(
        py("item like \"*Chicken*\""),
        "pl.col(\"item\").cast(pl.String).str.contains(\"(?s)^.*Chicken.*$\")"
    );
    assert_eq!(
        py("d = 2024.01.31"),
        "pl.col(\"d\") == pl.date(2024, 1, 31)"
    );
    assert_eq!(
        py("t > 2024.01.31T10:00:00.5"),
        "pl.col(\"t\") > pl.lit(\"2024-01-31T10:00:00.500\").str.to_datetime(\"%Y-%m-%dT%H:%M:%S%.3f\", time_unit=\"ms\")"
    );
}

#[test]
fn division_always_gives_a_fraction() {
    // q's `%` gives a float, even of two whole numbers; Python's `/` does too.
    assert_eq!(py("i / j"), "pl.col(\"i\") / pl.col(\"j\")");
    assert_eq!(py("j % i"), "pl.col(\"j\") / pl.col(\"i\")");
    assert_eq!(py("(i mod 3) / j"), "(pl.col(\"i\") % 3) / pl.col(\"j\")");
    assert_eq!(
        py("sum[i] / count[j]"),
        "pl.col(\"i\").sum().alias(\"sum_i\") / pl.col(\"j\").count().alias(\"count_j\")"
    );
    let df = df!("i" => &[7i64, -7], "j" => &[2i32, 2]).unwrap();
    let out = eval("select q: i % j, r: (i + 1) / j, w: floor[i % j]", &df);
    assert_eq!(values(&out, "q"), ["3.5", "-3.5"]);
    assert_eq!(values(&out, "r"), ["4.0", "-3.0"]);
    assert_eq!(values(&out, "w"), ["3.0", "-4.0"]);
}

#[test]
fn functions_and_accessors_read_as_python_polars() {
    assert_eq!(
        py("avg salary"),
        "pl.col(\"salary\").mean().alias(\"avg_salary\")"
    );
    assert_eq!(py("not null[x]"), "pl.col(\"x\").is_null().not_()");
    assert_eq!(py("log x"), "pl.col(\"x\").log()");
    assert_eq!(py("ts.year"), "pl.col(\"ts\").dt.year().alias(\"ts_year\")");
    assert_eq!(
        py("d.format[\"%Y-%m\"]"),
        "pl.col(\"d\").dt.to_string(\"%Y-%m\").alias(\"d_format_%Y-%m\")"
    );
    assert_eq!(
        py("code.part[\"-\", 0]"),
        "pl.col(\"code\").cast(pl.String).str.split(\"-\").list.get(0, null_on_oob=True).alias(\"code_part_-_0\")"
    );
    assert_eq!(
        py("s.slice[1]"),
        "pl.col(\"s\").cast(pl.String).str.slice(1).alias(\"s_slice_1\")"
    );
    assert_eq!(
        py("s.to_date[\"%Y%m%d\"]"),
        "pl.col(\"s\").cast(pl.String).str.to_date(\"%Y%m%d\", strict=False).alias(\"s_to_date_%Y%m%d\")"
    );
    assert_eq!(
        py("x.round[2]"),
        "pl.col(\"x\").round(2, mode=\"half_away_from_zero\").alias(\"x_round_2\")"
    );
    assert_eq!(
        py("x.int"),
        "pl.col(\"x\").cast(pl.Int64, strict=False).alias(\"x_int\")"
    );
    assert_eq!(
        py("w wavg v"),
        "((pl.col(\"w\") * pl.col(\"v\")).sum() / pl.when(pl.col(\"w\").filter((pl.col(\"w\") * pl.col(\"v\")).is_not_null()).sum() != 0).then(pl.col(\"w\").filter((pl.col(\"w\") * pl.col(\"v\")).is_not_null()).sum()).otherwise(pl.lit(None))).alias(\"wavg_v\")"
    );
}

#[test]
fn a_whole_query_reads_as_python_steps() {
    let steps = |q: &str, keys: &[&str]| {
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        parse_nodes(q).unwrap().python_steps(&keys)
    };
    assert_eq!(
        steps(
            "select name, pay: salary * 1.1 where dept = \"Sales\", (age > 30) | not senior",
            &[]
        ),
        vec![
            ".filter(pl.col(\"dept\") == \"Sales\")",
            ".filter((pl.col(\"age\") > 30.0) | pl.col(\"senior\").not_())",
            ".select(\"name\", (pl.col(\"salary\") * 1.1).alias(\"pay\"))",
        ]
    );
    assert_eq!(
        steps("select by dept", &["dept"]),
        vec![
            ".group_by(\"dept\")",
            ".agg(pl.all().exclude(\"dept\"))",
            ".sort(\"dept\", nulls_last=True, maintain_order=True)",
        ]
    );
    assert_eq!(
        steps("select distinct dept", &[]),
        vec![
            ".select(\"dept\")",
            ".unique(keep=\"first\", maintain_order=True)",
        ]
    );
    assert!(steps("", &[]).is_empty());
}

// --- q's `&` and `|`, and successive where conditions ---

/// One expression's node, as parsed.
fn node(expr: &str) -> Node {
    parse_node(&tokenize(expr).unwrap()).unwrap()
}

fn c(name: &str) -> Node {
    Node::Col(name.to_string())
}

#[test]
fn and_and_or_parse_right_to_left_like_every_operator() {
    let a_gt_5 = c("a").bin(BinOp::Gt, Node::Num(5.0));
    let b_lt_3 = c("b").bin(BinOp::Lt, Node::Num(3.0));
    let c_eq_1 = c("c").bin(BinOp::Eq, Node::Num(1.0));
    // a > (5 & (b < 3)): the leftmost operator is the root.
    let strict = c("a").bin(BinOp::Gt, Node::Num(5.0).bin(BinOp::Lesser, b_lt_3.clone()));
    assert_eq!(node("a > 5 & b < 3"), strict);
    assert_eq!(node("a > 5 and b < 3"), strict);
    // Parenthesize the left comparison for the logical and.
    let both = a_gt_5.clone().bin(BinOp::And, b_lt_3.clone());
    assert_eq!(node("(a > 5) & b < 3"), both);
    assert_eq!(node("(a>5) and b<3"), both);
    // (a > 5) or ((b < 3) and (c = 1)).
    let either = a_gt_5
        .clone()
        .bin(BinOp::Or, b_lt_3.clone().bin(BinOp::And, c_eq_1.clone()));
    assert_eq!(node("(a>5) | (b<3) & c=1"), either);
    assert_eq!(node("(a>5) or (b<3) and c=1"), either);
    // Numbers, or a column whose type the parser does not know, are min/max.
    assert_eq!(node("a & 3"), c("a").bin(BinOp::Lesser, Node::Num(3.0)));
    assert_eq!(node("0 | x"), Node::Num(0.0).bin(BinOp::Greater, c("x")));
    // A column named `and` or `or` still reads as one where an operand goes.
    assert_eq!(node("or"), c("or"));
    assert_eq!(node("and + 1"), c("and").bin(BinOp::Add, Node::Num(1.0)));
}

#[test]
fn not_takes_everything_to_its_right() {
    let a_gt_5 = c("a").bin(BinOp::Gt, Node::Num(5.0));
    let b_lt_3 = c("b").bin(BinOp::Lt, Node::Num(3.0));
    assert_eq!(node("not a > 5"), a_gt_5.clone().op(Op::Not));
    assert_eq!(
        node("not (a > 5) & b < 3"),
        a_gt_5.clone().bin(BinOp::And, b_lt_3.clone()).op(Op::Not)
    );
    assert_eq!(
        node("(not a > 5) & b < 3"),
        a_gt_5.op(Op::Not).bin(BinOp::And, b_lt_3)
    );
}

#[test]
fn and_or_are_logical_on_booleans_and_min_max_on_numbers() {
    let df = df!(
        "a" => &[Some(1i64), Some(4), Some(6), None, Some(8)],
        "b" => &[2i64, 1, 9, 5, 3],
        "f" => &[Some(true), None, Some(false), Some(true), None],
    )
    .unwrap();
    // Numbers: the smaller and the larger, row by row, and integers stay integers.
    // Any value exceeds a null in q: `&` with a null is null, `|` the other side.
    let out = eval("select lo: a & b, hi: a or b, floor: 0 | a - 5", &df);
    assert_eq!(values(&out, "lo"), ["1", "1", "6", "null", "3"]);
    assert_eq!(values(&out, "hi"), ["2", "4", "9", "5", "8"]);
    assert_eq!(values(&out, "floor"), ["0", "0", "1", "0", "3"]);
    // A boolean with a number is 0/1 in the number's type.
    let out = eval("select m: (a > 3) & 5, x: 0 | b > 2, y: (b > 2) | 0.5", &df);
    assert_eq!(values(&out, "m"), ["0", "1", "1", "null", "1"]);
    assert_eq!(values(&out, "x"), ["0", "0", "1", "1", "1"]);
    assert_eq!(values(&out, "y"), ["0.5", "0.5", "1.0", "1.0", "1.0"]);
    // Two booleans, a column among them, are and/or with the same null rule:
    // `false & null` is null, `false | null` is false.
    let out = eval("select b, k: f and b > 4, o: f | b > 4", &df);
    assert_eq!(
        values(&out, "k"),
        ["false", "null", "false", "true", "null"]
    );
    assert_eq!(
        values(&out, "o"),
        ["true", "false", "true", "true", "false"]
    );
    let out = eval("select b where f & b > 1", &df);
    assert_eq!(values(&out, "b"), ["2", "5"]);
    let out = eval("select b where f | b > 2", &df);
    assert_eq!(values(&out, "b"), ["2", "9", "5", "3"]);
    // `not` composes: not (f and (b > 4)), null where f is.
    let out = eval("select b where not f & b > 4", &df);
    assert_eq!(values(&out, "b"), ["2", "9"]);
}

#[test]
fn not_of_a_number_is_whether_it_is_zero() {
    let df = df!(
        "x" => &[Some(0i64), Some(3), None],
        "y" => &[0.0, 0.0, 1.0],
    )
    .unwrap();
    let out = eval("select n: not x, m: not y", &df);
    assert_eq!(values(&out, "n"), ["true", "false", "null"]);
    assert_eq!(values(&out, "m"), ["true", "true", "false"]);
    // Booleans, so `&` is and, not a bitwise and of -1s.
    let out = eval("select x where (not x) & not y", &df);
    assert_eq!(values(&out, "x"), ["0"]);
    let mut nodes = parse_nodes("select n: not x").unwrap();
    nodes.resolve_types(df.schema()).unwrap();
    assert_eq!(nodes.cols[0].python(), "(pl.col(\"x\") == 0).alias(\"n\")");
}

#[test]
fn and_or_types_follow_q() {
    let schema = Schema::from_iter([
        Field::new("d".into(), DataType::Date),
        Field::new("t".into(), DataType::Datetime(TimeUnit::Microseconds, None)),
        Field::new("s".into(), DataType::String),
        Field::new("i".into(), DataType::Int64),
        Field::new("u".into(), DataType::UInt64),
        Field::new("n".into(), DataType::Int32),
        Field::new("g".into(), DataType::Float32),
        Field::new("f".into(), DataType::Boolean),
    ]);
    let err = |q: &str| parse_query_over(q, Some(&schema)).unwrap_err();
    assert_eq!(
        err("select d & 5"),
        "`&` cannot combine a date or time with a number"
    );
    assert_eq!(
        err("select f | t"),
        "`|` cannot combine a boolean with a date or time"
    );
    assert_eq!(
        err("select s | 1"),
        "`|` cannot combine a string with a number"
    );
    assert_eq!(
        err("select where i > s & f"),
        "`&` cannot combine a string with a boolean"
    );
    // Two dates or times, or two strings, are fine.
    assert!(parse_query_over("select d & t, s | s", Some(&schema)).is_ok());
    let typed = |expr: &str| {
        let mut node = node(expr);
        node.resolve_types(&schema).unwrap();
        node
    };
    let dtype = |expr: &str| typed(expr).dtype(&schema).unwrap();
    // A boolean takes the number's type; a whole number typed next to an integer is
    // one, so the integer type stays.
    assert_eq!(dtype("f & n"), DataType::Int32);
    assert_eq!(dtype("g | f"), DataType::Float32);
    assert_eq!(dtype("i & 5"), DataType::Int64);
    assert_eq!(dtype("0 | i - 5"), DataType::Int64);
    assert_eq!(dtype("i & 2.5"), DataType::Float64);
    // Int64 with UInt64 is Int64, as q keeps a long (Polars would give Float64).
    assert_eq!(dtype("i | u"), DataType::Int64);
    assert_eq!(
        typed("f & n").python(),
        "pl.when(pl.col(\"f\").cast(pl.Int32, strict=False).is_null() | pl.col(\"n\").is_null()).then(None).otherwise(pl.min_horizontal(pl.col(\"f\").cast(pl.Int32, strict=False), pl.col(\"n\")))"
    );
}

#[test]
fn doubled_operators_and_empty_conditions_are_errors() {
    assert_eq!(parse_err("select where a && b"), "`&&` is not q: use `&`");
    assert_eq!(parse_err("select where a || b"), "`||` is not q: use `|`");
    assert_eq!(
        parse_err("select where a &| b"),
        "`&|` is not q: use `&` or `|`"
    );
    let empty = "Empty condition in where: a ',' with nothing before or after it";
    for q in [
        "select where f,, b > 2",
        "select where ,f",
        "select where f,",
    ] {
        assert_eq!(parse_err(q), empty, "{q}");
    }
    // A where with nothing after it is still every row.
    assert!(parse_query("select where").unwrap().filters.is_empty());
}

#[test]
fn a_long_chain_of_and_is_refused_not_grown() {
    let chain = vec!["x"; 40].join(" & ");
    let err = parse_err(&format!("select {chain}"));
    assert!(err.contains("nested wavg, xbar, in or &"), "{err}");
    // Tests joined with `&` repeat nothing, however many.
    let tests: Vec<String> = (0..60).map(|i| format!("(x > {i})")).collect();
    assert!(parse_query(&format!("select where {}", tests.join(" & "))).is_ok());
}

#[test]
fn a_comparison_left_of_or_reads_strictly() {
    let df = df!("a" => &[1i64, 3, 7, 12]).unwrap();
    // a > (10 | (a < 5)) is a > 10.
    assert_eq!(
        values(&eval("select a where a > 10 | a < 5", &df), "a"),
        ["12"]
    );
    // Parenthesized, the left comparison is its own operand.
    assert_eq!(
        values(&eval("select a where (a > 10) | a < 5", &df), "a"),
        ["1", "3", "12"]
    );
}

#[test]
fn where_conditions_run_in_turn() {
    let df = df!(
        "price" => &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "size" => &[100i64, 1, 1, 1, 1, 10],
    )
    .unwrap();
    // The second average is over the rows the first condition kept: sizes 1, 1, 10.
    let out = eval("select price where price > avg price, size > avg size", &df);
    assert_eq!(values(&out, "price"), ["6.0"]);
    // One condition sees every row: the average size is 19.
    let out = eval(
        "select price where (price > avg price) & size > avg size",
        &df,
    );
    assert_eq!(out.height(), 0);
    assert_eq!(
        parse_query("select where price > avg price, size > avg size")
            .unwrap()
            .filters
            .len(),
        2
    );
}

#[test]
fn a_comma_in_parentheses_is_an_error() {
    let expected = "`,` inside parentheses joins lists in q, which datui does not support; \
                    combine conditions with `&` or `|`";
    assert_eq!(parse_err("select where (a > 1, b < 2)"), expected);
    assert_eq!(parse_err("select where c = 1, (a > 1, b < 2)"), expected);
    assert_eq!(parse_err("select x: (a, b)"), expected);
    // Brackets keep their commas.
    assert!(parse_query("select where (a in [1, 2]) & b < 3").is_ok());
}

#[test]
fn and_or_read_as_python_polars() {
    let predicate = |expr: &str| node(expr).python_predicate();
    // As a where condition, only the true rows matter: Python's `&` and `|`.
    assert_eq!(
        predicate("(a > 5) & b < 3"),
        "(pl.col(\"a\") > 5.0) & (pl.col(\"b\") < 3.0)"
    );
    assert_eq!(
        predicate("(a>5) | (b<3) & c=1"),
        "(pl.col(\"a\") > 5.0) | ((pl.col(\"b\") < 3.0) & (pl.col(\"c\") == 1.0))"
    );
    // As a value, a null on either side of `&` is null: 1 × 1 is 1, and a null spreads.
    assert_eq!(
        py("(a > 5) & b < 3"),
        "((pl.col(\"a\") > 5.0).cast(pl.UInt8) * (pl.col(\"b\") < 3.0).cast(pl.UInt8)).cast(pl.Boolean)"
    );
    assert_eq!(
        py("(a > 5) | b < 3"),
        "pl.max_horizontal(pl.col(\"a\") > 5.0, pl.col(\"b\") < 3.0)"
    );
    assert_eq!(
        py("a & 3"),
        "pl.when(pl.col(\"a\").is_null() | pl.lit(3.0).is_null()).then(None).otherwise(pl.min_horizontal(pl.col(\"a\"), pl.lit(3.0)))"
    );
    assert_eq!(
        py("0 | x - 5"),
        "pl.max_horizontal(pl.lit(0.0), pl.col(\"x\") - 5.0)"
    );
    assert_eq!(
        predicate("not (a > 5) & b < 3"),
        "((pl.col(\"a\") > 5.0).cast(pl.UInt8) * (pl.col(\"b\") < 3.0).cast(pl.UInt8)).cast(pl.Boolean).not_()"
    );
    // Over the data's types: a boolean column settles to `&`, a boolean with a
    // number to 0/1 in its type, a whole number next to an integer to an integer.
    let schema = Schema::from_iter([
        Field::new("f".into(), DataType::Boolean),
        Field::new("a".into(), DataType::Int64),
    ]);
    let typed = |expr: &str| {
        let mut node = node(expr);
        node.resolve_types(&schema).unwrap();
        node
    };
    assert_eq!(
        typed("f & a > 1").python_predicate(),
        "pl.col(\"f\") & (pl.col(\"a\") > 1.0)"
    );
    assert_eq!(
        typed("a > 5 | a < 3").python(),
        "pl.col(\"a\") > pl.max_horizontal(pl.lit(5), (pl.col(\"a\") < 3.0).cast(pl.Int64, strict=False))"
    );
    assert_eq!(
        typed("a | 0").python(),
        "pl.max_horizontal(pl.col(\"a\"), pl.lit(0))"
    );
}

#[test]
fn successive_conditions_read_as_one_python_filter_each() {
    let steps = parse_nodes("select price where price > avg price, size > avg size")
        .unwrap()
        .python_steps(&[]);
    assert_eq!(
        steps,
        vec![
            ".filter(pl.col(\"price\") > pl.col(\"price\").mean().alias(\"avg_price\"))",
            ".filter(pl.col(\"size\") > pl.col(\"size\").mean().alias(\"avg_size\"))",
            ".select(\"price\")",
        ]
    );
}
