//! A SQL statement parsed as polars-sql parses it, but no deeper than `limits.sql_depth`.
//!
//! sqlparser retries the rest of a dotted name at every `.` when the name's tail fails
//! to parse (`parse_compound_expr`: `maybe_parse` of the tail, then an identifier and
//! on to the next `.`), so a broken statement costs about twice as much per level it
//! nests. Below the default bound that is milliseconds; a fuzzer's statement 40 levels
//! deep took 36 seconds per parse. polars-sql parses with the parser's own bound of 50,
//! so the statement is read here first, and one the bounded parse fails on is refused
//! before polars-sql sees it.

use polars::prelude::PolarsError;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError, ParserOptions};

/// sqlparser's own bound, in force when `limits.sql_depth` is 0 or past it.
const PARSER_DEFAULT_DEPTH: usize = 50;

/// The deepest a statement may nest under `limit`, as the parser counts levels. Past the
/// parser's own bound the guard would only cost more than polars-sql's parse does.
fn depth(limit: usize) -> usize {
    if limit == 0 {
        PARSER_DEFAULT_DEPTH
    } else {
        limit.min(PARSER_DEFAULT_DEPTH)
    }
}

/// The statements of `sql`, parsed with polars-sql's options so the two read the same
/// statement, within `limits.sql_depth`.
pub(crate) fn statements(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::new(&GenericDialect)
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .with_recursion_limit(depth(crate::limits::get().sql_depth))
        .try_with_sql(sql)?
        .parse_statements()
}

/// Whether polars-sql may parse `sql`: an error for a statement that nests deeper than
/// `limits.sql_depth`, naming the key, or for one with a mistake, worded as polars-sql
/// words it (`SQLInterface` of the parser's error). Any failure of the bounded parse is
/// final: sqlparser retries a keyword such as CASE or NOT as an identifier when what
/// follows it fails, with the depth error included, and reports an ordinary error instead,
/// so a statement that fails here is never handed to the parser's own bound of 50.
pub(crate) fn check(sql: &str) -> Result<(), PolarsError> {
    match statements(sql) {
        Ok(_) => Ok(()),
        Err(ParserError::RecursionLimitExceeded) => Err(PolarsError::SQLInterface(
            format!(
                "The statement nests more than {} levels deep {} limits.sql_depth raises it",
                depth(crate::limits::get().sql_depth),
                crate::glyphs::get().middot
            )
            .into(),
        )),
        Err(e) => Err(PolarsError::SQLInterface(e.to_string().into())),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The sqlparser hang the `sql_group_plan` fuzz target found: a dotted name whose tail
    /// fails, in five parentheses. 36 seconds per parse without a bound; refused at once.
    pub(crate) const FUZZ_HANG: &str = "SELECT a.deptCO(((((UN.a.d.da.d.dWN.COUN.a.d.dadeptCOa.d.da.d.dW.d.dWN.COUN.a.d.da.d.dW.d.dWN.a.d.[da=.d.da.d.dWIAIASR";

    /// The same shape behind a keyword sqlparser retries as an identifier when the
    /// expression after it fails: the bounded parse reports an ordinary error, not the
    /// depth, and the parser's own bound takes 16 seconds over it.
    const DEEP_TAIL: &str = "a.f(((a.d.a.d.a.d.a.d.a.d.a.d.a.d.a.d.a.d.a.d.a.d.a.d.d.[x=";

    #[test]
    fn a_statement_past_the_bound_is_refused_naming_the_key() {
        let message = check(FUZZ_HANG).expect_err("refused").to_string();
        assert!(message.contains("more than 24 levels deep"), "{message}");
        assert!(message.ends_with("limits.sql_depth raises it"), "{message}");
        assert_eq!(
            statements(FUZZ_HANG),
            Err(ParserError::RecursionLimitExceeded)
        );
    }

    /// Whatever the bounded parse reports, the statement goes no further: these would
    /// parse for 16 seconds under the parser's own bound.
    #[test]
    fn a_deep_statement_behind_case_or_not_is_refused_too() {
        for sql in [
            format!("SELECT CASE WHEN {DEEP_TAIL}"),
            format!("SELECT NOT {DEEP_TAIL}"),
        ] {
            let error = check(&sql).expect_err("refused");
            assert!(
                matches!(error, PolarsError::SQLInterface(_)),
                "{sql}: {error:?}"
            );
            assert!(
                error.to_string().starts_with("sql parser error:"),
                "{sql}: {error}"
            );
        }
    }

    /// A mistake within the bound is reported as polars-sql reports it; sound statements
    /// well under the bound pass.
    #[test]
    fn statements_within_the_bound_pass_or_fail_as_polars_sql_reads_them() {
        let error = check("SELECT f((((x.y.")
            .expect_err("a mistake")
            .to_string();
        assert!(error.starts_with("sql parser error:"), "{error}");
        assert!(!error.contains("limits.sql_depth"), "{error}");
        assert!(check("SELECT dept, COUNT(*) FROM df GROUP BY dept").is_ok());
        assert!(
            check(
                "SELECT * FROM df WHERE x IN (SELECT x FROM df WHERE y > (SELECT AVG(y) FROM df))"
            )
            .is_ok()
        );
        assert!(check("SELECT f(g(h(i(j(k(l(m(n(o(1)))))))))) FROM df").is_ok());
        assert!(check("SELECT a.b.c.d.e.f.g.h FROM df").is_ok());
    }

    #[test]
    fn zero_or_more_than_the_parsers_own_bound_means_that_bound() {
        assert_eq!(depth(0), PARSER_DEFAULT_DEPTH);
        assert_eq!(depth(24), 24);
        assert_eq!(depth(60), PARSER_DEFAULT_DEPTH);
    }
}
