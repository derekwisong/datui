//! A SQL statement parsed as polars-sql parses it, but no deeper than `limits.sql_depth`.
//!
//! sqlparser retries the rest of a dotted name at every `.` when the name's tail fails
//! to parse (`parse_compound_expr`: `maybe_parse` of the tail, then an identifier and
//! on to the next `.`), so a broken statement costs about twice as much per level it
//! nests. Below the default bound that is milliseconds; a fuzzer's statement 40 levels
//! deep took 36 seconds per parse. polars-sql parses with the parser's own bound of 50,
//! so the statement is read here first, and one past the bound is refused before
//! polars-sql sees it.

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError, ParserOptions};

/// sqlparser's own bound, in force when `limits.sql_depth` is 0.
const PARSER_DEFAULT_DEPTH: usize = 50;

/// The deepest a statement may nest under `limit`, as the parser counts levels.
fn depth(limit: usize) -> usize {
    if limit == 0 {
        PARSER_DEFAULT_DEPTH
    } else {
        limit
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

/// Why `sql` is refused when it nests deeper than `limits.sql_depth`, naming the key;
/// `None` for a statement within it, parsed or not, which polars-sql reads the same way.
pub(crate) fn too_deep(sql: &str) -> Option<String> {
    match statements(sql) {
        Err(ParserError::RecursionLimitExceeded) => Some(format!(
            "The statement nests more than {} levels deep (parentheses, calls, subqueries \
             and the parts of dotted names) {} limits.sql_depth raises it",
            depth(crate::limits::get().sql_depth),
            crate::glyphs::get().middot
        )),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The sqlparser hang the `sql_group_plan` fuzz target found: a dotted name whose tail
    /// fails, in five parentheses. 36 seconds per parse without a bound; refused at once.
    pub(crate) const FUZZ_HANG: &str = "SELECT a.deptCO(((((UN.a.d.da.d.dWN.COUN.a.d.dadeptCOa.d.da.d.dW.d.dWN.COUN.a.d.da.d.dW.d.dWN.a.d.[da=.d.da.d.dWIAIASR";

    #[test]
    fn a_statement_past_the_bound_is_refused_naming_the_key() {
        let message = too_deep(FUZZ_HANG).expect("refused");
        assert!(message.contains("24 levels"), "{message}");
        assert!(message.ends_with("limits.sql_depth raises it"), "{message}");
        assert_eq!(
            statements(FUZZ_HANG),
            Err(ParserError::RecursionLimitExceeded)
        );
    }

    /// A broken statement within the bound is left to polars-sql, which rejects it the
    /// same way; a sound one well under the bound parses.
    #[test]
    fn statements_within_the_bound_are_not_refused() {
        assert_eq!(too_deep("SELECT f((((x.y."), None);
        assert_eq!(
            too_deep("SELECT dept, COUNT(*) FROM df GROUP BY dept"),
            None
        );
        assert!(
            statements(
                "SELECT * FROM df WHERE x IN (SELECT x FROM df WHERE y > (SELECT AVG(y) FROM df))"
            )
            .is_ok()
        );
        assert!(statements("SELECT f(g(h(i(j(k(l(m(n(o(1)))))))))) FROM df").is_ok());
        assert!(statements("SELECT a.b.c.d.e.f.g.h FROM df").is_ok());
    }

    #[test]
    fn zero_means_the_parsers_own_bound() {
        assert_eq!(depth(0), PARSER_DEFAULT_DEPTH);
        assert_eq!(depth(24), 24);
    }
}
