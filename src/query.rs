use polars::prelude::*;
use std::ops::{Add, Div, Mul, Sub};

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Identifier(String),
    Number(f64),
    String(String),
    Op(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Colon,
    Pipe,
    Select,
    Where,
    By,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            ',' => {
                tokens.push(Token::Comma);
                chars.next();
            }
            ':' => {
                tokens.push(Token::Colon);
                chars.next();
            }
            '|' => {
                tokens.push(Token::Pipe);
                chars.next();
            }
            '(' => {
                tokens.push(Token::LParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                chars.next();
            }
            '[' => {
                tokens.push(Token::LBracket);
                chars.next();
            }
            ']' => {
                tokens.push(Token::RBracket);
                chars.next();
            }
            '"' => {
                // Parse string literal with escape sequences
                chars.next(); // consume opening quote
                let mut string_val = String::new();
                let mut found_closing_quote = false;
                while let Some(&c) = chars.peek() {
                    if c == '\\' {
                        chars.next(); // consume backslash
                        if let Some(&next_c) = chars.peek() {
                            match next_c {
                                'n' => {
                                    string_val.push('\n');
                                    chars.next();
                                }
                                't' => {
                                    string_val.push('\t');
                                    chars.next();
                                }
                                'r' => {
                                    string_val.push('\r');
                                    chars.next();
                                }
                                '\\' => {
                                    string_val.push('\\');
                                    chars.next();
                                }
                                '"' => {
                                    string_val.push('"');
                                    chars.next();
                                }
                                _ => {
                                    // Unknown escape, just include the backslash and next char
                                    string_val.push('\\');
                                    string_val.push(next_c);
                                    chars.next();
                                }
                            }
                        } else {
                            return Err("Unterminated escape sequence in string".to_string());
                        }
                    } else if c == '"' {
                        chars.next(); // consume closing quote
                        found_closing_quote = true;
                        break;
                    } else {
                        string_val.push(c);
                        chars.next();
                    }
                }
                if !found_closing_quote {
                    return Err("Unterminated string literal".to_string());
                }
                tokens.push(Token::String(string_val));
            }
            '+' | '-' | '*' | '%' | '=' | '<' | '>' | '!' => {
                let mut op = c.to_string();
                chars.next();
                if let Some(&next_c) = chars.peek() {
                    if (c == '<' && (next_c == '=' || next_c == '>'))
                        || (c == '>' && next_c == '=')
                        || (c == '!' && next_c == '=')
                    {
                        op.push(next_c);
                        chars.next();
                    }
                }
                tokens.push(Token::Op(op));
            }
            '0'..='9' | '.' => {
                let mut num_str = String::new();
                while let Some(&nc) = chars.peek() {
                    if nc.is_ascii_digit() || nc == '.' {
                        num_str.push(nc);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if let Ok(n) = num_str.parse::<f64>() {
                    tokens.push(Token::Number(n));
                } else {
                    return Err(format!("Invalid number: {}", num_str));
                }
            }
            _ if c.is_alphabetic() || c == '_' => {
                let mut ident = String::new();
                while let Some(&nc) = chars.peek() {
                    if nc.is_alphanumeric() || nc == '_' {
                        ident.push(nc);
                        chars.next();
                    } else {
                        break;
                    }
                }
                match ident.as_str() {
                    "select" => tokens.push(Token::Select),
                    "where" => tokens.push(Token::Where),
                    "by" => tokens.push(Token::By),
                    _ => tokens.push(Token::Identifier(ident)),
                }
            }
            _ => return Err(format!("Unexpected character: {}", c)),
        }
    }
    Ok(tokens)
}

fn split_tokens(tokens: &[Token], delimiter: &Token) -> Vec<Vec<Token>> {
    let mut result = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0;
    let mut bracket_depth = 0;

    for token in tokens {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::LBracket => bracket_depth += 1,
            Token::RBracket => bracket_depth -= 1,
            _ => {}
        }

        if depth == 0 && bracket_depth == 0 && token == delimiter {
            result.push(current);
            current = Vec::new();
        } else {
            current.push(token.clone());
        }
    }
    result.push(current);
    result
}

fn apply_op(left: Expr, op: &str, right: Expr) -> Result<Expr, String> {
    match op {
        "+" => Ok(left.add(right)),
        "-" => Ok(left.sub(right)),
        "*" => Ok(left.mul(right)),
        "%" => Ok(left.div(right)),
        "=" => Ok(left.eq(right)),
        "<" => Ok(left.lt(right)),
        ">" => Ok(left.gt(right)),
        "<=" => Ok(left.lt_eq(right)),
        ">=" => Ok(left.gt_eq(right)),
        "<>" | "!=" => Ok(left.neq(right)),
        _ => Err(format!("Unknown operator: {}", op)),
    }
}

// Parse aggregation function like avg[a], min[b], etc.
fn parse_agg_function(name: &str, args: &[Token]) -> Result<Expr, String> {
    if args.is_empty() {
        return Err(format!(
            "Aggregation function {} requires an argument",
            name
        ));
    }
    let expr = parse_expr(args)?;
    match name.to_lowercase().as_str() {
        "avg" | "mean" => Ok(expr.mean()),
        "min" => Ok(expr.min()),
        "max" => Ok(expr.max()),
        "count" => Ok(expr.count()),
        "std" | "stddev" => Ok(expr.std(1)),
        "med" | "median" => Ok(expr.median()),
        "sum" => Ok(expr.sum()),
        _ => Err(format!("Unknown aggregation function: {}", name)),
    }
}

// Parse function like not[a=b]
fn parse_function(name: &str, args: &[Token]) -> Result<Expr, String> {
    if args.is_empty() {
        return Err(format!("Function {} requires an argument", name));
    }
    let expr = parse_expr(args)?;
    match name.to_lowercase().as_str() {
        "not" => Ok(expr.not()),
        _ => Err(format!("Unknown function: {}", name)),
    }
}

// Check if an identifier is a known function name
fn is_function_name(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    matches!(
        name_lower.as_str(),
        "avg"
            | "mean"
            | "min"
            | "max"
            | "count"
            | "std"
            | "stddev"
            | "med"
            | "median"
            | "sum"
            | "not"
    )
}

fn parse_term(tokens: &[Token]) -> Result<(Expr, &[Token]), String> {
    if tokens.is_empty() {
        return Err("Unexpected end of expression".to_string());
    }
    match &tokens[0] {
        Token::Identifier(name) => {
            // Check if it's col[...] syntax for column names with spaces
            if name == "col" && tokens.len() > 1 && tokens[1] == Token::LBracket {
                // Find matching closing bracket
                let mut depth = 1;
                let mut i = 2;
                while i < tokens.len() && depth > 0 {
                    match tokens[i] {
                        Token::LBracket => depth += 1,
                        Token::RBracket => depth -= 1,
                        _ => {}
                    }
                    i += 1;
                }
                if depth > 0 {
                    return Err("Unmatched bracket in col[]".to_string());
                }
                // Extract column name from inside brackets
                let col_name_tokens = &tokens[2..i - 1];
                if col_name_tokens.len() != 1 {
                    return Err("col[] must contain a single string or identifier".to_string());
                }
                let col_name = match &col_name_tokens[0] {
                    Token::String(s) => s.clone(),
                    Token::Identifier(id) => id.clone(),
                    _ => return Err("col[] must contain a string or identifier".to_string()),
                };
                Ok((col(&col_name), &tokens[i..]))
            }
            // Check if it's a function call (using square brackets)
            else if tokens.len() > 1 && tokens[1] == Token::LBracket {
                // Find matching closing bracket
                let mut depth = 1;
                let mut i = 2;
                while i < tokens.len() && depth > 0 {
                    match tokens[i] {
                        Token::LBracket => depth += 1,
                        Token::RBracket => depth -= 1,
                        _ => {}
                    }
                    i += 1;
                }
                if depth > 0 {
                    return Err("Unmatched bracket in function call".to_string());
                }
                let args = &tokens[2..i - 1];
                // Try aggregation function first, then regular function
                match parse_agg_function(name, args) {
                    Ok(expr) => Ok((expr, &tokens[i..])),
                    Err(_) => {
                        // Try regular function (like not)
                        parse_function(name, args).map(|expr| (expr, &tokens[i..]))
                    }
                }
            } else {
                // Regular column reference
                // (Function calls without brackets are handled in parse_expr)
                Ok((col(name), &tokens[1..]))
            }
        }
        Token::Number(n) => Ok((lit(*n), &tokens[1..])),
        Token::String(s) => Ok((lit(s.as_str()), &tokens[1..])),
        Token::LParen => {
            let mut depth = 1;
            let mut i = 1;
            while i < tokens.len() && depth > 0 {
                match tokens[i] {
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
            if depth > 0 {
                return Err("Unmatched parenthesis".to_string());
            }
            let inner = parse_expr(&tokens[1..i - 1])?;
            Ok((inner, &tokens[i..]))
        }
        // Square brackets are only for function calls, not grouping
        // Parentheses are used for grouping
        _ => Err(format!("Unexpected token in term: {:?}", tokens[0])),
    }
}

// Parse expression with right-to-left operator precedence
// This means operators are evaluated from right to left: a+b*c is parsed as a+(b*c)
fn parse_expr(tokens: &[Token]) -> Result<Expr, String> {
    if tokens.is_empty() {
        return Err("Empty expression".to_string());
    }

    // First check if this starts with a function call (without brackets)
    // This needs to be checked before operator parsing to ensure correct precedence
    if let Token::Identifier(name) = &tokens[0] {
        if is_function_name(name) && tokens.len() > 1 && tokens[1] != Token::LBracket {
            // Function call without brackets - parse the rest as the argument
            let remaining = &tokens[1..];
            if remaining.is_empty() {
                return Err(format!("Function {} requires an argument", name));
            }
            // Parse the entire remaining expression as the function argument
            let expr = parse_expr(remaining)?;
            // Apply the function
            match name.to_lowercase().as_str() {
                "avg" | "mean" => return Ok(expr.mean()),
                "min" => return Ok(expr.min()),
                "max" => return Ok(expr.max()),
                "count" => return Ok(expr.count()),
                "std" | "stddev" => return Ok(expr.std(1)),
                "med" | "median" => return Ok(expr.median()),
                "sum" => return Ok(expr.sum()),
                "not" => return Ok(expr.not()),
                _ => {}
            }
        }
    }

    // Find the leftmost operator for right-to-left evaluation
    let mut op_pos = None;
    let mut depth = 0;
    let mut bracket_depth = 0;

    // Scan from left to right to find the leftmost operator
    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::LBracket => bracket_depth += 1,
            Token::RBracket => bracket_depth -= 1,
            Token::Op(_) if depth == 0 && bracket_depth == 0 => {
                op_pos = Some(i);
                break;
            }
            _ => {}
        }
    }

    if let Some(pos) = op_pos {
        // Split at the operator
        let left_tokens = &tokens[..pos];
        let op_token = &tokens[pos];
        let right_tokens = &tokens[pos + 1..];

        if let Token::Op(op) = op_token {
            // Parse right side first (right-to-left evaluation)
            // The right side contains any remaining operators that will be evaluated first
            let right = parse_expr(right_tokens)?;
            // Then parse left side
            let left = if left_tokens.is_empty() {
                return Err("Missing left operand".to_string());
            } else {
                parse_expr(left_tokens)?
            };
            // Apply operator: left op right
            // This gives us right-to-left evaluation: c>c%n becomes c > (c%n)
            apply_op(left, op, right)
        } else {
            Err("Expected operator".to_string())
        }
    } else {
        // No operator found, parse as term
        parse_term(tokens).map(|(expr, _)| expr)
    }
}

type ParseQueryResult = Result<(Vec<Expr>, Option<Expr>, Vec<Expr>, Vec<String>), String>;

pub fn parse_query(query: &str) -> ParseQueryResult {
    // Empty query is equivalent to "select" - return all columns with no filter or grouping
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok((Vec::new(), None, Vec::new(), Vec::new()));
    }

    let tokens = tokenize(query)?;
    if tokens.is_empty() || tokens[0] != Token::Select {
        return Err("Query must start with 'select'".to_string());
    }

    // Split by "where" first
    let mut parts = split_tokens(&tokens[1..], &Token::Where);
    let select_by_tokens = parts.remove(0);
    let where_tokens = if !parts.is_empty() {
        Some(parts.remove(0))
    } else {
        None
    };

    // Split select/by part
    let mut select_by_parts = split_tokens(&select_by_tokens, &Token::By);
    let cols_tokens = select_by_parts.remove(0);
    let by_tokens = if !select_by_parts.is_empty() {
        Some(select_by_parts.remove(0))
    } else {
        None
    };

    let mut cols = Vec::new();
    if !cols_tokens.is_empty() {
        for chunk in split_tokens(&cols_tokens, &Token::Comma) {
            if chunk.is_empty() {
                continue;
            }
            // Find colon position (if any) - need to account for col[...] syntax
            let mut colon_pos = None;
            let mut depth = 0;
            for (i, token) in chunk.iter().enumerate() {
                match token {
                    Token::LBracket => depth += 1,
                    Token::RBracket => depth -= 1,
                    Token::Colon if depth == 0 => {
                        colon_pos = Some(i);
                        break;
                    }
                    _ => {}
                }
            }
            if let Some(pos) = colon_pos {
                // Has alias: parse left side for alias name, right side for expression
                let alias_tokens = &chunk[..pos];
                let expr_tokens = &chunk[pos + 1..];

                // Parse alias - could be simple identifier or col[...]
                let alias_name = if alias_tokens.len() == 1 {
                    if let Token::Identifier(name) = &alias_tokens[0] {
                        name.clone()
                    } else {
                        return Err("Expected identifier or col[] for alias".to_string());
                    }
                } else if alias_tokens.len() == 4
                    && alias_tokens[0] == Token::Identifier("col".to_string())
                    && alias_tokens[1] == Token::LBracket
                    && alias_tokens[3] == Token::RBracket
                {
                    // col[...] syntax for alias
                    match &alias_tokens[2] {
                        Token::String(name) | Token::Identifier(name) => name.clone(),
                        _ => {
                            return Err(
                                "Expected string or identifier in col[] for alias".to_string()
                            )
                        }
                    }
                } else {
                    // Try to parse as expression and extract name (for simple cases)
                    // For now, require explicit identifier or col[]
                    return Err("Alias must be an identifier or col[]".to_string());
                };

                let expr = parse_expr(expr_tokens)?;
                cols.push(expr.alias(&alias_name));
            } else {
                cols.push(parse_expr(&chunk)?);
            }
        }
    }

    let mut group_by_cols = Vec::new();
    let mut group_by_col_names = Vec::new();
    if let Some(bt) = by_tokens {
        for chunk in split_tokens(&bt, &Token::Comma) {
            if chunk.is_empty() {
                continue;
            }
            // Support column assignment in by clause (like select)
            // Find colon position (if any) - need to account for col[...] syntax
            let mut colon_pos = None;
            let mut depth = 0;
            for (i, token) in chunk.iter().enumerate() {
                match token {
                    Token::LBracket => depth += 1,
                    Token::RBracket => depth -= 1,
                    Token::Colon if depth == 0 => {
                        colon_pos = Some(i);
                        break;
                    }
                    _ => {}
                }
            }
            if let Some(pos) = colon_pos {
                // Has alias: parse left side for alias name, right side for expression
                let alias_tokens = &chunk[..pos];
                let expr_tokens = &chunk[pos + 1..];

                // Parse alias - could be simple identifier or col[...]
                let alias_name = if alias_tokens.len() == 1 {
                    if let Token::Identifier(name) = &alias_tokens[0] {
                        name.clone()
                    } else {
                        return Err(
                            "Expected identifier or col[] for alias in by clause".to_string()
                        );
                    }
                } else if alias_tokens.len() == 4
                    && alias_tokens[0] == Token::Identifier("col".to_string())
                    && alias_tokens[1] == Token::LBracket
                    && alias_tokens[3] == Token::RBracket
                {
                    // col[...] syntax for alias
                    match &alias_tokens[2] {
                        Token::String(name) | Token::Identifier(name) => name.clone(),
                        _ => {
                            return Err(
                                "Expected string or identifier in col[] for alias in by clause"
                                    .to_string(),
                            )
                        }
                    }
                } else {
                    return Err("Alias must be an identifier or col[] in by clause".to_string());
                };

                let expr = parse_expr(expr_tokens)?;
                group_by_cols.push(expr.alias(&alias_name));
                group_by_col_names.push(alias_name); // Use alias name
            } else {
                let expr = parse_expr(&chunk)?;
                group_by_cols.push(expr.clone());
                // Try to extract column name from simple Expr
                // For simple identifiers: [Token::Identifier(name)]
                // For col[] syntax: [Token::Identifier("col"), Token::LBracket, Token::String/Identifier(name), Token::RBracket]
                if chunk.len() == 1 {
                    if let Token::Identifier(name) = &chunk[0] {
                        group_by_col_names.push(name.clone());
                    }
                } else if chunk.len() == 4
                    && chunk[0] == Token::Identifier("col".to_string())
                    && chunk[1] == Token::LBracket
                    && chunk[3] == Token::RBracket
                {
                    // col[...] syntax
                    match &chunk[2] {
                        Token::String(name) | Token::Identifier(name) => {
                            group_by_col_names.push(name.clone());
                        }
                        _ => {}
                    }
                } else {
                    // For complex expressions without alias, we can't extract a simple name
                    // The group_by_col_names will be incomplete, but that's okay -
                    // we'll use the Expr itself for sorting
                }
            }
        }
    }

    let mut filter: Option<Expr> = None;
    if let Some(wt) = where_tokens {
        for chunk in split_tokens(&wt, &Token::Comma) {
            if chunk.is_empty() {
                continue;
            }
            let mut or_expr: Option<Expr> = None;
            for or_chunk in split_tokens(&chunk, &Token::Pipe) {
                if or_chunk.is_empty() {
                    continue;
                }
                let e = parse_expr(&or_chunk)?;
                or_expr = match or_expr {
                    Some(curr) => Some(curr.or(e)),
                    None => Some(e),
                };
            }
            if let Some(e) = or_expr {
                filter = match filter {
                    Some(curr) => Some(curr.and(e)),
                    None => Some(e),
                };
            }
        }
    }

    Ok((cols, filter, group_by_cols, group_by_col_names))
}

#[cfg(test)]
mod tests {

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

        let (_, filter, _, _) = parse_query(query).unwrap();

        assert_eq!(filter, Some(col("a").eq(col("b")).not()));
    }

    #[test]

    fn test_parse_not_equivalent_to_neq() {
        let query1 = "select a where a != b";

        let query2 = "select a where not[a = b]";

        let query3 = "select a where not a = b";

        let (_, filter1, _, _) = parse_query(query1).unwrap();

        let (_, filter2, _, _) = parse_query(query2).unwrap();

        let (_, filter3, _, _) = parse_query(query3).unwrap();

        // All should produce equivalent expressions

        assert_eq!(filter1, Some(col("a").neq(col("b"))));

        assert_eq!(filter2, Some(col("a").eq(col("b")).not()));

        assert_eq!(filter3, Some(col("a").eq(col("b")).not()));
    }

    #[test]

    fn test_parse_avg_without_brackets() {
        let query = "select avg 5+a by category";

        let (cols, _, _, _) = parse_query(query).unwrap();

        assert_eq!(cols.len(), 1);

        // Should parse as avg[(5+a)]
    }

    #[test]

    fn test_parse_string_literal() {
        let query = "select a, b:\"foo\"";

        let (cols, _, _, _) = parse_query(query).unwrap();

        assert_eq!(cols.len(), 2);

        // First column is a, second is b with literal "foo"

        assert_eq!(cols[0], col("a"));

        assert_eq!(cols[1], lit("foo").alias("b"));
    }

    #[test]

    fn test_parse_string_in_where() {
        let query = "select a where name=\"george\", age > 7";

        let (_, filter, _, _) = parse_query(query).unwrap();

        // Should have name="george" AND age > 7

        assert!(filter.is_some());
    }

    #[test]

    fn test_parse_col_syntax() {
        let query = "select col[\"first name\"]";

        let (cols, _, _, _) = parse_query(query).unwrap();

        assert_eq!(cols.len(), 1);

        assert_eq!(cols[0], col("first name"));
    }

    #[test]

    fn test_parse_col_syntax_with_alias() {
        let query = "select a, b:col[\"first name\"]";

        let (cols, _, _, _) = parse_query(query).unwrap();

        assert_eq!(cols.len(), 2);

        assert_eq!(cols[0], col("a"));

        assert_eq!(cols[1], col("first name").alias("b"));
    }

    #[test]

    fn test_parse_col_syntax_with_string_literal() {
        let query = "select col[\"first name\"]:\"derek\", foo where foo > 7";

        let (cols, filter, _, _) = parse_query(query).unwrap();

        assert_eq!(cols.len(), 2);

        assert_eq!(cols[0], lit("derek").alias("first name"));

        assert_eq!(cols[1], col("foo"));

        assert!(filter.is_some());
    }

    #[test]

    fn test_parse_string_escape_sequences() {
        let query = "select a where name=\"george\\\"s name\"";

        let (_, filter, _, _) = parse_query(query).unwrap();

        // Should parse escaped quote correctly

        assert!(filter.is_some());
    }

    #[test]

    fn test_parse_query_simple_where() {
        let query = "select a where a > 10";

        let (_, filter, _, _) = parse_query(query).unwrap();

        assert_eq!(filter, Some(col("a").gt(lit(10.0))));
    }

    #[test]

    fn test_parse_query_alias() {
        let query = "select my_col:a + 1";

        let (cols, _, _, _) = parse_query(query).unwrap();

        assert_eq!(cols, vec![col("a").add(lit(1.0)).alias("my_col")]);
    }

    #[test]

    fn test_parse_query_and_or() {
        let query = "select a where a > 10 | a < 5, b = 2";

        let (_, filter, _, _) = parse_query(query).unwrap();

        let expected =
            (col("a").gt(lit(10.0)).or(col("a").lt(lit(5.0)))).and(col("b").eq(lit(2.0)));

        assert_eq!(filter, Some(expected));
    }

    #[test]

    fn test_parse_query_neq() {
        let query = "select a where a != 10";

        let (_, filter, _, _) = parse_query(query).unwrap();

        assert_eq!(filter, Some(col("a").neq(lit(10.0))));
    }

    #[test]

    fn test_parse_query_gte() {
        let query = "select a where a >= 10";

        let (_, filter, _, _) = parse_query(query).unwrap();

        assert_eq!(filter, Some(col("a").gt_eq(lit(10.0))));
    }

    #[test]

    fn test_parse_query_lte() {
        let query = "select a where a <= 10";

        let (_, filter, _, _) = parse_query(query).unwrap();

        assert_eq!(filter, Some(col("a").lt_eq(lit(10.0))));
    }

    #[test]

    fn test_empty_query() {
        let query = "select";

        let (cols, filter, _, _) = parse_query(query).unwrap();

        assert!(cols.is_empty());

        assert!(filter.is_none());
    }

    #[test]

    fn test_select_all_implicit() {
        let query = "select where a > 1";

        let (cols, filter, _, _) = parse_query(query).unwrap();

        assert!(cols.is_empty());

        assert_eq!(filter, Some(col("a").gt(lit(1.0))));
    }

    #[test]

    fn test_invalid_query_no_select() {
        let query = "a > 10";

        let result = parse_query(query);

        assert!(result.is_err());
    }

    #[test]

    fn test_invalid_query_unmatched_paren() {
        let query = "select (a + 1";

        let result = parse_query(query);

        assert!(result.is_err());
    }

    #[test]

    fn test_invalid_query_bad_token() {
        let query = "select a where a ? 10";

        let result = parse_query(query);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_right_to_left_operator_precedence() {
        // Test that operators are evaluated right-to-left
        // c>c%n should be parsed as c > (c % n), not (c > c) % n
        let query = "select t, v where c>c%n";

        let (_, filter, _, _) = parse_query(query).unwrap();

        // Should parse as c > (c % n)
        let expected = col("c").gt(col("c").div(col("n")));
        assert_eq!(filter, Some(expected));
    }
}
