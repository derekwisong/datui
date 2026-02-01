use polars::prelude::StrptimeOptions;
use polars::prelude::*;
use std::ops::{Add, Div, Mul, Sub};

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Identifier(String),
    Number(f64),
    String(String),
    /// Date literal in YYYY.MM.DD format, stored as ISO "YYYY-MM-DD" for Polars
    DateLiteral(String),
    /// Timestamp literal YYYY.MM.DDTHH:MM:SS[.fff...]
    TimestampLiteral {
        iso: String,
        format_str: String,
        time_unit: TimeUnit,
    },
    Op(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Colon,
    Pipe,
    Dot,
    Select,
    Where,
    By,
}

/// Parse YYYY.MM.DDTHH:MM:SS[.fff...] timestamp. Consumes from chars. Returns (iso_string, format, time_unit) or None.
fn parse_timestamp_literal(
    date_part: &str,
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Option<(String, String, TimeUnit)> {
    if chars.peek() != Some(&'T') {
        return None;
    }
    chars.next(); // consume 'T'
    let mut time_part = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() || c == ':' || c == '.' {
            time_part.push(c);
            chars.next();
        } else {
            break;
        }
    }
    let parts: Vec<&str> = time_part.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let (h, m, s) = (parts[0], parts[1], parts[2]);
    if h.len() != 2 || m.len() != 2 || s.len() < 2 {
        return None;
    }
    let (sec_part, frac) = match s.split_once('.') {
        Some((a, f)) => (a, f),
        None => (s, ""),
    };
    let (time_unit, format_str) = match frac.len() {
        0 => (TimeUnit::Microseconds, "%Y-%m-%dT%H:%M:%S".to_string()),
        1..=3 => (TimeUnit::Milliseconds, "%Y-%m-%dT%H:%M:%S%.3f".to_string()),
        4..=6 => (TimeUnit::Microseconds, "%Y-%m-%dT%H:%M:%S%.6f".to_string()),
        7..=9 => (TimeUnit::Nanoseconds, "%Y-%m-%dT%H:%M:%S%.9f".to_string()),
        _ => (TimeUnit::Nanoseconds, "%Y-%m-%dT%H:%M:%S%.9f".to_string()),
    };
    let iso_date = parse_date_literal(date_part)?;
    let frac_padded = match time_unit {
        TimeUnit::Milliseconds => format!("{:0<3}", frac),
        TimeUnit::Microseconds => format!("{:0<6}", frac),
        TimeUnit::Nanoseconds => format!("{:0<9}", frac),
    };
    let iso = if frac.is_empty() {
        format!("{}T{}:{}:{}", iso_date, h, m, sec_part)
    } else {
        format!("{}T{}:{}:{}.{}", iso_date, h, m, sec_part, frac_padded)
    };
    Some((iso, format_str, time_unit))
}

/// Parse YYYY.MM.DD date literal (e.g. 2021.01.01). Returns ISO string "YYYY-MM-DD" or None.
fn parse_date_literal(s: &str) -> Option<String> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: u32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;
    if parts[0].len() != 4 || !(1000..=9999).contains(&year) {
        return None;
    }
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(format!("{:04}-{:02}-{:02}", year, month, day))
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
            '^' => {
                tokens.push(Token::Op("^".to_string()));
                chars.next();
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
            '.' => {
                chars.next();
                if chars.peek().is_some_and(|nc| nc.is_ascii_digit()) {
                    let mut num_str = String::from('.');
                    while let Some(&nc) = chars.peek() {
                        if nc.is_ascii_digit() {
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
                } else {
                    tokens.push(Token::Dot);
                }
            }
            '0'..='9' => {
                let mut num_str = String::new();
                while let Some(&nc) = chars.peek() {
                    if nc.is_ascii_digit() || nc == '.' {
                        num_str.push(nc);
                        chars.next();
                    } else {
                        break;
                    }
                }
                // Check for YYYY.MM.DDTHH:MM:SS timestamp literal (peek for 'T' before consuming)
                let is_timestamp =
                    parse_date_literal(&num_str).is_some() && chars.peek() == Some(&'T');
                if is_timestamp {
                    if let Some((iso, format_str, time_unit)) =
                        parse_timestamp_literal(&num_str, &mut chars)
                    {
                        tokens.push(Token::TimestampLiteral {
                            iso,
                            format_str,
                            time_unit,
                        });
                        continue;
                    }
                }
                // Check for YYYY.MM.DD date literal
                if let Some(iso) = parse_date_literal(&num_str) {
                    tokens.push(Token::DateLiteral(iso));
                } else if let Ok(n) = num_str.parse::<f64>() {
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
        "^" => Ok(coalesce(&[left, right])),
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
        "first" => Ok(expr.first()),
        "last" => Ok(expr.last()),
        _ => Err(format!("Unknown aggregation function: {}", name)),
    }
}

// Parse function like not[a=b], null[col], len[x], upper[x], etc.
fn parse_function(name: &str, args: &[Token]) -> Result<Expr, String> {
    if args.is_empty() {
        return Err(format!("Function {} requires an argument", name));
    }
    let expr = parse_expr(args)?;
    match name.to_lowercase().as_str() {
        "not" => Ok(expr.not()),
        "null" => Ok(expr.is_null()),
        "len" | "length" => Ok(expr.str().len_chars()),
        "upper" => Ok(expr.str().to_uppercase()),
        "lower" => Ok(expr.str().to_lowercase()),
        "abs" => Ok(expr.abs()),
        "floor" => Ok(expr.floor()),
        "ceil" | "ceiling" => Ok(expr.ceil()),
        _ => Err(format!("Unknown function: {}", name)),
    }
}

/// Apply a date/datetime accessor to an expression.
fn apply_dt_accessor(expr: Expr, accessor: &str, _arg: Option<&str>) -> Result<Expr, String> {
    let dt = expr.dt();
    match accessor.to_lowercase().as_str() {
        "date" => Ok(dt.date()),
        "time" => Ok(dt.time()),
        "year" => Ok(dt.year()),
        "month" => Ok(dt.month()),
        "week" => Ok(dt.week()),
        "day" => Ok(dt.day()),
        "dow" => Ok(dt.weekday()),
        "weekday" => Ok(dt.weekday()),
        "month_start" => Ok(dt.month_start()),
        "month_end" => Ok(dt.month_end()),
        "format" => {
            let fmt = _arg.ok_or("format accessor requires an argument, e.g. .format[\"%Y-%m\"]")?;
            Ok(dt.to_string(fmt))
        }
        _ => Err(format!(
            "Unknown date/time accessor: '{}'. Valid: date, time, year, month, week, day, dow, month_start, month_end, format",
            accessor
        )),
    }
}

/// Apply a string accessor or method to an expression.
fn apply_str_accessor(expr: Expr, accessor: &str, arg: Option<&str>) -> Result<Expr, String> {
    let s = expr.str();
    match accessor.to_lowercase().as_str() {
        "len" | "length" => Ok(s.len_chars()),
        "upper" => Ok(s.to_uppercase()),
        "lower" => Ok(s.to_lowercase()),
        "starts_with" => {
            let pat = arg.ok_or("starts_with requires an argument, e.g. .starts_with[\"x\"]")?;
            Ok(s.starts_with(lit(pat)))
        }
        "ends_with" => {
            let pat = arg.ok_or("ends_with requires an argument, e.g. .ends_with[\"x\"]")?;
            Ok(s.ends_with(lit(pat)))
        }
        "contains" => {
            let pat = arg.ok_or("contains requires an argument, e.g. .contains[\"x\"]")?;
            Ok(s.contains_literal(lit(pat)))
        }
        _ => Err(format!(
            "Unknown string accessor: '{}'. Valid: len, upper, lower, starts_with, ends_with, contains",
            accessor
        )),
    }
}

/// Apply accessor (date, string, or string method). Tries date first, then string.
fn apply_accessor(expr: Expr, accessor: &str, arg: Option<&str>) -> Result<Expr, String> {
    if let Ok(e) = apply_dt_accessor(expr.clone(), accessor, arg) {
        return Ok(e);
    }
    if let Ok(e) = apply_str_accessor(expr, accessor, arg) {
        return Ok(e);
    }
    Err(format!(
        "Unknown accessor: '{}'. Valid date: date, time, year, month, week, day, dow, month_start, month_end, format. Valid string: len, upper, lower, starts_with, ends_with, contains",
        accessor
    ))
}

/// Parse optional dot accessors from remaining tokens. Returns (expr_with_accessors, remaining).
/// When base_name is Some, each accessor result is aliased to {base}_{accessor} (or {base}_{acc1}_{acc2} for chained)
/// to avoid duplicate column names.
fn parse_accessors<'a>(
    mut expr: Expr,
    mut tokens: &'a [Token],
    base_name: Option<&str>,
) -> Result<(Expr, &'a [Token]), String> {
    let mut alias_suffix = String::new();
    while tokens.len() >= 2 {
        if let (Token::Dot, Token::Identifier(accessor)) = (&tokens[0], &tokens[1]) {
            let (arg, consumed) = if tokens.len() >= 5
                && tokens[2] == Token::LBracket
                && matches!(tokens[3], Token::String(_) | Token::Identifier(_))
                && tokens[4] == Token::RBracket
            {
                // accessor["arg"] or accessor[arg]
                let arg = match &tokens[3] {
                    Token::String(s) => s.clone(),
                    Token::Identifier(id) => id.clone(),
                    _ => {
                        return Err(
                            "Bracket accessor requires string or identifier argument".to_string()
                        )
                    }
                };
                (Some(arg), 5)
            } else {
                (None, 2)
            };
            expr = apply_accessor(expr, accessor, arg.as_deref())?;
            if !alias_suffix.is_empty() {
                alias_suffix.push('_');
            }
            alias_suffix.push_str(accessor);
            if let Some(ref a) = arg {
                alias_suffix.push('_');
                alias_suffix.push_str(a);
            }
            tokens = &tokens[consumed..];
        } else {
            break;
        }
    }
    if !alias_suffix.is_empty() {
        let alias = match base_name {
            Some(name) => format!("{}_{}", name, alias_suffix),
            None => alias_suffix,
        };
        expr = expr.alias(&alias);
    }
    Ok((expr, tokens))
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
            | "first"
            | "last"
            | "len"
            | "length"
            | "not"
            | "null"
            | "upper"
            | "lower"
            | "abs"
            | "floor"
            | "ceil"
            | "ceiling"
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
                let expr = col(&col_name);
                let (expr, remaining) = parse_accessors(expr, &tokens[i..], Some(&col_name))?;
                Ok((expr, remaining))
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
                    Ok(expr) => {
                        let (expr, remaining) = parse_accessors(expr, &tokens[i..], None)?;
                        Ok((expr, remaining))
                    }
                    Err(_) => {
                        let expr = parse_function(name, args)?;
                        let (expr, remaining) = parse_accessors(expr, &tokens[i..], None)?;
                        Ok((expr, remaining))
                    }
                }
            } else {
                // Regular column reference
                // (Function calls without brackets are handled in parse_expr)
                let expr = col(name);
                let (expr, remaining) = parse_accessors(expr, &tokens[1..], Some(name))?;
                Ok((expr, remaining))
            }
        }
        Token::Number(n) => Ok((lit(*n), &tokens[1..])), // Numbers don't support accessors
        Token::String(s) => Ok((lit(s.as_str()), &tokens[1..])), // Strings don't support accessors
        Token::DateLiteral(iso) => {
            let opts = StrptimeOptions {
                format: Some("%Y-%m-%d".into()),
                ..Default::default()
            };
            Ok((lit(iso.as_str()).str().to_date(opts), &tokens[1..]))
        }
        Token::TimestampLiteral {
            iso,
            format_str,
            time_unit,
        } => {
            let opts = StrptimeOptions {
                format: Some(format_str.as_str().into()),
                ..Default::default()
            };
            Ok((
                lit(iso.as_str())
                    .str()
                    .to_datetime(Some(*time_unit), None, opts, lit("raise")),
                &tokens[1..],
            ))
        }
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
            let (expr, remaining) = parse_accessors(inner, &tokens[i..], None)?;
            Ok((expr, remaining))
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
                "first" => return Ok(expr.first()),
                "last" => return Ok(expr.last()),
                "len" | "length" => return Ok(expr.str().len_chars()),
                "not" => return Ok(expr.not()),
                "null" => return Ok(expr.is_null()),
                "upper" => return Ok(expr.str().to_uppercase()),
                "lower" => return Ok(expr.str().to_lowercase()),
                "abs" => return Ok(expr.abs()),
                "floor" => return Ok(expr.floor()),
                "ceil" | "ceiling" => return Ok(expr.ceil()),
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

/// Convert Polars-specific error messages to user-friendly query errors.
pub fn sanitize_query_error(msg: &str) -> String {
    let msg_lower = msg.to_lowercase();
    if msg_lower.contains("duplicate")
        && (msg_lower.contains("output name") || msg_lower.contains("projection"))
    {
        let name = msg
            .split('\'')
            .nth(1)
            .map(|s| s.to_string())
            .unwrap_or_else(|| "column".to_string());
        return format!(
            "Duplicate column name '{}' in result. Use aliases to rename columns, e.g. `select my_date: timestamp.date`",
            name
        );
    }
    if msg_lower.contains(".alias(") || msg_lower.contains("try renaming") {
        return "Duplicate column names in result. Use aliases to rename columns, e.g. `select my_date: timestamp.date`"
            .to_string();
    }
    msg.to_string()
}

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
    fn test_tokenize_decimal_number() {
        let tokens = tokenize(".5").unwrap();
        assert_eq!(tokens, vec![Token::Number(0.5)]);
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
        let (cols, _, _, _) = parse_query(query).unwrap();
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
        let (cols, _, _, _) = parse_query(query).unwrap();
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
        let (_, filter, _, _) = parse_query(query).unwrap();
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
        let (_, filter, _, _) = parse_query(query).unwrap();
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
        let (_, filter, _, _) = parse_query(query).unwrap();
        assert!(filter.is_some());
        // Verify the filter parses without error (date literal 2021.01.01 -> ISO 2021-01-01)
    }

    #[test]
    fn test_number_not_parsed_as_date() {
        let tokens = tokenize("2.5").unwrap();
        assert_eq!(tokens, vec![Token::Number(2.5)]);
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
        let (_, f1, _, _) = parse_query("select where null col1").unwrap();
        assert!(f1.is_some());
        let (_, f2, _, _) = parse_query("select where not null col1").unwrap();
        assert!(f2.is_some());
    }

    #[test]
    fn test_parse_coalesce() {
        let (cols, _, _, _) = parse_query("select a: coln^cola^colb").unwrap();
        assert_eq!(cols.len(), 1);
        // coalesce(coln, coalesce(cola, colb)) - parsing succeeds
    }

    #[test]
    fn test_parse_first_last_aggregation() {
        let (cols, _, _, _) = parse_query("select first[value], last[value] by group").unwrap();
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn test_parse_string_accessors() {
        let (_, filter, _, _) = parse_query("select where city_name.ends_with[\"lanta\"]").unwrap();
        assert!(filter.is_some());
        let (cols, _, _, _) = parse_query("select name.len, name.upper").unwrap();
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
        let (cols, _, group_by_cols, _) = parse_query(query).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(group_by_cols.len(), 1);
        assert_eq!(
            group_by_cols[0],
            col("order_date").dt().year().alias("order_date_year")
        );
    }
}
