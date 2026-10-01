use polars::prelude::StrptimeOptions;
use polars::prelude::*;
use std::ops::{Add, Div, Mul, Rem, Sub};

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
            '+' | '-' | '*' | '%' | '/' | '=' | '<' | '>' | '!' => {
                let mut op = c.to_string();
                chars.next();
                if let Some(&next_c) = chars.peek()
                    && ((c == '<' && (next_c == '=' || next_c == '>'))
                        || (c == '>' && next_c == '=')
                        || (c == '!' && next_c == '='))
                {
                    op.push(next_c);
                    chars.next();
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
                if is_timestamp
                    && let Some((iso, format_str, time_unit)) =
                        parse_timestamp_literal(&num_str, &mut chars)
                {
                    tokens.push(Token::TimestampLiteral {
                        iso,
                        format_str,
                        time_unit,
                    });
                    continue;
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

/// The token as the user typed it, for error messages.
fn token_text(token: &Token) -> String {
    match token {
        Token::Identifier(s) => s.clone(),
        Token::Number(n) => n.to_string(),
        Token::String(s) => format!("\"{}\"", s),
        Token::DateLiteral(iso) => iso.clone(),
        Token::TimestampLiteral { iso, .. } => iso.clone(),
        Token::Op(op) => op.clone(),
        Token::LParen => "(".to_string(),
        Token::RParen => ")".to_string(),
        Token::LBracket => "[".to_string(),
        Token::RBracket => "]".to_string(),
        Token::Comma => ",".to_string(),
        Token::Colon => ":".to_string(),
        Token::Pipe => "|".to_string(),
        Token::Dot => ".".to_string(),
        Token::Select => "select".to_string(),
        Token::Where => "where".to_string(),
        Token::By => "by".to_string(),
    }
}

/// Remedy shown when a clause keyword turns up out of place.
const CLAUSE_ORDER: &str = "clause order is select [by group] [where conditions]";

/// Infix operators spelled as words (q's names). They stay ordinary identifiers
/// everywhere else, so a column called `in` or `mod` still reads as one when it
/// opens an expression or follows a `.`.
const WORD_OPS: [&str; 5] = ["in", "like", "xbar", "mod", "wavg"];

/// The infix operator at `tokens[i]`, if there is one: a symbol, or an operator
/// word that has an operand before it.
fn infix_op_at(tokens: &[Token], i: usize) -> Option<&str> {
    match tokens.get(i)? {
        Token::Op(op) => Some(op.as_str()),
        Token::Identifier(word)
            if i > 0 && tokens[i - 1] != Token::Dot && WORD_OPS.contains(&word.as_str()) =>
        {
            Some(word.as_str())
        }
        _ => None,
    }
}

/// An operand of `mod` or `xbar`. A whole number is an integer literal, so an
/// integer column keeps its type: `5 xbar passenger_count` stays Int64 instead of
/// becoming 5.0, 10.0.
fn int_or_expr(tokens: &[Token]) -> Result<Expr, String> {
    let whole = |n: f64| n.fract() == 0.0 && n.abs() < i64::MAX as f64;
    match tokens {
        [Token::Number(n)] if whole(*n) => Ok(lit(*n as i64)),
        [Token::Op(minus), Token::Number(n)] if minus == "-" && whole(*n) => Ok(lit(-(*n as i64))),
        _ => parse_expr(tokens),
    }
}

/// True when no `]` in `tokens` closes a `[` from outside them.
fn brackets_balanced(tokens: &[Token]) -> bool {
    let mut depth = 0usize;
    tokens.iter().all(|t| match t {
        Token::LBracket => {
            depth += 1;
            true
        }
        Token::RBracket => depth.checked_sub(1).map(|d| depth = d).is_some(),
        _ => true,
    })
}

/// OR of the conditions as a balanced tree, so a long `in` list nests
/// logarithmically rather than one level per element.
fn any_of(mut conditions: Vec<Expr>) -> Expr {
    if conditions.len() <= 1 {
        return conditions.pop().unwrap_or_else(|| lit(false));
    }
    let right = conditions.split_off(conditions.len() / 2);
    any_of(conditions).or(any_of(right))
}

/// A `like` pattern as an anchored regex: `*` is any run of characters, `?` any
/// one character, everything else literal.
fn like_regex(pattern: &str) -> String {
    let mut re = String::from("(?s)^");
    for c in pattern.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            _ => re.push_str(&regex::escape(c.encode_utf8(&mut [0; 4]))),
        }
    }
    re.push('$');
    re
}

/// Operators whose right side is not an ordinary expression, or whose operands
/// need their tokens (literal checks, names). Everything else goes to `apply_op`.
fn apply_infix(left_tokens: &[Token], op: &str, right_tokens: &[Token]) -> Result<Expr, String> {
    match op {
        "in" => {
            let list = match right_tokens {
                // One list: the first `[` closes at the last token, so `[1] + [2]` is not.
                [Token::LBracket, inner @ .., Token::RBracket] if brackets_balanced(inner) => inner,
                _ => {
                    return Err(
                        "in takes a list on its right, e.g. name in [\"Emma\", \"Olivia\"]"
                            .to_string(),
                    );
                }
            };
            let items = split_tokens(list, &Token::Comma);
            if items.iter().any(|item| item.is_empty()) {
                return Err(
                    "in needs a list of values, e.g. name in [\"Emma\", \"Olivia\"]".to_string(),
                );
            }
            let left = parse_expr(left_tokens)?;
            // One `=` per value, so each value compares exactly as `x = value` would,
            // with the same literal casting (numbers, dates, timestamps).
            let conditions = items
                .iter()
                .map(|item| Ok(left.clone().eq(parse_expr(item)?)))
                .collect::<Result<Vec<_>, String>>()?;
            Ok(any_of(conditions))
        }
        "like" => {
            let [Token::String(pattern)] = right_tokens else {
                return Err(
                    "like takes a quoted pattern on its right, e.g. item like \"*Chicken*\""
                        .to_string(),
                );
            };
            let left = parse_expr(left_tokens)?;
            // Cast first so numeric codes (zip, station ids read as numbers) match too.
            Ok(left
                .cast(DataType::String)
                .str()
                .contains(lit(like_regex(pattern)), true))
        }
        "xbar" => {
            if let [Token::Number(n)] = left_tokens
                && *n <= 0.0
            {
                return Err(
                    "xbar needs a positive bucket size, e.g. 5 xbar fare_amount".to_string()
                );
            }
            let right = parse_expr(right_tokens)?;
            let size = int_or_expr(left_tokens)?;
            // floor_div floors toward negative infinity for both ints and floats,
            // which is what makes every value land in the bucket at or below it.
            Ok(right.floor_div(size.clone()).mul(size))
        }
        "mod" => {
            let right = int_or_expr(right_tokens)?;
            let left = int_or_expr(left_tokens)?;
            Ok(left.rem(right))
        }
        "wavg" => {
            let values = parse_expr(right_tokens)?;
            let weights = parse_expr(left_tokens)?;
            let weighted = weights.clone().mul(values);
            // Only pairs with both a weight and a value count toward the total weight;
            // a null value would otherwise still pull the average toward zero.
            let total = weights.filter(weighted.clone().is_not_null()).sum();
            // No weight at all (every pair null) is no average, not 0/0 = NaN.
            let total = when(total.clone().neq(lit(0)))
                .then(total)
                .otherwise(lit(NULL));
            let expr = weighted.sum().true_div(total);
            Ok(match simple_column_name(right_tokens) {
                Some(column) => expr.alias(format!("wavg_{}", column)),
                None => expr,
            })
        }
        _ => {
            // Parse right side first (right-to-left evaluation): it holds any
            // remaining operators, so c>c%n becomes c > (c%n).
            let right = parse_expr(right_tokens)?;
            let left = parse_expr(left_tokens)?;
            apply_op(left, op, right)
        }
    }
}

fn apply_op(left: Expr, op: &str, right: Expr) -> Result<Expr, String> {
    match op {
        "+" => Ok(left.add(right)),
        "-" => Ok(left.sub(right)),
        "*" => Ok(left.mul(right)),
        // `%` divides (q heritage); `/` is the alias everyone expects.
        "%" | "/" => Ok(left.div(right)),
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

/// The column name when the tokens are a bare column reference: `salary`, or
/// `col["first name"]` / `col[name]`. Anything more (literals, operators) is None.
fn simple_column_name(tokens: &[Token]) -> Option<String> {
    match tokens {
        [Token::Identifier(name)] => Some(name.clone()),
        [
            Token::Identifier(c),
            Token::LBracket,
            Token::String(name) | Token::Identifier(name),
            Token::RBracket,
        ] if c == "col" => Some(name.clone()),
        _ => None,
    }
}

const WAVG_USAGE: &str = "wavg goes between weights and values, e.g. passengers wavg fare";

/// Aggregation function names, lowercase.
const AGG_FUNCTIONS: [&str; 16] = [
    "avg", "mean", "min", "max", "count", "std", "stddev", "dev", "var", "med", "median", "sum",
    "first", "last", "nunique", "wavg",
];

/// Scalar function names, lowercase.
const SCALAR_FUNCTIONS: [&str; 13] = [
    "len", "length", "not", "null", "upper", "lower", "abs", "floor", "ceil", "ceiling", "sqrt",
    "log", "exp",
];

fn is_agg_function(name: &str) -> bool {
    AGG_FUNCTIONS.contains(&name.to_lowercase().as_str())
}

// Check if an identifier is a known function name
fn is_function_name(name: &str) -> bool {
    // `wavg` is infix (`w wavg x`), so it never opens an expression.
    let name = name.to_lowercase();
    name != "wavg" && (is_agg_function(&name) || SCALAR_FUNCTIONS.contains(&name.as_str()))
}

/// A function call, `fn[args]` or `fn args`. The name is checked before the
/// arguments are parsed, so each argument is parsed once; trying aggregates and
/// then scalars on the same arguments doubled the work at every nesting level.
fn parse_call(name: &str, args: &[Token]) -> Result<Expr, String> {
    if is_agg_function(name) {
        parse_agg_function(name, args)
    } else {
        parse_function(name, args)
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
    let fn_name = name.to_lowercase();
    if fn_name == "wavg" {
        return Err(WAVG_USAGE.to_string());
    }
    let expr = parse_expr(args)?;
    let expr = match fn_name.as_str() {
        "avg" | "mean" => expr.mean(),
        "min" => expr.min(),
        "max" => expr.max(),
        "count" => expr.count(),
        // Sample statistics (n - 1), like `std`; q's own var and dev divide by n.
        "std" | "stddev" | "dev" => expr.std(1),
        "var" => expr.var(1),
        "med" | "median" => expr.median(),
        "sum" => expr.sum(),
        "first" => expr.first(),
        "last" => expr.last(),
        "nunique" => expr.n_unique(),
        _ => return Err(format!("Unknown aggregation function: {}", name)),
    };
    // Left unnamed, two aggregates of one column collide ("avg salary, max salary"),
    // so a bare-column aggregate is named {fn}_{column}, the convention the dot
    // accessors already use. An explicit alias is applied later and overrides this.
    match simple_column_name(args) {
        Some(column) => Ok(expr.alias(format!("{}_{}", fn_name, column))),
        None => Ok(expr),
    }
}

// Parse function like not[a=b], null[col], len[x], upper[x], etc.
fn parse_function(name: &str, args: &[Token]) -> Result<Expr, String> {
    if args.is_empty() {
        return Err(format!("Function {} requires an argument", name));
    }
    let name_lower = name.to_lowercase();
    if !SCALAR_FUNCTIONS.contains(&name_lower.as_str()) {
        return Err(format!("Unknown function: {}", name));
    }
    let expr = parse_expr(args)?;
    match name_lower.as_str() {
        "not" => Ok(expr.not()),
        "null" => Ok(expr.is_null()),
        "len" | "length" => Ok(expr.str().len_chars()),
        "upper" => Ok(expr.str().to_uppercase()),
        "lower" => Ok(expr.str().to_lowercase()),
        "abs" => Ok(expr.abs()),
        "floor" => Ok(expr.floor()),
        "ceil" | "ceiling" => Ok(expr.ceil()),
        "sqrt" => Ok(expr.sqrt()),
        "log" => Ok(expr.log(lit(std::f64::consts::E))),
        "exp" => Ok(expr.exp()),
        _ => Err(format!("Unknown function: {}", name)),
    }
}

/// An accessor's bracketed argument: `.part["-", 0]` has a string and a number.
#[derive(Debug, Clone, PartialEq)]
enum AccessorArg {
    Str(String),
    Num(f64),
}

impl AccessorArg {
    /// As it goes into the result's auto-alias: `part_-_0`, not `part_-_0.0`.
    fn alias_text(&self) -> String {
        match self {
            AccessorArg::Str(s) => s.clone(),
            AccessorArg::Num(n) => n.to_string(),
        }
    }
}

/// Every accessor with the argument counts it takes and an example for errors.
const ACCESSORS: &[(&str, usize, usize, &str)] = &[
    // Date and time parts.
    ("date", 0, 0, ".date"),
    ("time", 0, 0, ".time"),
    ("year", 0, 0, ".year"),
    ("quarter", 0, 0, ".quarter"),
    ("month", 0, 0, ".month"),
    ("week", 0, 0, ".week"),
    ("day", 0, 0, ".day"),
    ("doy", 0, 0, ".doy"),
    ("dow", 0, 0, ".dow"),
    ("weekday", 0, 0, ".weekday"),
    ("hour", 0, 0, ".hour"),
    ("minute", 0, 0, ".minute"),
    ("second", 0, 0, ".second"),
    ("month_start", 0, 0, ".month_start"),
    ("month_end", 0, 0, ".month_end"),
    ("format", 1, 1, ".format[\"%Y-%m\"]"),
    // Strings.
    ("len", 0, 0, ".len"),
    ("length", 0, 0, ".length"),
    ("upper", 0, 0, ".upper"),
    ("lower", 0, 0, ".lower"),
    ("starts_with", 1, 1, ".starts_with[\"x\"]"),
    ("ends_with", 1, 1, ".ends_with[\"x\"]"),
    ("contains", 1, 1, ".contains[\"x\"]"),
    ("part", 2, 2, ".part[\"-\", 0]"),
    ("slice", 1, 2, ".slice[0, 4]"),
    ("replace", 2, 2, ".replace[\"(P)\", \"\"]"),
    ("strip", 0, 0, ".strip"),
    ("to_date", 0, 1, ".to_date[\"%Y%m%d\"]"),
    ("to_datetime", 0, 1, ".to_datetime[\"%Y-%m-%d %H:%M\"]"),
    // Numbers and casts.
    ("round", 0, 1, ".round[1]"),
    ("int", 0, 0, ".int"),
    ("float", 0, 0, ".float"),
    ("str", 0, 0, ".str"),
];

/// Names for the unknown-accessor error, by kind.
const ACCESSOR_HELP: &str = "Valid date/time: date, time, year, quarter, month, week, day, doy, dow, hour, minute, second, month_start, month_end, format. \
     Valid string: len, upper, lower, starts_with, ends_with, contains, part, slice, replace, strip, to_date, to_datetime. \
     Valid number: round, int, float, str";

fn arg_count_text(min: usize, max: usize) -> String {
    match (min, max) {
        (0, 0) => "no arguments".to_string(),
        (1, 1) => "1 argument".to_string(),
        (a, b) if a == b => format!("{} arguments", a),
        (a, b) => format!("{} to {} arguments", a, b),
    }
}

/// Apply accessor `name` with its bracketed arguments.
fn apply_accessor(expr: Expr, accessor: &str, args: &[AccessorArg]) -> Result<Expr, String> {
    let name = accessor.to_lowercase();
    let Some(&(_, min, max, usage)) = ACCESSORS.iter().find(|(n, ..)| *n == name) else {
        return Err(format!(
            "Unknown accessor: '{}'. {}",
            accessor, ACCESSOR_HELP
        ));
    };
    if args.len() < min || args.len() > max {
        return Err(format!(
            "{} takes {}, e.g. {}; got {}",
            name,
            arg_count_text(min, max),
            usage,
            args.len()
        ));
    }
    let text = |i: usize| match args.get(i) {
        Some(AccessorArg::Str(s)) => Ok(s.clone()),
        _ => Err(format!(
            "{}: argument {} must be quoted text, e.g. {}",
            name,
            i + 1,
            usage
        )),
    };
    let int = |i: usize| match args.get(i) {
        Some(AccessorArg::Num(n)) if n.fract() == 0.0 && n.abs() <= u32::MAX as f64 => {
            Ok(*n as i64)
        }
        _ => Err(format!(
            "{}: argument {} must be a whole number, e.g. {}",
            name,
            i + 1,
            usage
        )),
    };
    let strptime = |format: Option<String>| StrptimeOptions {
        format: format.map(Into::into),
        // A value that does not match becomes null, as a failed parse does in q,
        // rather than one stray row failing the whole query.
        strict: false,
        ..Default::default()
    };
    // The string pieces cast first, so they also work on numbers and dates read
    // as such (NOAA's DATE, an integer zip code); a cast from String is a no-op.
    let as_str = || expr.clone().cast(DataType::String).str();
    Ok(match name.as_str() {
        "date" => expr.dt().date(),
        "time" => expr.dt().time(),
        "year" => expr.dt().year(),
        "quarter" => expr.dt().quarter(),
        "month" => expr.dt().month(),
        "week" => expr.dt().week(),
        "day" => expr.dt().day(),
        "doy" => expr.dt().ordinal_day(),
        "dow" | "weekday" => expr.dt().weekday(),
        "hour" => expr.dt().hour(),
        "minute" => expr.dt().minute(),
        "second" => expr.dt().second(),
        "month_start" => expr.dt().month_start(),
        "month_end" => expr.dt().month_end(),
        "format" => expr.dt().to_string(&text(0)?),
        "len" | "length" => expr.str().len_chars(),
        "upper" => expr.str().to_uppercase(),
        "lower" => expr.str().to_lowercase(),
        "starts_with" => expr.str().starts_with(lit(text(0)?)),
        "ends_with" => expr.str().ends_with(lit(text(0)?)),
        "contains" => expr.str().contains_literal(lit(text(0)?)),
        // Past the last piece is null, not an error; a negative index counts from the end.
        "part" => as_str().split(lit(text(0)?)).list().get(lit(int(1)?), true),
        "slice" => {
            let start = int(0)?;
            let length = match args.len() {
                2 => {
                    let n = int(1)?;
                    if n < 0 {
                        return Err(format!(
                            "slice: the length cannot be negative, e.g. {}",
                            usage
                        ));
                    }
                    lit(n as u64)
                }
                // No length: to the end of the string.
                _ => lit(NULL),
            };
            as_str().slice(lit(start), length)
        }
        "replace" => as_str().replace_all(lit(text(0)?), lit(text(1)?), true),
        "strip" => as_str().strip_chars(lit(NULL)),
        "to_date" => as_str().to_date(strptime(args.first().map(|_| text(0)).transpose()?)),
        "to_datetime" => as_str().to_datetime(
            None,
            None,
            strptime(args.first().map(|_| text(0)).transpose()?),
            lit("raise"),
        ),
        "round" => {
            let decimals = if args.is_empty() { 0 } else { int(0)? };
            let decimals = u32::try_from(decimals)
                .map_err(|_| format!("round: decimals cannot be negative, e.g. {}", usage))?;
            // Half away from zero, the rounding people expect from a calculator or SQL.
            expr.round(decimals, RoundMode::HalfAwayFromZero)
        }
        // Non-strict casts: a value that does not convert becomes null.
        "int" => expr.cast(DataType::Int64),
        "float" => expr.cast(DataType::Float64),
        "str" => expr.cast(DataType::String),
        _ => {
            return Err(format!(
                "Unknown accessor: '{}'. {}",
                accessor, ACCESSOR_HELP
            ));
        }
    })
}

/// The arguments inside an accessor's brackets: literals separated by commas.
fn parse_accessor_args(accessor: &str, tokens: &[Token]) -> Result<Vec<AccessorArg>, String> {
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    split_tokens(tokens, &Token::Comma)
        .iter()
        .map(|arg| match arg.as_slice() {
            [Token::String(s)] | [Token::Identifier(s)] => Ok(AccessorArg::Str(s.clone())),
            [Token::Number(n)] => Ok(AccessorArg::Num(*n)),
            [Token::Op(minus), Token::Number(n)] if minus == "-" => Ok(AccessorArg::Num(-n)),
            _ => Err(format!(
                "{} takes literal arguments, quoted text or numbers, e.g. .part[\"-\", 0]",
                accessor
            )),
        })
        .collect()
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
    while let [Token::Dot, Token::Identifier(accessor), rest @ ..] = tokens {
        let (args, consumed) = if rest.first() == Some(&Token::LBracket) {
            let mut depth = 0;
            let close = rest
                .iter()
                .position(|t| {
                    match t {
                        Token::LBracket => depth += 1,
                        Token::RBracket => depth -= 1,
                        _ => {}
                    }
                    depth == 0
                })
                .ok_or_else(|| format!("Unmatched bracket after .{}", accessor))?;
            (parse_accessor_args(accessor, &rest[1..close])?, close + 3)
        } else {
            (Vec::new(), 2)
        };
        expr = apply_accessor(expr, accessor, &args)?;
        if !alias_suffix.is_empty() {
            alias_suffix.push('_');
        }
        alias_suffix.push_str(accessor);
        for arg in &args {
            alias_suffix.push('_');
            alias_suffix.push_str(&arg.alias_text());
        }
        tokens = &tokens[consumed..];
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
                let expr = parse_call(name, &tokens[2..i - 1])?;
                parse_accessors(expr, &tokens[i..], None)
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
        _ => Err(format!(
            "Unexpected '{}' where an expression was expected",
            token_text(&tokens[0])
        )),
    }
}

/// Deepest chain of nested subexpressions the parser will follow.
///
/// Parsing is recursive descent, so nesting in the query becomes nesting on the stack:
/// `select ------x` recurses once per sign and `select ((((x))))` once per parenthesis.
/// Without a ceiling a long enough chain overflows the stack and takes the process with
/// it, which is a crash rather than the error message a mistyped query deserves. Found
/// by the `parse_query` fuzz target.
///
/// The ceiling is set by the smallest stack this runs on, not by what is expressible.
/// One level of nesting costs a `parse_expr` frame and a `parse_term` frame, and in an
/// unoptimised build those come to roughly 10 KiB together — enough that a 2 MiB worker
/// thread runs out somewhere around 200. 64 leaves a wide margin there and a far wider
/// one in a release build, while staying far past any expression written by hand:
/// commas and `where` are split off before this runs, so the count is nesting within a
/// single expression.
const MAX_EXPR_DEPTH: u32 = 64;

thread_local! {
    static EXPR_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Holds the recursion counter up for as long as it is alive.
///
/// Every recursive path in this module passes back through `parse_expr`, so counting
/// there alone bounds the whole cycle. `parse_expr` returns from a dozen places, most
/// of them through `?`, so the decrement is tied to the scope rather than written out
/// at each exit.
struct DepthGuard;

impl DepthGuard {
    /// `None` once the limit is reached, leaving the counter untouched.
    fn enter() -> Option<Self> {
        EXPR_DEPTH.with(|depth| {
            let next = depth.get() + 1;
            if next > MAX_EXPR_DEPTH {
                return None;
            }
            depth.set(next);
            Some(DepthGuard)
        })
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        EXPR_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

// Parse expression with right-to-left operator precedence
// This means operators are evaluated from right to left: a+b*c is parsed as a+(b*c)
fn parse_expr(tokens: &[Token]) -> Result<Expr, String> {
    let Some(_depth_guard) = DepthGuard::enter() else {
        return Err(
            "Expression is nested too deeply. Simplify it or split it into steps.".to_string(),
        );
    };

    if tokens.is_empty() {
        return Err("Empty expression".to_string());
    }

    // First check if this starts with a function call (without brackets)
    // This needs to be checked before operator parsing to ensure correct precedence
    if let Token::Identifier(name) = &tokens[0]
        && is_function_name(name)
        && tokens.len() > 1
        && tokens[1] != Token::LBracket
    {
        // Function call without brackets - parse the rest as the argument, going
        // through the same builders as the bracketed form so both spellings get
        // the same expression and the same auto-alias.
        return parse_call(name, &tokens[1..]);
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
            _ if depth == 0 && bracket_depth == 0 && infix_op_at(tokens, i).is_some() => {
                op_pos = Some(i);
                break;
            }
            _ => {}
        }
    }

    if let Some(pos) = op_pos {
        // Split at the operator
        let left_tokens = &tokens[..pos];
        let right_tokens = &tokens[pos + 1..];

        if let Some(op) = infix_op_at(tokens, pos) {
            // Unary minus next to a literal with an operator on the other side: -0.1+discount → (-0.1)+discount
            if left_tokens.is_empty()
                && op == "-"
                && !right_tokens.is_empty()
                && matches!(right_tokens[0], Token::Number(_))
                && let Token::Number(n) = right_tokens[0]
            {
                if right_tokens.len() >= 3
                    && let Some(bin_op) = infix_op_at(right_tokens, 1)
                {
                    if WORD_OPS.contains(&bin_op) {
                        // The word operators read their operands as tokens, so hand
                        // them the negative number as one: -7 mod 3 is (-7) mod 3.
                        return apply_infix(&[Token::Number(-n)], bin_op, &right_tokens[2..]);
                    }
                    let right_expr = parse_expr(&right_tokens[2..])?;
                    return apply_op(lit(0).sub(lit(n)), bin_op, right_expr);
                }
                if right_tokens.len() == 1 {
                    return Ok(lit(0).sub(lit(n)));
                }
            }
            // Unary plus/minus when there is no left operand (e.g. -x, +x, -(a+b))
            if left_tokens.is_empty() && (op == "+" || op == "-") {
                let inner = parse_expr(right_tokens)?;
                return if op == "-" {
                    Ok(lit(0).sub(inner))
                } else {
                    Ok(inner)
                };
            }
            if left_tokens.is_empty() {
                return Err("Missing left operand".to_string());
            }
            apply_infix(left_tokens, op, right_tokens)
        } else {
            Err("Expected operator".to_string())
        }
    } else {
        // No operator found, parse as term. Every caller hands this a complete
        // expression, so leftover tokens are a mistake in the query; dropping them
        // here used to make `where x > 1 by dept` silently ignore `by dept`.
        let (expr, remaining) = parse_term(tokens)?;
        if let Some(extra) = remaining.first() {
            if matches!(&tokens[0], Token::Identifier(w) if w == "wavg") {
                return Err(WAVG_USAGE.to_string());
            }
            return Err(format!(
                "Unexpected '{}' after the expression",
                token_text(extra)
            ));
        }
        Ok(expr)
    }
}

/// A parsed q-style query, ready to apply to a LazyFrame.
#[derive(Debug, Default)]
pub struct ParsedQuery {
    /// The select list; empty means every column.
    pub cols: Vec<Expr>,
    /// The where clause, its terms ANDed.
    pub filter: Option<Expr>,
    /// The by expressions.
    pub group_by: Vec<Expr>,
    /// Names of the by columns that have one (a plain column or an alias).
    pub group_by_names: Vec<String>,
    /// `select distinct`: drop duplicate result rows.
    pub distinct: bool,
}

impl ParsedQuery {
    /// The query with its casts to text and date parts safe on a date past the
    /// calendar, where Polars panics ([`crate::past_calendar::guard_expr`]).
    /// `schema` is the data the query runs against: with it, only operations on a
    /// date or datetime change.
    pub fn past_calendar_safe(self, schema: Option<&Schema>) -> Self {
        let guard = |e: Expr| crate::past_calendar::guard_expr(e, schema);
        Self {
            cols: self.cols.into_iter().map(guard).collect(),
            filter: self.filter.map(guard),
            group_by: self.group_by.into_iter().map(guard).collect(),
            ..self
        }
    }
}

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

pub fn parse_query(query: &str) -> Result<ParsedQuery, String> {
    // Empty query is equivalent to "select" - return all columns with no filter or grouping
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(ParsedQuery::default());
    }

    let tokens = tokenize(query)?;
    if tokens.is_empty() || tokens[0] != Token::Select {
        return Err("Query must start with 'select'".to_string());
    }
    // `distinct` right after `select` is the keyword unless what follows makes it
    // a column or an alias (`select distinct: x`, `select distinct, a`, `distinct + 1`);
    // col["distinct"] always names the column.
    let distinct = tokens.get(1) == Some(&Token::Identifier("distinct".to_string()))
        && !matches!(
            tokens.get(2),
            Some(Token::Colon | Token::Comma | Token::Dot | Token::Op(_))
        );
    let body = &tokens[if distinct { 2 } else { 1 }..];

    // Split by "where" first
    let mut parts = split_tokens(body, &Token::Where);
    let select_by_tokens = parts.remove(0);
    let where_tokens = if !parts.is_empty() {
        Some(parts.remove(0))
    } else {
        None
    };
    if !parts.is_empty() {
        return Err(
            "Unexpected second 'where': combine conditions with ',' (and) or '|' (or)".to_string(),
        );
    }

    // `by` after `where` reads naturally but grouping comes first; catch it here,
    // outside parentheses and brackets, so the error can name the clause order.
    if let Some(ref wt) = where_tokens {
        let mut depth = 0;
        let mut bracket_depth = 0;
        for token in wt {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                Token::LBracket => bracket_depth += 1,
                Token::RBracket => bracket_depth -= 1,
                Token::By if depth == 0 && bracket_depth == 0 => {
                    return Err(format!(
                        "Unexpected 'by' after the where clause: {}",
                        CLAUSE_ORDER
                    ));
                }
                _ => {}
            }
        }
    }

    // Split select/by part
    let mut select_by_parts = split_tokens(&select_by_tokens, &Token::By);
    let cols_tokens = select_by_parts.remove(0);
    let by_tokens = if !select_by_parts.is_empty() {
        Some(select_by_parts.remove(0))
    } else {
        None
    };
    if !select_by_parts.is_empty() {
        return Err(format!("Unexpected second 'by': {}", CLAUSE_ORDER));
    }

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
                            );
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
                            );
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

    Ok(ParsedQuery {
        cols,
        filter,
        group_by: group_by_cols,
        group_by_names: group_by_col_names,
        distinct,
    })
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

        let filter = parse_query(query).unwrap().filter;

        assert_eq!(filter, Some(col("a").eq(col("b")).not()));
    }

    #[test]

    fn test_parse_not_equivalent_to_neq() {
        let query1 = "select a where a != b";

        let query2 = "select a where not[a = b]";

        let query3 = "select a where not a = b";

        let filter1 = parse_query(query1).unwrap().filter;

        let filter2 = parse_query(query2).unwrap().filter;

        let filter3 = parse_query(query3).unwrap().filter;

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

        let filter = parse_query(query).unwrap().filter;

        // Should have name="george" AND age > 7

        assert!(filter.is_some());
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

        let ParsedQuery { cols, filter, .. } = parse_query(query).unwrap();

        assert_eq!(cols.len(), 2);

        assert_eq!(cols[0], lit("derek").alias("first name"));

        assert_eq!(cols[1], col("foo"));

        assert!(filter.is_some());
    }

    #[test]

    fn test_parse_string_escape_sequences() {
        let query = "select a where name=\"george\\\"s name\"";

        let filter = parse_query(query).unwrap().filter;

        // Should parse escaped quote correctly

        assert!(filter.is_some());
    }

    #[test]

    fn test_parse_query_simple_where() {
        let query = "select a where a > 10";

        let filter = parse_query(query).unwrap().filter;

        assert_eq!(filter, Some(col("a").gt(lit(10.0))));
    }

    #[test]
    fn test_parse_query_unary_minus_in_where() {
        // Minus next to literal with operator on other side: -0.5+discount → (-0.5)+discount
        let query = "select sum total-1 by product where 0<-0.5+discount";
        let ParsedQuery { cols, filter, .. } = parse_query(query).unwrap();
        assert_eq!(cols.len(), 1);
        assert!(filter.is_some());
        // Filter: 0 < (-0.5) + discount
        let expected = lit(0.0).lt(lit(0).sub(lit(0.5)).add(col("discount")));
        assert_eq!(filter, Some(expected));
    }

    #[test]
    fn test_parse_query_negative_literal_where() {
        let query = "select where 0<-0.1+discount";
        let filter = parse_query(query).unwrap().filter;
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

    fn test_parse_query_and_or() {
        let query = "select a where a > 10 | a < 5, b = 2";

        let filter = parse_query(query).unwrap().filter;

        let expected =
            (col("a").gt(lit(10.0)).or(col("a").lt(lit(5.0)))).and(col("b").eq(lit(2.0)));

        assert_eq!(filter, Some(expected));
    }

    #[test]

    fn test_parse_query_neq() {
        let query = "select a where a != 10";

        let filter = parse_query(query).unwrap().filter;

        assert_eq!(filter, Some(col("a").neq(lit(10.0))));
    }

    #[test]

    fn test_parse_query_gte() {
        let query = "select a where a >= 10";

        let filter = parse_query(query).unwrap().filter;

        assert_eq!(filter, Some(col("a").gt_eq(lit(10.0))));
    }

    #[test]

    fn test_parse_query_lte() {
        let query = "select a where a <= 10";

        let filter = parse_query(query).unwrap().filter;

        assert_eq!(filter, Some(col("a").lt_eq(lit(10.0))));
    }

    #[test]

    fn test_empty_query() {
        let query = "select";

        let ParsedQuery { cols, filter, .. } = parse_query(query).unwrap();

        assert!(cols.is_empty());

        assert!(filter.is_none());
    }

    #[test]

    fn test_select_all_implicit() {
        let query = "select where a > 1";

        let ParsedQuery { cols, filter, .. } = parse_query(query).unwrap();

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

        let filter = parse_query(query).unwrap().filter;

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
        let filter = parse_query(query).unwrap().filter;
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
        let filter = parse_query(query).unwrap().filter;
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
        let filter = parse_query(query).unwrap().filter;
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
        let f1 = parse_query("select where null col1").unwrap().filter;
        assert!(f1.is_some());
        let f2 = parse_query("select where not null col1").unwrap().filter;
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
        let filter = parse_query("select where city_name.ends_with[\"lanta\"]")
            .unwrap()
            .filter;
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
        assert_eq!(slash, col("a").div(col("b")));
    }

    #[test]
    fn test_slash_right_to_left() {
        // Right-to-left like every other operator: 1/c+a is 1/(c+a).
        let expr = parse_expr(&tokenize("1/c+a").unwrap()).unwrap();
        assert_eq!(expr, lit(1.0).div(col("c").add(col("a"))));
    }

    #[test]
    fn test_slash_in_where_clause() {
        // Same shape as the existing % test: c>c/n is c > (c/n).
        let filter = parse_query("select t, v where c>c/n").unwrap().filter;
        assert_eq!(filter, Some(col("c").gt(col("c").div(col("n")))));
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
        assert!(err.contains("Unexpected ',' after the expression"), "{err}");
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

    // --- q-style additions (#367) ---

    /// Run a query over `df` the way `DataTableState::query` does.
    fn eval(query: &str, df: &DataFrame) -> DataFrame {
        let ParsedQuery {
            cols,
            filter,
            group_by: by,
            distinct,
            ..
        } = parse_query(query).unwrap();
        let mut lf = df.clone().lazy();
        if let Some(f) = filter {
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
            .select([col("ts").str().to_datetime(
                None,
                None,
                StrptimeOptions::default(),
                lit("raise"),
            )])
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
        let filter = parse_query("select where name in [\"a\", \"b\"]")
            .unwrap()
            .filter;
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
        let out = eval("select a: 1 + x mod 4", &df);
        assert_eq!(values(&out, "a"), ["4.0", "1.0", "2.0"]);
        let out = eval("select a: (1 + x) mod 4", &df);
        assert_eq!(values(&out, "a"), ["0.0", "1.0", "2.0"]);
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
}
