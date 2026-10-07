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

/// [`CLAUSE_ORDER`] with q's optional `from df`, for errors about it.
const FROM_ORDER: &str = "clause order is select [by group] [from df] [where conditions]";

/// The one table q reads: the one on screen, named as SQL names it.
const TABLE: &str = "df";

/// The table name after a `from` at `tokens[i]`: an identifier, or a dotted
/// path like `data.csv`, that is not an operator word. Returns it and the index
/// past it.
fn from_table_at(tokens: &[Token], i: usize) -> Option<(String, usize)> {
    if tokens.get(i) != Some(&Token::Identifier("from".to_string())) {
        return None;
    }
    let mut name = match tokens.get(i + 1) {
        Some(Token::Identifier(n)) if !WORD_OPS.contains(&n.as_str()) => n.clone(),
        _ => return None,
    };
    let mut end = i + 2;
    while let (Some(Token::Dot), Some(Token::Identifier(part))) =
        (tokens.get(end), tokens.get(end + 1))
    {
        name.push('.');
        name.push_str(part);
        end += 2;
    }
    Some((name, end))
}

/// The query body without q's `from df` (after the select list and `by`, before
/// `where`). `from` is the clause only where a column cannot be: followed by a table
/// name, then `where`, `by` or the end, outside brackets.
fn strip_from(body: &[Token]) -> Result<Vec<Token>, String> {
    let mut depth = 0i32;
    let mut found: Option<(usize, usize)> = None;
    for (i, token) in body.iter().enumerate() {
        match token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => depth -= 1,
            _ => {}
        }
        if depth != 0 {
            continue;
        }
        let Some((name, end)) = from_table_at(body, i) else {
            continue;
        };
        let after_where = body[..i].contains(&Token::Where);
        match body.get(end) {
            None | Some(Token::Where) | Some(Token::By) => {}
            _ => continue,
        }
        if name != TABLE {
            return Err("q reads the table on screen, named df: … from df …".to_string());
        }
        if after_where {
            return Err(format!(
                "Unexpected 'from df' after the where clause: {FROM_ORDER}"
            ));
        }
        if body.get(end) == Some(&Token::By) {
            return Err(format!("Unexpected 'by' after 'from df': {FROM_ORDER}"));
        }
        found = Some((i, end));
    }
    let mut body = body.to_vec();
    if let Some((start, end)) = found {
        body.drain(start..end);
    }
    Ok(body)
}

/// Infix operators spelled as words (q's); ordinary identifiers elsewhere, so a column
/// named `in` or `mod` still works at an expression's start or after `.`.
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

/// A parsed expression, before becoming a Polars expression ([`Node::to_expr`]) or
/// Python ([`Node::python`]): one parse, so "Copy as Python" is the query datui ran.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Node {
    Col(String),
    /// A number as typed: a float literal.
    Num(f64),
    /// A whole number where the operator keeps integers whole (`mod`, `xbar`).
    Int(i64),
    Str(String),
    Bool(bool),
    Null,
    /// A `YYYY.MM.DD` literal, as ISO `YYYY-MM-DD`.
    Date(String),
    /// A `YYYY.MM.DDTHH:MM:SS[.fff]` literal.
    Timestamp {
        iso: String,
        format: String,
        unit: TimeUnit,
        /// The zone of the column it meets, read as a clock there; none for a column
        /// without one. See [`Node::resolve_time_zones`].
        zone: Option<String>,
    },
    Bin(BinOp, Box<Node>, Box<Node>),
    Coalesce(Box<Node>, Box<Node>),
    /// The values of the first where the second holds.
    Filter(Box<Node>, Box<Node>),
    /// `when(condition).then(value).otherwise(other)`.
    When(Box<Node>, Box<Node>, Box<Node>),
    Op(Box<Node>, Op),
    Alias(Box<Node>, String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinOp {
    Add,
    Sub,
    Mul,
    /// Polars' `/` on two expressions.
    Div,
    TrueDiv,
    FloorDiv,
    Rem,
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
}

/// A method applied to one expression.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Op {
    Mean,
    Min,
    Max,
    Count,
    Std,
    Var,
    Median,
    Sum,
    First,
    Last,
    NUnique,
    Not,
    IsNull,
    IsNotNull,
    LenChars,
    Upper,
    Lower,
    Abs,
    Floor,
    Ceil,
    Sqrt,
    Ln,
    Exp,
    Date,
    Time,
    Year,
    Quarter,
    Month,
    Week,
    Day,
    OrdinalDay,
    Weekday,
    Hour,
    Minute,
    Second,
    MonthStart,
    MonthEnd,
    DtFormat(String),
    StartsWith(String),
    EndsWith(String),
    ContainsLiteral(String),
    /// A regex match, strict.
    ContainsRegex(String),
    /// Split on the text and take the piece at the index, null past the last.
    Part(String, i64),
    Slice(i64, Option<u64>),
    ReplaceAll(String, String),
    Strip,
    ToDate(Option<String>),
    ToDatetime(Option<String>),
    Round(u32),
    /// A non-strict cast.
    Cast(CastTo),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastTo {
    Int64,
    Float64,
    String,
}

impl CastTo {
    fn dtype(self) -> DataType {
        match self {
            CastTo::Int64 => DataType::Int64,
            CastTo::Float64 => DataType::Float64,
            CastTo::String => DataType::String,
        }
    }
}

/// The most nodes an expression may grow to: `wavg`, `xbar` and `in` repeat operands,
/// so nesting multiplies (found by the `parse_query` fuzz target).
const MAX_EXPR_NODES: usize = 10_000;

/// Err when `copies` of `node` would pass [`MAX_EXPR_NODES`].
fn check_copies(node: &Node, copies: usize) -> Result<(), String> {
    if node.size().saturating_mul(copies) > MAX_EXPR_NODES {
        return Err(
            "Expression is too large: nested wavg, xbar or in repeat what they are \
                    given. Simplify it or split it into steps."
                .to_string(),
        );
    }
    Ok(())
}

impl Node {
    /// How many nodes the tree has.
    fn size(&self) -> usize {
        1 + match self {
            Node::Col(_)
            | Node::Num(_)
            | Node::Int(_)
            | Node::Str(_)
            | Node::Bool(_)
            | Node::Null
            | Node::Date(_)
            | Node::Timestamp { .. } => 0,
            Node::Bin(_, a, b) | Node::Coalesce(a, b) | Node::Filter(a, b) => a.size() + b.size(),
            Node::When(a, b, c) => a.size() + b.size() + c.size(),
            Node::Op(a, _) | Node::Alias(a, _) => a.size(),
        }
    }

    fn op(self, op: Op) -> Node {
        Node::Op(Box::new(self), op)
    }

    fn bin(self, op: BinOp, right: Node) -> Node {
        Node::Bin(op, Box::new(self), Box::new(right))
    }

    fn alias(self, name: impl Into<String>) -> Node {
        Node::Alias(Box::new(self), name.into())
    }

    fn cast_text(self) -> Node {
        self.op(Op::Cast(CastTo::String))
    }

    /// The Polars expression.
    pub(crate) fn to_expr(&self) -> Expr {
        match self {
            Node::Col(name) => col(name),
            Node::Num(n) => lit(*n),
            Node::Int(n) => lit(*n),
            Node::Str(s) => lit(s.as_str()),
            Node::Bool(b) => lit(*b),
            Node::Null => lit(NULL),
            Node::Date(iso) => {
                let opts = StrptimeOptions {
                    format: Some("%Y-%m-%d".into()),
                    ..Default::default()
                };
                lit(iso.as_str()).str().to_date(opts)
            }
            Node::Timestamp {
                iso,
                format,
                unit,
                zone,
            } => {
                let opts = StrptimeOptions {
                    format: Some(format.as_str().into()),
                    ..Default::default()
                };
                // Set only from a column's own dtype, so it parses.
                let zone = TimeZone::opt_try_new(zone.as_deref()).ok().flatten();
                // A clock time a fall back repeats is its first instant.
                lit(iso.as_str())
                    .str()
                    .to_datetime(Some(*unit), zone, opts, lit("earliest"))
            }
            Node::Bin(op, left, right) => {
                let (left, right) = (left.to_expr(), right.to_expr());
                match op {
                    BinOp::Add => left.add(right),
                    BinOp::Sub => left.sub(right),
                    BinOp::Mul => left.mul(right),
                    BinOp::Div => left.div(right),
                    BinOp::TrueDiv => left.true_div(right),
                    BinOp::FloorDiv => left.floor_div(right),
                    BinOp::Rem => left.rem(right),
                    BinOp::Eq => left.eq(right),
                    BinOp::Neq => left.neq(right),
                    BinOp::Lt => left.lt(right),
                    BinOp::Gt => left.gt(right),
                    BinOp::LtEq => left.lt_eq(right),
                    BinOp::GtEq => left.gt_eq(right),
                    BinOp::And => left.and(right),
                    BinOp::Or => left.or(right),
                }
            }
            Node::Coalesce(left, right) => coalesce(&[left.to_expr(), right.to_expr()]),
            Node::Filter(values, predicate) => values.to_expr().filter(predicate.to_expr()),
            Node::When(condition, then, otherwise) => when(condition.to_expr())
                .then(then.to_expr())
                .otherwise(otherwise.to_expr()),
            Node::Op(inner, op) => apply_op_expr(inner.to_expr(), op),
            Node::Alias(inner, name) => inner.to_expr().alias(name.as_str()),
        }
    }

    /// The same node with every alias inside it taken off, as Polars' `undo_aliases`.
    pub(crate) fn without_aliases(&self) -> Node {
        let strip = |n: &Node| Box::new(n.without_aliases());
        match self {
            Node::Alias(inner, _) => inner.without_aliases(),
            Node::Bin(op, l, r) => Node::Bin(*op, strip(l), strip(r)),
            Node::Coalesce(l, r) => Node::Coalesce(strip(l), strip(r)),
            Node::Filter(v, p) => Node::Filter(strip(v), strip(p)),
            Node::When(c, t, o) => Node::When(strip(c), strip(t), strip(o)),
            Node::Op(inner, op) => Node::Op(strip(inner), op.clone()),
            leaf => leaf.clone(),
        }
    }

    /// Name each `/` as Polars runs it over `schema`: `Div` floor-divides integers and
    /// divides otherwise, so Python needs `//` or `/` by the quotient's type.
    pub(crate) fn resolve_division(&mut self, schema: &Schema) {
        match self {
            Node::Bin(_, left, right) | Node::Coalesce(left, right) | Node::Filter(left, right) => {
                left.resolve_division(schema);
                right.resolve_division(schema);
            }
            Node::When(c, t, o) => {
                c.resolve_division(schema);
                t.resolve_division(schema);
                o.resolve_division(schema);
            }
            Node::Op(inner, _) | Node::Alias(inner, _) => inner.resolve_division(schema),
            _ => {}
        }
        if let Node::Bin(BinOp::Div, ..) = self {
            let quotient = DataFrame::empty_with_schema(schema)
                .lazy()
                .select([self.to_expr()])
                .collect_schema()
                .ok()
                .and_then(|s| s.get_at_index(0).map(|(_, dtype)| dtype.is_integer()));
            if let (Some(whole), Node::Bin(op, ..)) = (quotient, self) {
                *op = if whole {
                    BinOp::FloorDiv
                } else {
                    BinOp::TrueDiv
                };
            }
        }
    }

    /// Each timestamp literal meeting a zoned column (comparison, arithmetic, `^`, `?`
    /// branches) takes that zone; Polars refuses zoned-naive comparisons.
    pub(crate) fn resolve_time_zones(&mut self, schema: &Schema) {
        match self {
            Node::Bin(_, left, right) | Node::Coalesce(left, right) => {
                left.resolve_time_zones(schema);
                right.resolve_time_zones(schema);
                Self::share_zone(left, right, schema);
            }
            Node::Filter(values, predicate) => {
                values.resolve_time_zones(schema);
                predicate.resolve_time_zones(schema);
            }
            Node::When(c, t, o) => {
                c.resolve_time_zones(schema);
                t.resolve_time_zones(schema);
                o.resolve_time_zones(schema);
                Self::share_zone(t, o, schema);
            }
            Node::Op(inner, _) | Node::Alias(inner, _) => inner.resolve_time_zones(schema),
            _ => {}
        }
    }

    /// An error for the first comparison of a typed temporal column with quoted text (a
    /// string in q, never a date), worded in q's terms.
    fn check_quoted_temporal(&self, schema: &Schema) -> Result<(), String> {
        match self {
            Node::Bin(op, left, right) => {
                left.check_quoted_temporal(schema)?;
                right.check_quoted_temporal(schema)?;
                let compares = matches!(
                    op,
                    BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::LtEq | BinOp::GtEq
                );
                if compares
                    && let Some(err) = quoted_temporal(left, right, schema)
                        .or_else(|| quoted_temporal(right, left, schema))
                {
                    return Err(err);
                }
            }
            Node::Coalesce(left, right) | Node::Filter(left, right) => {
                left.check_quoted_temporal(schema)?;
                right.check_quoted_temporal(schema)?;
            }
            Node::When(c, t, o) => {
                c.check_quoted_temporal(schema)?;
                t.check_quoted_temporal(schema)?;
                o.check_quoted_temporal(schema)?;
            }
            Node::Op(inner, _) | Node::Alias(inner, _) => inner.check_quoted_temporal(schema)?,
            _ => {}
        }
        Ok(())
    }

    /// Give a zoneless timestamp literal on one side the zone of the other side's type.
    fn share_zone(a: &mut Node, b: &mut Node, schema: &Schema) {
        if !Self::take_zone(a, b, schema) {
            Self::take_zone(b, a, schema);
        }
    }

    /// Whether `literal`, a zoneless timestamp literal, took the zone of `other`'s type.
    fn take_zone(literal: &mut Node, other: &Node, schema: &Schema) -> bool {
        if let Node::Timestamp {
            zone: zone @ None, ..
        } = literal
            && let Some(DataType::Datetime(_, Some(tz))) = other.dtype(schema)
        {
            *zone = Some(tz.to_string());
            return true;
        }
        false
    }

    /// The type the expression has over `schema`, when Polars can say.
    fn dtype(&self, schema: &Schema) -> Option<DataType> {
        DataFrame::empty_with_schema(schema)
            .lazy()
            .select([self.to_expr()])
            .collect_schema()
            .ok()
            .and_then(|s| s.get_at_index(0).map(|(_, dtype)| dtype.clone()))
    }

    /// The literal as Python, bare: `1.0`, `"a"`, `True`, `None`.
    fn python_literal(&self) -> Option<String> {
        Some(match self {
            Node::Num(n) => crate::python_script::py_float(*n),
            Node::Int(n) => n.to_string(),
            Node::Str(s) => crate::python_script::py_str(s),
            Node::Bool(b) => crate::python_script::py_bool(*b).to_string(),
            Node::Null => "None".to_string(),
            _ => return None,
        })
    }

    /// Python Polars code for the expression.
    pub(crate) fn python(&self) -> String {
        use crate::python_script::py_str;
        if let Some(literal) = self.python_literal() {
            return format!("pl.lit({literal})");
        }
        match self {
            Node::Col(name) => format!("pl.col({})", py_str(name)),
            Node::Date(iso) => {
                let parts: Vec<String> = iso
                    .split('-')
                    .map(|p| p.trim_start_matches('0').to_string())
                    .map(|p| if p.is_empty() { "0".to_string() } else { p })
                    .collect();
                format!("pl.date({})", parts.join(", "))
            }
            Node::Timestamp {
                iso,
                format,
                unit,
                zone,
            } => format!(
                "pl.lit({}).str.to_datetime({}, time_unit={}{})",
                py_str(iso),
                py_str(format),
                py_str(time_unit_name(*unit)),
                zone.as_ref().map_or(String::new(), |zone| format!(
                    ", time_zone={}, ambiguous=\"earliest\"",
                    py_str(zone)
                ))
            ),
            Node::Bin(op, left, right) => {
                // A literal on the right stays bare (`pl.col("a") > 1.0`); Python's
                // operators turn it into one.
                let right = match right.python_literal() {
                    Some(literal) => literal,
                    None => right.python_operand(),
                };
                format!("{} {} {}", left.python_operand(), op.python(), right)
            }
            Node::Coalesce(left, right) => {
                format!("pl.coalesce({}, {})", left.python(), right.python())
            }
            Node::Filter(values, predicate) => {
                format!("{}.filter({})", values.python_operand(), predicate.python())
            }
            Node::When(condition, then, otherwise) => format!(
                "pl.when({}).then({}).otherwise({})",
                condition.python(),
                then.python(),
                otherwise.python()
            ),
            Node::Op(inner, op) => format!("{}{}", inner.python_operand(), op.python()),
            Node::Alias(inner, name) => {
                // Only the outer name survives an alias of an alias.
                let mut inner = inner.as_ref();
                while let Node::Alias(deeper, _) = inner {
                    inner = deeper;
                }
                format!("{}.alias({})", inner.python_operand(), py_str(name))
            }
            _ => unreachable!("literals return above"),
        }
    }

    /// As [`Self::python`], parenthesized where an operator or a method after it
    /// would otherwise bind to part of it.
    fn python_operand(&self) -> String {
        match self {
            Node::Bin(..) => format!("({})", self.python()),
            _ => self.python(),
        }
    }
}

fn time_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Milliseconds => "ms",
        TimeUnit::Microseconds => "us",
        TimeUnit::Nanoseconds => "ns",
    }
}

impl BinOp {
    fn python(self) -> &'static str {
        match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div | BinOp::TrueDiv => "/",
            BinOp::FloorDiv => "//",
            BinOp::Rem => "%",
            BinOp::Eq => "==",
            BinOp::Neq => "!=",
            BinOp::Lt => "<",
            BinOp::Gt => ">",
            BinOp::LtEq => "<=",
            BinOp::GtEq => ">=",
            BinOp::And => "&",
            BinOp::Or => "|",
        }
    }
}

impl Op {
    /// The method call, from its dot: `.str.to_uppercase()`.
    fn python(&self) -> String {
        use crate::python_script::py_str;
        let fixed = match self {
            Op::Mean => ".mean()",
            Op::Min => ".min()",
            Op::Max => ".max()",
            Op::Count => ".count()",
            Op::Std => ".std()",
            Op::Var => ".var()",
            Op::Median => ".median()",
            Op::Sum => ".sum()",
            Op::First => ".first()",
            Op::Last => ".last()",
            Op::NUnique => ".n_unique()",
            Op::Not => ".not_()",
            Op::IsNull => ".is_null()",
            Op::IsNotNull => ".is_not_null()",
            Op::LenChars => ".str.len_chars()",
            Op::Upper => ".str.to_uppercase()",
            Op::Lower => ".str.to_lowercase()",
            Op::Abs => ".abs()",
            Op::Floor => ".floor()",
            Op::Ceil => ".ceil()",
            Op::Sqrt => ".sqrt()",
            Op::Ln => ".log()",
            Op::Exp => ".exp()",
            Op::Date => ".dt.date()",
            Op::Time => ".dt.time()",
            Op::Year => ".dt.year()",
            Op::Quarter => ".dt.quarter()",
            Op::Month => ".dt.month()",
            Op::Week => ".dt.week()",
            Op::Day => ".dt.day()",
            Op::OrdinalDay => ".dt.ordinal_day()",
            Op::Weekday => ".dt.weekday()",
            Op::Hour => ".dt.hour()",
            Op::Minute => ".dt.minute()",
            Op::Second => ".dt.second()",
            Op::MonthStart => ".dt.month_start()",
            Op::MonthEnd => ".dt.month_end()",
            Op::Strip => ".str.strip_chars()",
            Op::Cast(CastTo::Int64) => ".cast(pl.Int64, strict=False)",
            Op::Cast(CastTo::Float64) => ".cast(pl.Float64, strict=False)",
            Op::Cast(CastTo::String) => ".cast(pl.String)",
            _ => "",
        };
        if !fixed.is_empty() {
            return fixed.to_string();
        }
        let format_arg = |format: &Option<String>| match format {
            Some(f) => format!("{}, strict=False", py_str(f)),
            None => "strict=False".to_string(),
        };
        match self {
            Op::DtFormat(f) => format!(".dt.to_string({})", py_str(f)),
            Op::StartsWith(s) => format!(".str.starts_with({})", py_str(s)),
            Op::EndsWith(s) => format!(".str.ends_with({})", py_str(s)),
            Op::ContainsLiteral(s) => format!(".str.contains({}, literal=True)", py_str(s)),
            Op::ContainsRegex(r) => format!(".str.contains({})", py_str(r)),
            Op::Part(sep, i) => format!(
                ".str.split({}).list.get({i}, null_on_oob=True)",
                py_str(sep)
            ),
            Op::Slice(start, Some(len)) => format!(".str.slice({start}, {len})"),
            Op::Slice(start, None) => format!(".str.slice({start})"),
            Op::ReplaceAll(from, to) => format!(
                ".str.replace_all({}, {}, literal=True)",
                py_str(from),
                py_str(to)
            ),
            Op::ToDate(format) => format!(".str.to_date({})", format_arg(format)),
            Op::ToDatetime(format) => format!(".str.to_datetime({})", format_arg(format)),
            Op::Round(d) => format!(".round({d}, mode=\"half_away_from_zero\")"),
            _ => unreachable!("fixed calls return above"),
        }
    }
}

fn apply_op_expr(expr: Expr, op: &Op) -> Expr {
    let strptime = |format: &Option<String>| StrptimeOptions {
        format: format.as_deref().map(Into::into),
        // A value that does not match becomes null, as a failed parse does in q,
        // rather than one stray row failing the whole query.
        strict: false,
        ..Default::default()
    };
    match op {
        Op::Mean => expr.mean(),
        Op::Min => expr.min(),
        Op::Max => expr.max(),
        Op::Count => expr.count(),
        // Sample statistics (n - 1), like `std`; q's own var and dev divide by n.
        Op::Std => expr.std(1),
        Op::Var => expr.var(1),
        Op::Median => expr.median(),
        Op::Sum => expr.sum(),
        Op::First => expr.first(),
        Op::Last => expr.last(),
        Op::NUnique => expr.n_unique(),
        Op::Not => expr.not(),
        Op::IsNull => expr.is_null(),
        Op::IsNotNull => expr.is_not_null(),
        Op::LenChars => expr.str().len_chars(),
        Op::Upper => expr.str().to_uppercase(),
        Op::Lower => expr.str().to_lowercase(),
        Op::Abs => expr.abs(),
        Op::Floor => expr.floor(),
        Op::Ceil => expr.ceil(),
        Op::Sqrt => expr.sqrt(),
        Op::Ln => expr.log(lit(std::f64::consts::E)),
        Op::Exp => expr.exp(),
        Op::Date => expr.dt().date(),
        Op::Time => expr.dt().time(),
        Op::Year => expr.dt().year(),
        Op::Quarter => expr.dt().quarter(),
        Op::Month => expr.dt().month(),
        Op::Week => expr.dt().week(),
        Op::Day => expr.dt().day(),
        Op::OrdinalDay => expr.dt().ordinal_day(),
        Op::Weekday => expr.dt().weekday(),
        Op::Hour => expr.dt().hour(),
        Op::Minute => expr.dt().minute(),
        Op::Second => expr.dt().second(),
        Op::MonthStart => expr.dt().month_start(),
        Op::MonthEnd => expr.dt().month_end(),
        Op::DtFormat(f) => expr.dt().to_string(f),
        Op::StartsWith(s) => expr.str().starts_with(lit(s.as_str())),
        Op::EndsWith(s) => expr.str().ends_with(lit(s.as_str())),
        Op::ContainsLiteral(s) => expr.str().contains_literal(lit(s.as_str())),
        Op::ContainsRegex(r) => expr.str().contains(lit(r.as_str()), true),
        // Past the last piece is null, not an error; a negative index counts from the end.
        Op::Part(sep, i) => expr
            .str()
            .split(lit(sep.as_str()))
            .list()
            .get(lit(*i), true),
        Op::Slice(start, length) => {
            // No length: to the end of the string.
            let length = length.map_or_else(|| lit(NULL), lit);
            expr.str().slice(lit(*start), length)
        }
        Op::ReplaceAll(from, to) => {
            expr.str()
                .replace_all(lit(from.as_str()), lit(to.as_str()), true)
        }
        Op::Strip => expr.str().strip_chars(lit(NULL)),
        Op::ToDate(format) => expr.str().to_date(strptime(format)),
        Op::ToDatetime(format) => {
            expr.str()
                .to_datetime(None, None, strptime(format), lit("raise"))
        }
        // Half away from zero, the rounding people expect from a calculator or SQL.
        Op::Round(decimals) => expr.round(*decimals, RoundMode::HalfAwayFromZero),
        // Non-strict casts: a value that does not convert becomes null.
        Op::Cast(to) => expr.cast(to.dtype()),
    }
}

/// An operand of `mod` or `xbar`: whole numbers as integer literals, so integer columns
/// keep their type (`5 xbar passenger_count` stays Int64).
fn int_or_node(tokens: &[Token]) -> Result<Node, String> {
    let whole = |n: f64| n.fract() == 0.0 && n.abs() < i64::MAX as f64;
    match tokens {
        [Token::Number(n)] if whole(*n) => Ok(Node::Int(*n as i64)),
        [Token::Op(minus), Token::Number(n)] if minus == "-" && whole(*n) => {
            Ok(Node::Int(-(*n as i64)))
        }
        _ => parse_node(tokens),
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

/// The error for `column` of a temporal type compared with the quoted `text`, naming
/// the literal the type takes: the text itself when it is one once unquoted.
fn quoted_temporal(column: &Node, text: &Node, schema: &Schema) -> Option<String> {
    let (Node::Col(name), Node::Str(s)) = (column, text) else {
        return None;
    };
    let shown = q_name(name);
    let unquoted = tokenize(s).ok();
    let literal = |is_kind: fn(&Token) -> bool, example: &str| match unquoted.as_deref() {
        Some([token]) if is_kind(token) => s.trim().to_string(),
        _ => example.to_string(),
    };
    let (kind, remedy) = match schema.get(name)? {
        DataType::Date => (
            "date",
            format!(
                "A date is {}",
                literal(|t| matches!(t, Token::DateLiteral(_)), "2024.01.01")
            ),
        ),
        DataType::Datetime(..) => (
            "timestamp",
            format!(
                "A timestamp is {}",
                literal(
                    |t| matches!(t, Token::TimestampLiteral { .. }),
                    "2024.01.01T05:00:00"
                )
            ),
        ),
        DataType::Time => (
            "time",
            format!(
                "A time has no literal; compare {shown}.hour, {shown}.minute or {shown}.second with a number"
            ),
        ),
        DataType::Duration(_) => ("duration", "A duration has no literal".to_string()),
        _ => return None,
    };
    Some(format!(
        "{shown} is a {kind}; \"{s}\" is a string. {remedy}"
    ))
}

/// How q spells a column: bare when it can be, else `col["first name"]`.
pub(crate) fn q_name(name: &str) -> String {
    if is_plain_name(name) {
        name.to_string()
    } else {
        format!("col[\"{name}\"]")
    }
}

/// Whether `name` reads as a column when typed bare, rather than needing `col["…"]`.
fn is_plain_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_')
        && !matches!(name, "select" | "where" | "by")
}

/// OR of the conditions as a balanced tree, so a long `in` list nests
/// logarithmically rather than one level per element.
fn any_of(mut conditions: Vec<Node>) -> Node {
    if conditions.len() <= 1 {
        return conditions.pop().unwrap_or(Node::Bool(false));
    }
    let right = conditions.split_off(conditions.len() / 2);
    any_of(conditions).bin(BinOp::Or, any_of(right))
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
fn apply_infix(left_tokens: &[Token], op: &str, right_tokens: &[Token]) -> Result<Node, String> {
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
            let left = parse_node(left_tokens)?;
            // A column or literal repeated grows only as the list typed does; a larger
            // left side repeated per item is what multiplies.
            if left.size() > 1 {
                check_copies(&left, items.len())?;
            }
            // One `=` per value, so each value compares exactly as `x = value` would,
            // with the same literal casting (numbers, dates, timestamps).
            let conditions = items
                .iter()
                .map(|item| Ok(left.clone().bin(BinOp::Eq, parse_node(item)?)))
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
            let left = parse_node(left_tokens)?;
            // Cast first so numeric codes (zip, station ids read as numbers) match too.
            Ok(left.cast_text().op(Op::ContainsRegex(like_regex(pattern))))
        }
        "xbar" => {
            if let [Token::Number(n)] = left_tokens
                && *n <= 0.0
            {
                return Err(
                    "xbar needs a positive bucket size, e.g. 5 xbar fare_amount".to_string()
                );
            }
            let right = parse_node(right_tokens)?;
            let size = int_or_node(left_tokens)?;
            check_copies(&size, 2)?;
            // floor_div floors toward negative infinity for both ints and floats,
            // which is what makes every value land in the bucket at or below it.
            Ok(right
                .bin(BinOp::FloorDiv, size.clone())
                .bin(BinOp::Mul, size))
        }
        "mod" => {
            let right = int_or_node(right_tokens)?;
            let left = int_or_node(left_tokens)?;
            Ok(left.bin(BinOp::Rem, right))
        }
        "wavg" => {
            let values = parse_node(right_tokens)?;
            let weights = parse_node(left_tokens)?;
            // Below, weights appear five times and values three.
            check_copies(&weights, 5)?;
            check_copies(&values, 3)?;
            let weighted = weights.clone().bin(BinOp::Mul, values);
            // Only pairs with both a weight and a value count toward the total weight;
            // a null value would otherwise still pull the average toward zero.
            let total = Node::Filter(
                Box::new(weights),
                Box::new(weighted.clone().op(Op::IsNotNull)),
            )
            .op(Op::Sum);
            // No weight at all (every pair null) is no average, not 0/0 = NaN.
            let total = Node::When(
                Box::new(total.clone().bin(BinOp::Neq, Node::Int(0))),
                Box::new(total),
                Box::new(Node::Null),
            );
            let node = weighted.op(Op::Sum).bin(BinOp::TrueDiv, total);
            Ok(match simple_column_name(right_tokens) {
                Some(column) => node.alias(format!("wavg_{}", column)),
                None => node,
            })
        }
        _ => {
            // Parse right side first (right-to-left evaluation): it holds any
            // remaining operators, so c>c%n becomes c > (c%n).
            let right = parse_node(right_tokens)?;
            let left = parse_node(left_tokens)?;
            apply_op(left, op, right)
        }
    }
}

fn apply_op(left: Node, op: &str, right: Node) -> Result<Node, String> {
    let op = match op {
        "+" => BinOp::Add,
        "-" => BinOp::Sub,
        "*" => BinOp::Mul,
        // `%` divides (q heritage); `/` is the alias everyone expects.
        "%" | "/" => BinOp::Div,
        "^" => return Ok(Node::Coalesce(Box::new(left), Box::new(right))),
        "=" => BinOp::Eq,
        "<" => BinOp::Lt,
        ">" => BinOp::Gt,
        "<=" => BinOp::LtEq,
        ">=" => BinOp::GtEq,
        "<>" | "!=" => BinOp::Neq,
        _ => return Err(format!("Unknown operator: {}", op)),
    };
    Ok(left.bin(op, right))
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

/// A call, `fn[args]` or `fn args`. The name is checked first so arguments parse once
/// (trying aggregates then scalars doubled work per nesting level).
fn parse_call(name: &str, args: &[Token]) -> Result<Node, String> {
    if is_agg_function(name) {
        parse_agg_function(name, args)
    } else {
        parse_function(name, args)
    }
}

// Parse aggregation function like avg[a], min[b], etc.
fn parse_agg_function(name: &str, args: &[Token]) -> Result<Node, String> {
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
    let node = parse_node(args)?;
    let op = match fn_name.as_str() {
        "avg" | "mean" => Op::Mean,
        "min" => Op::Min,
        "max" => Op::Max,
        "count" => Op::Count,
        "std" | "stddev" | "dev" => Op::Std,
        "var" => Op::Var,
        "med" | "median" => Op::Median,
        "sum" => Op::Sum,
        "first" => Op::First,
        "last" => Op::Last,
        "nunique" => Op::NUnique,
        _ => return Err(format!("Unknown aggregation function: {}", name)),
    };
    let node = node.op(op);
    // A bare-column aggregate is named `{fn}_{column}` so two aggregates of one column do
    // not collide; an explicit alias overrides it later.
    match simple_column_name(args) {
        Some(column) => Ok(node.alias(format!("{}_{}", fn_name, column))),
        None => Ok(node),
    }
}

// Parse function like not[a=b], null[col], len[x], upper[x], etc.
fn parse_function(name: &str, args: &[Token]) -> Result<Node, String> {
    if args.is_empty() {
        return Err(format!("Function {} requires an argument", name));
    }
    let name_lower = name.to_lowercase();
    if !SCALAR_FUNCTIONS.contains(&name_lower.as_str()) {
        return Err(format!("Unknown function: {}", name));
    }
    let node = parse_node(args)?;
    let op = match name_lower.as_str() {
        "not" => Op::Not,
        "null" => Op::IsNull,
        "len" | "length" => Op::LenChars,
        "upper" => Op::Upper,
        "lower" => Op::Lower,
        "abs" => Op::Abs,
        "floor" => Op::Floor,
        "ceil" | "ceiling" => Op::Ceil,
        "sqrt" => Op::Sqrt,
        "log" => Op::Ln,
        "exp" => Op::Exp,
        _ => return Err(format!("Unknown function: {}", name)),
    };
    Ok(node.op(op))
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
fn apply_accessor(node: Node, accessor: &str, args: &[AccessorArg]) -> Result<Node, String> {
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
    // The string pieces cast first, so they also work on numbers and dates read
    // as such (NOAA's DATE, an integer zip code); a cast from String is a no-op.
    let as_str = || node.clone().cast_text();
    Ok(match name.as_str() {
        "date" => node.op(Op::Date),
        "time" => node.op(Op::Time),
        "year" => node.op(Op::Year),
        "quarter" => node.op(Op::Quarter),
        "month" => node.op(Op::Month),
        "week" => node.op(Op::Week),
        "day" => node.op(Op::Day),
        "doy" => node.op(Op::OrdinalDay),
        "dow" | "weekday" => node.op(Op::Weekday),
        "hour" => node.op(Op::Hour),
        "minute" => node.op(Op::Minute),
        "second" => node.op(Op::Second),
        "month_start" => node.op(Op::MonthStart),
        "month_end" => node.op(Op::MonthEnd),
        "format" => node.op(Op::DtFormat(text(0)?)),
        "len" | "length" => node.op(Op::LenChars),
        "upper" => node.op(Op::Upper),
        "lower" => node.op(Op::Lower),
        "starts_with" => node.op(Op::StartsWith(text(0)?)),
        "ends_with" => node.op(Op::EndsWith(text(0)?)),
        "contains" => node.op(Op::ContainsLiteral(text(0)?)),
        "part" => as_str().op(Op::Part(text(0)?, int(1)?)),
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
                    Some(n as u64)
                }
                // No length: to the end of the string.
                _ => None,
            };
            as_str().op(Op::Slice(start, length))
        }
        "replace" => as_str().op(Op::ReplaceAll(text(0)?, text(1)?)),
        "strip" => as_str().op(Op::Strip),
        "to_date" => as_str().op(Op::ToDate(args.first().map(|_| text(0)).transpose()?)),
        "to_datetime" => as_str().op(Op::ToDatetime(args.first().map(|_| text(0)).transpose()?)),
        "round" => {
            let decimals = if args.is_empty() { 0 } else { int(0)? };
            let decimals = u32::try_from(decimals)
                .map_err(|_| format!("round: decimals cannot be negative, e.g. {}", usage))?;
            node.op(Op::Round(decimals))
        }
        "int" => node.op(Op::Cast(CastTo::Int64)),
        "float" => node.op(Op::Cast(CastTo::Float64)),
        "str" => node.op(Op::Cast(CastTo::String)),
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

/// Parse optional dot accessors from the remaining tokens, returning
/// (expr_with_accessors, remaining). With `base_name`, results are aliased
/// `{base}_{accessor}` (chained: `{base}_{acc1}_{acc2}`) to avoid duplicates.
fn parse_accessors<'a>(
    mut expr: Node,
    mut tokens: &'a [Token],
    base_name: Option<&str>,
) -> Result<(Node, &'a [Token]), String> {
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
        expr = expr.alias(alias);
    }
    Ok((expr, tokens))
}

fn parse_term(tokens: &[Token]) -> Result<(Node, &[Token]), String> {
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
                let expr = Node::Col(col_name.clone());
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
                // (Function calls without brackets are handled in parse_node)
                let expr = Node::Col(name.clone());
                let (expr, remaining) = parse_accessors(expr, &tokens[1..], Some(name))?;
                Ok((expr, remaining))
            }
        }
        Token::Number(n) => Ok((Node::Num(*n), &tokens[1..])), // Numbers don't support accessors
        Token::String(s) => Ok((Node::Str(s.clone()), &tokens[1..])), // Strings don't support accessors
        Token::DateLiteral(iso) => Ok((Node::Date(iso.clone()), &tokens[1..])),
        Token::TimestampLiteral {
            iso,
            format_str,
            time_unit,
        } => Ok((
            Node::Timestamp {
                iso: iso.clone(),
                format: format_str.clone(),
                unit: *time_unit,
                zone: None,
            },
            &tokens[1..],
        )),
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
            let inner = parse_node(&tokens[1..i - 1])?;
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

/// Deepest nesting the recursive-descent parser follows, so a long chain (`------x`,
/// `((((x))))`) errors instead of overflowing the stack (found by the `parse_query`
/// fuzz target). Each level costs about 10 KiB of stack in a debug build, so 64 is safe
/// on a 2 MiB worker thread and far beyond handwritten nesting.
const MAX_EXPR_DEPTH: u32 = 64;

thread_local! {
    static EXPR_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Holds the recursion counter up while alive: every recursive path passes through
/// `parse_node`, which returns from many places, so the decrement is scoped.
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
fn parse_node(tokens: &[Token]) -> Result<Node, String> {
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
        // A call without brackets: the rest is the argument, built as the bracketed form is.
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
                    let right = parse_node(&right_tokens[2..])?;
                    return apply_op(Node::Int(0).bin(BinOp::Sub, Node::Num(n)), bin_op, right);
                }
                if right_tokens.len() == 1 {
                    return Ok(Node::Int(0).bin(BinOp::Sub, Node::Num(n)));
                }
            }
            // Unary plus/minus when there is no left operand (e.g. -x, +x, -(a+b))
            if left_tokens.is_empty() && (op == "+" || op == "-") {
                let inner = parse_node(right_tokens)?;
                return if op == "-" {
                    Ok(Node::Int(0).bin(BinOp::Sub, inner))
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
        // No operator: a term. Callers pass complete expressions, so leftover tokens are an
        // error (not silently dropped, as `where x > 1 by dept` once lost `by dept`).
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

/// A parsed q query, ready to apply to a LazyFrame.
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
    /// The query with text casts and date parts guarded for dates past the calendar,
    /// where Polars panics ([`crate::past_calendar::guard_expr`]). With `schema`, only
    /// date and datetime operations change.
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

/// A q query as parsed, before it becomes Polars expressions: what
/// [`parse_query`] runs and "Copy as Python" writes out.
#[derive(Debug, Default)]
pub(crate) struct QueryNodes {
    pub cols: Vec<Node>,
    pub filter: Option<Node>,
    pub group_by: Vec<Node>,
    pub group_by_names: Vec<String>,
    pub distinct: bool,
}

impl QueryNodes {
    fn into_parsed(self) -> ParsedQuery {
        let lower = |nodes: Vec<Node>| nodes.iter().map(Node::to_expr).collect();
        ParsedQuery {
            cols: lower(self.cols),
            filter: self.filter.as_ref().map(Node::to_expr),
            group_by: lower(self.group_by),
            group_by_names: self.group_by_names,
            distinct: self.distinct,
        }
    }

    /// Each `/` named as Polars runs it over `schema`, the data the query reads; see
    /// [`Node::resolve_division`].
    pub(crate) fn resolve_division(&mut self, schema: &Schema) {
        let nodes = self
            .cols
            .iter_mut()
            .chain(self.filter.iter_mut())
            .chain(self.group_by.iter_mut());
        for node in nodes {
            node.resolve_division(schema);
        }
    }

    /// Each timestamp literal read in the zone of the column it meets in `schema`; see
    /// [`Node::resolve_time_zones`].
    pub(crate) fn resolve_time_zones(&mut self, schema: &Schema) {
        let nodes = self
            .cols
            .iter_mut()
            .chain(self.filter.iter_mut())
            .chain(self.group_by.iter_mut());
        for node in nodes {
            node.resolve_time_zones(schema);
        }
    }

    /// Fails on a temporal column compared with quoted text; see
    /// [`Node::check_quoted_temporal`].
    fn check_quoted_temporal(&self, schema: &Schema) -> Result<(), String> {
        self.cols
            .iter()
            .chain(self.filter.iter())
            .chain(self.group_by.iter())
            .try_for_each(|node| node.check_quoted_temporal(schema))
    }

    /// The where clause as a Python `.filter(...)` call, if there is one.
    pub(crate) fn python_filter(&self) -> Option<String> {
        self.filter
            .as_ref()
            .map(|f| format!(".filter({})", f.python()))
    }

    /// Python calls doing what `DataTableState::query` does: the where clause, then the
    /// grouping (ordered by keys named `key_names`) or the select list, then `distinct`.
    pub(crate) fn python_steps(&self, key_names: &[String]) -> Vec<String> {
        let mut steps: Vec<String> = self.python_filter().into_iter().collect();
        if !self.group_by.is_empty() {
            let keys = python_list(&self.group_by);
            let aggs = if !self.cols.is_empty() {
                python_list(&self.cols)
            } else if self.group_by_names.is_empty() {
                "pl.all()".to_string()
            } else {
                let names: Vec<String> = self
                    .group_by_names
                    .iter()
                    .map(|n| crate::python_script::py_str(n))
                    .collect();
                format!("pl.all().exclude({})", names.join(", "))
            };
            steps.push(format!(".group_by({keys})"));
            steps.push(format!(".agg({aggs})"));
            steps.push(crate::python_script::sort_call(
                key_names,
                &vec![false; key_names.len()],
            ));
        } else if !self.cols.is_empty() {
            steps.push(format!(".select({})", python_list(&self.cols)));
        }
        if self.distinct {
            steps.push(".unique(keep=\"first\", maintain_order=True)".to_string());
        }
        steps
    }
}

/// Expressions as Python arguments: a plain column by its name, as Polars reads a
/// string there, anything else as an expression.
fn python_list(nodes: &[Node]) -> String {
    nodes
        .iter()
        .map(|n| match n {
            Node::Col(name) => crate::python_script::py_str(name),
            n => n.python(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn parse_query(query: &str) -> Result<ParsedQuery, String> {
    parse_nodes(query).map(QueryNodes::into_parsed)
}

/// [`parse_query`] for data of `schema`: timestamp literals against zoned columns read
/// in that zone, and temporal columns compared with quoted text are errors.
pub fn parse_query_over(query: &str, schema: Option<&Schema>) -> Result<ParsedQuery, String> {
    let mut nodes = parse_nodes(query)?;
    if let Some(schema) = schema {
        nodes.resolve_time_zones(schema);
        nodes.check_quoted_temporal(schema)?;
    }
    Ok(nodes.into_parsed())
}

/// Parse a q query into nodes. An empty query selects every column.
pub(crate) fn parse_nodes(query: &str) -> Result<QueryNodes, String> {
    // Empty query is equivalent to "select" - return all columns with no filter or grouping
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(QueryNodes::default());
    }

    let tokens = tokenize(query)?;
    if tokens.is_empty() || tokens[0] != Token::Select {
        return Err("Query must start with 'select'".to_string());
    }
    // `distinct` after `select` is the keyword unless it is a column or alias
    // (`distinct: x`, `distinct, a`, `distinct + 1`); `col["distinct"]` always names it.
    let distinct = tokens.get(1) == Some(&Token::Identifier("distinct".to_string()))
        && !matches!(
            tokens.get(2),
            Some(Token::Colon | Token::Comma | Token::Dot | Token::Op(_))
        );
    let body = strip_from(&tokens[if distinct { 2 } else { 1 }..])?;
    let body = &body[..];

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

                let expr = parse_node(expr_tokens)?;
                cols.push(expr.alias(alias_name));
            } else {
                cols.push(parse_node(&chunk)?);
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

                let expr = parse_node(expr_tokens)?;
                group_by_cols.push(expr.alias(alias_name.clone()));
                group_by_col_names.push(alias_name); // Use alias name
            } else {
                let expr = parse_node(&chunk)?;
                group_by_cols.push(expr.clone());
                // The column name of a simple expression: `name`, or `col[name]`.
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
                    // Complex expressions have no simple name; sorting uses the expression itself.
                }
            }
        }
    }

    let mut filter: Option<Node> = None;
    if let Some(wt) = where_tokens {
        for chunk in split_tokens(&wt, &Token::Comma) {
            if chunk.is_empty() {
                continue;
            }
            let mut or_expr: Option<Node> = None;
            for or_chunk in split_tokens(&chunk, &Token::Pipe) {
                if or_chunk.is_empty() {
                    continue;
                }
                let e = parse_node(&or_chunk)?;
                or_expr = match or_expr {
                    Some(curr) => Some(curr.bin(BinOp::Or, e)),
                    None => Some(e),
                };
            }
            if let Some(e) = or_expr {
                filter = match filter {
                    Some(curr) => Some(curr.bin(BinOp::And, e)),
                    None => Some(e),
                };
            }
        }
    }

    Ok(QueryNodes {
        cols,
        filter,
        group_by: group_by_cols,
        group_by_names: group_by_col_names,
        distinct,
    })
}

/// One expression as Polars runs it.
#[cfg(test)]
fn parse_expr(tokens: &[Token]) -> Result<Expr, String> {
    parse_node(tokens).map(|n| n.to_expr())
}

#[cfg(test)]
mod tests;
