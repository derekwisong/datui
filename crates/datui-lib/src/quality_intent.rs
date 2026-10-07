//! Declared column intent for Data Quality: what a column must hold, said by the
//! user, measured on the rows a run reads anyway.
//!
//! The profile can say a column is nearly unique; only a declaration can say it is a
//! key, so that a repeat is a defect and not a category. Every rule is opt-in, and a
//! violation is counted as a fact out of the rows or values checked, never folded
//! into a score.

use crate::data_quality::{
    DataQualityPlan, ObservationKind, QualityObservation, QualityPrecision, TimeInterpretation,
    TimeKind,
};
use crate::statistics::collect_lazy;
use color_eyre::Result;
use polars::prelude::*;

/// The most values an allowed set holds.
pub const MAX_ALLOWED_VALUES: usize = 100;

/// Values outside the allowed set, or text that does not read as a number, kept as
/// examples where the rows are in memory.
pub const MAX_INTENT_EXAMPLES: usize = 3;

const PREFIX: &str = "__datui_intent::";

/// How text read as a number is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumberReading {
    Whole,
    Decimal,
}

impl NumberReading {
    pub fn label(self) -> &'static str {
        match self {
            Self::Whole => "whole number",
            Self::Decimal => "decimal",
        }
    }

    fn dtype(self) -> DataType {
        match self {
            Self::Whole => DataType::Int64,
            Self::Decimal => DataType::Float64,
        }
    }
}

/// What one column must hold. An empty intent declares nothing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ColumnIntent {
    pub column: String,
    /// Every row has a value.
    pub required: bool,
    /// The values the column may hold, as typed; empty for any.
    pub allowed: Vec<String>,
    /// The lowest value allowed, as typed; a number, a date or a date and time.
    pub min: Option<String>,
    pub max: Option<String>,
    /// Text read as a number: a value that does not read is counted, and the range
    /// compares the number.
    pub number: Option<NumberReading>,
}

impl ColumnIntent {
    pub fn new(column: &str) -> Self {
        Self {
            column: column.to_string(),
            ..Self::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.required
            && self.allowed.is_empty()
            && self.min.is_none()
            && self.max.is_none()
            && self.number.is_none()
    }

    /// The range in words: `0 to 100`, `at least 0`, `at most 2024-12-31`.
    pub fn range_label(&self) -> Option<String> {
        match (&self.min, &self.max) {
            (Some(min), Some(max)) => Some(format!("{min} to {max}")),
            (Some(min), None) => Some(format!("at least {min}")),
            (None, Some(max)) => Some(format!("at most {max}")),
            (None, None) => None,
        }
    }

    /// The allowed set as typed, cut to `shown` values: `open, closed +3 more`.
    pub fn allowed_label(&self, shown: usize) -> String {
        let mut label = format_allowed(&self.allowed[..self.allowed.len().min(shown)]);
        if self.allowed.len() > shown {
            label.push_str(&format!(" +{} more", self.allowed.len() - shown));
        }
        label
    }

    /// Each rule in a few words: `required`, `one of 3`, `0 to 100`, `read as decimal`.
    pub fn rules(&self) -> Vec<String> {
        let mut rules = Vec::new();
        if self.required {
            rules.push("required".to_string());
        }
        if let Some(number) = self.number {
            rules.push(format!("read as {}", number.label()));
        }
        if !self.allowed.is_empty() {
            rules.push(format!(
                "one of {}",
                crate::numfmt::group_chrome(self.allowed.len())
            ));
        }
        if let Some(range) = self.range_label() {
            rules.push(range);
        }
        rules
    }
}

/// Everything a study declares about its columns: the key, and each column's rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct DeclaredIntent {
    /// The columns whose values together name one row, in the order declared.
    pub key: Vec<String>,
    pub columns: Vec<ColumnIntent>,
}

impl DeclaredIntent {
    pub fn is_empty(&self) -> bool {
        self.key.is_empty() && self.columns.is_empty()
    }

    pub fn column(&self, name: &str) -> Option<&ColumnIntent> {
        self.columns.iter().find(|intent| intent.column == name)
    }

    /// Declare `intent` for its column in place of what it had; an empty intent
    /// removes the column's rules.
    pub fn set(&mut self, intent: ColumnIntent) {
        match self
            .columns
            .iter()
            .position(|known| known.column == intent.column)
        {
            Some(index) if intent.is_empty() => {
                self.columns.remove(index);
            }
            Some(index) => self.columns[index] = intent,
            None if intent.is_empty() => {}
            None => self.columns.push(intent),
        }
    }

    /// Put `column` in the key, or take it out.
    pub fn set_key(&mut self, column: &str, in_key: bool) {
        let known = self.key.iter().any(|name| name == column);
        if in_key && !known {
            self.key.push(column.to_string());
        } else if !in_key {
            self.key.retain(|name| name != column);
        }
    }

    /// The columns any rule names, the key first.
    pub fn declared_columns(&self) -> Vec<&str> {
        let mut names = self.key.iter().map(String::as_str).collect::<Vec<_>>();
        for intent in &self.columns {
            if !names.contains(&intent.column.as_str()) {
                names.push(intent.column.as_str());
            }
        }
        names
    }

    /// What Setup's row says: `key id, region · 2 columns with rules`.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if !self.key.is_empty() {
            parts.push(format!("key {}", self.key.join(", ")));
        }
        match self.columns.len() {
            0 => {}
            1 => parts.push(format!(
                "{}: {}",
                self.columns[0].column,
                self.columns[0].rules().join(", ")
            )),
            count => parts.push(format!("rules on {count} columns")),
        }
        parts.join(&format!(" {} ", crate::glyphs::get().middot))
    }
}

/// What a column's values are, once read as declared: what a range can compare and
/// a set can list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Number,
    Date,
    Datetime,
    Text,
    Boolean,
    Other,
}

impl ValueKind {
    /// `column`'s values, read through `number` when it is text read as a number,
    /// or `time` when it is text read as time.
    pub fn of(
        dtype: &DataType,
        number: Option<NumberReading>,
        time: Option<&TimeInterpretation>,
    ) -> Self {
        if let Some(time) = time {
            return match time.kind {
                TimeKind::Date => Self::Date,
                TimeKind::Datetime => Self::Datetime,
            };
        }
        if number.is_some() && is_text(dtype) {
            return Self::Number;
        }
        match dtype {
            DataType::Date => Self::Date,
            DataType::Datetime(..) => Self::Datetime,
            DataType::String | DataType::Categorical(..) => Self::Text,
            DataType::Boolean => Self::Boolean,
            dtype if dtype.is_primitive_numeric() || dtype.is_decimal() => Self::Number,
            _ => Self::Other,
        }
    }

    /// Whether a range applies: numbers and times have an order worth declaring.
    pub fn ranges(self) -> bool {
        matches!(self, Self::Number | Self::Date | Self::Datetime)
    }

    /// What a bound is typed as, for the form's hint.
    pub fn bound_hint(self) -> &'static str {
        match self {
            Self::Number => "a number",
            Self::Date => "a date, 2024-01-31",
            Self::Datetime => "a date or 2024-01-31 08:00:00",
            _ => "",
        }
    }
}

fn is_text(dtype: &DataType) -> bool {
    matches!(dtype, DataType::String | DataType::Categorical(..))
}

/// Whether an allowed set applies: values compared as stored, text, whole numbers or
/// true and false. A float or a time is a measurement, not a code.
pub fn allows_set(dtype: &DataType) -> bool {
    is_text(dtype) || dtype.is_integer() || matches!(dtype, DataType::Boolean)
}

/// Whether the column can be read as a number: text.
pub fn reads_as_number(dtype: &DataType) -> bool {
    is_text(dtype)
}

/// A range bound, as compared: a number, or a time in microseconds since the epoch,
/// read as UTC when it has no zone, as every other time in the study is.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Bound {
    Number(f64),
    Micros(i64),
}

impl Bound {
    fn lit(self) -> Expr {
        match self {
            Self::Number(value) => lit(value),
            Self::Micros(value) => lit(value),
        }
    }

    fn value(self) -> f64 {
        match self {
            Self::Number(value) => value,
            Self::Micros(value) => value as f64,
        }
    }
}

/// A date alone as a time's maximum takes in its whole day: `at most 2024-06-30`
/// keeps 2024-06-30 08:00, as it reads.
fn parse_bound(kind: ValueKind, text: &str, upper: bool) -> std::result::Result<Bound, String> {
    let text = text.trim();
    let midnight = |date: chrono::NaiveDate| {
        date.and_hms_opt(0, 0, 0)
            .map(|time| Bound::Micros(time.and_utc().timestamp_micros()))
    };
    let day_end = |date: chrono::NaiveDate| {
        date.succ_opt()
            .and_then(|next| next.and_hms_opt(0, 0, 0))
            .map(|time| Bound::Micros(time.and_utc().timestamp_micros() - 1))
    };
    let parsed = match kind {
        ValueKind::Number => text
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(Bound::Number),
        ValueKind::Date => chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()
            .and_then(midnight),
        ValueKind::Datetime => ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"]
            .into_iter()
            .find_map(|format| chrono::NaiveDateTime::parse_from_str(text, format).ok())
            .map(|time| Bound::Micros(time.and_utc().timestamp_micros()))
            .or_else(|| {
                chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
                    .ok()
                    .and_then(|date| if upper { day_end(date) } else { midnight(date) })
            }),
        _ => None,
    };
    parsed.ok_or_else(|| format!("{text:?} is not {}", kind.bound_hint()))
}

/// The values an allowed set holds, from what was typed: separated by commas, outer
/// spaces dropped, each once, in the order typed. A value in double quotes is taken
/// as it stands, commas and spaces included, with `""` for a quote inside it.
pub fn parse_allowed(dtype: &DataType, text: &str) -> std::result::Result<Vec<String>, String> {
    let values = split_allowed(text)?;
    check_allowed(dtype, &values)?;
    Ok(values)
}

fn split_allowed(text: &str) -> std::result::Result<Vec<String>, String> {
    let mut values: Vec<String> = Vec::new();
    let mut push = |value: String| {
        if !values.contains(&value) {
            values.push(value);
        }
    };
    let mut chars = text.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        if chars.next_if_eq(&'"').is_some() {
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some('"') if chars.next_if_eq(&'"').is_some() => value.push('"'),
                    Some('"') => break,
                    Some(c) => value.push(c),
                    None => return Err("A quoted value has no closing quote".to_string()),
                }
            }
            while chars.next_if(|c| c.is_whitespace()).is_some() {}
            match chars.next() {
                None | Some(',') => push(value),
                Some(_) => return Err(format!("Put a comma after {value:?}")),
            }
        } else {
            let mut value = String::new();
            for c in chars.by_ref() {
                if c == ',' {
                    break;
                }
                value.push(c);
            }
            let value = value.trim();
            if !value.is_empty() {
                push(value.to_string());
            }
        }
        if chars.peek().is_none() {
            break;
        }
    }
    Ok(values)
}

/// Whether `values` can be compared with a column of `dtype`, and few enough.
fn check_allowed(dtype: &DataType, values: &[String]) -> std::result::Result<(), String> {
    // Read as the set compares it, so what is accepted here is what matches.
    if dtype.is_integer() || matches!(dtype, DataType::Boolean) {
        for value in values {
            crate::typed_value::parse(value, dtype)?;
        }
    }
    if values.len() > MAX_ALLOWED_VALUES {
        return Err(format!("At most {MAX_ALLOWED_VALUES} allowed values"));
    }
    Ok(())
}

/// An allowed set as it is typed: values joined by commas, quoted where a comma, a
/// quote or outer spaces would change how it reads back.
pub fn format_allowed(values: &[String]) -> String {
    values
        .iter()
        .map(|value| {
            let plain = !value.is_empty()
                && value.trim() == value
                && !value.contains(',')
                && !value.starts_with('"');
            if plain {
                value.clone()
            } else {
                format!("\"{}\"", value.replace('"', "\"\""))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Why `intent` cannot be measured on a column of `dtype`, said on the form: a bound
/// that is not a value of the column's kind, or a minimum above the maximum.
pub fn check_intent(
    intent: &ColumnIntent,
    dtype: &DataType,
    time: Option<&TimeInterpretation>,
) -> std::result::Result<(), String> {
    let kind = ValueKind::of(dtype, intent.number, time);
    if !intent.allowed.is_empty() {
        if !allows_set(dtype) {
            return Err("Allowed values are for text, whole numbers or true/false".to_string());
        }
        check_allowed(dtype, &intent.allowed)?;
    }
    let bound = |text: &Option<String>, upper: bool| {
        text.as_deref()
            .map(|text| parse_bound(kind, text, upper))
            .transpose()
    };
    if intent.min.is_some() || intent.max.is_some() {
        if !kind.ranges() {
            return Err("A range is for numbers, dates and times".to_string());
        }
        if let (Some(min), Some(max)) = (bound(&intent.min, false)?, bound(&intent.max, true)?)
            && min.value() > max.value()
        {
            return Err("Minimum is above maximum".to_string());
        }
    }
    if intent.number.is_some() && !reads_as_number(dtype) {
        return Err("Only text is read as a number".to_string());
    }
    Ok(())
}

/// A date or millisecond datetime held to what microseconds since the epoch can
/// count, so converting it to them cannot overflow, which made a date past the
/// calendar null and never compared. One held at a limit (about 292,000 years from
/// 1970) is still further out than any bound, which the calendar keeps within
/// 262,143 years. Any other value as it is; a batch with none so far out costs a
/// min and a max.
fn within_micros(value: Expr) -> Expr {
    value.map(
        |c| {
            let limit = match c.dtype() {
                DataType::Date => i64::MAX / 86_400_000_000,
                DataType::Datetime(TimeUnit::Milliseconds, _) => i64::MAX / 1_000,
                _ => return Ok(c),
            };
            let series = c.as_materialized_series();
            let stored = series.to_physical_repr().cast(&DataType::Int64)?;
            let stored = stored.i64()?;
            let fits = |v: i64| (-limit..=limit).contains(&v);
            if [stored.min(), stored.max()].into_iter().flatten().all(fits) {
                return Ok(c);
            }
            let held = stored.apply_values(|v| v.clamp(-limit, limit));
            Ok(held
                .into_series()
                .cast(c.dtype())?
                .with_name(series.name().clone())
                .into_column())
        },
        |_, field| Ok(field.clone()),
    )
}

/// One column's declared rules, as a run measures them: the declaration, the type it
/// met, and the expressions that count it.
struct Measured<'a> {
    intent: &'a ColumnIntent,
    dtype: DataType,
    time: Option<TimeInterpretation>,
}

impl Measured<'_> {
    fn stored(&self) -> Expr {
        col(self.intent.column.as_str())
    }

    fn kind(&self) -> ValueKind {
        ValueKind::of(&self.dtype, self.intent.number, self.time.as_ref())
    }

    /// The value as declared: text read as a number or a time, otherwise as stored.
    /// Read as `str.cast` and the time format read it for the profile, so the counts
    /// agree with "Numbers as text" and "Unparsed times".
    fn value(&self) -> Expr {
        if let Some(time) = &self.time {
            return time.expr();
        }
        match self.intent.number {
            Some(number) if is_text(&self.dtype) => {
                self.stored().cast(DataType::String).cast(number.dtype())
            }
            _ => self.stored(),
        }
    }

    /// What a range compares: a number as a float, a time as microseconds since the
    /// epoch (an instant's own; a time with no zone read as UTC).
    fn compared(&self) -> Option<Expr> {
        let value = self.value();
        Some(match self.kind() {
            ValueKind::Number => value.cast(DataType::Float64),
            ValueKind::Date => within_micros(value)
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .dt()
                .timestamp(TimeUnit::Microseconds),
            ValueKind::Datetime => within_micros(value).dt().timestamp(TimeUnit::Microseconds),
            _ => return None,
        })
    }

    fn bounds(&self) -> (Option<Bound>, Option<Bound>) {
        let kind = self.kind();
        let bound = |text: &Option<String>, upper: bool| {
            text.as_deref()
                .and_then(|text| parse_bound(kind, text, upper).ok())
        };
        (
            bound(&self.intent.min, false),
            bound(&self.intent.max, true),
        )
    }

    fn below(&self) -> Option<Expr> {
        Some(self.compared()?.lt(self.bounds().0?.lit()))
    }

    fn above(&self) -> Option<Expr> {
        Some(self.compared()?.gt(self.bounds().1?.lit()))
    }

    fn out_of_range(&self) -> Option<Expr> {
        match (self.below(), self.above()) {
            (Some(below), Some(above)) => Some(below.or(above)),
            (below, above) => below.or(above),
        }
    }

    /// Stored values in the allowed set, each compared at the column's own type: text
    /// exactly as stored, a whole number as the width it is stored at, so a `u64`
    /// past `i64::MAX` is still itself.
    fn in_set(&self) -> Option<Expr> {
        if self.intent.allowed.is_empty() || !allows_set(&self.dtype) {
            return None;
        }
        let stored = self.stored();
        self.intent
            .allowed
            .iter()
            .filter_map(|value| {
                let value = crate::typed_value::parse(value, &self.dtype).ok()?;
                Some(stored.clone().eq(lit(value)))
            })
            .reduce(Expr::or)
    }

    fn outside(&self) -> Option<Expr> {
        Some(self.stored().is_not_null().and(self.in_set()?.not()))
    }

    /// Text that does not read as the declared number.
    fn unparsed(&self) -> Option<Expr> {
        if self.intent.number.is_none() || !is_text(&self.dtype) || self.time.is_some() {
            return None;
        }
        Some(self.stored().is_not_null().and(self.value().is_null()))
    }
}

fn measured<'a>(plan: &'a DataQualityPlan, schema: &Schema) -> Vec<Measured<'a>> {
    plan.intent
        .columns
        .iter()
        .filter_map(|intent| {
            Some(Measured {
                intent,
                dtype: schema.get(&intent.column)?.clone(),
                time: plan.time_format(&intent.column).cloned(),
            })
        })
        .collect()
}

fn name(index: usize, what: &str) -> String {
    format!("{PREFIX}{index}::{what}")
}

/// The key's columns, when every one of them is in `schema`.
fn key_columns(plan: &DataQualityPlan, schema: &Schema) -> Option<Vec<String>> {
    let key = &plan.intent.key;
    (!key.is_empty() && key.iter().all(|column| schema.get(column).is_some())).then(|| key.clone())
}

fn any_null(key: &[String]) -> Expr {
    key.iter()
        .map(|column| col(column.as_str()).is_null())
        .reduce(Expr::or)
        .unwrap_or_else(|| lit(false))
}

/// Every rule's counts, as aggregations over the scope: added to the pass that
/// profiles the columns, so they cost that pass nothing but the sums.
pub(crate) fn intent_exprs(plan: &DataQualityPlan, schema: &Schema) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (index, rules) in measured(plan, schema).iter().enumerate() {
        exprs.push(
            rules
                .stored()
                .is_not_null()
                .sum()
                .alias(name(index, "values")),
        );
        if rules.intent.required {
            exprs.push(rules.stored().is_null().sum().alias(name(index, "missing")));
        }
        if let Some(unparsed) = rules.unparsed() {
            exprs.push(unparsed.sum().alias(name(index, "unparsed")));
        }
        if let Some(outside) = rules.outside() {
            exprs.push(outside.sum().alias(name(index, "outside")));
        }
        if let Some(compared) = rules.compared().filter(|_| rules.out_of_range().is_some()) {
            exprs.push(compared.is_not_null().sum().alias(name(index, "compared")));
            let value = rules.value();
            if let Some(below) = rules.below() {
                exprs.push(below.clone().sum().alias(name(index, "below")));
                exprs.push(
                    value
                        .clone()
                        .filter(below)
                        .min()
                        .alias(name(index, "lowest")),
                );
            }
            if let Some(above) = rules.above() {
                exprs.push(above.clone().sum().alias(name(index, "above")));
                exprs.push(value.filter(above).max().alias(name(index, "highest")));
            }
        }
    }
    if let Some(key) = key_columns(plan, schema) {
        exprs.push(any_null(&key).sum().alias(format!("{PREFIX}key::missing")));
    }
    exprs
}

/// How often the declared key's value repeats, over the rows with every part of it:
/// groups of rows sharing one value, the rows beyond one per value, and every row in
/// such a group. One grouping of the key's columns alone, as duplicate rows are
/// counted: over rows in memory it reads nothing, and over the scope it is a pass of
/// its own, which Setup names before Run.
pub(crate) fn key_repeats(
    lf: &LazyFrame,
    plan: &DataQualityPlan,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Option<(usize, usize, usize)>> {
    let Some(key) = key_columns(plan, schema) else {
        return Ok(None);
    };
    const COUNT: &str = "__datui_intent_key_rows";
    let columns = key
        .iter()
        .map(|name| col(name.as_str()))
        .collect::<Vec<_>>();
    let query = lf
        .clone()
        .select(columns.clone())
        .filter(any_null(&key).not())
        .group_by(columns)
        .agg([len().alias(COUNT)])
        .filter(col(COUNT).gt(lit(1u32)))
        .select([
            len().alias("groups"),
            (col(COUNT) - lit(1u32)).sum().alias("extra"),
            col(COUNT).sum().alias("involved"),
        ]);
    let summary = collect_lazy(query, polars_streaming)?;
    Ok(Some((
        count_at(&summary, "groups").unwrap_or(0),
        count_at(&summary, "extra").unwrap_or(0),
        count_at(&summary, "involved").unwrap_or(0),
    )))
}

fn count_at(df: &DataFrame, name: &str) -> Option<usize> {
    match df.column(name).ok()?.get(0).ok()? {
        AnyValue::UInt32(value) => Some(value as usize),
        AnyValue::UInt64(value) => Some(value as usize),
        AnyValue::Int32(value) => usize::try_from(value).ok(),
        AnyValue::Int64(value) => usize::try_from(value).ok(),
        _ => None,
    }
}

fn text_at(df: &DataFrame, name: &str) -> Option<String> {
    let value = df.column(name).ok()?.get(0).ok()?;
    (!value.is_null()).then(|| crate::exact::str_value(&value).into_owned())
}

/// The commonest values `rows` holds in `value`, with how many rows hold each: over
/// rows in memory only.
fn commonest(lf: &LazyFrame, rows: Expr, value: Expr) -> Result<Vec<(String, usize)>> {
    const VALUE: &str = "__datui_intent_value";
    const COUNT: &str = "__datui_intent_count";
    let top = lf
        .clone()
        .filter(rows)
        .select([value.cast(DataType::String).alias(VALUE)])
        .group_by([col(VALUE)])
        .agg([len().alias(COUNT)])
        .sort_by_exprs(
            [col(COUNT), col(VALUE)],
            SortMultipleOptions::default().with_order_descending_multi([true, false]),
        )
        .limit(MAX_INTENT_EXAMPLES as IdxSize)
        .collect()?;
    let (values, counts) = (top.column(VALUE)?, top.column(COUNT)?);
    Ok((0..top.height())
        .filter_map(|row| {
            let value = values.get(row).ok()?;
            let count = match counts.get(row).ok()? {
                AnyValue::UInt32(count) => count as usize,
                AnyValue::UInt64(count) => count as usize,
                _ => return None,
            };
            Some((crate::exact::str_value(&value).into_owned(), count))
        })
        .collect())
}

/// Whether the declared key was checked against every row in scope, or a sample's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyCheck {
    pub columns: Vec<String>,
    /// Rows with no value in some part of the key.
    pub missing: usize,
    /// Key values held by more than one row.
    pub groups: usize,
    /// Rows beyond one per key value.
    pub extra_rows: usize,
    /// Rows that share their key value with another row.
    pub rows_involved: usize,
}

/// What a column's declared rules found.
#[derive(Debug, Clone)]
pub struct ColumnCheck {
    pub intent: ColumnIntent,
    /// The column's type in the scope measured.
    pub dtype: DataType,
    /// The format text was read as time with, under Text as time.
    pub time: Option<TimeInterpretation>,
    /// Rows with a value, as stored.
    pub values: usize,
    /// Required: rows with no value.
    pub missing: Option<usize>,
    /// Read as a number: values that do not read as one.
    pub unparsed: Option<usize>,
    /// Allowed: values outside the set.
    pub outside: Option<usize>,
    /// Range: values the range compared, the ones read.
    pub compared: Option<usize>,
    pub below: Option<usize>,
    pub above: Option<usize>,
    /// The lowest value below the minimum and the highest above the maximum.
    pub lowest: Option<String>,
    pub highest: Option<String>,
    /// The commonest values outside the set, with their rows; from rows in memory.
    pub outside_examples: Vec<(String, usize)>,
    /// The commonest text that does not read as the number; from rows in memory.
    pub unparsed_examples: Vec<(String, usize)>,
}

impl ColumnCheck {
    fn rules(&self) -> Measured<'_> {
        Measured {
            intent: &self.intent,
            dtype: self.dtype.clone(),
            time: self.time.clone(),
        }
    }

    /// Values outside the range, both ways.
    pub fn out_of_range(&self) -> Option<usize> {
        match (self.below, self.above) {
            (None, None) => None,
            (below, above) => Some(below.unwrap_or(0) + above.unwrap_or(0)),
        }
    }
}

/// What a run found of the declared intent.
#[derive(Debug, Clone)]
pub struct IntentResults {
    /// False when the run read no values, so nothing declared was checked.
    pub measured: bool,
    pub precision: QualityPrecision,
    /// Rows checked: the scope's on a full read, the sample's otherwise.
    pub evaluated_rows: usize,
    pub key: Option<KeyCheck>,
    pub columns: Vec<ColumnCheck>,
    /// Every column a rule names, the key's first.
    pub declared: Vec<String>,
    /// Declared columns the scope measured does not have; their rules did not run.
    pub absent: Vec<String>,
}

impl IntentResults {
    /// A run that read no values: what was declared, and that none of it ran.
    pub(crate) fn unmeasured(plan: &DataQualityPlan, schema: &Schema) -> Option<Self> {
        if plan.intent.is_empty() {
            return None;
        }
        Some(Self {
            measured: false,
            precision: QualityPrecision::Metadata,
            evaluated_rows: 0,
            key: None,
            columns: Vec::new(),
            declared: declared(plan),
            absent: absent(plan, schema),
        })
    }

    /// The rules' counts from `counts`, the row that [`intent_exprs`] aggregated, and
    /// the key's repeats from [`key_repeats`]. `rows` are the rows in memory, when the
    /// run measured those: the examples come from them and from nothing else.
    pub(crate) fn from_counts(
        plan: &DataQualityPlan,
        schema: &Schema,
        counts: &DataFrame,
        repeats: Option<(usize, usize, usize)>,
        evaluated_rows: usize,
        precision: QualityPrecision,
        rows: Option<&LazyFrame>,
    ) -> Result<Option<Self>> {
        if plan.intent.is_empty() {
            return Ok(None);
        }
        let mut columns = Vec::new();
        for (index, rules) in measured(plan, schema).iter().enumerate() {
            let count = |what: &str| count_at(counts, &name(index, what));
            let examples = |predicate: Option<Expr>, value: Expr| match (rows, predicate) {
                (Some(rows), Some(predicate)) => commonest(rows, predicate, value),
                _ => Ok(Vec::new()),
            };
            let unparsed = count("unparsed");
            let outside = count("outside");
            columns.push(ColumnCheck {
                intent: rules.intent.clone(),
                dtype: rules.dtype.clone(),
                time: rules.time.clone(),
                values: count("values").unwrap_or(0),
                missing: count("missing"),
                unparsed,
                outside,
                compared: count("compared"),
                below: count("below"),
                above: count("above"),
                lowest: text_at(counts, &name(index, "lowest")),
                highest: text_at(counts, &name(index, "highest")),
                outside_examples: if outside.unwrap_or(0) > 0 {
                    examples(rules.outside(), rules.stored())?
                } else {
                    Vec::new()
                },
                unparsed_examples: if unparsed.unwrap_or(0) > 0 {
                    examples(rules.unparsed(), rules.stored())?
                } else {
                    Vec::new()
                },
            });
        }
        let key = key_columns(plan, schema).map(|key| {
            let (groups, extra_rows, rows_involved) = repeats.unwrap_or((0, 0, 0));
            KeyCheck {
                columns: key,
                missing: count_at(counts, &format!("{PREFIX}key::missing")).unwrap_or(0),
                groups,
                extra_rows,
                rows_involved,
            }
        });
        Ok(Some(Self {
            measured: true,
            precision,
            evaluated_rows,
            key,
            columns,
            declared: declared(plan),
            absent: absent(plan, schema),
        }))
    }

    /// A rule's violations as observations, one per column; the key's once per key
    /// column, so its finding names each.
    pub(crate) fn observations(&self) -> Vec<QualityObservation> {
        let mut observations = Vec::new();
        let push = |observations: &mut Vec<QualityObservation>,
                    kind: ObservationKind,
                    column: &str,
                    affected_rows: usize,
                    evaluated_rows: usize| {
            observations.push(QualityObservation {
                kind,
                column: column.to_string(),
                affected_rows,
                evaluated_rows,
                fact: String::new(),
                normalized_category: None,
                files: Vec::new(),
                time_format: None,
                full_scale: None,
            });
        };
        if !self.measured {
            return observations;
        }
        if let Some(key) = &self.key {
            for column in &key.columns {
                if key.rows_involved > 0 {
                    push(
                        &mut observations,
                        ObservationKind::KeyRepeated,
                        column,
                        key.rows_involved,
                        self.evaluated_rows,
                    );
                }
                if key.missing > 0 {
                    push(
                        &mut observations,
                        ObservationKind::KeyMissing,
                        column,
                        key.missing,
                        self.evaluated_rows,
                    );
                }
            }
        }
        for check in &self.columns {
            let column = check.intent.column.as_str();
            if let Some(missing) = check.missing.filter(|missing| *missing > 0) {
                push(
                    &mut observations,
                    ObservationKind::RequiredMissing,
                    column,
                    missing,
                    self.evaluated_rows,
                );
            }
            if let Some(unparsed) = check.unparsed.filter(|unparsed| *unparsed > 0) {
                push(
                    &mut observations,
                    ObservationKind::UnparsedNumber,
                    column,
                    unparsed,
                    check.values,
                );
            }
            if let Some(outside) = check.outside.filter(|outside| *outside > 0) {
                push(
                    &mut observations,
                    ObservationKind::NotAllowed,
                    column,
                    outside,
                    check.values,
                );
            }
            if let Some(out) = check.out_of_range().filter(|out| *out > 0) {
                push(
                    &mut observations,
                    ObservationKind::OutOfRange,
                    column,
                    out,
                    check.compared.unwrap_or(0),
                );
            }
        }
        observations
    }

    /// Rows sharing a value of the declared key, the key whole.
    pub fn repeated_key(&self) -> Option<Expr> {
        let key = self.key.as_ref()?;
        let columns = key
            .columns
            .iter()
            .map(|name| col(name.as_str()))
            .collect::<Vec<_>>();
        let shared = len().over(columns).ok()?.gt(lit(1u32));
        Some(any_null(&key.columns).not().and(shared))
    }

    /// Rows with no value in some part of the declared key.
    pub fn missing_key(&self) -> Option<Expr> {
        Some(any_null(&self.key.as_ref()?.columns))
    }

    /// Rows with no value in a column declared required.
    pub fn required_missing(&self, column: &str) -> Option<Expr> {
        Some(self.column(column)?.rules().stored().is_null())
    }

    /// Text in `column` that does not read as its declared number.
    pub fn unparsed_number(&self, column: &str) -> Option<Expr> {
        self.column(column)?.rules().unparsed()
    }

    /// Values of `column` outside its declared set.
    pub fn not_allowed(&self, column: &str) -> Option<Expr> {
        self.column(column)?.rules().outside()
    }

    /// Values of `column` outside its declared range.
    pub fn out_of_range(&self, column: &str) -> Option<Expr> {
        self.column(column)?.rules().out_of_range()
    }

    /// What the column's declaration was measured with, for a finding's detail.
    pub fn column(&self, column: &str) -> Option<&ColumnCheck> {
        self.columns
            .iter()
            .find(|check| check.intent.column == column)
    }
}

fn declared(plan: &DataQualityPlan) -> Vec<String> {
    plan.intent
        .declared_columns()
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn absent(plan: &DataQualityPlan, schema: &Schema) -> Vec<String> {
    plan.intent
        .declared_columns()
        .into_iter()
        .filter(|column| schema.get(column).is_none())
        .map(str::to_string)
        .collect()
}

/// A column declared to be the key answers what "Nearly unique" can only suggest, so
/// the suggestion goes: the key's own count says whether it repeats.
pub(crate) fn supersede(observations: &mut Vec<QualityObservation>, plan: &DataQualityPlan) {
    if let [key] = plan.intent.key.as_slice() {
        observations.retain(|observation| {
            !(observation.kind == ObservationKind::KeyLike && &observation.column == key)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_quality::fixtures::measure;
    use crate::data_quality::{DataQualityResults, QualityCompute, compute_data_quality};
    use crate::quality_report::{Outcome, build_report, checks, coverage, describe};

    fn fixture() -> DataFrame {
        df!(
            "id" => &[Some(1i64), Some(2), Some(2), Some(3), Some(4), Some(5), None],
            "status" => &[Some("open"), Some("closed"), Some("open"), Some("void"), Some("Open"), None, Some("closed")],
            "amount" => &[5.0f64, 50.0, -1.0, 20.0, 200.0, 10.0, 0.0],
            "code" => &["1", "2", "x", "4", "5", "6", "7"],
        )
        .unwrap()
    }

    fn declared() -> DeclaredIntent {
        DeclaredIntent {
            key: vec!["id".to_string()],
            columns: vec![
                ColumnIntent {
                    required: true,
                    allowed: vec!["open".to_string(), "closed".to_string()],
                    ..ColumnIntent::new("status")
                },
                ColumnIntent {
                    min: Some("0".to_string()),
                    max: Some("100".to_string()),
                    ..ColumnIntent::new("amount")
                },
                ColumnIntent {
                    number: Some(NumberReading::Whole),
                    min: Some("2".to_string()),
                    ..ColumnIntent::new("code")
                },
            ],
        }
    }

    fn plan(compute: QualityCompute) -> DataQualityPlan {
        DataQualityPlan {
            compute,
            intent: declared(),
            ..DataQualityPlan::default()
        }
    }

    fn run(df: &DataFrame, plan: &DataQualityPlan) -> DataQualityResults {
        measure(&df.clone().lazy(), Some(df.height()), plan)
    }

    fn affected(results: &DataQualityResults, kind: ObservationKind, column: &str) -> usize {
        results
            .observations
            .iter()
            .find(|observation| observation.kind == kind && observation.column == column)
            .map_or(0, |observation| observation.affected_rows)
    }

    /// Rows the evidence predicate picks out of `df`: the rows the finding counted.
    fn matching(
        df: &DataFrame,
        results: &DataQualityResults,
        kind: ObservationKind,
        column: &str,
    ) -> usize {
        let predicate = results
            .observations
            .iter()
            .find(|observation| observation.kind == kind && observation.column == column)
            .and_then(|observation| observation.evidence_predicate(results))
            .expect("a predicate");
        df.clone()
            .lazy()
            .filter(predicate)
            .collect()
            .unwrap()
            .height()
    }

    /// Every rule on a full read, counted exactly, its rows found by its predicate.
    #[test]
    fn a_full_read_counts_every_declared_rule() {
        let df = fixture();
        let results = run(&df, &plan(QualityCompute::Full));
        let intent = results.intent.as_ref().expect("intent measured");
        assert!(intent.measured);
        assert_eq!(intent.precision, QualityPrecision::Exact);
        let key = intent.key.as_ref().unwrap();
        assert_eq!(
            (key.missing, key.groups, key.extra_rows, key.rows_involved),
            (1, 1, 1, 2)
        );
        let status = intent.column("status").unwrap();
        assert_eq!(status.missing, Some(1));
        assert_eq!(status.values, 6);
        assert_eq!(status.outside, Some(2));
        let amount = intent.column("amount").unwrap();
        assert_eq!((amount.below, amount.above), (Some(1), Some(1)));
        assert_eq!(amount.lowest.as_deref(), Some("-1.0"));
        assert_eq!(amount.highest.as_deref(), Some("200.0"));
        let code = intent.column("code").unwrap();
        assert_eq!(code.unparsed, Some(1));
        // The range compares what reads as a number: six of the seven.
        assert_eq!((code.compared, code.below), (Some(6), Some(1)));
        // A full read keeps no rows, so it lists no examples.
        assert!(status.outside_examples.is_empty());

        for (kind, column, count) in [
            (ObservationKind::KeyRepeated, "id", 2),
            (ObservationKind::KeyMissing, "id", 1),
            (ObservationKind::RequiredMissing, "status", 1),
            (ObservationKind::NotAllowed, "status", 2),
            (ObservationKind::OutOfRange, "amount", 2),
            (ObservationKind::UnparsedNumber, "code", 1),
            (ObservationKind::OutOfRange, "code", 1),
        ] {
            assert_eq!(affected(&results, kind, column), count, "{kind:?} {column}");
            assert_eq!(
                matching(&df, &results, kind, column),
                count,
                "{kind:?} {column}"
            );
        }

        // Problems, stated as facts, and the check names its reach.
        let report = build_report(&results);
        for kind in [
            ObservationKind::KeyRepeated,
            ObservationKind::KeyMissing,
            ObservationKind::RequiredMissing,
            ObservationKind::NotAllowed,
            ObservationKind::OutOfRange,
            ObservationKind::UnparsedNumber,
        ] {
            let finding = report
                .findings
                .iter()
                .find(|finding| finding.kind == Some(kind))
                .unwrap_or_else(|| panic!("{kind:?}"));
            assert_eq!(finding.severity, crate::quality_report::Severity::Problem);
        }
        let all = checks(&results, &report);
        assert_eq!(all[0].name, crate::quality_report::INTENT_CHECK);
        assert_eq!(all[0].basis, QualityPrecision::Exact);
        assert!(matches!(all[0].outcome, Outcome::Found { .. }));
        let repeated = report
            .findings
            .iter()
            .find(|finding| finding.kind == Some(ObservationKind::KeyRepeated))
            .unwrap();
        let (headline, _) = describe(repeated, &results);
        assert_eq!(
            headline,
            "2 of 7 rows (28.6%) share their key with another row"
        );
        // A key with no repeat on a full read leaves no limit behind.
        assert!(
            !coverage(&results, &all, &plan(QualityCompute::Full))
                .limits()
                .iter()
                .any(|limit| limit.contains("key"))
        );
    }

    /// A sample counts what its rows show, says so, and keeps examples from them.
    #[test]
    fn a_sample_counts_its_rows_and_says_what_it_cannot() {
        let ids = (0..2_000i64).map(|row| row % 1_000).collect::<Vec<_>>();
        let status = (0..2_000)
            .map(|row| if row % 10 == 0 { "lost" } else { "open" })
            .collect::<Vec<_>>();
        let df = df!("id" => ids, "status" => status).unwrap();
        let plan = DataQualityPlan {
            dataset_rows: 400,
            intent: DeclaredIntent {
                key: vec!["id".to_string()],
                columns: vec![ColumnIntent {
                    allowed: vec!["open".to_string()],
                    ..ColumnIntent::new("status")
                }],
            },
            ..DataQualityPlan::default()
        };
        let results = run(&df, &plan);
        assert_eq!(results.precision, QualityPrecision::Sampled);
        let intent = results.intent.as_ref().unwrap();
        assert_eq!(intent.precision, QualityPrecision::Sampled);
        assert_eq!(intent.evaluated_rows, 400);
        let status = intent.column("status").unwrap();
        let outside = status.outside.unwrap();
        assert!(outside > 0 && outside < 400);
        // The examples come from the rows in memory, with their counts.
        assert_eq!(status.outside_examples, vec![("lost".to_string(), outside)]);
        // Every id repeats once in the data; the sample holds some of the pairs, and
        // a repeat among its distinct rows is one in the data.
        let key = intent.key.as_ref().unwrap();
        assert!(key.rows_involved <= 400);
        assert_eq!(key.rows_involved, key.groups * 2);

        let report = build_report(&results);
        let all = checks(&results, &report);
        assert_eq!(all[0].basis, QualityPrecision::Sampled);
        let limits = coverage(&results, &all, &plan).limits();
        assert!(
            limits.contains(&"key repeats among 400 sampled rows only".to_string()),
            "{limits:?}"
        );
        let not_allowed = report
            .findings
            .iter()
            .find(|finding| finding.kind == Some(ObservationKind::NotAllowed))
            .unwrap();
        let (_, evidence) = describe(not_allowed, &results);
        assert!(
            evidence
                .iter()
                .any(|line| line.starts_with("Found: \"lost\"")),
            "{evidence:?}"
        );
        if key.groups > 0 {
            let repeated = report
                .findings
                .iter()
                .find(|finding| finding.kind == Some(ObservationKind::KeyRepeated))
                .unwrap();
            let (headline, evidence) = describe(repeated, &results);
            assert!(headline.contains("of 400 sampled rows"), "{headline}");
            assert!(evidence.iter().any(|line| line.contains("not checked")));
        }
    }

    /// No repeat in a sample is not a unique key: the check passes on the sampled
    /// rows and the coverage says how far that reaches.
    #[test]
    fn a_sample_without_repeats_claims_only_its_rows() {
        let df = df!("id" => (0..5_000i64).collect::<Vec<_>>()).unwrap();
        let plan = DataQualityPlan {
            dataset_rows: 500,
            intent: DeclaredIntent {
                key: vec!["id".to_string()],
                columns: Vec::new(),
            },
            ..DataQualityPlan::default()
        };
        let results = run(&df, &plan);
        let report = build_report(&results);
        assert!(
            !report
                .findings
                .iter()
                .any(|finding| finding.kind == Some(ObservationKind::KeyRepeated))
        );
        let all = checks(&results, &report);
        assert_eq!(all[0].outcome, Outcome::Passed);
        assert_eq!(all[0].basis, QualityPrecision::Sampled);
        assert!(
            coverage(&results, &all, &plan)
                .limits()
                .contains(&"key repeats among 500 sampled rows only".to_string())
        );
    }

    /// Declaring a column the key answers what "Nearly unique" could only suggest.
    #[test]
    fn a_declared_key_replaces_the_nearly_unique_note() {
        let ids = (0..100i64)
            .map(|row| if row == 99 { 0 } else { row })
            .collect::<Vec<_>>();
        let df = df!("id" => ids).unwrap();
        let undeclared = run(
            &df,
            &DataQualityPlan {
                compute: QualityCompute::Full,
                ..DataQualityPlan::default()
            },
        );
        assert!(
            undeclared
                .observations
                .iter()
                .any(|o| o.kind == ObservationKind::KeyLike)
        );
        let declared = run(
            &df,
            &DataQualityPlan {
                compute: QualityCompute::Full,
                intent: DeclaredIntent {
                    key: vec!["id".to_string()],
                    columns: Vec::new(),
                },
                ..DataQualityPlan::default()
            },
        );
        assert!(
            !declared
                .observations
                .iter()
                .any(|o| o.kind == ObservationKind::KeyLike)
        );
        assert_eq!(affected(&declared, ObservationKind::KeyRepeated, "id"), 2);
    }

    /// A composite key repeats only where every part does.
    #[test]
    fn a_composite_key_repeats_where_all_its_parts_do() {
        let df = df!(
            "region" => &[Some("east"), Some("east"), Some("west"), Some("west"), Some("east"), Some("east"), None],
            "id" => &[Some(1i64), Some(2), Some(1), Some(1), None, None, Some(1)],
        )
        .unwrap();
        let results = run(
            &df,
            &DataQualityPlan {
                compute: QualityCompute::Full,
                intent: DeclaredIntent {
                    key: vec!["region".to_string(), "id".to_string()],
                    columns: Vec::new(),
                },
                ..DataQualityPlan::default()
            },
        );
        let key = results.intent.as_ref().unwrap().key.clone().unwrap();
        // Rows missing a part are incomplete, never a repeat of each other.
        assert_eq!((key.groups, key.rows_involved, key.missing), (1, 2, 3));
        assert_eq!(
            matching(&df, &results, ObservationKind::KeyRepeated, "region"),
            2
        );
        assert_eq!(
            matching(&df, &results, ObservationKind::KeyMissing, "id"),
            3
        );
        // One finding names both columns.
        let report = build_report(&results);
        let repeated = report
            .findings
            .iter()
            .find(|f| f.kind == Some(ObservationKind::KeyRepeated))
            .unwrap();
        assert_eq!(repeated.columns, vec!["region", "id"]);
    }

    /// Values not read, nothing checked: the check says so rather than passing.
    #[test]
    fn metadata_only_leaves_the_intent_unavailable() {
        let results = run(&fixture(), &plan(QualityCompute::Metadata));
        let intent = results.intent.as_ref().unwrap();
        assert!(!intent.measured);
        let report = build_report(&results);
        let all = checks(&results, &report);
        assert_eq!(all[0].outcome, Outcome::Unavailable("values not read"));
        assert!(intent.observations().is_empty());
    }

    /// Dates and times compare as instants, text read as time through its format.
    #[test]
    fn ranges_compare_dates_and_text_read_as_time() {
        let df = df!(
            "day" => &["2024-01-01", "2024-02-15", "2023-12-31", "bad"],
        )
        .unwrap();
        let mut plan = DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: Vec::new(),
                columns: vec![ColumnIntent {
                    min: Some("2024-01-01".to_string()),
                    max: Some("2024-01-31".to_string()),
                    ..ColumnIntent::new("day")
                }],
            },
            ..DataQualityPlan::default()
        };
        crate::analysis_modal::set_time_format(
            &mut plan,
            "day",
            Some((TimeKind::Date, "%Y-%m-%d")),
        );
        let results = run(&df, &plan);
        let day = results
            .intent
            .as_ref()
            .unwrap()
            .column("day")
            .unwrap()
            .clone();
        assert_eq!(
            (day.compared, day.below, day.above),
            (Some(3), Some(1), Some(1))
        );
        assert_eq!(day.lowest.as_deref(), Some("2023-12-31"));
        assert_eq!(
            matching(&df, &results, ObservationKind::OutOfRange, "day"),
            2
        );
    }

    /// A date or datetime past the calendar is the furthest out of range a value can
    /// be, and counts so, where its conversion to microseconds overflowed and the
    /// rule never compared it (#518). Values in range count as they did.
    #[test]
    fn a_date_past_the_calendar_is_out_of_range() {
        const DAY_MS: i64 = 86_400_000;
        let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
        let df = df!(
            "d" => &[Some(19_737i32), Some(19_000), Some(i32::MAX), Some(i32::MIN), None],
            "ms" => &[
                Some(19_737 * DAY_MS),
                Some(19_000 * DAY_MS),
                Some(i64::MAX),
                Some(i64::MIN + 1),
                None,
            ],
            "us" => &[
                Some(19_737 * DAY_MS * 1000),
                Some(19_000 * DAY_MS * 1000),
                Some(i64::MAX),
                Some(i64::MIN + 1),
                None,
            ],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("d").cast(DataType::Date),
            col("ms").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
            col("us").cast(DataType::Datetime(TimeUnit::Microseconds, paris)),
        ])
        .collect()
        .unwrap();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: Vec::new(),
                columns: ["d", "ms", "us"]
                    .into_iter()
                    .map(|column| ColumnIntent {
                        min: Some("2024-01-01".to_string()),
                        max: Some("2024-12-31".to_string()),
                        ..ColumnIntent::new(column)
                    })
                    .collect(),
            },
            ..DataQualityPlan::default()
        };
        for streaming in [false, true] {
            let results = compute_data_quality(
                &df.clone().lazy(),
                Some(df.height()),
                &plan,
                None,
                streaming,
            )
            .unwrap();
            for (column, unit) in [("d", "days"), ("ms", "ms"), ("us", "us")] {
                let check = results.intent.as_ref().unwrap().column(column).unwrap();
                // 2024-01-15 in range; 2022-01-08 and the two past the calendar out.
                assert_eq!(
                    (check.compared, check.below, check.above),
                    (Some(4), Some(2), Some(1)),
                    "{column}"
                );
                let (low, high) = match unit {
                    "days" => (i64::from(i32::MIN), i64::from(i32::MAX)),
                    _ => (i64::MIN + 1, i64::MAX),
                };
                let since = |v: i64| match unit {
                    "days" => format!("{v} days since 1970-01-01"),
                    unit => format!("{v} {unit} since 1970-01-01 UTC"),
                };
                assert_eq!(check.lowest, Some(since(low)), "{column}");
                assert_eq!(check.highest, Some(since(high)), "{column}");
                assert_eq!(
                    matching(&df, &results, ObservationKind::OutOfRange, column),
                    3,
                    "{column}"
                );
            }
        }
    }

    #[test]
    fn allowed_values_are_split_trimmed_and_checked_against_the_type() {
        assert_eq!(
            parse_allowed(&DataType::String, " open, closed ,,open ").unwrap(),
            vec!["open", "closed"]
        );
        assert!(parse_allowed(&DataType::Int64, "1, two").is_err());
        assert_eq!(
            parse_allowed(&DataType::Boolean, "true").unwrap(),
            vec!["true"]
        );
        let many = (0..=MAX_ALLOWED_VALUES)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse_allowed(&DataType::String, &many).is_err());
    }

    /// An allowed set compares at the column's own type: a `u64` past `i64::MAX` is
    /// itself, and a category is compared with its name.
    #[test]
    fn an_allowed_set_compares_at_the_columns_type() {
        let mut df = df!(
            "code" => &[u64::MAX, 1, u64::MAX - 1, 2],
            "kind" => &["open", "closed", "open", "lost"],
        )
        .unwrap();
        df = df
            .lazy()
            .with_column(col("kind").cast(DataType::from_categories(Categories::global())))
            .collect()
            .unwrap();
        assert!(parse_allowed(&DataType::UInt64, &u64::MAX.to_string()).is_ok());
        assert!(parse_allowed(&DataType::UInt64, "-1").is_err());
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: Vec::new(),
                columns: vec![
                    ColumnIntent {
                        allowed: vec![u64::MAX.to_string(), "1".into()],
                        ..ColumnIntent::new("code")
                    },
                    ColumnIntent {
                        allowed: vec!["open".into(), "closed".into()],
                        ..ColumnIntent::new("kind")
                    },
                ],
            },
            ..DataQualityPlan::default()
        };
        let results = run(&df, &plan);
        let intent = results.intent.as_ref().unwrap();
        assert_eq!(intent.column("code").unwrap().outside, Some(2));
        assert_eq!(intent.column("kind").unwrap().outside, Some(1));
        assert_eq!(
            matching(&df, &results, ObservationKind::NotAllowed, "code"),
            2
        );
        assert_eq!(
            matching(&df, &results, ObservationKind::NotAllowed, "kind"),
            1
        );
    }

    /// A quoted value keeps its commas, spaces and doubled quotes, and is written
    /// back quoted, so reopening the form reads the same set.
    #[test]
    fn a_quoted_allowed_value_holds_commas_and_spaces() {
        let typed = r#""a, b", c, " open", "say ""hi""", 5" pipe"#;
        let values = parse_allowed(&DataType::String, typed).unwrap();
        assert_eq!(values, vec!["a, b", "c", " open", "say \"hi\"", "5\" pipe"]);
        assert_eq!(
            parse_allowed(&DataType::String, &format_allowed(&values)).unwrap(),
            values
        );
        assert_eq!(
            parse_allowed(&DataType::Int64, r#""1", 2"#).unwrap(),
            vec!["1", "2"]
        );
        assert!(parse_allowed(&DataType::String, r#""a, b"#).is_err());
        assert!(parse_allowed(&DataType::String, r#""a" b, c"#).is_err());

        // Compared as stored: case and spaces count.
        let df = df!("label" => &["a, b", "c", " open", "open", "A, B"]).unwrap();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: Vec::new(),
                columns: vec![ColumnIntent {
                    allowed: values,
                    ..ColumnIntent::new("label")
                }],
            },
            ..DataQualityPlan::default()
        };
        let results = run(&df, &plan);
        let label = results.intent.as_ref().unwrap().column("label").unwrap();
        assert_eq!(label.outside, Some(2));
        assert_eq!(
            matching(&df, &results, ObservationKind::NotAllowed, "label"),
            2
        );
    }

    /// A date alone as a time's maximum keeps that whole day; as its minimum, the day
    /// starts at midnight. Both bounds are in range.
    #[test]
    fn a_date_bounds_a_datetime_column_by_whole_days() {
        let at = |text: &str| {
            chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_utc()
                .timestamp_micros()
        };
        let df = df!(
            "at" => &[
                at("2024-06-01 00:00:00"),
                at("2024-06-30 23:59:59"),
                at("2024-07-01 00:00:00"),
                at("2024-05-31 23:59:59"),
            ],
        )
        .unwrap()
        .lazy()
        .with_column(col("at").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
        .collect()
        .unwrap();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: Vec::new(),
                columns: vec![ColumnIntent {
                    min: Some("2024-06-01".to_string()),
                    max: Some("2024-06-30".to_string()),
                    ..ColumnIntent::new("at")
                }],
            },
            ..DataQualityPlan::default()
        };
        let results = run(&df, &plan);
        let check = results.intent.as_ref().unwrap().column("at").unwrap();
        assert_eq!((check.below, check.above), (Some(1), Some(1)));
        // The same day as both ends is a day, not an empty range.
        let one_day = ColumnIntent {
            min: Some("2024-06-30".to_string()),
            max: Some("2024-06-30".to_string()),
            ..ColumnIntent::new("at")
        };
        assert!(
            check_intent(
                &one_day,
                &DataType::Datetime(TimeUnit::Microseconds, None),
                None
            )
            .is_ok()
        );
    }

    #[test]
    fn a_range_needs_bounds_of_the_columns_kind_in_order() {
        let number = ColumnIntent {
            min: Some("10".to_string()),
            max: Some("2".to_string()),
            ..ColumnIntent::new("amount")
        };
        assert_eq!(
            check_intent(&number, &DataType::Float64, None),
            Err("Minimum is above maximum".to_string())
        );
        let date = ColumnIntent {
            min: Some("yesterday".to_string()),
            ..ColumnIntent::new("day")
        };
        assert!(check_intent(&date, &DataType::Date, None).is_err());
        let text = ColumnIntent {
            min: Some("a".to_string()),
            ..ColumnIntent::new("name")
        };
        assert!(check_intent(&text, &DataType::String, None).is_err());
        // Text read as a number takes a numeric range.
        let parsed = ColumnIntent {
            number: Some(NumberReading::Decimal),
            min: Some("0".to_string()),
            ..ColumnIntent::new("price")
        };
        assert_eq!(check_intent(&parsed, &DataType::String, None), Ok(()));
    }

    #[test]
    fn an_empty_intent_removes_the_column() {
        let mut declared = DeclaredIntent::default();
        declared.set(ColumnIntent {
            required: true,
            ..ColumnIntent::new("id")
        });
        assert_eq!(declared.columns.len(), 1);
        declared.set(ColumnIntent::new("id"));
        assert!(declared.is_empty());
        declared.set_key("id", true);
        declared.set_key("id", true);
        assert_eq!(declared.key, vec!["id"]);
        declared.set_key("id", false);
        assert!(declared.is_empty());
    }
}
