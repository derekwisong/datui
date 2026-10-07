//! Declared column intent for Data Quality: what a column must hold, said by the
//! user, measured on the rows a run reads anyway.
//!
//! The profile can say a column is nearly unique; only a declaration can say it is a
//! key, so that a repeat is a defect and not a category. Every rule is opt-in, and a
//! violation is counted as a fact out of the rows or values checked, never folded
//! into a score.

use crate::analysis::data_quality::{
    DataQualityPlan, ObservationKind, QualityObservation, QualityPrecision, TimeInterpretation,
    TimeKind,
};
use crate::analysis::statistics::collect_lazy;
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

/// A date or ms datetime clamped to what epoch microseconds can count, so converting
/// cannot overflow (which nulled dates past the calendar). Clamped values still lie
/// beyond any bound the calendar allows. Others unchanged; a batch without such values
/// costs a min and a max.
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
mod tests;
