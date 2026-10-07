//! Trends and gaps: a report's segments in order, pooled into the bars a terminal
//! has room for, and the time windows a study said to expect checked against the
//! segments a run counted. Built from the measurements a run kept: nothing here
//! reads.

use crate::analysis::data_quality::{
    DataQualityPlan, DataQualityResults, QualityComparison, QualityGrain, QualityMetric,
    QualityPrecision, QualityScope, SegmentQualityProfile, beyond_noise, parse_scope_time,
    segment_cmp,
};
use crate::analysis::quality_report::THIN_SEGMENT_ROWS;
use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, Timelike, Weekday};
use std::collections::HashMap;
use std::ops::Range;

/// One segment in the order Trends draws them: one a run profiled, or one it counted
/// rows in and sampled none of.
#[derive(Debug, Clone, Copy)]
pub struct TrendSlot<'a> {
    pub label: &'a str,
    /// Rows the scope holds in it, when counted.
    pub total: Option<usize>,
    /// Rows the run measured in it.
    pub evaluated: usize,
    pub profile: Option<&'a SegmentQualityProfile>,
}

/// Every segment in order, the ones the sample missed among the ones it drew.
pub fn trend_slots(results: &DataQualityResults) -> Vec<TrendSlot<'_>> {
    let mut slots = results
        .segments
        .iter()
        .map(|segment| TrendSlot {
            label: &segment.label,
            total: segment.total_rows,
            evaluated: segment.evaluated_rows,
            profile: Some(segment),
        })
        .chain(results.unsampled_segments.iter().map(|segment| TrendSlot {
            label: &segment.label,
            total: Some(segment.total_rows),
            evaluated: 0,
            profile: None,
        }))
        .collect::<Vec<_>>();
    // Stable: segments keep the order the run gave them, and each missed one goes
    // where its label falls among them.
    slots.sort_by(|left, right| segment_cmp(left.label, right.label));
    slots
}

/// What a Trends line measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrendMeasure {
    /// The rows each segment holds, by exact count.
    Rows,
    /// The rows a run measured in each segment: the sample's reach.
    SampledRows,
    /// A column's measure, by the column's place in a segment's profile.
    Column(usize),
}

/// One line of the Trends table: the rows each segment holds, the rows the sample
/// drew, or a column's measure, pooled into bars of consecutive segments.
#[derive(Debug, Clone, PartialEq)]
pub struct TrendRow {
    /// The columns whose lines are the same line, as columns missing together are.
    pub names: Vec<String>,
    pub measure: TrendMeasure,
    /// Each bar's value: rows per segment on a rows line, a rate otherwise. `None`
    /// where the bar holds nothing the measure applies to.
    pub bars: Vec<Option<f64>>,
    /// What each bar's value is taken from: rows and segments on a rows line, the
    /// rows a rate counts and the rows it is out of otherwise.
    pub parts: Vec<(f64, f64)>,
    pub low: f64,
    pub high: f64,
}

impl TrendRow {
    /// Whether this is a rows line, counted rather than a rate.
    pub fn rows(&self) -> bool {
        !matches!(self.measure, TrendMeasure::Column(_))
    }
}

/// A column's count and what it is out of in one segment, when it has a value there.
type Cell = Option<(f64, f64)>;

/// One bar: the consecutive segments it pools, and how much of them the run saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrendBar {
    /// Its segments, as indices into [`TrendView::slots`].
    pub slots: Range<usize>,
    /// Segments the scope has rows in and the sample drew none of.
    pub unsampled: usize,
    /// Segments the sample drew fewer than [`THIN_SEGMENT_ROWS`] rows of. Only on a
    /// sample: an exact count is never thin.
    pub thin: usize,
    pub evaluated: usize,
    /// Rows its segments hold, when every one was counted.
    pub eligible: Option<usize>,
}

impl TrendBar {
    pub fn segments(&self) -> usize {
        self.slots.len()
    }
}

/// The Trends table for a measure, as wide as it is drawn.
#[derive(Debug, Clone)]
pub struct TrendView<'a> {
    pub slots: Vec<TrendSlot<'a>>,
    pub lines: Vec<TrendRow>,
    pub bars: Vec<TrendBar>,
    /// How many segments a bar holds; the last may hold fewer.
    pub per_bar: usize,
    /// The numbers are a sample's.
    pub sampled: bool,
}

impl TrendView<'_> {
    /// Segments with rows the sample drew none of, and segments it drew few of.
    pub fn coverage(&self) -> (usize, usize) {
        self.bars.iter().fold((0, 0), |(unsampled, thin), bar| {
            (unsampled + bar.unsampled, thin + bar.thin)
        })
    }
}

/// Whether the segments are a sample's, then the segments with rows the sample drew
/// none of and the segments it drew few of: what [`TrendView::coverage`] counts,
/// without building the view.
pub fn segment_coverage(results: &DataQualityResults) -> (bool, usize, usize) {
    let sampled = matches!(results.precision, QualityPrecision::Sampled);
    let thin = if sampled {
        results
            .segments
            .iter()
            .filter(|segment| segment.evaluated_rows < THIN_SEGMENT_ROWS)
            .count()
    } else {
        0
    };
    (sampled, results.unsampled_segments.len(), thin)
}

/// The Trends table for `metric`, `bars` wide: rows per segment first (the exact
/// count, then on a sample the rows it drew), then every column the measure is above
/// zero in somewhere, the one that moves most first. Each bar pools consecutive
/// segments (their counts over their rows), so a daily grain over years reads as
/// years, and a thin day's sample does not make a bar alone. A segment the sample
/// missed is a slot like any other, so a bar of them says so rather than vanish.
pub fn trend_view(
    results: &DataQualityResults,
    metric: QualityMetric,
    bars: usize,
) -> TrendView<'_> {
    let slots = trend_slots(results);
    let sampled = matches!(results.precision, QualityPrecision::Sampled);
    if slots.is_empty() || bars == 0 {
        return TrendView {
            slots,
            lines: Vec::new(),
            bars: Vec::new(),
            per_bar: 1,
            sampled,
        };
    }
    let per_bar = slots.len().div_ceil(bars);
    let ranges = (0..slots.len())
        .step_by(per_bar)
        .map(|start| start..(start + per_bar).min(slots.len()))
        .collect::<Vec<_>>();
    let pooled = ranges
        .iter()
        .map(|range| {
            let slots = &slots[range.clone()];
            TrendBar {
                slots: range.clone(),
                unsampled: slots.iter().filter(|slot| slot.profile.is_none()).count(),
                thin: if sampled {
                    slots
                        .iter()
                        .filter(|slot| slot.profile.is_some() && slot.evaluated < THIN_SEGMENT_ROWS)
                        .count()
                } else {
                    0
                },
                evaluated: slots.iter().map(|slot| slot.evaluated).sum(),
                eligible: slots.iter().map(|slot| slot.total).sum(),
            }
        })
        .collect::<Vec<_>>();
    let summarize = |names: Vec<String>, measure: TrendMeasure, parts: Vec<(f64, f64)>| {
        let bars = parts
            .iter()
            .map(|(part, whole)| (*whole > 0.0).then(|| part / whole))
            .collect::<Vec<_>>();
        let known = bars.iter().flatten().copied();
        let low = known.clone().fold(f64::INFINITY, f64::min);
        let high = known.fold(0.0, f64::max);
        TrendRow {
            names,
            measure,
            bars,
            parts,
            low: if low.is_finite() { low } else { 0.0 },
            high,
        }
    };
    let rows_line = |name: &str, measure: TrendMeasure, rows: &dyn Fn(&TrendSlot<'_>) -> usize| {
        summarize(
            vec![name.to_string()],
            measure,
            ranges
                .iter()
                .map(|range| {
                    let total = slots[range.clone()].iter().map(rows).sum::<usize>();
                    (total as f64, range.len() as f64)
                })
                .collect(),
        )
    };
    // Exact rows where every segment's count is known; the rows a sample drew beside
    // them, which is where its reach thins out.
    let counted = slots.iter().all(|slot| slot.total.is_some());
    let mut lines = Vec::new();
    if counted {
        lines.push(rows_line("rows", TrendMeasure::Rows, &|slot| {
            slot.total.unwrap_or(0)
        }));
    }
    if sampled || !counted {
        lines.push(rows_line(
            "sampled rows",
            TrendMeasure::SampledRows,
            &|slot| slot.evaluated,
        ));
    }
    let Some(first) = results.segments.first() else {
        return TrendView {
            slots,
            lines,
            bars: pooled,
            per_bar,
            sampled,
        };
    };
    // Each column's count and denominator in each segment, read once.
    let cell = |slot: &TrendSlot<'_>, index: usize| -> Cell {
        let column = slot.profile?.columns.get(index)?;
        let value = metric.value(column)?;
        let rows = metric.denominator(column) as f64;
        Some((value * rows, rows))
    };
    let mut columns: Vec<(Vec<Cell>, TrendRow)> = Vec::new();
    for (index, profile) in first.columns.iter().enumerate() {
        let cells = slots
            .iter()
            .map(|slot| cell(slot, index))
            .collect::<Vec<_>>();
        let row = summarize(
            vec![profile.name.clone()],
            TrendMeasure::Column(index),
            pool(&cells, &ranges),
        );
        if row.high == 0.0 {
            continue;
        }
        // Columns that go missing together, segment by segment, draw the same line:
        // draw it once. Judged per segment, so how many bars fit changes nothing.
        match columns.iter_mut().find(|(other, _)| *other == cells) {
            Some((_, other)) => other.names.push(profile.name.clone()),
            None => columns.push((cells, row)),
        }
    }
    // The column that moves most first, judged on a fixed pooling rather than the
    // bars that fit: a line keeps its place at any width, so the line a bar detail
    // opened is the line it shows.
    let canonical = (0..slots.len())
        .step_by(slots.len().div_ceil(ORDER_BARS))
        .map(|start| start..(start + slots.len().div_ceil(ORDER_BARS)).min(slots.len()))
        .collect::<Vec<_>>();
    let spread = |cells: &[Cell]| {
        let known = pool(cells, &canonical)
            .into_iter()
            .filter(|(_, whole)| *whole > 0.0)
            .map(|(part, whole)| part / whole)
            .collect::<Vec<_>>();
        let high = known.iter().copied().fold(0.0, f64::max);
        let low = known.iter().copied().fold(high, f64::min);
        (high - low, high)
    };
    let mut columns = columns
        .into_iter()
        .map(|(cells, row)| (spread(&cells), row))
        .collect::<Vec<_>>();
    columns.sort_by(
        |((left_spread, left_high), _), ((right_spread, right_high), _)| {
            right_spread
                .total_cmp(left_spread)
                .then_with(|| right_high.total_cmp(left_high))
        },
    );
    lines.extend(columns.into_iter().map(|(_, row)| row));
    TrendView {
        slots,
        lines,
        bars: pooled,
        per_bar,
        sampled,
    }
}

/// How many bars the order of Trends lines is judged on, whatever the width.
const ORDER_BARS: usize = 32;

/// `cells` summed within each of `ranges`: a count over what it is out of.
fn pool(cells: &[Cell], ranges: &[Range<usize>]) -> Vec<(f64, f64)> {
    ranges
        .iter()
        .map(|range| {
            cells[range.clone()]
                .iter()
                .flatten()
                .fold((0.0, 0.0), |(part, whole), (p, w)| (part + p, whole + w))
        })
        .collect()
}

/// The 95% Wilson score interval for `count` of `n`: where the whole's rate likely
/// sits, given a simple random sample. Unlike the normal interval it stays inside 0 to
/// 1 and does not shrink to nothing at a count of zero, which a thin sample often has.
pub fn wilson_interval(count: f64, n: f64) -> Option<(f64, f64)> {
    if n <= 0.0 {
        return None;
    }
    const Z: f64 = 1.96;
    let p = (count / n).clamp(0.0, 1.0);
    let z2 = Z * Z;
    let centre = (p + z2 / (2.0 * n)) / (1.0 + z2 / n);
    let half = Z * (p * (1.0 - p) / n + z2 / (4.0 * n * n)).sqrt() / (1.0 + z2 / n);
    Some(((centre - half).max(0.0), (centre + half).min(1.0)))
}

/// The bar a bar is compared with: the one holding the baseline segment when the
/// plan compares with one, and otherwise the bar before.
pub fn compared_bar(view: &TrendView<'_>, bar: usize, plan: &DataQualityPlan) -> Option<usize> {
    let other = if plan.comparison == QualityComparison::Baseline {
        let slot = match plan.baseline_segment.as_deref() {
            Some(label) => view.slots.iter().position(|slot| slot.label == label)?,
            None => 0,
        };
        view.bars
            .iter()
            .position(|candidate| candidate.slots.contains(&slot))?
    } else {
        bar.checked_sub(1)?
    };
    (other != bar).then_some(other)
}

/// How a bar's rate stands against another's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarChange {
    pub before: f64,
    pub now: f64,
    /// A point or more, and past sampling noise on a sample.
    pub clear: bool,
}

impl BarChange {
    pub fn points(&self) -> f64 {
        (self.now - self.before) * 100.0
    }
}

/// `line`'s value in `bar` beside its value in `other`, judged as Segments judges a
/// change: a point or more, past sampling noise unless the counts are exact. A rows
/// line moves by its count, so only rates are judged.
pub fn bar_change(line: &TrendRow, bar: usize, other: usize, exact: bool) -> Option<BarChange> {
    let now = (*line.bars.get(bar)?)?;
    let before = (*line.bars.get(other)?)?;
    let clear = !line.rows()
        && (now - before).abs() * 100.0 >= crate::analysis::data_quality::MATERIAL_CHANGE_PP
        && (exact
            || beyond_noise(
                now,
                line.parts[bar].1 as usize,
                before,
                line.parts[other].1 as usize,
            ));
    Some(BarChange { before, now, clear })
}

/// Where window `label` starts, read back from the label a run gave it: the inverse
/// of `time_window_label`. `None` for the rows with no time, and for a label that
/// is not a window's.
pub fn window_start(label: &str, every: &str) -> Option<NaiveDateTime> {
    let date = |text: &str| NaiveDate::parse_from_str(text, "%Y-%m-%d").ok();
    match every {
        "1h" => NaiveDateTime::parse_from_str(label, "%Y-%m-%d %H:%M").ok(),
        "1d" => date(label)?.and_hms_opt(0, 0, 0),
        "1w" => date(label.strip_prefix("week of ")?)?.and_hms_opt(0, 0, 0),
        "1mo" => date(&format!("{label}-01"))?.and_hms_opt(0, 0, 0),
        _ => None,
    }
}

/// The start of the window after the one starting at `start`.
pub fn next_window(start: NaiveDateTime, every: &str) -> Option<NaiveDateTime> {
    match every {
        "1h" => start.checked_add_signed(Duration::hours(1)),
        "1d" => start.checked_add_signed(Duration::days(1)),
        "1w" => start.checked_add_signed(Duration::weeks(1)),
        "1mo" => start.checked_add_months(Months::new(1)),
        _ => None,
    }
}

/// The start of the window `time` falls in, cut as a run cuts them: hours on the
/// hour, days at midnight, weeks from Monday, months from the first.
pub fn floor_window(time: NaiveDateTime, every: &str) -> NaiveDateTime {
    let midnight = |date: NaiveDate| date.and_hms_opt(0, 0, 0).unwrap_or(time);
    match every {
        "1h" => time.date().and_hms_opt(time.hour(), 0, 0).unwrap_or(time),
        "1w" => {
            midnight(time.date() - Duration::days(i64::from(time.weekday().num_days_from_monday())))
        }
        "1mo" => midnight(time.date().with_day(1).unwrap_or(time.date())),
        _ => midnight(time.date()),
    }
}

/// A span of windows in calendar terms, inclusive: `2024-01-01 to 2024-01-28` for
/// days, weeks and months, to the minute for hours.
pub fn calendar_span(first: NaiveDateTime, last: NaiveDateTime, every: &str) -> String {
    let end = next_window(last, every)
        .and_then(|end| end.checked_sub_signed(Duration::minutes(1)))
        .unwrap_or(last);
    let text = |time: NaiveDateTime| {
        if every == "1h" {
            time.format("%Y-%m-%d %H:%M").to_string()
        } else {
            time.format("%Y-%m-%d").to_string()
        }
    };
    let (first, end) = (text(first), text(end));
    if first == end {
        first
    } else {
        format!("{first} to {end}")
    }
}

/// What a bar spans: the calendar range of its windows on a time grain, and its
/// first and last segment otherwise. Rows with no time, which sort last, are said
/// apart from the calendar.
pub fn bar_span(view: &TrendView<'_>, bar: &TrendBar, grain: &QualityGrain) -> String {
    let slots = &view.slots[bar.slots.clone()];
    let (Some(first), Some(last)) = (slots.first(), slots.last()) else {
        return String::new();
    };
    if let QualityGrain::TimeWindows { every, .. } = grain {
        let starts = slots
            .iter()
            .filter_map(|slot| window_start(slot.label, every))
            .collect::<Vec<_>>();
        let undated = slots.len() - starts.len();
        let calendar = match (starts.first(), starts.last()) {
            (Some(first), Some(last)) => calendar_span(*first, *last, every),
            _ => String::new(),
        };
        return match (calendar.is_empty(), undated) {
            (_, 0) => calendar,
            (true, _) => "rows with no time".to_string(),
            (false, _) => format!("{calendar}, and rows with no time"),
        };
    }
    if first.label == last.label {
        first.label.to_string()
    } else {
        format!("{} to {}", first.label, last.label)
    }
}

/// The most windows a stated range is checked over. A daily grain over half a century,
/// or an hourly one over two years; past it, a coarser grain or a shorter range.
pub const MAX_EXPECTED_WINDOWS: usize = 20_000;

/// The most runs of gap windows listed; the rest are counted.
pub const MAX_GAP_RUNS: usize = 500;

/// Why an expected window has no rows to show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapKind {
    /// The scope holds no rows in it, by exact count.
    Empty,
    /// The scope holds rows in it, and the sample drew none.
    Unsampled,
    /// It lies outside the time range the scope reads, wholly or in part: the run
    /// never looked.
    OutOfScope,
}

impl GapKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Unsampled => "not sampled",
            Self::OutOfScope => "out of scope",
        }
    }
}

/// Consecutive expected windows that share a kind of gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapRun {
    pub kind: GapKind,
    /// Where its first and last windows start.
    pub first: NaiveDateTime,
    pub last: NaiveDateTime,
    pub windows: usize,
    /// Rows the scope holds in it, for windows the sample missed, when counted.
    pub rows: Option<usize>,
}

/// The stated windows, checked against the segments a run counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapCheck {
    pub every: String,
    pub column: String,
    /// The first expected window's start, and where the last one ends.
    pub from: NaiveDateTime,
    pub before: NaiveDateTime,
    /// Windows expected in the range.
    pub expected: usize,
    /// Weekend windows in the range, not expected.
    pub weekend: usize,
    pub with_rows: usize,
    pub empty: usize,
    pub unsampled: usize,
    pub out_of_scope: usize,
    /// Whether a window with no rows found is known to be empty: every row was read,
    /// or every window counted. Otherwise a window the sample missed is not sampled,
    /// whether or not it has rows.
    pub counted: bool,
    pub runs: Vec<GapRun>,
    /// Runs past [`MAX_GAP_RUNS`], counted and not listed.
    pub more_runs: usize,
}

impl GapCheck {
    pub fn gaps(&self) -> usize {
        self.empty + self.unsampled + self.out_of_scope
    }
}

/// What checking the stated windows came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Gaps {
    /// A report of file metadata counted no windows.
    NoValues,
    /// No range was stated and the run found no window to take one from.
    NoWindows,
    /// The range holds more windows than are checked.
    TooMany {
        windows: usize,
    },
    Checked(GapCheck),
}

/// The expected windows of `plan` checked against `results`, or `None` when the plan
/// states none. Windows with rows are found among the segments; one the sample
/// missed is told from one with no rows by the exact count the run took, and one
/// outside the time range the scope reads is out of scope, never empty.
pub fn expected_gaps(plan: &DataQualityPlan, results: &DataQualityResults) -> Option<Gaps> {
    let expected = plan.expected_windows()?;
    let QualityGrain::TimeWindows { column, every } = &plan.grain else {
        return None;
    };
    if results.precision == QualityPrecision::Metadata {
        return Some(Gaps::NoValues);
    }
    let slots = trend_slots(results);
    let mut found = HashMap::new();
    for slot in &slots {
        if let Some(start) = window_start(slot.label, every) {
            found.insert(start, (slot.evaluated, slot.total));
        }
    }
    let (from, before) = expected.bounds();
    let to_time =
        |micros: i64| chrono::DateTime::from_timestamp_micros(micros).map(|at| at.naive_utc());
    let from = match from.and_then(to_time) {
        Some(from) => floor_window(from, every),
        None => match found.keys().min() {
            Some(first) => *first,
            None => return Some(Gaps::NoWindows),
        },
    };
    let before = match before.and_then(to_time) {
        Some(before) => before,
        None => match found
            .keys()
            .max()
            .and_then(|last| next_window(*last, every))
        {
            Some(end) => end,
            None => return Some(Gaps::NoWindows),
        },
    };
    let windows = window_count(from, before, every);
    if windows > MAX_EXPECTED_WINDOWS {
        return Some(Gaps::TooMany { windows });
    }
    // Only a range on the grain's own column says which windows the scope left out.
    let scope = match &plan.scope {
        QualityScope::SourceTimeRange {
            column: scoped,
            start,
            end,
        } if scoped == column => parse_scope_time(start)
            .and_then(to_time)
            .zip(parse_scope_time(end).and_then(to_time)),
        _ => None,
    };
    let counted = results.precision == QualityPrecision::Exact
        || slots.iter().all(|slot| slot.total.is_some());
    let mut check = GapCheck {
        every: every.clone(),
        column: column.clone(),
        from,
        before,
        expected: 0,
        weekend: 0,
        with_rows: 0,
        empty: 0,
        unsampled: 0,
        out_of_scope: 0,
        counted,
        runs: Vec::new(),
        more_runs: 0,
    };
    let weekdays =
        expected.weekdays && crate::analysis::data_quality::ExpectedWindows::weekdays_apply(every);
    let mut start = from;
    let mut open: Option<GapRun> = None;
    while start < before {
        let Some(end) = next_window(start, every) else {
            break;
        };
        if weekdays && matches!(start.weekday(), Weekday::Sat | Weekday::Sun) {
            check.weekend += 1;
            start = end;
            continue;
        }
        check.expected += 1;
        // Rows found settle it, even in a window the scope cuts through; only a window
        // with none is out of scope for lying outside it.
        let gap = match found.get(&start) {
            Some((evaluated, _)) if *evaluated > 0 => None,
            Some((_, total)) => Some((GapKind::Unsampled, *total)),
            None if scope.is_some_and(|(first, last)| start < first || end > last) => {
                Some((GapKind::OutOfScope, None))
            }
            None if counted => Some((GapKind::Empty, None)),
            None => Some((GapKind::Unsampled, None)),
        };
        match gap {
            None => {
                check.with_rows += 1;
                close_run(&mut check, open.take());
            }
            Some((kind, rows)) => {
                match kind {
                    GapKind::Empty => check.empty += 1,
                    GapKind::Unsampled => check.unsampled += 1,
                    GapKind::OutOfScope => check.out_of_scope += 1,
                }
                match open.as_mut() {
                    Some(run) if run.kind == kind => {
                        run.last = start;
                        run.windows += 1;
                        run.rows = run.rows.zip(rows).map(|(a, b)| a + b);
                    }
                    _ => {
                        close_run(&mut check, open.take());
                        open = Some(GapRun {
                            kind,
                            first: start,
                            last: start,
                            windows: 1,
                            rows,
                        });
                    }
                }
            }
        }
        start = end;
    }
    close_run(&mut check, open);
    Some(Gaps::Checked(check))
}

fn close_run(check: &mut GapCheck, run: Option<GapRun>) {
    let Some(run) = run else {
        return;
    };
    if check.runs.len() < MAX_GAP_RUNS {
        check.runs.push(run);
    } else {
        check.more_runs += 1;
    }
}

/// How many windows of `every` start in `[from, before)`, `from` on a window start.
fn window_count(from: NaiveDateTime, before: NaiveDateTime, every: &str) -> usize {
    if before <= from {
        return 0;
    }
    let span = before - from;
    let per = |unit: Duration| {
        let (span, unit) = (span.num_seconds(), unit.num_seconds().max(1));
        usize::try_from((span + unit - 1) / unit).unwrap_or(usize::MAX)
    };
    match every {
        "1h" => per(Duration::hours(1)),
        "1w" => per(Duration::weeks(1)),
        "1mo" => {
            let months =
                |time: NaiveDateTime| i64::from(time.year()) * 12 + i64::from(time.month0());
            let whole = months(before) - months(from);
            let past = before > floor_window(before, "1mo");
            usize::try_from(whole + i64::from(past)).unwrap_or(usize::MAX)
        }
        _ => per(Duration::days(1)),
    }
}

#[cfg(test)]
mod tests;
