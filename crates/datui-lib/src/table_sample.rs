//! The sample as a step of the view: drawn into memory a chunk at a time, while the
//! table shows the rows kept so far.
//!
//! [`draw`] reads the rows [`crate::sampling`] would, but hands each chunk that is
//! final when kept to [`SampleRows`] as it lands: a seeded run of one Parquet or IPC
//! file, a batch of the head or of every row, and a batch's share of a Bernoulli
//! sample when the total is known. A reservoir (an unknown total) and an equal per
//! value sample keep evicting rows until the end, so their rows arrive as one chunk
//! when the read ends. Rows land in arrival order; [`SampleRows::in_source_order`]
//! puts them in the order the source holds them once the draw ends.
//!
//! Nothing is written to disk: saving a sample is export's job.

use std::sync::{Arc, Mutex};

use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::prelude::*;

use crate::sampling::{CANCELLED, ReadWatch, Sample, SampleMethod};
use crate::sampling::{sample_rank, stream_batches};

/// The setting that caps a sample's memory, as every message about it names it.
pub const MEMORY_SETTING: &str = "analysis.sample_memory_limit";

/// The row index a Bernoulli pass ranks rows by, dropped before a row is kept.
const POSITION: &str = "__datui_table_sample_position";

/// The chunks a draw has kept, shared between its worker and the view that shows
/// them. The data is held once: the view's frame is built on these chunks.
#[derive(Default)]
pub struct SampleRows {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Each chunk with where its first row sat in the rows sampled, in arrival order.
    chunks: Vec<(u64, DataFrame)>,
    /// Chunks the view has taken.
    taken: usize,
    rows: usize,
    bytes: usize,
    /// Why the draw stopped before its end, when memory stopped it.
    stopped: Option<String>,
}

impl SampleRows {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Keep `df`, whose first row sat at `key` in the rows sampled.
    pub fn push(&self, key: u64, df: DataFrame) {
        let mut inner = self.lock();
        inner.rows += df.height();
        inner.bytes += df.estimated_size();
        inner.chunks.push((key, df));
    }

    /// The chunks kept since the last take, in arrival order.
    pub fn take_new(&self) -> Vec<DataFrame> {
        let mut inner = self.lock();
        let from = inner.taken;
        inner.taken = inner.chunks.len();
        inner.chunks[from..]
            .iter()
            .map(|(_, df)| df.clone())
            .collect()
    }

    /// Rows kept so far.
    pub fn rows(&self) -> usize {
        self.lock().rows
    }

    /// Bytes the rows kept take, as Polars estimates them.
    pub fn bytes(&self) -> usize {
        self.lock().bytes
    }

    /// Why memory stopped the draw, if it did.
    pub fn stopped(&self) -> Option<String> {
        self.lock().stopped.clone()
    }

    fn stop(&self, reason: String) {
        self.lock().stopped = Some(reason);
    }

    /// Every chunk, in the order the source holds its rows, as one frame of one
    /// chunk per column, the chunks let go: what the view keeps once the draw ends.
    pub fn take_in_source_order(&self) -> Result<Option<DataFrame>> {
        let ordered = self.in_source_order()?;
        let mut inner = self.lock();
        inner.chunks.clear();
        inner.taken = 0;
        drop(inner);
        Ok(ordered.map(|mut frame| {
            frame.rechunk_mut_par();
            frame
        }))
    }

    /// Every chunk, in the order the source holds its rows, as one frame on the
    /// same buffers. `None` before anything was kept.
    pub fn in_source_order(&self) -> Result<Option<DataFrame>> {
        let inner = self.lock();
        let mut order: Vec<&(u64, DataFrame)> = inner.chunks.iter().collect();
        // Stable: two chunks never share a first row, but an empty one may.
        order.sort_by_key(|(key, _)| *key);
        let mut out: Option<DataFrame> = None;
        for (_, df) in order {
            match out.as_mut() {
                Some(frame) => {
                    frame.vstack_mut(df)?;
                }
                None => out = Some(df.clone()),
            }
        }
        Ok(out)
    }
}

/// Where the memory a draw may take is measured against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// The memory available now: `analysis.sample_memory_limit` unset.
    Available,
    /// A fixed ceiling, in bytes.
    Fixed(u64),
    /// No warning and no running stop: the setting is 0, or the draw goes ahead anyway.
    Off,
}

impl Limit {
    /// The limit `analysis.sample_memory_limit` sets.
    pub fn of_setting(setting: Option<crate::config::ByteSize>) -> Self {
        match setting.map(|size| size.bytes()) {
            None => Self::Available,
            Some(0) => Self::Off,
            Some(bytes) => Self::Fixed(bytes),
        }
    }
}

/// Reads the memory available now, in bytes; `None` when the system will not say.
pub type MemoryProbe = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// The memory available now, as the system (or the cgroup it runs in) says.
pub fn available_memory() -> Option<u64> {
    static SYSTEM: std::sync::LazyLock<Mutex<sysinfo::System>> =
        std::sync::LazyLock::new(|| Mutex::new(sysinfo::System::new()));
    let mut system = SYSTEM.lock().unwrap_or_else(|e| e.into_inner());
    system.refresh_memory_specifics(sysinfo::MemoryRefreshKind::nothing().with_ram());
    let available = match system.cgroup_limits() {
        Some(limits) => limits.free_memory,
        None => system.available_memory(),
    };
    (available > 0).then_some(available)
}

/// The memory a draw is checked against, before it starts and as it runs.
#[derive(Clone)]
pub struct MemoryCheck {
    pub limit: Limit,
    pub probe: MemoryProbe,
}

impl MemoryCheck {
    /// No check at all: the draw goes ahead whatever it costs.
    pub fn off() -> Self {
        Self {
            limit: Limit::Off,
            probe: Arc::new(|| None),
        }
    }

    /// The room left for the sample: the ceiling less what it holds, or what is
    /// available now. `None` when nothing is checked or nothing can be measured.
    fn room(&self, held: u64) -> Option<u64> {
        match self.limit {
            Limit::Off => None,
            Limit::Fixed(bytes) => Some(bytes.saturating_sub(held)),
            Limit::Available => (self.probe)(),
        }
    }

    /// Why a sample estimated at `estimate` bytes should not be drawn, before it
    /// starts: two lines, the cost against the room, then the way through.
    pub fn refuses(&self, estimate: u64) -> Option<String> {
        let room = self.room(0)?;
        if estimate <= room {
            return None;
        }
        let bytes = |n: u64| crate::numfmt::bytes(n);
        let against = match self.limit {
            Limit::Fixed(limit) => format!("more than {MEMORY_SETTING} ({})", bytes(limit)),
            _ => format!("more than the {} available now", bytes(room)),
        };
        Some(format!(
            "~{}, {against}\nEnter again to draw anyway {} set a limit: -c {MEMORY_SETTING}=8GiB",
            bytes(estimate),
            crate::glyphs::get().middot
        ))
    }

    /// Why the draw stops now, holding `rows`, when `still` more bytes are to come.
    fn stops(&self, rows: &SampleRows, still: u64) -> Option<String> {
        self.past(rows.bytes() as u64, rows.rows(), still)
    }

    /// Why a sampler that keeps its rows to the end (a reservoir, equal per value)
    /// stops, holding `held` bytes in `rows` rows: it may come to hold as much again
    /// before it trims, so that much more must fit.
    pub fn holds_too_much(&self, held: u64, rows: usize) -> Option<String> {
        self.past(held, rows, held)
    }

    fn past(&self, held: u64, rows: usize, still: u64) -> Option<String> {
        let room = self.room(held)?;
        (still > room).then(|| {
            let why = match self.limit {
                Limit::Fixed(_) => format!("{MEMORY_SETTING} reached"),
                _ => "memory ran low".to_string(),
            };
            format!(
                "Sample stopped at {} ({} rows): {why}; -c {MEMORY_SETTING}=0 draws on",
                crate::numfmt::bytes(held),
                crate::numfmt::group_chrome(rows)
            )
        })
    }
}

/// What a draw keeps its chunks in and how it says so.
#[derive(Clone)]
pub struct Live {
    pub rows: Arc<SampleRows>,
    /// Told after each chunk is kept.
    pub notify: Arc<dyn Fn() + Send + Sync>,
    pub memory: MemoryCheck,
    pub watch: ReadWatch,
    /// Bytes per row, as the table measured them, for what is still to come before
    /// any row of the sample has been kept.
    pub bytes_per_row: Option<usize>,
}

impl Live {
    /// Keep `df`, the chunk whose first row sat at `key`. False when the draw stops:
    /// told to, or because the rest would not fit. `expected` is the rows the whole
    /// sample holds, when that is known.
    fn keep(&self, key: u64, df: DataFrame, expected: Option<usize>) -> bool {
        let last = df.estimated_size() as u64;
        if df.height() > 0 {
            self.rows.push(key, df);
            (self.notify)();
        }
        let held = self.rows.rows();
        let per_row = match self.rows.bytes().checked_div(held) {
            Some(measured) if measured > 0 => measured,
            _ => self.bytes_per_row.unwrap_or(0),
        } as u64;
        // With no total to go by, the next chunks are taken to be like the last.
        let still = match expected {
            Some(rows) => rows.saturating_sub(held) as u64 * per_row,
            None => last.saturating_mul(2),
        };
        if let Some(reason) = self.memory.stops(&self.rows, still) {
            self.rows.stop(reason);
            self.watch.stop();
            return false;
        }
        !self.watch.stopped()
    }
}

/// `plan` scanning `new` wherever it scanned `old`: the frames built on a sample's
/// frame read the rows that arrived since, with nothing rebuilt.
pub(crate) fn rebind(
    plan: &mut polars::lazy::dsl::DslPlan,
    old: &Arc<DataFrame>,
    new: &Arc<DataFrame>,
) {
    use polars::lazy::dsl::DslPlan;
    match plan {
        // A plan asked for its schema is wrapped as IR, which would run as converted:
        // rebind the plan it came from, and leave the IR behind.
        DslPlan::IR { dsl, .. } => {
            let mut inner = Arc::unwrap_or_clone(dsl.clone());
            rebind(&mut inner, old, new);
            *plan = inner;
            return;
        }
        DslPlan::DataFrameScan { df, .. } if Arc::ptr_eq(df, old) => {
            *df = Arc::clone(new);
            return;
        }
        _ => {}
    }
    crate::table::for_each_input(plan, &mut |input| rebind(input, old, new));
}

/// How a random sample of a stream was drawn: what makes the same seed draw the same
/// rows again, so a view keeps it and a redraw takes it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DrawPath {
    /// Exactly the size, the rows with the lowest seeded rank: shown at the end.
    Reservoir,
    /// Each row kept with chance size ÷ `of`, as it is read: shown as it arrives.
    Bernoulli { of: usize },
}

/// What a draw read, beside the rows it kept.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Drawn {
    /// Rows in the scope sampled, when the draw learned it.
    pub total: Option<usize>,
    /// Kept by chance, row by row: the size is about the size asked for.
    pub about: bool,
    /// Rows kept per value of an equal-per-value sample.
    pub per_value: Option<usize>,
    /// The draw ended before its end: stopped, or out of memory. The rows so far stay.
    pub cut: bool,
    /// How a random sample of a stream was drawn; `None` for every other read.
    pub path: Option<DrawPath>,
}

/// Draw `sample` from `lf`, already cut to its scope, into `live`'s chunks.
///
/// `known_total` is the scope's row count when the table knows it: it saves a count
/// before seeded runs, and picks a Bernoulli sample of a stream over a reservoir,
/// unless `path` says which, as a redraw or a view does: the same rows again.
pub fn draw(
    lf: &LazyFrame,
    sample: &Sample,
    known_total: Option<usize>,
    path: Option<DrawPath>,
    polars_streaming: bool,
    live: &Live,
) -> Result<Drawn> {
    let n = sample.rows.max(1);
    let drawn = match &sample.method {
        SampleMethod::EveryRow => {
            let seen = stream(lf, live, known_total)?;
            Drawn {
                total: Some(seen),
                ..Drawn::default()
            }
        }
        SampleMethod::FirstRows => {
            let seen = stream(&lf.clone().limit(n as IdxSize), live, Some(n))?;
            Drawn {
                total: known_total.or((seen < n).then_some(seen)),
                ..Drawn::default()
            }
        }
        SampleMethod::Spread if crate::sampling::slices_reach_into_the_scan(lf) => {
            let total = match known_total {
                Some(total) => total,
                None => crate::sampling::count_rows(lf, polars_streaming)?,
            };
            if total <= n {
                stream(lf, live, Some(total))?;
            } else {
                let on_run = |offset: usize, run: &DataFrame| {
                    live.keep(offset as u64, run.clone(), Some(n));
                };
                let read = crate::sampling::block_sample_live(
                    lf,
                    total,
                    n,
                    sample.seed,
                    polars_streaming,
                    &live.watch,
                    &on_run,
                );
                match read {
                    Ok(Some(df)) => {
                        // Under twice the sample the table was read whole and cut: one
                        // chunk, in order already.
                        live.keep(0, df, Some(n));
                    }
                    Ok(None) => {}
                    Err(error) if error.to_string() == CANCELLED => {}
                    Err(error) => return Err(error),
                }
            }
            Drawn {
                total: Some(total),
                ..Drawn::default()
            }
        }
        SampleMethod::Spread => match path.unwrap_or(match known_total {
            Some(of) => DrawPath::Bernoulli { of },
            None => DrawPath::Reservoir,
        }) {
            DrawPath::Bernoulli { of } => {
                // Every row, when the sample is the whole scope.
                bernoulli(lf, n, of, sample.seed, live)?;
                Drawn {
                    total: Some(live.watch.rows_seen().unwrap_or(of)),
                    about: of > n,
                    path: Some(DrawPath::Bernoulli { of }),
                    ..Drawn::default()
                }
            }
            DrawPath::Reservoir => {
                let read = crate::sampling::acquire(
                    lf,
                    sample,
                    None,
                    polars_streaming,
                    Some(&live.watch),
                    None,
                )?;
                let total = read.rows.total_rows;
                live.keep(0, read.rows.df, Some(n));
                Drawn {
                    total: Some(total),
                    path: Some(DrawPath::Reservoir),
                    ..Drawn::default()
                }
            }
        },
        SampleMethod::PerPartition { .. } => {
            let read = crate::sampling::acquire(
                lf,
                sample,
                known_total,
                polars_streaming,
                Some(&live.watch),
                None,
            )?;
            let total = read.rows.total_rows;
            let per_value = read.rows.per_value.as_ref().map(|per_value| per_value.kept);
            live.keep(0, read.rows.df, None);
            Drawn {
                total: Some(total),
                per_value,
                ..Drawn::default()
            }
        }
    };
    // A sampler that keeps its rows to the end stops itself when they would not fit.
    if let Some(reason) = live.watch.memory_stopped()
        && live.rows.stopped().is_none()
    {
        live.rows.stop(reason);
    }
    let cut = live.watch.stopped() || live.rows.stopped().is_some();
    if cut && live.rows.rows() == 0 {
        return Err(Report::msg(CANCELLED));
    }
    Ok(Drawn { cut, ..drawn })
}

/// Every row of `lf`, a batch at a time, each kept as it streams past. Returns the
/// rows seen. `expected` is the rows the whole read holds, when known.
fn stream(lf: &LazyFrame, live: &Live, expected: Option<usize>) -> Result<usize> {
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&seen);
    let kept = live.clone();
    stream_batches(lf.clone(), Some(&live.watch), true, move |batch| {
        let key = counted.fetch_add(batch.height(), std::sync::atomic::Ordering::Relaxed);
        Ok(!kept.keep(key as u64, batch, expected))
    })?;
    Ok(seen.load(std::sync::atomic::Ordering::Relaxed))
}

/// Keep each row of `lf` with chance `n / total`, by its seeded rank, so every row
/// kept is final the moment it is: the sample grows in place, about `n` rows (n ±
/// √n), and the same seed keeps the same rows.
fn bernoulli(lf: &LazyFrame, n: usize, total: usize, seed: u64, live: &Live) -> Result<()> {
    let bar = bernoulli_bar(n, total);
    let kept = live.clone();
    stream_batches(
        lf.clone().with_row_index(POSITION, None),
        Some(&live.watch),
        true,
        move |batch| {
            let (first, rows) = bernoulli_keep(&batch, seed, bar)?;
            Ok(!kept.keep(first, rows, Some(n)))
        },
    )
}

/// The rank under which a row is kept, for `n` of `total` rows.
fn bernoulli_bar(n: usize, total: usize) -> u128 {
    let share = (n as f64 / total.max(1) as f64).min(1.0);
    (share * (u64::MAX as f64 + 1.0)) as u128
}

/// The rows of `batch` whose rank is under `bar`, without the position column, and
/// where the batch's first row sat.
fn bernoulli_keep(batch: &DataFrame, seed: u64, bar: u128) -> PolarsResult<(u64, DataFrame)> {
    let positions = batch.column(POSITION)?.idx()?;
    let first = positions.get(0).unwrap_or(0) as u64;
    let picked: Vec<IdxSize> = positions
        .into_no_null_iter()
        .enumerate()
        .filter(|(_, position)| (sample_rank(seed, *position as u64) as u128) < bar)
        .map(|(index, _)| index as IdxSize)
        .collect();
    let kept = batch
        .take(&IdxCa::from_vec("kept".into(), picked))?
        .drop(POSITION)?;
    Ok((first, kept))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(rows: i64) -> LazyFrame {
        df!("value" => (0..rows).collect::<Vec<_>>())
            .unwrap()
            .lazy()
    }

    fn live() -> Live {
        Live {
            rows: Arc::new(SampleRows::default()),
            notify: Arc::new(|| {}),
            memory: MemoryCheck::off(),
            watch: ReadWatch::default(),
            bytes_per_row: None,
        }
    }

    fn values(df: &DataFrame) -> Vec<i64> {
        df.column("value")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect()
    }

    /// A Bernoulli sample keeps about n rows (n ± √n, well inside four of them), each
    /// row in the order the source holds it, and the same seed keeps the same rows.
    #[test]
    fn bernoulli_keeps_about_n_rows_in_order_and_repeats() {
        let (n, total) = (2_000usize, 100_000usize);
        let one = live();
        bernoulli(&table(total as i64), n, total, 42_891, &one).unwrap();
        let kept = one.rows.in_source_order().unwrap().unwrap();
        let spread = 4.0 * (n as f64).sqrt();
        assert!(
            (kept.height() as f64 - n as f64).abs() < spread,
            "{} rows",
            kept.height()
        );
        let rows = values(&kept);
        assert!(
            rows.windows(2).all(|pair| pair[0] < pair[1]),
            "source order"
        );
        assert!(kept.column(POSITION).is_err(), "no helper column leaks");
        let two = live();
        bernoulli(&table(total as i64), n, total, 42_891, &two).unwrap();
        assert_eq!(values(&two.rows.in_source_order().unwrap().unwrap()), rows);
        let other = live();
        bernoulli(&table(total as i64), n, total, 7, &other).unwrap();
        assert_ne!(
            values(&other.rows.in_source_order().unwrap().unwrap()),
            rows
        );
    }

    /// The bar keeps every row when the sample is the table, and none of a table
    /// asked for none.
    #[test]
    fn the_bernoulli_bar_spans_none_to_all() {
        let batch = df!(POSITION => (0..1_000 as IdxSize).collect::<Vec<_>>(), "value" => (0..1_000i64).collect::<Vec<_>>()).unwrap();
        let (_, all) = bernoulli_keep(&batch, 1, bernoulli_bar(1_000, 1_000)).unwrap();
        assert_eq!(all.height(), 1_000);
        let (_, none) = bernoulli_keep(&batch, 1, bernoulli_bar(0, 1_000)).unwrap();
        assert_eq!(none.height(), 0);
    }

    /// Chunks show in the order they arrive, and once the draw ends, in the order
    /// the source holds them.
    #[test]
    fn chunks_arrive_in_any_order_and_end_in_source_order() {
        let rows = SampleRows::default();
        let chunk = |from: i64| df!("value" => (from..from + 3).collect::<Vec<_>>()).unwrap();
        rows.push(30, chunk(30));
        rows.push(10, chunk(10));
        let first = rows.take_new();
        assert_eq!(
            first.iter().flat_map(values).collect::<Vec<_>>(),
            [30, 31, 32, 10, 11, 12]
        );
        rows.push(20, chunk(20));
        let next = rows.take_new();
        assert_eq!(next.len(), 1, "only what arrived since");
        assert_eq!(rows.rows(), 9);
        assert_eq!(
            values(&rows.in_source_order().unwrap().unwrap()),
            [10, 11, 12, 20, 21, 22, 30, 31, 32]
        );
    }

    /// Every row and the head stream their batches in; the head stops at its size.
    #[test]
    fn every_row_and_the_head_stream_in() {
        let all = live();
        let sample = Sample {
            method: SampleMethod::EveryRow,
            ..Sample::default()
        };
        let drawn = draw(&table(5_000), &sample, None, None, false, &all).unwrap();
        assert_eq!((drawn.total, all.rows.rows()), (Some(5_000), 5_000));
        let head = live();
        let sample = Sample {
            method: SampleMethod::FirstRows,
            rows: 120,
            ..Sample::default()
        };
        draw(&table(5_000), &sample, Some(5_000), None, false, &head).unwrap();
        assert_eq!(
            values(&head.rows.in_source_order().unwrap().unwrap()),
            (0..120).collect::<Vec<_>>()
        );
    }

    /// A known total draws by chance row by row; an unknown one keeps a reservoir
    /// of exactly n, one chunk at the end.
    #[test]
    fn a_known_total_draws_about_n_and_an_unknown_one_exactly_n() {
        let sample = Sample {
            rows: 500,
            ..Sample::default()
        };
        let known = live();
        let drawn = draw(&table(20_000), &sample, Some(20_000), None, false, &known).unwrap();
        assert!(drawn.about);
        let unknown = live();
        let drawn = draw(&table(20_000), &sample, None, None, false, &unknown).unwrap();
        assert!(!drawn.about);
        assert_eq!((drawn.total, unknown.rows.rows()), (Some(20_000), 500));
    }

    /// Memory that will not hold the rest stops the draw and keeps the rows so far,
    /// and says why, naming the setting. Only the streaming engine reads in batches.
    #[cfg(feature = "streaming")]
    #[test]
    fn low_memory_stops_the_draw_and_keeps_what_it_has() {
        let mut low = live();
        low.memory = MemoryCheck {
            limit: Limit::Available,
            probe: Arc::new(|| Some(1)),
        };
        let sample = Sample {
            method: SampleMethod::EveryRow,
            ..Sample::default()
        };
        // Five frames read one after another: batches, not one frame whole.
        let parts: Vec<LazyFrame> = (0..5).map(|_| table(100_000)).collect();
        let lf = concat(parts, UnionArgs::default()).unwrap();
        let drawn = draw(&lf, &sample, Some(500_000), None, false, &low).unwrap();
        assert!(drawn.cut);
        let held = low.rows.rows();
        assert!(held > 0 && held < 500_000, "{held}");
        let reason = low.rows.stopped().unwrap();
        assert!(reason.contains("memory ran low"), "{reason}");
        assert!(reason.contains(MEMORY_SETTING), "{reason}");
    }

    /// A path given draws that way whatever is known now: a reservoir with the count
    /// in, and a Bernoulli sample of the total recorded with none. The same seed and
    /// path keep the same rows.
    #[test]
    fn a_recorded_path_draws_the_same_rows_whatever_is_known_now() {
        let sample = Sample {
            rows: 500,
            ..Sample::default()
        };
        let rows = |live: &Live| values(&live.rows.in_source_order().unwrap().unwrap());
        let reservoir = live();
        draw(&table(20_000), &sample, None, None, false, &reservoir).unwrap();
        let counted = live();
        let drawn = draw(
            &table(20_000),
            &sample,
            Some(20_000),
            Some(DrawPath::Reservoir),
            false,
            &counted,
        )
        .unwrap();
        assert_eq!(drawn.path, Some(DrawPath::Reservoir));
        assert_eq!(rows(&counted), rows(&reservoir));

        let known = live();
        let drawn = draw(&table(20_000), &sample, Some(20_000), None, false, &known).unwrap();
        assert_eq!(drawn.path, Some(DrawPath::Bernoulli { of: 20_000 }));
        let uncounted = live();
        let path = drawn.path;
        draw(&table(20_000), &sample, None, path, false, &uncounted).unwrap();
        assert_eq!(rows(&uncounted), rows(&known));
    }

    /// A reservoir holds its rows to the end: past the memory there is, it stops
    /// there, keeps what it holds, and says why as a live draw does.
    #[cfg(feature = "streaming")]
    #[test]
    fn a_reservoir_past_the_memory_stops_and_keeps_what_it_holds() {
        let check = MemoryCheck {
            limit: Limit::Fixed(1),
            probe: Arc::new(|| None),
        };
        let mut low = live();
        low.watch = ReadWatch::judging_held(Arc::new(move |bytes, rows| {
            check.holds_too_much(bytes, rows)
        }));
        let parts: Vec<LazyFrame> = (0..5).map(|_| table(100_000)).collect();
        let lf = concat(parts, UnionArgs::default()).unwrap();
        let sample = Sample {
            rows: 400_000,
            ..Sample::default()
        };
        let drawn = draw(&lf, &sample, None, None, false, &low).unwrap();
        assert!(drawn.cut);
        assert!(low.rows.rows() > 0);
        assert!(drawn.total.unwrap() < 500_000, "it stopped early");
        let reason = low.rows.stopped().unwrap();
        assert!(reason.contains(MEMORY_SETTING), "{reason}");
    }

    /// Without the streaming engine the read is one batch, whole before the draw sees
    /// it: there is no partway to stop at, so the rows read are kept.
    #[cfg(not(feature = "streaming"))]
    #[test]
    fn without_streaming_the_read_arrives_whole() {
        let mut low = live();
        low.memory = MemoryCheck {
            limit: Limit::Available,
            probe: Arc::new(|| Some(1)),
        };
        let sample = Sample {
            method: SampleMethod::EveryRow,
            ..Sample::default()
        };
        let parts: Vec<LazyFrame> = (0..5).map(|_| table(100_000)).collect();
        let lf = concat(parts, UnionArgs::default()).unwrap();
        let drawn = draw(&lf, &sample, Some(500_000), None, false, &low).unwrap();
        assert!(!drawn.cut);
        assert_eq!(low.rows.rows(), 500_000);
        assert_eq!(low.rows.take_new().len(), 1, "one batch");
    }

    /// Before a draw, an estimate past the room is refused with the way through.
    #[test]
    fn an_estimate_past_the_room_is_refused_with_the_way_through() {
        let check = MemoryCheck {
            limit: Limit::Available,
            probe: Arc::new(|| Some(4 << 30)),
        };
        let refused = check.refuses(6 << 30).unwrap();
        assert!(refused.contains("available now"), "{refused}");
        assert!(refused.contains("Enter again"), "{refused}");
        assert!(
            refused.contains("-c analysis.sample_memory_limit"),
            "{refused}"
        );
        assert!(check.refuses(1 << 30).is_none());
        assert!(MemoryCheck::off().refuses(u64::MAX).is_none());
        let fixed = MemoryCheck {
            limit: Limit::Fixed(1 << 20),
            probe: Arc::new(|| None),
        };
        assert!(fixed.refuses(2 << 20).unwrap().contains(MEMORY_SETTING));
    }
}
