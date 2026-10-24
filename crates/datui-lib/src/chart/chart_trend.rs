//! A scatter chart's trend: a line fitted through its points, and a plain reading of
//! it. The fit reads the points as prepared, in the data's own units, whatever Log
//! scale or Range draws: only the line drawn follows those. Every fit runs as a job
//! on the chart's own series (shared, not cloned) and stops when superseded. Least
//! squares reads them in one pass; Spearman's ranks copy up to [`RANKED_MAX`] points,
//! robust's residuals and local's sorted points and buffers about three times the
//! points, so local's peak is the largest.

use serde::{Deserialize, Serialize};

use crate::analysis::statistics::PairSums;
use crate::chart::chart_data::XAxisTemporalKind;
use crate::chart::chart_plot::{Axis, per_second};
use crate::widgets::axis_numbers::AxisNumbers;

/// What a scatter chart fits through its points.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Trend {
    #[default]
    Off,
    /// Least squares.
    Linear,
    /// Least squares on ln y, over the points with y above 0.
    Exponential,
    /// Least squares on ln y against ln x, over the points with both above 0.
    Power,
    /// Theil–Sen: the median of the slopes between pairs of points.
    Robust,
    /// LOWESS: a line through local weighted fits.
    Local,
    /// The line y = x, fitted to nothing: a reference.
    #[serde(rename = "y=x")]
    Identity,
}

impl Trend {
    pub const ALL: [Self; 7] = [
        Self::Off,
        Self::Linear,
        Self::Exponential,
        Self::Power,
        Self::Robust,
        Self::Local,
        Self::Identity,
    ];

    pub fn is_off(&self) -> bool {
        *self == Self::Off
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Linear => "linear",
            Self::Exponential => "exponential",
            Self::Power => "power",
            Self::Robust => "robust",
            Self::Local => "local",
            Self::Identity => "y = x",
        }
    }

    /// Fitted per Color series or over every point, as Over says.
    pub fn takes_over(self) -> bool {
        !matches!(self, Self::Off | Self::Identity)
    }

    /// Fits over a date, time or duration X: power takes the log of X, and y = x
    /// compares X with Y in the same units.
    pub fn takes_time(self) -> bool {
        !matches!(self, Self::Power | Self::Identity)
    }
}

/// Whether a colored scatter gets a fit per Color series or one over every point.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrendOver {
    #[default]
    Color,
    Overall,
}

impl TrendOver {
    pub const ALL: [Self; 2] = [Self::Color, Self::Overall];

    pub fn is_color(&self) -> bool {
        *self == Self::Color
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Color => "per color",
            Self::Overall => "all points",
        }
    }
}

/// What a set of fits is of: the method, and per series or over every point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrendKey {
    pub trend: Trend,
    pub over: TrendOver,
}

/// The fewest points a series is fitted on.
pub const MIN_POINTS: usize = 10;

/// The most pairs Theil–Sen takes slopes of: past it, this many pairs drawn at random
/// with the sample's seed.
pub const PAIR_BUDGET: usize = 1_000_000;

/// LOWESS: the share of the points each local fit reads, its robustness passes, and
/// the share of X's range within which fits are joined by straight lines rather than
/// fitted (Cleveland's and statsmodels' defaults, and its suggested `delta`).
pub const LOWESS_FRAC: f64 = 2.0 / 3.0;
pub const LOWESS_ITERATIONS: usize = 3;
pub const LOWESS_DELTA: f64 = 0.01;

/// The most points a LOWESS curve keeps to draw.
const CURVE_POINTS_MAX: usize = 600;

/// How far apart Pearson's r and Spearman's ρ are before the reading says the
/// points are curved or have outliers.
pub const RANK_GAP: f64 = 0.2;

/// What a fit draws: a line, a curve made of its formula, or LOWESS's points. X is
/// centered where it can be, so a date's nanoseconds keep their precision.
#[derive(Debug, Clone, PartialEq)]
pub enum Model {
    /// `y = y0 + slope (x - x0)`.
    Line { x0: f64, y0: f64, slope: f64 },
    /// `y = exp(ln_y0 + rate (x - x0))`.
    Exp { x0: f64, ln_y0: f64, rate: f64 },
    /// `y = exp(ln_y0 + power (ln x - ln_x0))`.
    Power { ln_x0: f64, ln_y0: f64, power: f64 },
    /// `y = x`.
    Identity,
    /// LOWESS: the fitted value at points in X order.
    Curve(Vec<(f64, f64)>),
}

impl Model {
    /// The line's value at `x`: NaN where it has none (a power at x of 0 or less).
    pub fn at(&self, x: f64) -> f64 {
        match self {
            Self::Line { x0, y0, slope } => y0 + slope * (x - x0),
            Self::Exp { x0, ln_y0, rate } => (ln_y0 + rate * (x - x0)).exp(),
            Self::Power {
                ln_x0,
                ln_y0,
                power,
            } if x > 0.0 => (ln_y0 + power * (x.ln() - ln_x0)).exp(),
            Self::Power { .. } => f64::NAN,
            Self::Identity => x,
            Self::Curve(points) => interpolate(points, x),
        }
    }

    /// The slope in Y per unit of X: a line's, or an exponential's or power's at `x`.
    pub fn slope_at(&self, x: f64) -> Option<f64> {
        match self {
            Self::Line { slope, .. } => Some(*slope),
            Self::Exp { rate, .. } => Some(rate * self.at(x)),
            Self::Power { power, .. } if x > 0.0 => Some(power * self.at(x) / x),
            Self::Identity => Some(1.0),
            Self::Power { .. } | Self::Curve(_) => None,
        }
    }
}

/// `points`' line, in X order, at `x`: joined straight between them, NaN outside.
fn interpolate(points: &[(f64, f64)], x: f64) -> f64 {
    let i = points.partition_point(|&(px, _)| px < x);
    match (i.checked_sub(1).map(|j| points[j]), points.get(i)) {
        (_, Some(&(px, py))) if px == x => py,
        (Some((x0, y0)), Some(&(x1, y1))) => y0 + (y1 - y0) * (x - x0) / (x1 - x0),
        _ => f64::NAN,
    }
}

/// Theil–Sen's pairs: how many slopes it took, of how many pairs, and whether they
/// were a random draw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pairs {
    pub used: usize,
    pub of: usize,
    pub sampled: bool,
}

/// One fitted line: of a series (its index), or of every point (`None`).
#[derive(Debug, Clone, PartialEq)]
pub struct Fit {
    pub series: Option<usize>,
    pub model: Model,
    /// The least and greatest X of the points fitted: the line stops there.
    pub x_range: [f64; 2],
    /// Points fitted.
    pub used: usize,
    /// Points an exponential or power fit left out for a value at 0 or less.
    pub dropped: usize,
    /// Of those, the ones a power fit left out by X, counted first as a log X does.
    pub dropped_x: usize,
    /// Pearson's r of the points as fitted (of ln y, or ln y and ln x); NaN for the
    /// fits it does not read, or with no spread.
    pub r: f64,
    /// Spearman's ρ of a linear fit's points; NaN otherwise.
    pub rho: f64,
    /// The points ρ ranked, when an even stride of them ([`RANKED_MAX`]).
    pub rho_of: Option<usize>,
    /// The residual standard deviation: in Y's units, or of ln y for an exponential
    /// or power fit. For y = x, the root mean square of y - x.
    pub resid_sd: f64,
    /// The median distance of a point from the line, in Y's units.
    pub spread: f64,
    /// For y = x, the share of the points above the line.
    pub above: f64,
    /// Theil–Sen's pairs.
    pub pairs: Option<Pairs>,
}

impl Fit {
    pub fn r_squared(&self) -> f64 {
        self.r * self.r
    }

    /// Whether Pearson and Spearman disagree enough that the points are curved or
    /// have outliers, which a straight least-squares line does not show.
    pub fn ranks_disagree(&self) -> bool {
        self.r.is_finite() && self.rho.is_finite() && (self.r - self.rho).abs() >= RANK_GAP
    }
}

/// The fits of one [`TrendKey`] over a chart's series.
#[derive(Debug, Clone, PartialEq)]
pub struct TrendFits {
    pub key: TrendKey,
    pub fits: Vec<Fit>,
    /// Series not fitted for having fewer than [`MIN_POINTS`] points (to fit).
    pub too_few: usize,
    /// The seed Theil–Sen drew its pairs with, when it drew them.
    pub seed: Option<u64>,
    /// Why a job could not fit them.
    pub failed: Option<String>,
}

impl TrendFits {
    /// No fits, for `key`, with why: what a job that failed leaves, so it is not
    /// fitted again.
    pub fn failed(key: TrendKey, why: String) -> Self {
        Self {
            key,
            fits: Vec::new(),
            too_few: 0,
            seed: None,
            failed: Some(why),
        }
    }
}

/// Whether the job fitting was superseded: a long fit stops at its next check.
pub type Stop<'a> = &'a dyn Fn() -> bool;

/// One series' points, borrowed.
type Part<'a> = &'a [(f64, f64)];

/// How often Theil–Sen's draws and LOWESS's local fits ask whether to stop.
const STOP_EVERY: usize = 1 << 14;

/// The most points Spearman's ρ ranks: past it, an even stride of them, which puts ρ
/// within about 0.001 at a fraction of the memory.
pub const RANKED_MAX: usize = 1_000_000;

/// Points read in place: one series', or several series' as one, never copied.
#[derive(Clone, Copy)]
pub struct Source<'a> {
    parts: &'a [&'a [(f64, f64)]],
    len: usize,
}

impl<'a> Source<'a> {
    pub fn new(parts: &'a [&'a [(f64, f64)]]) -> Self {
        Self {
            parts,
            len: parts.iter().map(|p| p.len()).sum(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = (f64, f64)> + 'a {
        self.parts.iter().flat_map(|p| p.iter().copied())
    }

    fn get(&self, mut i: usize) -> (f64, f64) {
        for part in self.parts {
            if i < part.len() {
                return part[i];
            }
            i -= part.len();
        }
        (f64::NAN, f64::NAN)
    }
}

/// Whether `trend` fits the point `(x, y)`: finite, and above 0 where it takes a log.
fn takes(trend: Trend) -> impl Fn(&(f64, f64)) -> bool {
    move |&(x, y)| {
        x.is_finite()
            && y.is_finite()
            && match trend {
                Trend::Exponential => y > 0.0,
                Trend::Power => x > 0.0 && y > 0.0,
                _ => true,
            }
    }
}

/// A point as `trend` fits it: of ln y, or ln y against ln x.
fn as_fitted(trend: Trend) -> impl Fn((f64, f64)) -> (f64, f64) {
    move |(x, y)| match trend {
        Trend::Exponential => (x, y.ln()),
        Trend::Power => (x.ln(), y.ln()),
        _ => (x, y),
    }
}

/// Fit `key` over `series` (the last of them Other when `other`): one fit of every
/// point, Other's included, or one per series but Other, each of at least
/// [`MIN_POINTS`] points. `seed` draws Theil–Sen's pairs past its budget. `None` once
/// `stop` says so.
pub fn fit_all(
    series: &[Vec<(f64, f64)>],
    other: bool,
    key: TrendKey,
    seed: u64,
    stop: Stop<'_>,
) -> Option<TrendFits> {
    let mut out = TrendFits {
        key,
        fits: Vec::new(),
        too_few: 0,
        seed: None,
        failed: None,
    };
    if key.trend.is_off() {
        return Some(out);
    }
    let overall = key.trend == Trend::Identity || key.over == TrendOver::Overall;
    let fitted = series.len() - usize::from(other && !series.is_empty());
    let every: Vec<Part> = series.iter().map(Vec::as_slice).collect();
    // Each line's series index (none overall) and the parts it is fitted from.
    let groups: Vec<(Option<usize>, &[Part])> = if overall {
        vec![(None, &every[..])]
    } else {
        (0..fitted)
            .filter(|&i| !series[i].is_empty())
            .map(|i| (Some(i), std::slice::from_ref(&every[i])))
            .collect()
    };
    for (index, parts) in groups {
        let fit = fit_source(Source::new(parts), key.trend, seed, stop);
        if stop() {
            return None;
        }
        match fit {
            Some(mut fit) => {
                fit.series = index;
                if fit.pairs.is_some_and(|p| p.sampled) {
                    out.seed = Some(seed);
                }
                out.fits.push(fit);
            }
            None => out.too_few += 1,
        }
    }
    Some(out)
}

/// One fit of `points` by `trend`; `None` with too few points to fit, or no spread
/// in X.
pub fn fit_one(points: &[(f64, f64)], trend: Trend, seed: u64) -> Option<Fit> {
    fit_source(Source::new(&[points]), trend, seed, &|| false)
}

/// One fit of `points` by `trend`, reading them in place: least squares in one
/// pass, and a buffer of one value per point at most for a median or LOWESS.
fn fit_source(points: Source<'_>, trend: Trend, seed: u64, stop: Stop<'_>) -> Option<Fit> {
    let ok = takes(trend);
    let finite = takes(Trend::Linear);
    let blank = |model: Model, used: usize, x_range: [f64; 2]| Fit {
        series: None,
        model,
        x_range,
        used,
        dropped: 0,
        dropped_x: 0,
        r: f64::NAN,
        rho: f64::NAN,
        rho_of: None,
        resid_sd: f64::NAN,
        spread: f64::NAN,
        above: f64::NAN,
        pairs: None,
    };
    let (used, x_lo, x_hi) = points.iter().filter(&ok).fold(
        (0, f64::INFINITY, f64::NEG_INFINITY),
        |(n, lo, hi), (x, _)| (n + 1, lo.min(x), hi.max(x)),
    );
    let x_range = (x_lo.is_finite() && x_hi > x_lo).then_some([x_lo, x_hi])?;
    if trend == Trend::Identity {
        let n = used as f64;
        let mut fit = blank(Model::Identity, used, x_range);
        let (above, ss) = points
            .iter()
            .filter(&ok)
            .fold((0usize, 0.0), |(a, ss), (x, y)| {
                (a + usize::from(y > x), ss + (y - x).powi(2))
            });
        fit.above = above as f64 / n;
        fit.resid_sd = (ss / n).sqrt();
        let mut off: Vec<f64> = points
            .iter()
            .filter(&ok)
            .map(|(x, y)| (y - x).abs())
            .collect();
        fit.spread = median(&mut off);
        return Some(fit);
    }
    if used < MIN_POINTS {
        return None;
    }
    let dropped = points.iter().filter(&finite).count() - used;
    let ols = Ols::of(points.iter().filter(&ok).map(as_fitted(trend)))?;
    let mut fit = blank(Model::Identity, used, x_range);
    fit.dropped = dropped;
    if trend == Trend::Power {
        fit.dropped_x = points
            .iter()
            .filter(|&(x, y)| x.is_finite() && y.is_finite() && x <= 0.0)
            .count();
    }
    fit.r = ols.r;
    fit.resid_sd = ols.resid_sd;
    // Distances from the line, in Y's units, filled once the line is known.
    let mut off: Vec<f64> = Vec::new();
    match trend {
        Trend::Linear => {
            fit.model = Model::Line {
                x0: ols.x0,
                y0: ols.y0,
                slope: ols.slope,
            };
            let stride = used.div_ceil(RANKED_MAX);
            let ranked: Vec<(f64, f64)> = points.iter().filter(&ok).step_by(stride).collect();
            fit.rho = spearman(&ranked);
            fit.rho_of = (stride > 1).then_some(ranked.len());
        }
        Trend::Exponential => {
            fit.model = Model::Exp {
                x0: ols.x0,
                ln_y0: ols.y0,
                rate: ols.slope,
            };
        }
        Trend::Power => {
            fit.model = Model::Power {
                ln_x0: ols.x0,
                ln_y0: ols.y0,
                power: ols.slope,
            };
        }
        Trend::Robust => {
            let (slope, pairs) = theil_sen(points, &ok, PAIR_BUDGET, seed, stop)?;
            // The line through the median point of what is left: scipy's
            // `theilslopes(method="joint")`, centered on X's median for precision.
            off = points.iter().filter(&ok).map(|(x, _)| x).collect();
            let x0 = median(&mut off);
            off.clear();
            off.extend(points.iter().filter(&ok).map(|(x, y)| y - slope * (x - x0)));
            let y0 = median(&mut off);
            fit.model = Model::Line { x0, y0, slope };
            fit.pairs = Some(pairs);
            off.clear();
        }
        Trend::Local => {
            let mut sorted: Vec<(f64, f64)> = points.iter().filter(&ok).collect();
            sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
            let delta = LOWESS_DELTA * (x_range[1] - x_range[0]);
            let smooth = lowess(&sorted, LOWESS_FRAC, LOWESS_ITERATIONS, delta, stop)?;
            let n = sorted.len() as f64;
            let ss: f64 = sorted
                .iter()
                .zip(&smooth)
                .map(|((_, y), f)| (y - f).powi(2))
                .sum();
            fit.resid_sd = (ss / (n - 2.0)).sqrt();
            off = sorted
                .iter()
                .zip(&smooth)
                .map(|((_, y), f)| (y - f).abs())
                .collect();
            fit.spread = median(&mut off);
            fit.model = Model::Curve(curve_points(&sorted, &smooth));
            return Some(fit);
        }
        Trend::Off | Trend::Identity => return None,
    }
    off.extend(
        points
            .iter()
            .filter(&ok)
            .map(|(x, y)| (y - fit.model.at(x)).abs())
            .filter(|d| d.is_finite()),
    );
    if trend == Trend::Robust {
        let n = off.len() as f64;
        let ss: f64 = off.iter().map(|d| d * d).sum();
        fit.resid_sd = (ss / (n - 2.0)).sqrt();
    }
    fit.spread = median(&mut off);
    Some(fit)
}

/// A least-squares line: centered on the points' means, with Pearson's r and the
/// residual standard deviation (over n - 2).
#[derive(Debug, Clone, Copy)]
struct Ols {
    x0: f64,
    y0: f64,
    slope: f64,
    r: f64,
    resid_sd: f64,
}

impl Ols {
    /// In one pass, shifted by the first point so large values (a date's
    /// nanoseconds) keep their precision.
    fn of(mut points: impl Iterator<Item = (f64, f64)>) -> Option<Self> {
        let (sx, sy) = points.next()?;
        let mut sums = PairSums::default();
        sums.add(0.0, 0.0);
        for (x, y) in points {
            sums.add(x - sx, y - sy);
        }
        let n = sums.count as f64;
        let (sxx, syy, sxy) = sums.spreads();
        if sums.count < 2 || sxx <= sums.xx * 1e-12 || sxx <= 0.0 {
            return None;
        }
        let slope = sxy / sxx;
        let r = sums.correlation();
        let ss = (syy - slope * sxy).max(0.0);
        Some(Self {
            x0: sx + sums.x / n,
            y0: sy + sums.y / n,
            slope,
            r,
            resid_sd: if sums.count > 2 {
                (ss / (n - 2.0)).sqrt()
            } else {
                0.0
            },
        })
    }
}

/// Spearman's ρ: Pearson's r of the ranks, ties at their average rank.
pub fn spearman(points: &[(f64, f64)]) -> f64 {
    let xs = ranks(points.iter().map(|&(x, _)| x));
    let ys = ranks(points.iter().map(|&(_, y)| y));
    let mut sums = PairSums::default();
    // Ranks run from 1 to n: shifted by the middle, the sums stay small.
    let mid = (points.len() as f64 + 1.0) / 2.0;
    for (x, y) in xs.into_iter().zip(ys) {
        sums.add(x - mid, y - mid);
    }
    sums.correlation()
}

/// Each value's rank from 1, ties at their average.
fn ranks(values: impl Iterator<Item = f64>) -> Vec<f64> {
    let values: Vec<f64> = values.collect();
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&a, &b| values[a].total_cmp(&values[b]));
    let mut out = vec![0.0; values.len()];
    let mut start = 0;
    while start < order.len() {
        let mut end = start + 1;
        while end < order.len() && values[order[end]] == values[order[start]] {
            end += 1;
        }
        let rank = (start + 1 + end) as f64 / 2.0;
        for &i in &order[start..end] {
            out[i] = rank;
        }
        start = end;
    }
    out
}

/// The median of `values`, which it reorders; NaN when empty. Of an even count, the
/// mean of the middle two.
pub fn median(values: &mut [f64]) -> f64 {
    let n = values.len();
    if n == 0 {
        return f64::NAN;
    }
    let mid = n / 2;
    let (low, &mut upper, _) = values.select_nth_unstable_by(mid, f64::total_cmp);
    if n % 2 == 1 {
        upper
    } else {
        let lower = low.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        (lower + upper) / 2.0
    }
}

/// Theil–Sen's slope of the points `ok` takes: the median of the slopes between
/// every pair of them with different X, or, past `budget` pairs, between `budget`
/// pairs drawn at random with `seed` (some of which, at one X or a point `ok` leaves
/// out, give no slope). `None` with no pair at different X, or once `stop` says so.
pub fn theil_sen(
    points: Source<'_>,
    ok: &dyn Fn(&(f64, f64)) -> bool,
    budget: usize,
    seed: u64,
    stop: Stop<'_>,
) -> Option<(f64, Pairs)> {
    let kept = points.iter().filter(ok).count();
    let of = kept * kept.saturating_sub(1) / 2;
    let sampled = of > budget;
    let mut slopes = Vec::with_capacity(of.min(budget));
    let slope = |a: (f64, f64), b: (f64, f64)| {
        (ok(&a) && ok(&b) && a.0 != b.0).then(|| (b.1 - a.1) / (b.0 - a.0))
    };
    if sampled {
        let n = points.len as u64;
        let mut rng = crate::analysis::distribution_fit::Rng::new(seed);
        for draw in 0..budget {
            if draw % STOP_EVERY == 0 && stop() {
                return None;
            }
            let i = rng.next_u64() % n;
            let mut j = rng.next_u64() % (n - 1);
            if j >= i {
                j += 1;
            }
            slopes.extend(slope(points.get(i as usize), points.get(j as usize)));
        }
    } else {
        let kept: Vec<(f64, f64)> = points.iter().filter(ok).collect();
        for (i, &a) in kept.iter().enumerate() {
            for &b in &kept[i + 1..] {
                slopes.extend(slope(a, b));
            }
        }
    }
    let used = slopes.len();
    (used > 0).then(|| (median(&mut slopes), Pairs { used, of, sampled }))
}

/// LOWESS (Cleveland 1979) as statsmodels computes it: at each X (in order), a
/// least-squares line weighted by the tricube of the distance over the nearest
/// `frac` of the points, then `iterations` passes reweighting each point by the
/// bisquare of its residual over six times the median residual. Fits within `delta`
/// of the last one are joined straight rather than fitted. `points` in X order.
/// `None` once `stop` says so.
pub fn lowess(
    points: &[(f64, f64)],
    frac: f64,
    iterations: usize,
    delta: f64,
    stop: Stop<'_>,
) -> Option<Vec<f64>> {
    let n = points.len();
    if n == 0 {
        return Some(Vec::new());
    }
    let x = |i: usize| points[i].0;
    let k = ((frac * n as f64 + 1e-10) as usize).clamp(2.min(n), n);
    let mut fitted = vec![0.0; n];
    let mut robust = vec![1.0; n];
    let mut weights = vec![0.0; k];
    let mut scratch = vec![0.0; n];
    for _ in 0..=iterations {
        let (mut i, mut last): (usize, Option<usize>) = (0, None);
        let (mut left, mut right) = (0, k);
        fitted.iter_mut().for_each(|f| *f = 0.0);
        // Counted by the window's points each fit reads: a window holds 2/3 of the
        // points, so a pass of a few hundred fits is still billions of steps.
        let mut work = 0usize;
        loop {
            work += k;
            if work >= STOP_EVERY {
                work = 0;
                if stop() {
                    return None;
                }
            }
            let at = x(i);
            // The nearest k points, as the window slides along X.
            while right < n && at > (x(left) + x(right)) / 2.0 {
                left += 1;
                right += 1;
            }
            let radius = (at - x(left)).max(x(right - 1) - at);
            fitted[i] = local_fit(
                &points[left..right],
                (&robust[left..right], &mut weights[..right - left]),
                at,
                radius,
            )
            .unwrap_or(points[i].1);
            // The points skipped since the last fit, joined straight.
            if let Some(last) = last
                && last + 1 < i
            {
                let (x0, y0, y1) = (x(last), fitted[last], fitted[i]);
                for (j, f) in fitted.iter_mut().enumerate().take(i).skip(last + 1) {
                    let a = (x(j) - x0) / (at - x0);
                    *f = a * y1 + (1.0 - a) * y0;
                }
            }
            // The next point to fit: the farthest within delta, ties taking this fit.
            let mut fit_at = i;
            let cut = at + delta;
            let mut k_at = i;
            for j in i + 1..n {
                k_at = j;
                if x(j) > cut {
                    break;
                }
                if x(j) == x(fit_at) {
                    fitted[j] = fitted[fit_at];
                    fit_at = j;
                }
            }
            // A loop that ran to the end leaves its last index, as Python's does.
            last = Some(fit_at);
            if fit_at + 1 >= n {
                break;
            }
            i = k_at.saturating_sub(1).max(fit_at + 1);
        }
        if stop() {
            return None;
        }
        let residual = |(p, f): (&(f64, f64), &f64)| (p.1 - f).abs();
        for (s, r) in scratch
            .iter_mut()
            .zip(points.iter().zip(&fitted).map(residual))
        {
            *s = r;
        }
        let scale = 6.0 * median(&mut scratch);
        for (w, r) in robust
            .iter_mut()
            .zip(points.iter().zip(&fitted).map(residual))
        {
            let u = if scale == 0.0 {
                f64::from(u8::from(r > 0.0))
            } else {
                (r / scale).min(1.0)
            };
            *w = (1.0 - u * u).powi(2);
        }
    }
    Some(fitted)
}

/// One local fit at `x`: least squares over `points` weighted by the tricube of the
/// distance over `radius` times the points' `robust` weights; `None` with fewer than
/// two points of weight.
fn local_fit(
    points: &[(f64, f64)],
    (robust, weights): (&[f64], &mut [f64]),
    x: f64,
    radius: f64,
) -> Option<f64> {
    let mut sum = 0.0;
    let mut nonzero = 0;
    for ((w, &(xj, _)), &r) in weights.iter_mut().zip(points).zip(robust) {
        let d = (xj - x).abs() / radius;
        let tri = (1.0 - d * d * d).powi(3);
        *w = tri * r;
        sum += *w;
        nonzero += usize::from(*w > 1e-12);
    }
    if nonzero < 2 || !sum.is_finite() {
        return None;
    }
    weights.iter_mut().for_each(|w| *w /= sum);
    let mean: f64 = weights.iter().zip(points).map(|(w, (xj, _))| w * xj).sum();
    let spread: f64 = weights
        .iter()
        .zip(points)
        .map(|(w, (xj, _))| w * (xj - mean).powi(2))
        .sum::<f64>()
        .max(1e-12);
    Some(
        weights
            .iter()
            .zip(points)
            .map(|(w, (xj, yj))| w * (1.0 + (x - mean) * (xj - mean) / spread) * yj)
            .sum(),
    )
}

/// A LOWESS fit's points to draw: one per X, at most [`CURVE_POINTS_MAX`], the ends
/// kept. Reads `points` (in X order) twice rather than copying them.
fn curve_points(points: &[(f64, f64)], fitted: &[f64]) -> Vec<(f64, f64)> {
    let firsts = || {
        points
            .iter()
            .zip(fitted)
            .enumerate()
            .filter(|(i, _)| *i == 0 || points[i - 1].0 != points[*i].0)
            .map(|(_, (&(x, _), &y))| (x, y))
    };
    let distinct = firsts().count();
    if distinct <= CURVE_POINTS_MAX {
        return firsts().collect();
    }
    let last = distinct - 1;
    let mut wanted = (0..CURVE_POINTS_MAX)
        .map(|i| i * last / (CURVE_POINTS_MAX - 1))
        .peekable();
    firsts()
        .enumerate()
        .filter(|(i, _)| {
            let keep = wanted.peek() == Some(i);
            while wanted.peek() == Some(i) {
                wanted.next();
            }
            keep
        })
        .map(|(_, p)| p)
        .collect()
}

// ----- Reading -----

/// What a reading needs of the chart: its axes, its columns and their units, and
/// the aggregate a point is.
pub struct Words<'a> {
    pub x: &'a Axis,
    pub y: &'a Axis,
    /// X's column: `per unit of distance`, `1% more distance`.
    pub x_name: String,
    /// X's unit, when it has one, as its spec writes it: `per unit of distance (mi)`.
    pub x_unit: Option<String>,
    /// Y's column: `about 0.82% more air_time`.
    pub y_name: String,
    /// Y's unit, after a value: `+2.3 fps`.
    pub y_unit: Option<String>,
    /// What a point is under an aggregate, plural: `means`.
    pub points: Option<&'static str>,
}

/// The lengths of time a slope or a doubling time is said per, in seconds.
const TIME_UNITS: [(&str, f64); 7] = [
    ("second", 1.0),
    ("minute", 60.0),
    ("hour", 3_600.0),
    ("day", 86_400.0),
    ("week", 604_800.0),
    ("month", 2_629_746.0),
    ("year", 31_556_952.0),
];

impl Words<'_> {
    /// The unit X is read per: on a time X, the largest length of time the fit's range
    /// holds three of (`day`), and how many X units it is; otherwise one of X, by its
    /// unit (`mi`) or as `unit of distance`.
    fn per(&self, x_range: [f64; 2]) -> (String, f64) {
        match per_second(self.x.kind) {
            Some(per) => {
                let span = (x_range[1] - x_range[0]) / per;
                let (name, seconds) = TIME_UNITS
                    .iter()
                    .rev()
                    .find(|(_, s)| span >= 3.0 * s)
                    .copied()
                    .unwrap_or(TIME_UNITS[0]);
                (name.to_string(), seconds * per)
            }
            None => match &self.x_unit {
                // As written: a unit may be a plural word, an abbreviation or neither.
                Some(unit) => (format!("unit of {} ({unit})", self.x_name), 1.0),
                None => (format!("unit of {}", self.x_name), 1.0),
            },
        }
    }

    /// A Y value as the reading writes it: three significant digits in Y's numbers,
    /// a duration in its units, then Y's unit.
    fn y_value(&self, v: f64, signed: bool) -> String {
        let mut text = self.y_number(v, signed);
        if let Some(unit) = &self.y_unit {
            text.push(' ');
            text.push_str(unit);
        }
        text
    }

    /// A Y value without Y's unit, for a range that names it once.
    fn y_number(&self, v: f64, signed: bool) -> String {
        let mut text = if self.y.kind == XAxisTemporalKind::Duration {
            let step = 10f64.powi(v.abs().log10().floor() as i32 - 2);
            crate::widgets::ticks::elapsed_tick(v, if step.is_finite() { step } else { 1.0 })
        } else {
            significant(v, 3, &self.y.numbers)
        };
        if signed && v > 0.0 {
            text.insert(0, '+');
        }
        text
    }

    /// A length of X, `t` X units: on a time X in the largest length of time it holds
    /// one of (`3.2 days`); otherwise in X's unit (`857 mi`) or `857 units of distance`.
    fn x_length(&self, t: f64) -> String {
        match per_second(self.x.kind) {
            Some(per) => {
                let seconds = t / per;
                let (name, unit) = TIME_UNITS
                    .iter()
                    .rev()
                    .find(|(_, s)| seconds >= *s)
                    .copied()
                    .unwrap_or(TIME_UNITS[0]);
                let n = seconds / unit;
                let plural = if (n - 1.0).abs() < 0.05 { "" } else { "s" };
                format!(
                    "{} {name}{plural}",
                    significant(n, 2, &AxisNumbers::default())
                )
            }
            None => {
                let n = significant(t, 3, &self.x.numbers.clone().fractional());
                match &self.x_unit {
                    Some(unit) => format!("{n} {unit}"),
                    None if (t - 1.0).abs() < 0.005 => format!("{n} unit of {}", self.x_name),
                    None => format!("{n} units of {}", self.x_name),
                }
            }
        }
    }

    /// The point a time X's line is read from: the fit's first X, as the axis writes it.
    fn x_value(&self, x: f64) -> String {
        crate::widgets::crosshair::format_x(x, self.x.kind, &self.x.numbers)
    }
}

/// `v` to `digits` significant digits in `numbers`' separators.
pub fn significant(v: f64, digits: i32, numbers: &AxisNumbers) -> String {
    if !v.is_finite() || v == 0.0 {
        return numbers
            .clone()
            .fractional()
            .write(if v.is_finite() { 0.0 } else { v });
    }
    let magnitude = v.abs().log10().floor() as i32;
    let places = (digits - 1 - magnitude).clamp(0, 12);
    let scale = 10f64.powi(places);
    let rounded = (v * scale).round() / scale;
    AxisNumbers {
        format: crate::numfmt::NumberFormat {
            float_precision: Some(places as u8),
            ..numbers.format.clone()
        },
        whole: false,
    }
    .write(rounded)
}

/// The plain reading of `fits`, as the line under the plot says it, in parts a
/// narrow line drops from the right: a fit's slope or growth, how strong, and how far
/// points typically fall from the line; several fits counted, with the range of their
/// slopes (the crosshair reads each). `None` with nothing fitted.
pub fn reading(
    fits: &TrendFits,
    words: &Words<'_>,
    g: &crate::glyphs::Glyphs,
) -> Option<Vec<String>> {
    if let Some(why) = &fits.failed {
        return Some(vec![format!("could not fit: {why}")]);
    }
    match fits.fits.as_slice() {
        [] => None,
        [fit] => Some(one_reading(fit, fits.key.trend, words, g)),
        many => Some(several_reading(many, words)),
    }
}

/// Several fits: how many, and the range of what each says.
fn several_reading(fits: &[Fit], words: &Words<'_>) -> Vec<String> {
    let mut parts = vec![format!("{} fits", fits.len())];
    let span = fits
        .iter()
        .fold([f64::INFINITY, f64::NEG_INFINITY], |[lo, hi], f| {
            [lo.min(f.x_range[0]), hi.max(f.x_range[1])]
        });
    let (per, units) = words.per(span);
    let range = |values: Vec<f64>, write: &dyn Fn(f64) -> String| {
        let lo = values.iter().copied().fold(f64::INFINITY, f64::min);
        let hi = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let (lo, hi) = (write(lo), write(hi));
        if lo == hi {
            lo
        } else {
            format!("{lo} to {hi}")
        }
    };
    let models: Vec<&Model> = fits.iter().map(|f| &f.model).collect();
    match models[0] {
        Model::Line { .. } => {
            let slopes = models
                .iter()
                .filter_map(|m| match m {
                    Model::Line { slope, .. } => Some(slope * units),
                    _ => None,
                })
                .collect();
            let values = range(slopes, &|v| words.y_number(v, true));
            let unit = words
                .y_unit
                .as_ref()
                .map_or(String::new(), |unit| format!(" {unit}"));
            parts.push(format!("slopes {values}{unit} per {per}"));
        }
        Model::Exp { .. } => {
            let changes = models
                .iter()
                .filter_map(|m| match m {
                    Model::Exp { rate, .. } => Some(((rate * units).exp() - 1.0) * 100.0),
                    _ => None,
                })
                .collect();
            let values = range(changes, &|v| format!("{}%", signed(v, 2)));
            parts.push(format!("{values} per {per}"));
        }
        Model::Power { .. } => {
            let powers = models
                .iter()
                .filter_map(|m| match m {
                    Model::Power { power, .. } => Some(*power),
                    _ => None,
                })
                .collect();
            let values = range(powers, &|v| significant(v, 3, &AxisNumbers::default()));
            parts.push(format!("powers {values}"));
        }
        Model::Curve(_) | Model::Identity => {}
    }
    parts
}

/// `v` to `digits` significant digits, with its sign.
fn signed(v: f64, digits: i32) -> String {
    let text = significant(v, digits, &AxisNumbers::default());
    if v > 0.0 { format!("+{text}") } else { text }
}

/// The mark when Pearson and Spearman disagree on a linear fit: the points are
/// curved or have outliers. With several fits, it names the series.
pub fn rank_mark(fits: &TrendFits, names: &[String], g: &crate::glyphs::Glyphs) -> Option<String> {
    if fits.key.trend != Trend::Linear {
        return None;
    }
    let fit = fits.fits.iter().find(|f| f.ranks_disagree())?;
    let of = match fit.series.and_then(|i| names.get(i)) {
        Some(name) if fits.fits.len() > 1 => format!(" for {name}"),
        _ => String::new(),
    };
    Some(format!(
        "{} r{}{}{of}: try robust",
        g.warning, g.not_equal, g.rho
    ))
}

fn strength(fit: &Fit, g: &crate::glyphs::Glyphs) -> Option<String> {
    fit.r.is_finite().then(|| {
        let r2 = format!("{} {:.2}", g.r_squared, fit.r_squared());
        match crate::widgets::analysis::correlation_strength(fit.r) {
            "none" => format!("no clear trend, {r2}"),
            word => format!("{word}, {r2}"),
        }
    })
}

/// An exponential's rate as a doubling or halving and the change per `per`; `flat`
/// with none.
fn growth(rate: f64, words: &Words<'_>, x_range: [f64; 2]) -> Vec<String> {
    let (per, units) = words.per(x_range);
    let change = ((rate * units).exp() - 1.0) * 100.0;
    if !rate.is_finite() || rate == 0.0 || !change.is_finite() {
        return vec!["flat".to_string()];
    }
    let verb = if rate > 0.0 { "doubles" } else { "halves" };
    vec![
        format!(
            "{verb} every {}",
            words.x_length(std::f64::consts::LN_2 / rate.abs())
        ),
        format!("{}% per {per}", signed(change, 2)),
    ]
}

fn one_reading(
    fit: &Fit,
    trend: Trend,
    words: &Words<'_>,
    g: &crate::glyphs::Glyphs,
) -> Vec<String> {
    let pm = g.plus_minus;
    let typically = |sd: String| format!("typically {pm}{sd} off the line");
    // Off a curve of ln y, a factor: e^sd above or below the line.
    let within = |sd: f64| {
        format!(
            "typically within {}{} of the line",
            g.times,
            significant(sd.exp(), 3, &AxisNumbers::default())
        )
    };
    let mut parts: Vec<String> = Vec::new();
    match (&fit.model, trend) {
        (Model::Line { slope, .. }, Trend::Robust) => {
            let (per, units) = words.per(fit.x_range);
            parts.push(format!("{} per {per}", words.y_value(slope * units, true)));
            parts.push(format!(
                "half the points within {pm}{} of it",
                words.y_value(fit.spread, false)
            ));
        }
        (Model::Line { slope, .. }, _) => {
            let (per, units) = words.per(fit.x_range);
            parts.push(format!("{} per {per}", words.y_value(slope * units, true)));
            parts.extend(strength(fit, g));
            parts.push(typically(words.y_value(fit.resid_sd, false)));
        }
        (Model::Exp { rate, .. }, _) => {
            parts.extend(growth(*rate, words, fit.x_range));
            parts.extend(strength(fit, g));
            parts.push(within(fit.resid_sd));
        }
        (Model::Power { power, .. }, _) => {
            let plain = AxisNumbers::default();
            parts.push(format!("y ~ x^{}", significant(*power, 3, &plain)));
            parts.push(format!(
                "1% more {}, about {}% more {}",
                words.x_name,
                significant(*power, 2, &plain),
                words.y_name
            ));
            parts.extend(strength(fit, g));
            parts.push(within(fit.resid_sd));
        }
        (Model::Curve(_), _) => {
            parts.push("local curve, each point from its nearest 2/3 of the data".to_string());
            parts.push(typically(words.y_value(fit.resid_sd, false)));
        }
        (Model::Identity, _) => {
            parts.push(format!(
                "{}% of points above y = x",
                (fit.above * 100.0).round()
            ));
            parts.push(typically(words.y_value(fit.resid_sd, false)));
        }
    }
    parts
}

/// A fit's slope as the crosshair says it: `+2.3 fps per day`, an exponential's
/// growth `+24% per day`; `None` for a power or LOWESS, whose slope a number does not
/// say.
pub fn slope_text(fit: &Fit, words: &Words<'_>) -> Option<String> {
    let (per, units) = words.per(fit.x_range);
    match fit.model {
        Model::Line { slope, .. } => {
            Some(format!("{} per {per}", words.y_value(slope * units, true)))
        }
        Model::Exp { rate, .. } => Some(growth(rate, words, fit.x_range).pop()?),
        _ => None,
    }
}

/// What was fitted, for the notes: `fit on 4,812 · 188 null`, `fit on 54 means`, the
/// points left out for a value at 0 or less, series too small to fit, and Theil–Sen's
/// pairs when drawn at random, with their seed unless the notes say it already
/// (`seed_said`: the chart's sample is drawn with the same one).
pub fn notes(
    fits: &TrendFits,
    nulls: usize,
    points: Option<&'static str>,
    seed_said: bool,
    g: &crate::glyphs::Glyphs,
) -> Vec<String> {
    let middot = g.middot;
    let mut notes = Vec::new();
    let group = crate::numfmt::group_chrome;
    let used: usize = fits.fits.iter().map(|f| f.used).sum();
    if used > 0 && fits.key.trend != Trend::Identity {
        let mut note = match points {
            Some(points) => format!("fit on {} {points}", group(used)),
            None => format!("fit on {}", group(used)),
        };
        if nulls > 0 && points.is_none() {
            note.push_str(&format!(" {middot} {} null", group(nulls)));
        }
        notes.push(note);
    }
    // Worded as the plot's own left-out notes, so the same count on a log axis
    // reads the same and is said once.
    let (x, all) = fits
        .fits
        .iter()
        .fold((0, 0), |(x, all), f| (x + f.dropped_x, all + f.dropped));
    notes.extend(
        crate::chart::chart_plot::LeftOut {
            x,
            y: all - x,
            counted: crate::chart::chart_plot::Counted::Points,
        }
        .notes(g.at_most),
    );
    if fits.too_few > 0 {
        notes.push(format!(
            "{} series under {MIN_POINTS} points not fitted",
            group(fits.too_few)
        ));
    }
    if let Some(seed) = fits.seed {
        let pairs = fits
            .fits
            .iter()
            .filter_map(|f| f.pairs)
            .fold((0, 0), |(u, o), p| (u + p.used, o + p.of));
        let mut note = format!(
            "slopes of {} of {} pairs",
            crate::home::discover::format_rows(pairs.0),
            crate::home::discover::format_rows(pairs.1)
        );
        if !seed_said {
            note.push_str(&format!(" {middot} seed {seed}"));
        }
        notes.push(note);
    }
    notes
}

/// A fit's equation: `y = 2.31x + 40.2`; over a time X, where an intercept at the
/// epoch says nothing, its value where it starts and its slope (`y = 112 at
/// 2024-01-01, +28.9 per month`). `None` for LOWESS.
pub fn equation(fit: &Fit, words: &Words<'_>) -> Option<String> {
    let plain = AxisNumbers::default();
    let n = |v: f64| significant(v, 3, &plain);
    let time = per_second(words.x.kind).is_some();
    match &fit.model {
        Model::Line { .. } | Model::Exp { .. } if time => {
            let x0 = fit.x_range[0];
            Some(format!(
                "y = {} at {}, {}",
                words.y_value(fit.model.at(x0), false),
                words.x_value(x0),
                slope_text(fit, words)?
            ))
        }
        Model::Line { x0, y0, slope } => {
            let b = y0 - slope * x0;
            let sign = if b < 0.0 { "-" } else { "+" };
            Some(format!("y = {}x {sign} {}", n(*slope), n(b.abs())))
        }
        Model::Exp { x0, ln_y0, rate } => {
            let a = (ln_y0 - rate * x0).exp();
            Some(format!("y = {}e^({}x)", n(a), n(*rate)))
        }
        Model::Power {
            ln_x0,
            ln_y0,
            power,
        } => {
            let c = (ln_y0 - power * ln_x0).exp();
            Some(format!("y = {}x^{}", n(c), n(*power)))
        }
        Model::Identity => Some("y = x".to_string()),
        Model::Curve(_) => None,
    }
}

/// The fit as text to copy: per fit, its series, equation, slope, R² and its
/// p-value, the residual standard deviation, the points used and left out, and the
/// seed of a sample; the notes it was drawn with first.
pub fn copy_text(
    fits: &TrendFits,
    names: &[String],
    words: &Words<'_>,
    header: &[String],
) -> String {
    let mut out = String::new();
    let method = match fits.key.trend {
        Trend::Linear => "linear (least squares)",
        Trend::Exponential => "exponential (least squares on ln y)",
        Trend::Power => "power (least squares on ln y against ln x)",
        Trend::Robust => {
            "robust (Theil-Sen; intercept the median of y - slope*x, as scipy's method='joint')"
        }
        Trend::Local => "local (LOWESS, 2/3 of the points, 3 robustness passes)",
        Trend::Identity => "y = x",
        Trend::Off => "off",
    };
    out.push_str(&format!("Trend: {method}\n"));
    for line in header {
        out.push_str(line);
        out.push('\n');
    }
    let plain = AxisNumbers::default();
    let n = |v: f64| significant(v, 4, &plain);
    for fit in &fits.fits {
        out.push('\n');
        let name = match fit.series {
            Some(i) => names.get(i).cloned().unwrap_or_default(),
            None => "all points".to_string(),
        };
        out.push_str(&format!("{name}\n"));
        if let Some(eq) = equation(fit, words) {
            out.push_str(&format!("  equation: {eq}\n"));
        }
        // Over a time X the equation says the slope already.
        if let Some(slope) = fit.model.slope_at(fit.x_range[0])
            && matches!(fit.model, Model::Line { .. })
            && per_second(words.x.kind).is_none()
        {
            let (per, units) = words.per(fit.x_range);
            out.push_str(&format!(
                "  slope: {} per {per}\n",
                words.y_value(slope * units, true)
            ));
        }
        if fit.r.is_finite() {
            let p = crate::analysis::statistics::compute_correlation_p_value(fit.r, fit.used);
            out.push_str(&format!(
                "  R squared: {:.4} (r {:.4}, p {})\n",
                fit.r_squared(),
                fit.r,
                if p < 1e-4 {
                    "< 0.0001".to_string()
                } else {
                    format!("{p:.4}")
                }
            ));
        }
        if fit.rho.is_finite() {
            out.push_str(&format!("  Spearman rho: {:.4}", fit.rho));
            if let Some(of) = fit.rho_of {
                out.push_str(&format!(
                    " (of {} points, an even stride)",
                    crate::numfmt::group_chrome(of)
                ));
            }
            out.push('\n');
        }
        if fit.resid_sd.is_finite() {
            let sd = match fit.model {
                Model::Exp { .. } | Model::Power { .. } => {
                    format!("{} (of ln y)", n(fit.resid_sd))
                }
                _ => words.y_value(fit.resid_sd, false),
            };
            out.push_str(&format!("  residual SD: {sd}\n"));
        }
        if fit.spread.is_finite() {
            out.push_str(&format!(
                "  median distance from the line: {}\n",
                words.y_value(fit.spread, false)
            ));
        }
        if fit.above.is_finite() {
            out.push_str(&format!("  above the line: {:.1}%\n", fit.above * 100.0));
        }
        let mut used = format!("  points: {} used", crate::numfmt::group_chrome(fit.used));
        if fit.dropped > 0 {
            used.push_str(&format!(
                ", {} at 0 or less left out",
                crate::numfmt::group_chrome(fit.dropped)
            ));
        }
        out.push_str(&used);
        out.push('\n');
        if let Some(pairs) = fit.pairs {
            out.push_str(&format!(
                "  pairs: {} of {}{}\n",
                crate::numfmt::group_chrome(pairs.used),
                crate::numfmt::group_chrome(pairs.of),
                if pairs.sampled {
                    ", drawn at random"
                } else {
                    ""
                }
            ));
        }
    }
    if let Some(seed) = fits.seed {
        out.push_str(&format!("\npair seed: {seed}\n"));
    }
    out
}

/// What a point is under `aggregate`, plural, for `fit on 54 means`.
pub fn points_of(aggregate: crate::chart::chart_modal::Aggregate) -> Option<&'static str> {
    use crate::chart::chart_modal::Aggregate;
    Some(match aggregate {
        Aggregate::None => return None,
        Aggregate::Count => "counts",
        Aggregate::Distinct => "distinct counts",
        Aggregate::Sum => "sums",
        Aggregate::Mean => "means",
        Aggregate::Median => "medians",
        Aggregate::Stdev => "deviations",
        Aggregate::Quantile => "quantiles",
        Aggregate::Min => "minimums",
        Aggregate::Max => "maximums",
        Aggregate::First => "first values",
        Aggregate::Last => "last values",
    })
}

#[cfg(test)]
mod tests;
