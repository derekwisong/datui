//! Fitting a column to each candidate distribution and testing each fit. One parameter
//! set per family, estimated once, serves the test, Q-Q plot and histogram overlay. The
//! test is Kolmogorov-Smirnov calibrated by parametric bootstrap (the statistic ranked
//! among those of samples drawn from the fit and refitted), so it is a valid p-value
//! with estimated parameters and for discrete families. A p-value is not the
//! probability of the model, and the largest is not the best: among unrejected
//! families, the lowest AIC wins.

use crate::statistics::DistributionType;

/// Every family tested, in the order a tie is listed.
pub const FAMILIES: [DistributionType; 14] = [
    DistributionType::Normal,
    DistributionType::LogNormal,
    DistributionType::Uniform,
    DistributionType::Exponential,
    DistributionType::Gamma,
    DistributionType::ChiSquared,
    DistributionType::Beta,
    DistributionType::StudentsT,
    DistributionType::Weibull,
    DistributionType::PowerLaw,
    DistributionType::Poisson,
    DistributionType::Bernoulli,
    DistributionType::Binomial,
    DistributionType::Geometric,
];

/// Simulated samples per test. The smallest p-value it can give is 1/200.
pub const REPLICATES: usize = 199;

/// The most values a test is run on. The bootstrap costs this times [`REPLICATES`]
/// fits per family; past a few hundred values, a KS test rejects any real-world data
/// anyway, which says more about the sample size than about the fit.
pub const TEST_VALUES: usize = 500;

/// A distribution with its parameters, as fitted to a column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Fitted {
    Normal {
        mean: f64,
        sd: f64,
    },
    LogNormal {
        mu: f64,
        sigma: f64,
    },
    Uniform {
        low: f64,
        high: f64,
    },
    Exponential {
        rate: f64,
    },
    Gamma {
        shape: f64,
        scale: f64,
    },
    ChiSquared {
        df: f64,
    },
    Beta {
        alpha: f64,
        beta: f64,
    },
    StudentsT {
        df: f64,
        location: f64,
        scale: f64,
    },
    Weibull {
        shape: f64,
        scale: f64,
    },
    PowerLaw {
        xmin: f64,
        alpha: f64,
    },
    Poisson {
        rate: f64,
    },
    Bernoulli {
        p: f64,
    },
    Binomial {
        trials: u64,
        p: f64,
    },
    /// Failures before the first success when `start` is 0; trials up to and including
    /// it when `start` is 1.
    Geometric {
        p: f64,
        start: u64,
    },
}

/// How a family fared against a column.
#[derive(Debug, Clone, PartialEq)]
pub enum FitOutcome {
    Tested(FitTest),
    /// The family cannot describe these values, and why: `needs positive values`.
    NotApplicable(&'static str),
}

impl FitOutcome {
    pub fn p_value(&self) -> Option<f64> {
        match self {
            Self::Tested(test) => Some(test.p_value),
            Self::NotApplicable(_) => None,
        }
    }

    pub fn test(&self) -> Option<&FitTest> {
        match self {
            Self::Tested(test) => Some(test),
            Self::NotApplicable(_) => None,
        }
    }
}

/// A fit and its test.
#[derive(Debug, Clone, PartialEq)]
pub struct FitTest {
    /// Fitted to every value read, and the parameters every view of this family uses.
    pub fitted: Fitted,
    /// The bootstrap p-value: `(1 + beyond) / (1 + replicates)`.
    pub p_value: f64,
    /// Simulated samples whose statistic was at least the column's.
    pub beyond: usize,
    pub replicates: usize,
    /// Values the test was run on.
    pub tested_on: usize,
    /// On every value read; lower is better, and only comparable within one kind
    /// (densities or probabilities).
    pub aic: f64,
}

impl FitTest {
    /// Whether no simulated sample reached the column's statistic, so the p-value is
    /// only a bound: `< 0.005`, not `0.005`.
    pub fn at_bound(&self) -> bool {
        self.beyond == 0
    }
}

impl Fitted {
    pub fn family(&self) -> DistributionType {
        match self {
            Self::Normal { .. } => DistributionType::Normal,
            Self::LogNormal { .. } => DistributionType::LogNormal,
            Self::Uniform { .. } => DistributionType::Uniform,
            Self::Exponential { .. } => DistributionType::Exponential,
            Self::Gamma { .. } => DistributionType::Gamma,
            Self::ChiSquared { .. } => DistributionType::ChiSquared,
            Self::Beta { .. } => DistributionType::Beta,
            Self::StudentsT { .. } => DistributionType::StudentsT,
            Self::Weibull { .. } => DistributionType::Weibull,
            Self::PowerLaw { .. } => DistributionType::PowerLaw,
            Self::Poisson { .. } => DistributionType::Poisson,
            Self::Bernoulli { .. } => DistributionType::Bernoulli,
            Self::Binomial { .. } => DistributionType::Binomial,
            Self::Geometric { .. } => DistributionType::Geometric,
        }
    }

    /// Whether this is a distribution over whole numbers.
    pub fn discrete(&self) -> bool {
        matches!(
            self,
            Self::Poisson { .. }
                | Self::Bernoulli { .. }
                | Self::Binomial { .. }
                | Self::Geometric { .. }
        )
    }

    fn parameters(&self) -> usize {
        match self {
            Self::Exponential { .. }
            | Self::ChiSquared { .. }
            | Self::Poisson { .. }
            | Self::Bernoulli { .. } => 1,
            // The start is read off the values, as the minimum is for a power law.
            Self::Geometric { .. } => 2,
            Self::StudentsT { .. } => 3,
            _ => 2,
        }
    }

    /// `P(X <= x)`.
    pub fn cdf(&self, x: f64) -> f64 {
        match *self {
            Self::Normal { mean, sd } => normal_cdf((x - mean) / sd),
            Self::LogNormal { mu, sigma } => {
                if x <= 0.0 {
                    0.0
                } else {
                    normal_cdf((x.ln() - mu) / sigma)
                }
            }
            Self::Uniform { low, high } => ((x - low) / (high - low)).clamp(0.0, 1.0),
            Self::Exponential { rate } => {
                if x <= 0.0 {
                    0.0
                } else {
                    -(-rate * x).exp_m1()
                }
            }
            Self::Gamma { shape, scale } => {
                if x <= 0.0 {
                    0.0
                } else {
                    gamma_p(shape, x / scale)
                }
            }
            Self::ChiSquared { df } => {
                if x <= 0.0 {
                    0.0
                } else {
                    gamma_p(df / 2.0, x / 2.0)
                }
            }
            Self::Beta { alpha, beta } => {
                if x <= 0.0 {
                    0.0
                } else if x >= 1.0 {
                    1.0
                } else {
                    beta_inc(alpha, beta, x)
                }
            }
            Self::StudentsT {
                df,
                location,
                scale,
            } => t_cdf((x - location) / scale, df),
            Self::Weibull { shape, scale } => {
                if x <= 0.0 {
                    0.0
                } else {
                    -(-(x / scale).powf(shape)).exp_m1()
                }
            }
            Self::PowerLaw { xmin, alpha } => {
                if x < xmin {
                    0.0
                } else {
                    1.0 - (x / xmin).powf(1.0 - alpha)
                }
            }
            Self::Poisson { rate } => {
                if x < 0.0 {
                    0.0
                } else {
                    // P(X <= k) = Q(k + 1, rate), the upper regularized gamma.
                    gamma_q(x.floor() + 1.0, rate)
                }
            }
            Self::Bernoulli { p } => {
                if x < 0.0 {
                    0.0
                } else if x < 1.0 {
                    1.0 - p
                } else {
                    1.0
                }
            }
            Self::Binomial { trials, p } => {
                let k = x.floor();
                if k < 0.0 {
                    0.0
                } else if k >= trials as f64 {
                    1.0
                } else {
                    // P(X <= k) = I_{1-p}(n - k, k + 1).
                    beta_inc(trials as f64 - k, k + 1.0, 1.0 - p)
                }
            }
            Self::Geometric { p, start } => {
                let k = x.floor() - start as f64;
                if k < 0.0 {
                    0.0
                } else {
                    -((k + 1.0) * (-p).ln_1p()).exp_m1()
                }
            }
        }
    }

    /// `P(X < x)`: the same as [`Self::cdf`] where no single value carries weight.
    pub fn cdf_below(&self, x: f64) -> f64 {
        if !self.discrete() {
            return self.cdf(x);
        }
        if x == x.floor() {
            self.cdf(x - 1.0)
        } else {
            self.cdf(x)
        }
    }

    /// The smallest `x` with `P(X <= x) >= p`.
    pub fn quantile(&self, p: f64) -> f64 {
        if p.is_nan() {
            return f64::NAN;
        }
        match *self {
            Self::Normal { mean, sd } => mean + sd * normal_quantile(p),
            Self::LogNormal { mu, sigma } => (mu + sigma * normal_quantile(p)).exp(),
            Self::Uniform { low, high } => low + (high - low) * p.clamp(0.0, 1.0),
            Self::Exponential { rate } => -(-p).ln_1p() / rate,
            Self::Weibull { shape, scale } => scale * (-(-p).ln_1p()).powf(1.0 / shape),
            Self::PowerLaw { xmin, alpha } => xmin * (1.0 - p).powf(-1.0 / (alpha - 1.0)),
            Self::StudentsT {
                df,
                location,
                scale,
            } => location + scale * t_quantile(p, df),
            Self::Gamma { .. } | Self::ChiSquared { .. } | Self::Beta { .. } => {
                self.continuous_quantile_by_search(p)
            }
            Self::Poisson { .. }
            | Self::Bernoulli { .. }
            | Self::Binomial { .. }
            | Self::Geometric { .. } => self.discrete_quantile(p),
        }
    }

    /// Bisection on the CDF, for the families with no closed-form inverse.
    fn continuous_quantile_by_search(&self, p: f64) -> f64 {
        if p <= 0.0 {
            return match self {
                Self::Beta { .. } => 0.0,
                _ => 0.0,
            };
        }
        if p >= 1.0 {
            return match self {
                Self::Beta { .. } => 1.0,
                _ => f64::INFINITY,
            };
        }
        let (mut low, mut high) = match self {
            Self::Beta { .. } => (0.0, 1.0),
            _ => {
                let mut high = 1.0;
                while self.cdf(high) < p && high < 1e300 {
                    high *= 2.0;
                }
                (0.0, high)
            }
        };
        for _ in 0..200 {
            let mid = 0.5 * (low + high);
            if self.cdf(mid) < p {
                low = mid;
            } else {
                high = mid;
            }
            if high - low <= 1e-12 * high.abs().max(1e-300) {
                break;
            }
        }
        0.5 * (low + high)
    }

    /// The smallest whole number whose CDF reaches `p`, by bisection from a guess (log of
    /// the spread in evaluations; stepping one at a time was a million steps per draw for
    /// large geometric fits).
    fn discrete_quantile(&self, p: f64) -> f64 {
        let start = match *self {
            Self::Geometric { start, .. } => start as f64,
            _ => 0.0,
        };
        if p <= 0.0 {
            return start;
        }
        if p >= 1.0 {
            return match *self {
                Self::Bernoulli { .. } => 1.0,
                Self::Binomial { trials, .. } => trials as f64,
                _ => f64::INFINITY,
            };
        }
        let (guess, spread) = match *self {
            // The geometric's own inverse, exact up to rounding.
            Self::Geometric { p: q, .. } => {
                let k = ((-p).ln_1p() / (-q).ln_1p()).ceil() - 1.0;
                (start + k.max(0.0), 1.0)
            }
            Self::Poisson { rate } => (rate + rate.sqrt() * normal_quantile(p), rate.sqrt()),
            Self::Binomial { trials, p: q } => {
                let n = trials as f64;
                let sd = (n * q * (1.0 - q)).sqrt();
                (n * q + sd * normal_quantile(p), sd)
            }
            _ => (start, 1.0),
        };
        let top = match *self {
            Self::Bernoulli { .. } => 1.0,
            Self::Binomial { trials, .. } => trials as f64,
            _ => 1e15,
        };
        let guess = guess.floor().clamp(start, top);
        let step = spread.max(1.0).ceil();
        // The answer lies in (low, high]: cdf(low) < p <= cdf(high), with `low` below
        // the support when nothing in it falls short.
        let (mut low, mut high);
        if self.cdf(guess) >= p {
            high = guess;
            low = start - 1.0;
            let mut step = step;
            while high > start {
                let next = (high - step).max(start);
                if self.cdf(next) < p {
                    low = next;
                    break;
                }
                high = next;
                step *= 2.0;
            }
        } else {
            low = guess;
            high = top;
            let mut step = step;
            while low < top {
                let next = (low + step).min(top);
                if self.cdf(next) >= p {
                    high = next;
                    break;
                }
                low = next;
                step *= 2.0;
            }
        }
        while high - low > 1.0 {
            let mid = (low + (high - low) / 2.0).floor();
            if self.cdf(mid) >= p {
                high = mid;
            } else {
                low = mid;
            }
        }
        high
    }

    /// `ln` of the density, or of the probability at a whole number for a discrete
    /// family; `-inf` outside the support.
    fn ln_density(&self, x: f64) -> f64 {
        match *self {
            Self::Normal { mean, sd } => {
                let z = (x - mean) / sd;
                -0.5 * z * z - sd.ln() - 0.5 * LN_2PI
            }
            Self::LogNormal { mu, sigma } => {
                if x <= 0.0 {
                    return f64::NEG_INFINITY;
                }
                let z = (x.ln() - mu) / sigma;
                -0.5 * z * z - sigma.ln() - x.ln() - 0.5 * LN_2PI
            }
            Self::Uniform { low, high } => {
                if x < low || x > high {
                    f64::NEG_INFINITY
                } else {
                    -(high - low).ln()
                }
            }
            Self::Exponential { rate } => {
                if x < 0.0 {
                    f64::NEG_INFINITY
                } else {
                    rate.ln() - rate * x
                }
            }
            Self::Gamma { shape, scale } => gamma_ln_pdf(x, shape, scale),
            Self::ChiSquared { df } => gamma_ln_pdf(x, df / 2.0, 2.0),
            Self::Beta { alpha, beta } => {
                if x <= 0.0 || x >= 1.0 {
                    return f64::NEG_INFINITY;
                }
                (alpha - 1.0) * x.ln() + (beta - 1.0) * (-x).ln_1p() - ln_beta(alpha, beta)
            }
            Self::StudentsT {
                df,
                location,
                scale,
            } => {
                let t = (x - location) / scale;
                ln_gamma((df + 1.0) / 2.0)
                    - ln_gamma(df / 2.0)
                    - 0.5 * (df * std::f64::consts::PI).ln()
                    - scale.ln()
                    - (df + 1.0) / 2.0 * (t * t / df).ln_1p()
            }
            Self::Weibull { shape, scale } => {
                if x < 0.0 {
                    return f64::NEG_INFINITY;
                }
                let z = x / scale;
                shape.ln() - scale.ln() + (shape - 1.0) * z.ln() - z.powf(shape)
            }
            Self::PowerLaw { xmin, alpha } => {
                if x < xmin {
                    return f64::NEG_INFINITY;
                }
                (alpha - 1.0).ln() - xmin.ln() - alpha * (x / xmin).ln()
            }
            Self::Poisson { rate } => {
                if x < 0.0 || x != x.floor() {
                    return f64::NEG_INFINITY;
                }
                x * rate.ln() - rate - ln_gamma(x + 1.0)
            }
            Self::Bernoulli { p } => match x {
                0.0 => (-p).ln_1p(),
                1.0 => p.ln(),
                _ => f64::NEG_INFINITY,
            },
            Self::Binomial { trials, p } => {
                let n = trials as f64;
                if x < 0.0 || x > n || x != x.floor() {
                    return f64::NEG_INFINITY;
                }
                ln_gamma(n + 1.0) - ln_gamma(x + 1.0) - ln_gamma(n - x + 1.0)
                    + x * p.ln()
                    + (n - x) * (-p).ln_1p()
            }
            Self::Geometric { p, start } => {
                let k = x - start as f64;
                if k < 0.0 || x != x.floor() {
                    return f64::NEG_INFINITY;
                }
                p.ln() + k * (-p).ln_1p()
            }
        }
    }

    /// The density, or the probability at a whole number for a discrete family.
    pub fn density(&self, x: f64) -> f64 {
        self.ln_density(x).exp()
    }

    fn sample(&self, rng: &mut Rng) -> f64 {
        match *self {
            Self::Gamma { shape, scale } => rng.gamma(shape) * scale,
            Self::ChiSquared { df } => rng.gamma(df / 2.0) * 2.0,
            Self::Beta { alpha, beta } => {
                let a = rng.gamma(alpha);
                let b = rng.gamma(beta);
                a / (a + b)
            }
            Self::StudentsT {
                df,
                location,
                scale,
            } => {
                let z = rng.normal();
                let v = rng.gamma(df / 2.0) * 2.0;
                location + scale * z / (v / df).sqrt()
            }
            // Inverting the CDF costs a few dozen incomplete gammas or betas per draw,
            // each longer as the counts grow: seconds to minutes per test at counts in
            // the millions.
            Self::Poisson { rate } if rate >= 10.0 => rng.poisson(rate),
            Self::Binomial { trials, p } if trials as f64 * p.min(1.0 - p) >= 10.0 => {
                rng.binomial(trials, p)
            }
            _ => self.quantile(rng.uniform()),
        }
    }

    /// Estimate a family's parameters from `values`, or say why the family cannot
    /// describe them.
    pub fn fit(family: DistributionType, values: &[f64]) -> Result<Fitted, &'static str> {
        let n = values.len();
        if n < 5 {
            return Err("needs 5 or more values");
        }
        let nf = n as f64;
        let mean = values.iter().sum::<f64>() / nf;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (nf - 1.0);
        let min = values.iter().copied().fold(f64::INFINITY, f64::min);
        let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let counts = values.iter().all(|v| *v >= 0.0 && *v == v.floor());
        if variance <= 0.0 || !variance.is_finite() {
            return Err("needs values that vary");
        }
        let positive = || {
            if min > 0.0 {
                Ok(())
            } else {
                Err("needs positive values")
            }
        };
        let fitted = match family {
            DistributionType::Normal => Fitted::Normal {
                mean,
                sd: variance.sqrt(),
            },
            DistributionType::LogNormal => {
                positive()?;
                let logs: Vec<f64> = values.iter().map(|v| v.ln()).collect();
                let mu = logs.iter().sum::<f64>() / nf;
                let var = logs.iter().map(|v| (v - mu).powi(2)).sum::<f64>() / (nf - 1.0);
                Fitted::LogNormal {
                    mu,
                    sigma: var.sqrt(),
                }
            }
            DistributionType::Uniform => Fitted::Uniform {
                low: min,
                high: max,
            },
            DistributionType::Exponential => {
                if min < 0.0 {
                    return Err("needs values of zero or more");
                }
                Fitted::Exponential { rate: 1.0 / mean }
            }
            DistributionType::Gamma => {
                positive()?;
                // Minka's closed-form approximation to the maximum-likelihood shape.
                let mean_log = values.iter().map(|v| v.ln()).sum::<f64>() / nf;
                let s = mean.ln() - mean_log;
                if s <= 0.0 {
                    return Err("needs values that vary");
                }
                let shape = (3.0 - s + ((s - 3.0).powi(2) + 24.0 * s).sqrt()) / (12.0 * s);
                Fitted::Gamma {
                    shape,
                    scale: mean / shape,
                }
            }
            DistributionType::ChiSquared => {
                positive()?;
                Fitted::ChiSquared { df: mean }
            }
            DistributionType::Beta => {
                if min <= 0.0 || max >= 1.0 {
                    return Err("needs values strictly between 0 and 1");
                }
                let common = mean * (1.0 - mean) / variance - 1.0;
                if common <= 0.0 {
                    return Err("spread too wide for a beta");
                }
                // Moments to start, then maximum likelihood, as every family here is
                // fitted where it can be: AIC compares likelihoods, and a family
                // fitted by moments competes with a handicap.
                beta_mle(values, mean * common, (1.0 - mean) * common)
            }
            DistributionType::StudentsT => {
                let fourth = values.iter().map(|v| (v - mean).powi(4)).sum::<f64>() / nf;
                let excess = fourth / (variance * variance) - 3.0;
                if excess <= 0.0 {
                    return Err("tails no heavier than a normal's");
                }
                // Kurtosis 6 / (df - 4) above a normal's, for df above 4.
                let df = (4.0 + 6.0 / excess).clamp(2.5, 1_000.0);
                Fitted::StudentsT {
                    df,
                    location: mean,
                    scale: (variance * (df - 2.0) / df).sqrt(),
                }
            }
            DistributionType::Weibull => {
                positive()?;
                weibull_mle(values).ok_or("no weibull fits these values")?
            }
            DistributionType::PowerLaw => {
                positive()?;
                let sum_log = values.iter().map(|v| (v / min).ln()).sum::<f64>();
                if sum_log <= 0.0 {
                    return Err("needs values that vary");
                }
                Fitted::PowerLaw {
                    xmin: min,
                    alpha: 1.0 + nf / sum_log,
                }
            }
            DistributionType::Poisson => {
                if !counts {
                    return Err("needs counts: whole numbers, zero or more");
                }
                Fitted::Poisson { rate: mean }
            }
            DistributionType::Bernoulli => {
                if !values.iter().all(|v| *v == 0.0 || *v == 1.0) {
                    return Err("needs only 0 and 1");
                }
                Fitted::Bernoulli { p: mean }
            }
            DistributionType::Binomial => {
                if !counts {
                    return Err("needs counts: whole numbers, zero or more");
                }
                if variance >= mean {
                    return Err("spread too wide for a binomial");
                }
                // Moments, then at least as many trials as the largest count.
                let p = 1.0 - variance / mean;
                let trials = ((mean / p).round() as u64).max(max as u64).max(1);
                Fitted::Binomial {
                    trials,
                    p: (mean / trials as f64).clamp(1e-12, 1.0 - 1e-12),
                }
            }
            DistributionType::Geometric => {
                if !counts {
                    return Err("needs counts: whole numbers, zero or more");
                }
                // Counted from 1 (trials) when no zero appears, from 0 (failures) when
                // one does.
                let start = if min >= 1.0 { 1 } else { 0 };
                Fitted::Geometric {
                    p: 1.0 / (1.0 + mean - start as f64),
                    start,
                }
            }
            DistributionType::Constant | DistributionType::Unknown => {
                return Err("not a distribution");
            }
        };
        if fitted.valid() {
            Ok(fitted)
        } else {
            Err("no fit for these values")
        }
    }

    fn valid(&self) -> bool {
        let finite_positive = |x: f64| x.is_finite() && x > 0.0;
        match *self {
            Self::Normal { mean, sd } => mean.is_finite() && finite_positive(sd),
            Self::LogNormal { mu, sigma } => mu.is_finite() && finite_positive(sigma),
            Self::Uniform { low, high } => low.is_finite() && high.is_finite() && high > low,
            Self::Exponential { rate } => finite_positive(rate),
            Self::Gamma { shape, scale } => finite_positive(shape) && finite_positive(scale),
            Self::ChiSquared { df } => finite_positive(df),
            Self::Beta { alpha, beta } => finite_positive(alpha) && finite_positive(beta),
            Self::StudentsT {
                df,
                location,
                scale,
            } => finite_positive(df) && location.is_finite() && finite_positive(scale),
            Self::Weibull { shape, scale } => finite_positive(shape) && finite_positive(scale),
            Self::PowerLaw { xmin, alpha } => {
                finite_positive(xmin) && alpha.is_finite() && alpha > 1.0
            }
            Self::Poisson { rate } => finite_positive(rate),
            Self::Bernoulli { p } | Self::Geometric { p, .. } => p > 0.0 && p < 1.0,
            Self::Binomial { trials, p } => trials >= 1 && p > 0.0 && p < 1.0,
        }
    }
}

/// Maximum-likelihood Beta by Newton's method on both shapes, from `alpha` and `beta`:
/// `ψ(a) - ψ(a + b)` and `ψ(b) - ψ(a + b)` equal the mean logs of `x` and `1 - x`.
fn beta_mle(values: &[f64], alpha: f64, beta: f64) -> Fitted {
    let n = values.len() as f64;
    let mean_log = values.iter().map(|v| v.ln()).sum::<f64>() / n;
    let mean_log_rest = values.iter().map(|v| (-v).ln_1p()).sum::<f64>() / n;
    let (mut a, mut b) = (alpha, beta);
    for _ in 0..100 {
        let both = digamma(a + b);
        let g1 = digamma(a) - both - mean_log;
        let g2 = digamma(b) - both - mean_log_rest;
        let t = trigamma(a + b);
        let (j11, j22, j12) = (trigamma(a) - t, trigamma(b) - t, -t);
        let det = j11 * j22 - j12 * j12;
        if det.abs() < 1e-300 {
            break;
        }
        let da = (g1 * j22 - g2 * j12) / det;
        let db = (g2 * j11 - g1 * j12) / det;
        // Halved until both stay positive.
        let mut step = 1.0;
        while (a - step * da <= 0.0 || b - step * db <= 0.0) && step > 1e-6 {
            step /= 2.0;
        }
        let (next_a, next_b) = (a - step * da, b - step * db);
        let done = (next_a - a).abs() < 1e-10 * a && (next_b - b).abs() < 1e-10 * b;
        a = next_a;
        b = next_b;
        if done {
            break;
        }
    }
    Fitted::Beta { alpha: a, beta: b }
}

/// `ψ(x)`, the digamma function, for `x > 0`: shifted up past 6, then its asymptotic
/// series.
fn digamma(mut x: f64) -> f64 {
    let mut result = 0.0;
    while x < 6.0 {
        result -= 1.0 / x;
        x += 1.0;
    }
    let inv = 1.0 / x;
    let inv2 = inv * inv;
    result + x.ln()
        - 0.5 * inv
        - inv2 * (1.0 / 12.0 - inv2 * (1.0 / 120.0 - inv2 * (1.0 / 252.0 - inv2 / 240.0)))
}

/// `ψ'(x)`, the trigamma function, for `x > 0`.
fn trigamma(mut x: f64) -> f64 {
    let mut result = 0.0;
    while x < 6.0 {
        result += 1.0 / (x * x);
        x += 1.0;
    }
    let inv = 1.0 / x;
    let inv2 = inv * inv;
    result
        + inv
        + 0.5 * inv2
        + inv * inv2 * (1.0 / 6.0 - inv2 * (1.0 / 30.0 - inv2 * (1.0 / 42.0 - inv2 / 30.0)))
}

/// Maximum-likelihood Weibull by Newton's method on the shape.
fn weibull_mle(values: &[f64]) -> Option<Fitted> {
    let n = values.len() as f64;
    let logs: Vec<f64> = values.iter().map(|v| v.ln()).collect();
    let mean_log = logs.iter().sum::<f64>() / n;
    // Scaled so the powers do not overflow for large values.
    let top = values.iter().copied().fold(0.0, f64::max);
    let scaled: Vec<f64> = values.iter().map(|v| v / top).collect();
    let scaled_logs: Vec<f64> = scaled.iter().map(|v| v.ln()).collect();
    let mean_scaled_log = scaled_logs.iter().sum::<f64>() / n;
    let sd_log = (logs.iter().map(|l| (l - mean_log).powi(2)).sum::<f64>() / n).sqrt();
    // The shape a Gumbel on the logs suggests, as a start.
    let mut k = if sd_log > 0.0 { 1.2825 / sd_log } else { 1.0 };
    for _ in 0..100 {
        let (mut s0, mut s1, mut s2) = (0.0, 0.0, 0.0);
        for (x, l) in scaled.iter().zip(&scaled_logs) {
            let xk = x.powf(k);
            s0 += xk;
            s1 += xk * l;
            s2 += xk * l * l;
        }
        if s0 <= 0.0 {
            return None;
        }
        let f = s1 / s0 - 1.0 / k - mean_scaled_log;
        let df = (s2 * s0 - s1 * s1) / (s0 * s0) + 1.0 / (k * k);
        let step = f / df;
        let next = (k - step).clamp(k / 2.0, k * 2.0);
        let done = (next - k).abs() < 1e-10 * k;
        k = next;
        if done {
            break;
        }
    }
    let s0 = scaled.iter().map(|x| x.powf(k)).sum::<f64>();
    let scale = top * (s0 / n).powf(1.0 / k);
    Some(Fitted::Weibull { shape: k, scale })
}

/// The two-sided KS statistic of sorted `values` against `fitted`: the largest CDF gap
/// on either side of each step. Ties are one step, compared at and just below the value,
/// so discrete fits are measured at their jumps.
pub fn ks_statistic(sorted: &[f64], fitted: &Fitted) -> f64 {
    let n = sorted.len() as f64;
    let mut d: f64 = 0.0;
    let mut i = 0;
    while i < sorted.len() {
        let x = sorted[i];
        let mut j = i;
        while j < sorted.len() && sorted[j] == x {
            j += 1;
        }
        let below = i as f64 / n;
        let at = j as f64 / n;
        d = d
            .max((at - fitted.cdf(x)).abs())
            .max((fitted.cdf_below(x) - below).abs());
        i = j;
    }
    d
}

/// Test one family against `values`, all of them as read.
pub fn test_family(family: DistributionType, values: &[f64], seed: u64) -> FitOutcome {
    let fitted = match Fitted::fit(family, values) {
        Ok(fitted) => fitted,
        Err(reason) => return FitOutcome::NotApplicable(reason),
    };
    let ln_likelihood: f64 = values.iter().map(|v| fitted.ln_density(*v)).sum();
    let aic = 2.0 * fitted.parameters() as f64 - 2.0 * ln_likelihood;

    // The test runs on a seeded random subset, refitted: the bootstrap below refits
    // each simulated sample, and the column has to be treated the same way.
    let mut rng = Rng::new(seed ^ family as u64);
    let mut subset = values.to_vec();
    if subset.len() > TEST_VALUES {
        for i in 0..TEST_VALUES {
            let j = i + (rng.next_u64() % (subset.len() - i) as u64) as usize;
            subset.swap(i, j);
        }
        subset.truncate(TEST_VALUES);
    }
    subset.sort_by(f64::total_cmp);
    let Ok(sub_fit) = Fitted::fit(family, &subset) else {
        return FitOutcome::NotApplicable("no fit for these values");
    };
    let observed = ks_statistic(&subset, &sub_fit);

    let mut beyond = 0;
    let mut simulated = vec![0.0; subset.len()];
    for _ in 0..REPLICATES {
        for value in simulated.iter_mut() {
            *value = sub_fit.sample(&mut rng);
        }
        simulated.sort_by(f64::total_cmp);
        // A simulated sample the family cannot refit counts against the fit: the test
        // leans towards not rejecting rather than towards a p-value it did not earn.
        let statistic = match Fitted::fit(family, &simulated) {
            Ok(refit) => ks_statistic(&simulated, &refit),
            Err(_) => f64::INFINITY,
        };
        if statistic >= observed {
            beyond += 1;
        }
    }
    FitOutcome::Tested(FitTest {
        fitted,
        p_value: (1 + beyond) as f64 / (1 + REPLICATES) as f64,
        beyond,
        replicates: REPLICATES,
        tested_on: subset.len(),
        aic,
    })
}

/// Test every family, in parallel: each is independent, and each is a few hundred
/// thousand CDF evaluations.
pub fn test_all(values: &[f64], seed: u64) -> Vec<(DistributionType, FitOutcome)> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = FAMILIES
            .iter()
            .map(|family| scope.spawn(move || (*family, test_family(*family, values, seed))))
            .collect();
        handles
            .into_iter()
            .filter_map(|handle| handle.join().ok())
            .collect()
    })
}

/// Below this p-value a family is rejected. At 0.05 the true family of a column is
/// rejected one time in twenty by design, and a verdict that flips between samples of
/// the same data says more about the sample.
pub const REJECT_P: f64 = 0.01;

/// A family that contains a simpler one fits whatever the simpler one does: uniform
/// values are a Beta(1, 1). When the simpler one is not rejected, it is the answer.
fn nested_in(family: DistributionType) -> &'static [DistributionType] {
    match family {
        DistributionType::Beta => &[DistributionType::Uniform],
        DistributionType::Gamma => &[DistributionType::Exponential, DistributionType::ChiSquared],
        DistributionType::Weibull => &[DistributionType::Exponential],
        DistributionType::StudentsT => &[DistributionType::Normal],
        // A binomial of one trial is a Bernoulli; of many trials and a small p, a
        // Poisson.
        DistributionType::Binomial => &[DistributionType::Bernoulli, DistributionType::Poisson],
        _ => &[],
    }
}

/// The family the values are consistent with, or `Unknown` when all are rejected: the
/// lowest AIC among the unrejected, preferring a count distribution for counts (density
/// and probability are not on one scale).
pub fn select(results: &[(DistributionType, FitOutcome)], counts: bool) -> DistributionType {
    let held: Vec<&FitTest> = results
        .iter()
        .filter_map(|(_, outcome)| outcome.test())
        .filter(|test| test.p_value >= REJECT_P)
        .collect();
    let discrete_held = counts && held.iter().any(|test| test.fitted.discrete());
    let best = held
        .iter()
        .filter(|test| test.fitted.discrete() == discrete_held)
        .min_by(|a, b| a.aic.total_cmp(&b.aic));
    let Some(best) = best else {
        return DistributionType::Unknown;
    };
    let family = best.fitted.family();
    // The simpler of two nested families when it holds and the richer one is not
    // decisively better: an AIC gap of 10 or more is the usual line for "no support".
    // A test on a few hundred values can miss a t's tails that every value shows.
    nested_in(family)
        .iter()
        .find(|simpler| {
            held.iter()
                .any(|test| test.fitted.family() == **simpler && test.aic - best.aic < DECISIVE_AIC)
        })
        .copied()
        .unwrap_or(family)
}

/// An AIC gap at which the family behind has essentially no support.
const DECISIVE_AIC: f64 = 10.0;

/// The families as a list is read: tested ones by p-value, highest first, then the
/// ones that do not apply, in [`FAMILIES`] order.
pub fn listing_order(results: &[(DistributionType, FitOutcome)]) -> Vec<DistributionType> {
    let mut tested: Vec<(DistributionType, f64)> = results
        .iter()
        .filter_map(|(family, outcome)| outcome.p_value().map(|p| (*family, p)))
        .collect();
    tested.sort_by(|a, b| b.1.total_cmp(&a.1));
    let mut order: Vec<DistributionType> = tested.into_iter().map(|(family, _)| family).collect();
    let missing: Vec<DistributionType> = FAMILIES
        .iter()
        .filter(|family| !order.contains(family))
        .copied()
        .collect();
    order.extend(missing);
    order
}

/// Theoretical quantiles for a Q-Q plot of `n` sorted values: the fitted quantile at
/// each plotting position `i / (n + 1)`.
pub fn qq_quantiles(fitted: &Fitted, n: usize) -> Vec<f64> {
    (1..=n)
        .map(|i| fitted.quantile(i as f64 / (n as f64 + 1.0)))
        .collect()
}

const LN_2PI: f64 = 1.837_877_066_409_345_5;

/// `ln Γ(x)` for `x > 0`, by the Lanczos approximation (g = 7, nine terms), accurate
/// to about 1e-15.
pub fn ln_gamma(x: f64) -> f64 {
    const COEFFICIENTS: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        // Reflection, for the half-integers the t density needs near zero.
        let pi = std::f64::consts::PI;
        return (pi / (pi * x).sin()).ln() - ln_gamma(1.0 - x);
    }
    let x = x - 1.0;
    let mut sum = COEFFICIENTS[0];
    for (i, c) in COEFFICIENTS.iter().enumerate().skip(1) {
        sum += c / (x + i as f64);
    }
    let t = x + 7.5;
    0.5 * LN_2PI + (x + 0.5) * t.ln() - t + sum.ln()
}

fn ln_beta(a: f64, b: f64) -> f64 {
    ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b)
}

fn gamma_ln_pdf(x: f64, shape: f64, scale: f64) -> f64 {
    if x <= 0.0 {
        return f64::NEG_INFINITY;
    }
    (shape - 1.0) * x.ln() - x / scale - shape * scale.ln() - ln_gamma(shape)
}

/// The regularized lower incomplete gamma `P(a, x)`.
pub fn gamma_p(a: f64, x: f64) -> f64 {
    let (p, _) = incomplete_gamma(a, x);
    p
}

/// Its complement `Q(a, x) = 1 - P(a, x)`, computed directly: in the upper tail `P` is
/// within 1e-16 of 1, and `1 - P` would be noise.
pub fn gamma_q(a: f64, x: f64) -> f64 {
    let (_, q) = incomplete_gamma(a, x);
    q
}

/// `(P(a, x), Q(a, x))`: by the series below `a + 1`, by the continued fraction for
/// `Q` above (Numerical Recipes, `gser` and `gcf`), each side taken from whichever is
/// computed without cancellation.
fn incomplete_gamma(a: f64, x: f64) -> (f64, f64) {
    if x <= 0.0 {
        return (0.0, 1.0);
    }
    if !x.is_finite() {
        return (1.0, 0.0);
    }
    let ln_prefix = a * x.ln() - x - ln_gamma(a);
    if x < a + 1.0 {
        let mut term = 1.0 / a;
        let mut sum = term;
        let mut ap = a;
        for _ in 0..10_000 {
            ap += 1.0;
            term *= x / ap;
            sum += term;
            if term.abs() < sum.abs() * 1e-16 {
                break;
            }
        }
        let p = (sum.ln() + ln_prefix).exp().clamp(0.0, 1.0);
        (p, 1.0 - p)
    } else {
        // Lentz's method for the continued fraction of Q(a, x).
        let tiny = 1e-300;
        let mut b = x + 1.0 - a;
        let mut c = 1.0 / tiny;
        let mut d = 1.0 / b;
        let mut h = d;
        for i in 1..10_000 {
            let an = -(i as f64) * (i as f64 - a);
            b += 2.0;
            d = an * d + b;
            if d.abs() < tiny {
                d = tiny;
            }
            c = b + an / c;
            if c.abs() < tiny {
                c = tiny;
            }
            d = 1.0 / d;
            let delta = d * c;
            h *= delta;
            if (delta - 1.0).abs() < 1e-16 {
                break;
            }
        }
        let q = (ln_prefix + h.ln()).exp().clamp(0.0, 1.0);
        (1.0 - q, q)
    }
}

/// The regularized incomplete beta `I_x(a, b)` (Numerical Recipes, `betai`).
pub fn beta_inc(a: f64, b: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let ln_front = a * x.ln() + b * (-x).ln_1p() - ln_beta(a, b);
    if x < (a + 1.0) / (a + b + 2.0) {
        (ln_front.exp() * beta_fraction(a, b, x) / a).clamp(0.0, 1.0)
    } else {
        (1.0 - ln_front.exp() * beta_fraction(b, a, 1.0 - x) / b).clamp(0.0, 1.0)
    }
}

fn beta_fraction(a: f64, b: f64, x: f64) -> f64 {
    let tiny = 1e-300;
    let (qab, qap, qam) = (a + b, a + 1.0, a - 1.0);
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < tiny {
        d = tiny;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..10_000 {
        let m = m as f64;
        let m2 = 2.0 * m;
        let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < tiny {
            d = tiny;
        }
        c = 1.0 + aa / c;
        if c.abs() < tiny {
            c = tiny;
        }
        d = 1.0 / d;
        h *= d * c;
        let aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < tiny {
            d = tiny;
        }
        c = 1.0 + aa / c;
        if c.abs() < tiny {
            c = tiny;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < 1e-16 {
            break;
        }
    }
    h
}

/// The standard normal CDF, through `erfc(x) = Q(1/2, x²)`: accurate in the tails,
/// where `1 - erf` loses everything.
pub fn normal_cdf(z: f64) -> f64 {
    if z.is_nan() {
        return f64::NAN;
    }
    let tail = 0.5 * gamma_q(0.5, 0.5 * z * z);
    if z < 0.0 { tail } else { 1.0 - tail }
}

/// The standard normal quantile. Acklam's rational approximation, then one Halley step
/// on the exact CDF: good to about 1e-15. Exactly 0 at the median, `-inf` and `inf` at
/// 0 and 1, NaN outside them.
pub fn normal_quantile(p: f64) -> f64 {
    if !(0.0..=1.0).contains(&p) {
        return f64::NAN;
    }
    if p == 0.0 {
        return f64::NEG_INFINITY;
    }
    if p == 1.0 {
        return f64::INFINITY;
    }
    if p == 0.5 {
        return 0.0;
    }
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    let low = 0.02425;
    let x = if p < low {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= 1.0 - low {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (-p).ln_1p()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    // One Halley step against the exact CDF.
    let e = normal_cdf(x) - p;
    let u = e * (2.0 * std::f64::consts::PI).sqrt() * (0.5 * x * x).exp();
    x - u / (1.0 + 0.5 * x * u)
}

/// Student's t CDF with `df` degrees of freedom.
pub fn t_cdf(t: f64, df: f64) -> f64 {
    if t.is_nan() {
        return f64::NAN;
    }
    let tail = 0.5 * beta_inc(0.5 * df, 0.5, df / (df + t * t));
    if t > 0.0 { 1.0 - tail } else { tail }
}

fn t_quantile(p: f64, df: f64) -> f64 {
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    if p == 0.5 {
        return 0.0;
    }
    let mut high = 1.0;
    let target = p.max(1.0 - p);
    while t_cdf(high, df) < target && high < 1e300 {
        high *= 2.0;
    }
    let (mut low, mut hi) = (0.0, high);
    for _ in 0..200 {
        let mid = 0.5 * (low + hi);
        if t_cdf(mid, df) < target {
            low = mid;
        } else {
            hi = mid;
        }
        if hi - low <= 1e-13 * hi {
            break;
        }
    }
    let t = 0.5 * (low + hi);
    if p < 0.5 { -t } else { t }
}

/// A small seeded generator (SplitMix64): the same seed draws the same samples on
/// every machine, which a test of a test needs.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform on (0, 1), never either end.
    pub fn uniform(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }

    pub fn normal(&mut self) -> f64 {
        normal_quantile(self.uniform())
    }

    /// A Gamma(shape, 1) draw, by Marsaglia and Tsang's method.
    pub fn gamma(&mut self, shape: f64) -> f64 {
        if shape < 1.0 {
            let boost = self.uniform().powf(1.0 / shape);
            return self.gamma(shape + 1.0) * boost;
        }
        let d = shape - 1.0 / 3.0;
        let c = 1.0 / (9.0 * d).sqrt();
        loop {
            let x = self.normal();
            let v = 1.0 + c * x;
            if v <= 0.0 {
                continue;
            }
            let v = v * v * v;
            let u = self.uniform();
            if u.ln() < 0.5 * x * x + d - d * v + d * v.ln() {
                return d * v;
            }
        }
    }

    /// A Poisson(rate) draw for a rate of 10 or more, by Hörmann's transformed
    /// rejection (PTRS): exact, and a constant cost whatever the rate.
    pub fn poisson(&mut self, rate: f64) -> f64 {
        let ln_rate = rate.ln();
        let b = 0.931 + 2.53 * rate.sqrt();
        let a = -0.059 + 0.02483 * b;
        let ln_inv_alpha = (1.1239 + 1.1328 / (b - 3.4)).ln();
        let v_r = 0.9277 - 3.6224 / (b - 2.0);
        loop {
            let u = self.uniform() - 0.5;
            let v = self.uniform();
            let us = 0.5 - u.abs();
            let k = ((2.0 * a / us + b) * u + rate + 0.43).floor();
            if us >= 0.07 && v <= v_r {
                return k;
            }
            if k < 0.0 || (us < 0.013 && v > us) {
                continue;
            }
            if v.ln() + ln_inv_alpha - (a / (us * us) + b).ln()
                <= -rate + k * ln_rate - ln_gamma(k + 1.0)
            {
                return k;
            }
        }
    }

    /// A Binomial(trials, p) draw for `trials * min(p, 1 - p)` of 10 or more, by
    /// Hörmann's transformed rejection (BTRS): exact, and a constant cost whatever
    /// the trials.
    pub fn binomial(&mut self, trials: u64, p: f64) -> f64 {
        if p > 0.5 {
            return trials as f64 - self.binomial(trials, 1.0 - p);
        }
        let n = trials as f64;
        let q = 1.0 - p;
        let spq = (n * p * q).sqrt();
        let b = 1.15 + 2.53 * spq;
        let a = -0.0873 + 0.0248 * b + 0.01 * p;
        let c = n * p + 0.5;
        let v_r = 0.92 - 4.2 / b;
        let alpha = (2.83 + 5.1 / b) * spq;
        let ln_pq = (p / q).ln();
        let m = ((n + 1.0) * p).floor();
        let h = ln_gamma(m + 1.0) + ln_gamma(n - m + 1.0);
        loop {
            let u = self.uniform() - 0.5;
            let v = self.uniform();
            let us = 0.5 - u.abs();
            let k = ((2.0 * a / us + b) * u + c).floor();
            if k < 0.0 || k > n {
                continue;
            }
            if us >= 0.07 && v <= v_r {
                return k;
            }
            let v = (v * alpha / (a / (us * us) + b)).ln();
            if v <= h - ln_gamma(k + 1.0) - ln_gamma(n - k + 1.0) + (k - m) * ln_pq {
                return k;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64, tolerance: f64) -> bool {
        (a - b).abs() <= tolerance * b.abs().max(1.0)
    }

    /// Reference values from Wichura's AS241 (Python's `NormalDist().inv_cdf`).
    #[test]
    fn normal_quantiles_match_reference_values() {
        for (p, z) in [
            (0.975, 1.959_963_984_540_054),
            (0.841_344_746_068_542_9, 1.0),
            (0.001, -3.090_232_306_167_813),
            (1e-10, -6.361_340_902_404_056),
            (0.3, -0.524_400_512_708_041),
        ] {
            assert!(
                close(normal_quantile(p), z, 1e-12),
                "{p}: {}",
                normal_quantile(p)
            );
        }
        assert_eq!(normal_quantile(0.5), 0.0);
        assert_eq!(normal_quantile(0.0), f64::NEG_INFINITY);
        assert_eq!(normal_quantile(1.0), f64::INFINITY);
        assert!(normal_quantile(1.5).is_nan());
        let grid: Vec<f64> = (1..1000)
            .map(|i| normal_quantile(i as f64 / 1000.0))
            .collect();
        assert!(grid.windows(2).all(|pair| pair[0] < pair[1]));
        for i in 1..500 {
            let p = i as f64 / 1000.0;
            assert!(close(normal_quantile(p), -normal_quantile(1.0 - p), 1e-12));
        }
    }

    #[test]
    fn cdfs_match_reference_values() {
        assert!(close(normal_cdf(1.959_963_984_540_054), 0.975, 1e-14));
        assert!(close(normal_cdf(-8.0), 6.220_960_574_271_784e-16, 1e-9));
        // Student's t with 5 df at 2.015048373: 0.95.
        assert!(close(t_cdf(2.015_048_372_669_157, 5.0), 0.95, 1e-10));
        // Chi-squared, 3 df, at 7.814727903: 0.95.
        let chi = Fitted::ChiSquared { df: 3.0 };
        assert!(close(chi.cdf(7.814_727_903_251_178), 0.95, 1e-10));
        // Beta(2, 5) at 0.5: 57/64.
        let beta = Fitted::Beta {
            alpha: 2.0,
            beta: 5.0,
        };
        assert!(close(beta.cdf(0.5), 57.0 / 64.0, 1e-12));
        // I_0.5(2, 3) = 11/16; Beta(1, 1) is the uniform.
        assert!(close(beta_inc(2.0, 3.0, 0.5), 0.6875, 1e-12));
        assert!(close(beta_inc(1.0, 1.0, 0.3), 0.3, 1e-12));
        // The 97.5th percentile of t with 5 degrees of freedom is 2.5706.
        assert!(close(t_cdf(2.5706, 5.0), 0.975, 1e-4));
        assert!(close(t_cdf(-2.5706, 5.0), 0.025, 1e-4));
        assert!(close(t_cdf(0.0, 5.0), 0.5, 1e-12));
        // Poisson(3): P(X <= 2) = 8.5 e^-3.
        let poisson = Fitted::Poisson { rate: 3.0 };
        assert!(close(poisson.cdf(2.0), 8.5 * (-3.0f64).exp(), 1e-12));
        // Binomial(10, 0.3): P(X <= 3) = 0.6496107184.
        let binomial = Fitted::Binomial { trials: 10, p: 0.3 };
        assert!(close(binomial.cdf(3.0), 0.649_610_718_4, 1e-9));
        assert!(close(ln_gamma(0.5), 0.572_364_942_924_700_1, 1e-14));
        assert!(close(ln_gamma(10.0), 12.801_827_480_081_469, 1e-14));
    }

    /// Every family's quantile inverts its CDF and rises with the probability, and
    /// the log-normal's stay positive.
    #[test]
    fn quantiles_invert_their_cdfs() {
        let fits = [
            Fitted::Normal { mean: 3.0, sd: 2.0 },
            Fitted::LogNormal {
                mu: 0.5,
                sigma: 0.8,
            },
            Fitted::Uniform {
                low: -1.0,
                high: 4.0,
            },
            Fitted::Exponential { rate: 2.0 },
            Fitted::Gamma {
                shape: 2.5,
                scale: 0.7,
            },
            Fitted::ChiSquared { df: 5.0 },
            Fitted::Beta {
                alpha: 2.0,
                beta: 5.0,
            },
            Fitted::StudentsT {
                df: 5.0,
                location: 1.0,
                scale: 2.0,
            },
            Fitted::Weibull {
                shape: 1.7,
                scale: 3.0,
            },
            Fitted::PowerLaw {
                xmin: 1.0,
                alpha: 2.5,
            },
        ];
        for fit in fits {
            let qs: Vec<f64> = (1..100).map(|i| fit.quantile(i as f64 / 100.0)).collect();
            assert!(qs.windows(2).all(|pair| pair[0] < pair[1]), "{fit:?}");
            for (i, q) in qs.iter().enumerate() {
                let p = (i + 1) as f64 / 100.0;
                assert!(
                    close(fit.cdf(*q), p, 1e-8),
                    "{fit:?} at {p}: {}",
                    fit.cdf(*q)
                );
            }
        }
        let lognormal = Fitted::LogNormal {
            mu: 0.0,
            sigma: 1.0,
        };
        assert!(lognormal.quantile(1e-9) > 0.0);
        let discrete = [
            Fitted::Poisson { rate: 4.0 },
            Fitted::Binomial { trials: 20, p: 0.4 },
            Fitted::Geometric { p: 0.3, start: 1 },
            Fitted::Bernoulli { p: 0.3 },
            // Fitted to counts in the millions, where walking to the answer is a
            // million steps.
            Fitted::Poisson { rate: 1e6 },
            Fitted::Binomial {
                trials: 2_000_000,
                p: 0.4,
            },
            Fitted::Geometric { p: 1e-6, start: 0 },
        ];
        for fit in discrete {
            for i in 1..100 {
                let p = i as f64 / 100.0;
                let k = fit.quantile(p);
                assert!(fit.cdf(k) >= p && fit.cdf(k - 1.0) < p, "{fit:?} at {p}");
            }
        }
    }

    /// Both sides of each step count: values bunched high against a uniform are far
    /// from it below the first one, which a check at the top of each step misses.
    #[test]
    fn the_ks_statistic_is_two_sided() {
        let uniform = Fitted::Uniform {
            low: 0.0,
            high: 1.0,
        };
        assert!(close(ks_statistic(&[0.5, 0.6, 0.9], &uniform), 0.5, 1e-12));
        assert!(close(ks_statistic(&[0.1, 0.4, 0.7], &uniform), 0.3, 1e-12));
        // Ties are one step: three 1s against a fair coin are 0.5 away at 1.
        let coin = Fitted::Bernoulli { p: 0.5 };
        assert!(close(ks_statistic(&[1.0, 1.0, 1.0], &coin), 0.5, 1e-12));
    }

    fn draw(fit: Fitted, n: usize, seed: u64) -> Vec<f64> {
        let mut rng = Rng::new(seed);
        (0..n).map(|_| fit.sample(&mut rng)).collect()
    }

    /// The Poisson and binomial samplers for large counts draw the distribution, not
    /// an approximation: whole numbers whose CDF matches the family's.
    #[test]
    fn large_count_draws_follow_their_family() {
        let fits = [
            Fitted::Poisson { rate: 10.0 },
            Fitted::Poisson { rate: 37.5 },
            Fitted::Poisson { rate: 1e6 },
            Fitted::Binomial { trials: 40, p: 0.7 },
            Fitted::Binomial {
                trials: 2_000_000,
                p: 0.4,
            },
        ];
        for fit in fits {
            let mut values = draw(fit, 5_000, 9);
            assert!(values.iter().all(|v| *v >= 0.0 && *v == v.floor()));
            values.sort_by(f64::total_cmp);
            // 1.63 / sqrt(n) is the 1% critical value of the KS statistic.
            let d = ks_statistic(&values, &fit);
            assert!(d < 1.63 / (5_000f64).sqrt(), "{fit:?}: D = {d}");
        }
    }

    /// The bootstrap p-value is calibrated: values drawn from a family are rejected at
    /// 5% about 5% of the time, and values from another are rejected.
    #[test]
    fn the_bootstrap_p_value_is_calibrated() {
        let normal = Fitted::Normal {
            mean: 10.0,
            sd: 3.0,
        };
        let rejected = (0..40)
            .filter(|seed| {
                let values = draw(normal, 120, 1_000 + seed);
                test_family(DistributionType::Normal, &values, *seed)
                    .p_value()
                    .unwrap()
                    < 0.05
            })
            .count();
        assert!(
            rejected <= 6,
            "{rejected} of 40 normal samples rejected as normal"
        );

        let skewed = draw(
            Fitted::LogNormal {
                mu: 0.0,
                sigma: 1.0,
            },
            400,
            7,
        );
        let outcome = test_family(DistributionType::Normal, &skewed, 7);
        let test = outcome.test().unwrap();
        assert!(test.at_bound(), "{test:?}");
        assert!(test.p_value < 0.01);
    }

    /// A family that does not apply says why and has no p-value to rank.
    #[test]
    fn a_family_that_does_not_apply_has_no_p_value() {
        let values = draw(Fitted::Normal { mean: 0.0, sd: 1.0 }, 50, 3);
        let outcome = test_family(DistributionType::LogNormal, &values, 3);
        assert_eq!(outcome, FitOutcome::NotApplicable("needs positive values"));
        let results = vec![
            (DistributionType::LogNormal, outcome),
            (
                DistributionType::Normal,
                test_family(DistributionType::Normal, &values, 3),
            ),
        ];
        assert_eq!(
            listing_order(&results)[..2],
            [DistributionType::Normal, DistributionType::LogNormal]
        );
    }

    /// Selection is among the families not rejected, by AIC, the simpler of two
    /// nested ones when both hold: exponential values are a Gamma of shape 1 too.
    #[test]
    fn selection_prefers_the_simpler_family_that_holds() {
        let values = draw(Fitted::Exponential { rate: 2.0 }, 2_000, 11);
        let results = test_all(&values, 11);
        assert_eq!(select(&results, false), DistributionType::Exponential);

        let counts = draw(Fitted::Poisson { rate: 5.0 }, 2_000, 12);
        let results = test_all(&counts, 12);
        assert_eq!(select(&results, true), DistributionType::Poisson);
    }
}
