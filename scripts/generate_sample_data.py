#!/usr/bin/env python3
"""
Generate sample data files for datui testing.

This script generates various CSV, Parquet, IPC/Arrow, Avro, and Excel files:
- Different data types
- Missing data (nulls)
- Empty tables
- Quoted and unquoted strings
- Files for grouping operations
- Files for aggregate calculations
- Large and small datasets
- Error case testing
- Pivot and Melt reshape testing (long-format for pivot, wide-format for melt)
- Correlation matrix demo (100k rows, 10 numeric columns with varying correlations)
- Tiny SafeTensors and GGUF model files, written by hand with struct and NumPy
- GPS logs: an NMEA 0183 drive and a GPX ride, written as text
- Short WAV, Broadcast WAV and AIFF files: the wave module, and struct where it cannot
- SQLite databases: one of several tables and one of a single table, with sqlite3
- NumPy arrays and archives: each dtype, structured, 2-D in both orders, .npz
- A tiny ELF executable, written by hand with struct
- Flight logs: a PX4 ULog and an ArduPilot DataFlash log, written by hand with struct
- CAN logs as candump writes them, and DBC files with Motorola, signed and multiplexed signals

Uses Polars for most formats; fastavro for Avro; openpyxl for Excel.
"""

import os
import sys
from pathlib import Path
import polars as pl
import numpy as np
from datetime import date, datetime, timedelta
import random
import gzip
import json
import math
import sqlite3
import struct
import wave

# Optional deps for extra formats (fail gracefully if missing)
try:
    import fastavro
except ImportError:
    fastavro = None
try:
    import openpyxl
except ImportError:
    openpyxl = None

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "tests" / "sample-data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def generate_people_data():
    """Generate a people database with cities, states, etc. for grouping."""
    np.random.seed(42)
    random.seed(42)

    cities = ["Springfield", "Riverside", "Franklin", "Greenville", "Bristol",
              "Madison", "Clinton", "Marion", "Georgetown", "Salem"]
    states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"]
    job_titles = ["Manager", "Senior", "Junior", "Lead", "Director", "Analyst"]

    n = 1000
    data = {
        "id": list(range(1, n + 1)),
        "first_name": [f"Person{i}" for i in range(1, n + 1)],
        "last_name": [f"Lastname{i}" for i in range(1, n + 1)],
        "age": np.random.randint(22, 65, n).tolist(),
        "city": np.random.choice(cities, n).tolist(),
        "state": np.random.choice(states, n).tolist(),
        "department": np.random.choice(departments, n).tolist(),
        "job_title": np.random.choice(job_titles, n).tolist(),
        "salary": np.random.randint(40000, 150000, n).tolist(),
        "start_date": [(datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).date() for _ in range(n)],
        "active": np.random.choice([True, False], n, p=[0.8, 0.2]).tolist(),
    }

    df = pl.DataFrame(data)

    # Add some nulls - create a mask for each column
    null_count = int(n * 0.1)
    for col in ["city", "department", "salary"]:
        null_indices = set(np.random.choice(n, size=null_count, replace=False))
        mask = [i in null_indices for i in range(n)]
        if col == "salary":
            df = df.with_columns(
                pl.when(pl.Series("mask", mask))
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
        else:
            df = df.with_columns(
                pl.when(pl.Series("mask", mask))
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )

    return df


def generate_infer_schema_length_data():
    """Generate data that looks like ints for 100 rows and then is an "N/A" followed by more ints"""
    data = ["column"] + [str(i + 1) for i in range(100)] + ["N/A"] + [str(i + 101) for i in range(100)]
    return data

def save_infer_schema_length_data(data, filename):
    path = OUTPUT_DIR / filename
    with open(path, "w") as f:
        f.writelines(("\n".join(data)))
    print(f"Generated: {path}")

def generate_sales_data():
    """Generate sales data for aggregate calculations."""
    np.random.seed(43)
    random.seed(43)

    products = ["Widget A", "Widget B", "Widget C", "Gadget X", "Gadget Y", "Tool 1", "Tool 2"]
    regions = ["North", "South", "East", "West", "Central"]

    n = 5000
    data = {
        "date": [(datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))).date() for _ in range(n)],
        "product": np.random.choice(products, n).tolist(),
        "region": np.random.choice(regions, n).tolist(),
        "quantity": np.random.randint(1, 100, n).tolist(),
        "unit_price": [round(random.uniform(10.0, 500.0), 2) for _ in range(n)],
        "discount": [round(random.uniform(0.0, 0.3), 2) for _ in range(n)],
    }

    df = pl.DataFrame(data)
    df = df.with_columns(
        (pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount"))).alias("total")
    )

    # Add some nulls
    null_count = int(n * 0.05)
    for col in ["quantity", "unit_price", "discount"]:
        null_indices = set(np.random.choice(n, size=null_count, replace=False))
        mask = [i in null_indices for i in range(n)]
        df = df.with_columns(
            pl.when(pl.Series("mask", mask))
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
        )

    # Recalculate total where we have nulls
    df = df.with_columns(
        pl.when(pl.col("quantity").is_null() | pl.col("unit_price").is_null() | pl.col("discount").is_null())
        .then(None)
        .otherwise(pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount")))
        .alias("total")
    )

    return df

def generate_mixed_types():
    """Generate data with various types including nulls."""
    np.random.seed(44)

    n = 200
    data = {
        "id": list(range(1, n + 1)),
        "integer_col": np.random.randint(-100, 100, n).tolist(),
        "float_col": [round(random.uniform(-50.0, 50.0), 3) for _ in range(n)],
        "string_col": [f"text_{i}" for i in range(n)],
        "boolean_col": np.random.choice([True, False], n).tolist(),
        "date_col": [(datetime(2020, 1, 1) + timedelta(days=i)).date() for i in range(n)],
    }

    df = pl.DataFrame(data)

    # Add nulls to various columns
    null_count = int(n * 0.15)
    for col in ["integer_col", "float_col", "string_col", "boolean_col", "date_col"]:
        null_indices = set(np.random.choice(n, size=null_count, replace=False))
        mask = [i in null_indices for i in range(n)]
        df = df.with_columns(
            pl.when(pl.Series("mask", mask))
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
        )

    return df

def generate_quoted_strings():
    """Generate CSV with quoted strings containing commas, newlines, etc."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": [
            "Normal Name",
            "Name, with comma",
            "Name\nwith newline",
            'Name "with quotes"',
            "Name, with\nmultiple, issues",
        ],
        "description": [
            "Simple description",
            "Description, with comma, and more",
            "Description\nwith\nnewlines",
            'Description with "quotes" and, commas',
            "Complex: has, commas\nand newlines\nand \"quotes\"",
        ],
        "value": [10, 20, 30, 40, 50],
    }

    df = pl.DataFrame(data)
    return df

def generate_empty_table():
    """Generate an empty table with schema."""
    df = pl.DataFrame({
        "id": pl.Series([], dtype=pl.Int64),
        "name": pl.Series([], dtype=pl.Utf8),
        "value": pl.Series([], dtype=pl.Float64),
        "date": pl.Series([], dtype=pl.Date),
    })
    return df

def generate_single_row():
    """Generate a table with a single row."""
    data = {
        "id": [1],
        "name": ["Single Row"],
        "value": [42],
        "date": [datetime(2024, 1, 1).date()],
    }
    df = pl.DataFrame(data)
    return df

def generate_large_dataset():
    """Generate a large dataset for performance testing with various distributions."""
    np.random.seed(45)
    random.seed(45)

    n = 1000000

    # Generate distributions with various characteristics
    # Preserve distribution characteristics for proper detection testing

    # Normal distribution (can have negative values) - keep natural scale
    normal_data = np.random.normal(loc=0.0, scale=1.0, size=n)

    # LogNormal distribution (positive values) - keep natural scale
    lognormal_data = np.random.lognormal(mean=0.0, sigma=1.0, size=n)

    # Uniform distribution - keep in [0, 1] (natural for uniform)
    uniform_data = np.random.uniform(0.0, 1.0, n)

    # Power Law distribution (positive values) - keep natural scale
    # Generate using inverse transform: x = xmin * (1 - u)^(-1/(alpha-1))
    # where u is uniform [0,1] and alpha > 1
    alpha = 2.5
    xmin = 1.0  # Start from 1.0 for better power-law characteristics
    powerlaw_data = xmin * np.power(1.0 - np.random.uniform(0.0, 1.0, n), -1.0 / (alpha - 1.0))

    # Exponential distribution (positive values) - keep natural scale
    lambda_param = 2.0
    exponential_data = np.random.exponential(scale=1.0/lambda_param, size=n)

    # Beta distribution - naturally in [0, 1], keep as is
    beta_data = np.random.beta(a=2.0, b=5.0, size=n)

    # Gamma distribution (positive values) - keep natural scale
    shape = 2.0
    scale = 0.5
    gamma_data = np.random.gamma(shape=shape, scale=scale, size=n)

    # Chi-squared distribution (non-negative) - keep natural scale
    df = 5.0
    chisq_data = np.random.chisquare(df=df, size=n)

    # Student's t distribution (can have negative values) - keep natural scale
    t_df = 5.0
    t_data = np.random.standard_t(df=t_df, size=n)

    # Poisson distribution (non-negative integers) - KEEP AS INTEGERS
    lambda_poisson = 5.0
    poisson_data = np.random.poisson(lam=lambda_poisson, size=n).astype(int)

    # Bernoulli distribution - KEEP AS BINARY INTEGERS [0, 1]
    p_bernoulli = 0.3
    bernoulli_data = np.random.binomial(n=1, p=p_bernoulli, size=n).astype(int)

    # Binomial distribution (non-negative integers) - KEEP AS INTEGERS
    n_binomial = 20
    p_binomial = 0.4
    binomial_data = np.random.binomial(n=n_binomial, p=p_binomial, size=n).astype(int)

    # Geometric distribution (non-negative integers) - KEEP AS INTEGERS
    p_geometric = 0.3
    geometric_data = np.random.geometric(p=p_geometric, size=n).astype(int)

    # Weibull distribution (positive values) - keep natural scale
    weibull_shape = 2.0
    weibull_scale = 1.0
    weibull_data = weibull_scale * np.power(-np.log(np.random.uniform(0.001, 1.0, n)), 1.0 / weibull_shape)

    # Generate all 2-letter combinations (AA, AB, ..., ZZ) = 26*26 = 676 categories
    categories = [f"{chr(65+i)}{chr(65+j)}" for i in range(26) for j in range(26)]
    num_categories = len(categories)

    # Generate power-law distributed categories
    power_law_values = np.random.power(a=1.5, size=n)   # 1.5 alpha for power law
    category_indices = (power_law_values * num_categories).astype(int)
    category_indices = np.clip(category_indices, 0, num_categories - 1)
    category_data = [categories[idx] for idx in category_indices]

    data = {
        "id": list(range(1, n + 1)),
        "category": category_data,
        "value1": np.random.randint(0, 1000, n).tolist(),
        "value2": [round(random.uniform(0.0, 100.0), 2) for _ in range(n)],
        "value3": np.random.choice([True, False], n).tolist(),
        "timestamp": [datetime(2024, 1, 1) + timedelta(seconds=i) for i in range(n)],
        # Distribution columns
        # Continuous distributions: round to 6 decimal places
        "dist_normal": [round(float(x), 6) for x in normal_data],
        "dist_lognormal": [round(float(x), 6) for x in lognormal_data],
        "dist_uniform": [round(float(x), 6) for x in uniform_data],
        "dist_powerlaw": [round(float(x), 6) for x in powerlaw_data],
        "dist_exponential": [round(float(x), 6) for x in exponential_data],
        "dist_beta": [round(float(x), 6) for x in beta_data],
        "dist_gamma": [round(float(x), 6) for x in gamma_data],
        "dist_chisquared": [round(float(x), 6) for x in chisq_data],
        "dist_students_t": [round(float(x), 6) for x in t_data],
        "dist_weibull": [round(float(x), 6) for x in weibull_data],
        # Discrete distributions: keep as integers (no rounding needed, but convert to list)
        "dist_poisson": poisson_data.tolist(),
        "dist_bernoulli": bernoulli_data.tolist(),
        "dist_binomial": binomial_data.tolist(),
        "dist_geometric": geometric_data.tolist(),
    }

    df = pl.DataFrame(data)
    return df

def generate_error_cases():
    """Generate files that test error cases."""
    error_cases = {}

    # Case 1: Inconsistent types in column (convert all to strings to simulate mixed types)
    error_cases["inconsistent_types"] = pl.DataFrame({
        "id": pl.Series(["1", "2", "3", "not_a_number", "5"], dtype=pl.Utf8),  # All strings, but some look like numbers
        "value": [10, 20, 30, 40, 50],
    })

    # Case 2: Very long strings
    error_cases["long_strings"] = pl.DataFrame({
        "id": list(range(1, 11)),
        "long_text": ["A" * 1000] * 10,
    })

    # Case 3: Special characters
    error_cases["special_chars"] = pl.DataFrame({
        "id": list(range(1, 6)),
        "text": ["\x00", "\t", "\n", "\r", "\\"],
        "unicode": ["αβγ", "🚀", "中文", "العربية", "русский"],
    })

    return error_cases


# -----------------------------------------------------------------------------
# Pivot / Melt testing (see plans/pivot-melt-plan.md)
# -----------------------------------------------------------------------------


def generate_pivot_long():
    """
    Long-format data for Pivot tab testing.

    Schema: id, date, key, value (float).
    - Multiple rows per (id, date) with distinct keys "A", "B", "C".
    - Some (id, date, key) duplicates to exercise aggregation (last, first, min, max, etc.).
    - Deterministic (seed 50) for reproducible tests.
    """
    np.random.seed(50)
    random.seed(50)

    keys = ["A", "B", "C"]
    n_groups = 40
    base_date = datetime(2024, 1, 1).date()

    rows = []
    for g in range(n_groups):
        uid = (g % 20) + 1
        d = base_date + timedelta(days=g % 31)
        for k in keys:
            rows.append({"id": uid, "date": d, "key": k, "value": round(random.uniform(10.0, 100.0), 2)})

    # Add duplicates for aggregation tests: same (id, date, key), different value
    n_dup = 24
    for _ in range(n_dup):
        r = random.choice(rows)
        rows.append({
            "id": r["id"],
            "date": r["date"],
            "key": r["key"],
            "value": round(random.uniform(10.0, 100.0), 2),
        })

    return pl.DataFrame(rows)


def generate_pivot_long_string():
    """
    Long-format data with string value column for Pivot + first/last aggregation.

    Schema: id, date, key, value (str). Same structure as pivot_long; value is
    "low", "mid", or "high" so only first/last aggregation is meaningful.
    """
    np.random.seed(51)
    random.seed(51)

    keys = ["X", "Y", "Z"]
    labels = ["low", "mid", "high"]
    n_groups = 30
    base_date = datetime(2024, 1, 1).date()

    rows = []
    for g in range(n_groups):
        uid = (g % 15) + 1
        d = base_date + timedelta(days=g % 28)
        for k in keys:
            rows.append({"id": uid, "date": d, "key": k, "value": random.choice(labels)})

    return pl.DataFrame(rows)


def generate_melt_wide():
    """
    Wide-format data for Melt tab testing.

    Schema: id, date, Q1_2024..Q4_2024, metric_foo, metric_bar, label.
    - Pattern-friendly names for regex tests (Q[1-4]_2024, metric_*).
    - Mix of numeric and string (label) for "by type" tests.
    """
    np.random.seed(52)
    random.seed(52)

    n = 80
    base_date = datetime(2024, 1, 1).date()
    data = {
        "id": list(range(1, n + 1)),
        "date": [base_date + timedelta(days=random.randint(0, 365)) for _ in range(n)],
        "Q1_2024": [round(random.uniform(0, 100), 2) for _ in range(n)],
        "Q2_2024": [round(random.uniform(0, 100), 2) for _ in range(n)],
        "Q3_2024": [round(random.uniform(0, 100), 2) for _ in range(n)],
        "Q4_2024": [round(random.uniform(0, 100), 2) for _ in range(n)],
        "metric_foo": [round(random.uniform(0, 50), 2) for _ in range(n)],
        "metric_bar": [round(random.uniform(0, 50), 2) for _ in range(n)],
        "label": random.choices(["alpha", "beta", "gamma"], k=n),
    }
    return pl.DataFrame(data)


def generate_melt_wide_many():
    """
    Wide-format data with many value columns for Melt "all except index" / pattern stress.

    Schema: id, date, col_1, col_2, ..., col_50. All numeric except id/date.
    """
    np.random.seed(53)
    random.seed(53)

    n = 60
    n_cols = 50
    base_date = datetime(2024, 1, 1).date()

    data = {
        "id": list(range(1, n + 1)),
        "date": [base_date + timedelta(days=random.randint(0, 200)) for _ in range(n)],
    }
    for i in range(1, n_cols + 1):
        data[f"col_{i}"] = [round(random.uniform(0, 100), 2) for _ in range(n)]

    return pl.DataFrame(data)


def generate_charting_demo():
    """
    Generate daily time-series data for chart view demos and testing.

    One row per day for 10 years (2015-01-01 through 2024-12-31). Columns:
    - date: sequential daily dates
    - day_of_week: Mon, Tue, ..., Sun (categorical)
    - stock_market: fictitious index (random walk with drift/volatility)
    - high_temp: daily high (seasonal + noise)
    - 20d_avg_high_temp: 20-day rolling average of high_temp
    - customer_count: integer (seasonal + weekday + noise)
    - shark_sightings: integer daily count (low with occasional spikes)
    """
    np.random.seed(55)
    random.seed(55)

    base = datetime(2015, 1, 1).date()
    days = (datetime(2024, 12, 31).date() - base).days + 1
    dates = [base + timedelta(days=i) for i in range(days)]
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # day_of_week from date (2015-01-01 is Thursday → weekday 3)
    day_of_week = [
        day_names[(base.weekday() + i) % 7] for i in range(days)
    ]

    # Stock market: random walk with slight upward drift and volatility
    walk = np.zeros(days)
    walk[0] = 1000.0
    for i in range(1, days):
        walk[i] = walk[i - 1] + np.random.normal(0.5, 15.0)
    stock_market = [round(float(x), 2) for x in walk]

    # High temp: seasonal sine + noise (roughly 30–100 °F)
    t = np.arange(days, dtype=float)
    seasonal = 65.0 + 25.0 * np.sin(2 * np.pi * t / 365.25 - 1.6)
    noise = np.random.normal(0, 5.0, days)
    high_temp = [round(float(np.clip(seasonal[i] + noise[i], 25, 105)), 1) for i in range(days)]

    # 20-day rolling average of high_temp
    high_temp_arr = np.array(high_temp, dtype=float)
    avg_20d = np.zeros(days)
    for i in range(days):
        lo = max(0, i - 19)
        avg_20d[i] = high_temp_arr[lo : i + 1].mean()
    avg_20d_list = [round(float(x), 1) for x in avg_20d]

    # Customer count: base + weekday effect + seasonal + noise (non-negative int)
    weekday_effect = np.array([0, 0, 0, 0, 10, 25, 15])  # Fri/Sat/Sun higher
    base_cust = 500
    cust = np.zeros(days)
    for i in range(days):
        wd = (base.weekday() + i) % 7
        seasonal_cust = 80 * np.sin(2 * np.pi * i / 365.25)
        cust[i] = base_cust + weekday_effect[wd] + seasonal_cust + np.random.normal(0, 30)
    customer_count = [max(0, int(round(x))) for x in cust]

    # Shark sightings: low counts, occasional spikes (Poisson-like with rare spikes)
    lam = np.ones(days) * 0.3
    for _ in range(12):
        idx = random.randint(0, days - 1)
        lam[idx] = 8.0 + random.uniform(0, 5)
    shark_sightings = [np.random.poisson(l) for l in lam]

    data = {
        "date": dates,
        "day_of_week": day_of_week,
        "stock_market": stock_market,
        "high_temp": high_temp,
        "20d_avg_high_temp": avg_20d_list,
        "customer_count": customer_count,
        "shark_sightings": shark_sightings,
    }
    return pl.DataFrame(data)


def generate_correlation_matrix_data():
    """
    Generate numeric data with designed pairwise correlations for correlation matrix demos.

    Produces 100_000 rows and 10 numeric columns with realistic names. Correlations
    are chosen to span the full range (strong negative to strong positive) so the
    correlation matrix heatmap uses the full color scale.

    Columns: revenue, profit, operating_cost, margin_pct, unit_volume, price_index,
             growth_rate, market_share, roi, cash_flow
    """
    np.random.seed(54)
    random.seed(54)

    n = 100_000
    col_names = [
        "revenue",
        "profit",
        "operating_cost",
        "margin_pct",
        "unit_volume",
        "price_index",
        "growth_rate",
        "market_share",
        "roi",
        "cash_flow",
    ]
    k = len(col_names)

    # Target correlation matrix (symmetric, 1 on diagonal). Order matches col_names.
    # Designed for variety: strong +/-, moderate +/-, weak +/-, near zero.
    R = np.array(
        [
            [1.00, 0.92, 0.88, 0.45, 0.72, 0.15, 0.08, -0.05, 0.68, 0.85],   # revenue
            [0.92, 1.00, 0.78, 0.82, 0.58, 0.22, 0.12, 0.02, 0.90, 0.88],   # profit
            [0.88, 0.78, 1.00, -0.75, 0.65, -0.10, -0.02, -0.08, 0.52, 0.62], # operating_cost
            [0.45, 0.82, -0.75, 1.00, 0.20, 0.55, 0.18, 0.25, 0.78, 0.70],   # margin_pct
            [0.72, 0.58, 0.65, 0.20, 1.00, -0.35, 0.30, 0.40, 0.35, 0.48],   # unit_volume
            [0.15, 0.22, -0.10, 0.55, -0.35, 1.00, 0.05, 0.12, 0.28, 0.18], # price_index
            [0.08, 0.12, -0.02, 0.18, 0.30, 0.05, 1.00, 0.42, 0.15, 0.10],   # growth_rate
            [-0.05, 0.02, -0.08, 0.25, 0.40, 0.12, 0.42, 1.00, 0.08, 0.02],  # market_share
            [0.68, 0.90, 0.52, 0.78, 0.35, 0.28, 0.15, 0.08, 1.00, 0.82],   # roi
            [0.85, 0.88, 0.62, 0.70, 0.48, 0.18, 0.10, 0.02, 0.82, 1.00],   # cash_flow
        ],
        dtype=np.float64,
    )

    # Standard deviations (scale each column to realistic ranges)
    scales = np.array([2.5e6, 4e5, 1.8e6, 8.0, 1.2e4, 15.0, 0.12, 5.0, 0.25, 3e5])
    # Covariance = diag(scales) @ R @ diag(scales)
    cov = np.outer(scales, scales) * R

    # Ensure covariance is positive definite (numerical safety)
    cov = (cov + cov.T) / 2
    min_eig = np.min(np.linalg.eigvalsh(cov))
    if min_eig < 1e-6:
        cov += (1e-6 - min_eig) * np.eye(k)

    mean = np.array(
        [1e7, 1.5e6, 6e6, 22.0, 5e4, 100.0, 0.05, 12.0, 0.15, 1e6],
        dtype=np.float64,
    )

    raw = np.random.multivariate_normal(mean, cov, size=n)

    # Clip to plausible non-negative ranges where needed (e.g. revenue, profit, %)
    raw[:, 0] = np.clip(raw[:, 0], 1e5, None)   # revenue
    raw[:, 1] = np.clip(raw[:, 1], -1e6, None)  # profit can be negative
    raw[:, 2] = np.clip(raw[:, 2], 1e4, None)   # operating_cost
    raw[:, 3] = np.clip(raw[:, 3], 0.1, 60.0)  # margin_pct
    raw[:, 4] = np.clip(raw[:, 4], 100, None)   # unit_volume
    raw[:, 5] = np.clip(raw[:, 5], 50, 200)    # price_index
    raw[:, 6] = np.clip(raw[:, 6], -0.5, 0.8)  # growth_rate
    raw[:, 7] = np.clip(raw[:, 7], 0, 40)      # market_share
    raw[:, 8] = np.clip(raw[:, 8], -0.2, 0.6)  # roi
    raw[:, 9] = np.clip(raw[:, 9], -5e5, None) # cash_flow can be negative

    data = {col_names[i]: [round(float(x), 4) for x in raw[:, i]] for i in range(k)}
    return pl.DataFrame(data)


def save_csv(df, filename, **kwargs):
    """Save DataFrame as CSV, compressed with gzip."""
    # Remove .csv extension if present, we'll add .csv.gz
    base_name = filename.replace('.csv', '')
    filepath = OUTPUT_DIR / f"{base_name}.csv.gz"

    # Write to temporary file first, then compress
    temp_path = OUTPUT_DIR / f"{base_name}.csv.tmp"
    df.write_csv(temp_path, **kwargs)

    # Compress the CSV file
    with open(temp_path, 'rb') as f_in:
        with gzip.open(filepath, 'wb', compresslevel=6) as f_out:
            f_out.writelines(f_in)

    # Remove temporary file
    temp_path.unlink()

    print(f"Generated: {filepath}")

def save_parquet(df, filename):
    """Save DataFrame as Parquet."""
    filepath = OUTPUT_DIR / filename
    df.write_parquet(filepath)
    print(f"Generated: {filepath}")


def save_ipc(df, filename):
    """Save DataFrame as Arrow IPC / Feather (e.g. .arrow, .ipc)."""
    base = filename.replace(".arrow", "").replace(".ipc", "")
    filepath = OUTPUT_DIR / f"{base}.arrow"
    df.write_ipc(filepath)
    print(f"Generated: {filepath}")


def _as_hugging_face_table(df):
    """The frame as Hugging Face `datasets` holds it: 32-bit string offsets."""
    import pyarrow as pa

    table = df.to_arrow()
    fields = [
        pa.field(f.name, pa.string()) if pa.types.is_large_string(f.type) or pa.types.is_string_view(f.type) else f
        for f in table.schema
    ]
    return table.cast(pa.schema(fields))


def _write_stream(table, filepath, **options):
    """Write `table` as an Arrow IPC stream (no footer), several record batches."""
    import pyarrow as pa

    with pa.OSFile(str(filepath), "wb") as sink:
        with pa.ipc.new_stream(sink, table.schema, options=pa.ipc.IpcWriteOptions(**options)) as writer:
            for batch in table.to_batches(max_chunksize=7):
                writer.write_batch(batch)
    print(f"Generated: {filepath}")


def save_ipc_streams(df):
    """Arrow IPC streams, the format of a Hugging Face `datasets` cache: plain, with
    LZ4 and ZSTD buffers, in the legacy layout without the continuation marker, and a
    `save_to_disk` directory of shards with its two JSON files."""
    import json

    table = _as_hugging_face_table(df)
    _write_stream(table, OUTPUT_DIR / "people_stream.arrow")
    _write_stream(table, OUTPUT_DIR / "people_stream_lz4.arrow", compression="lz4")
    _write_stream(table, OUTPUT_DIR / "people_stream_zstd.arrow", compression="zstd")
    _write_stream(table, OUTPUT_DIR / "people_stream_legacy", use_legacy_format=True)

    shards_dir = OUTPUT_DIR / "hf_shards"
    shards_dir.mkdir(exist_ok=True)
    n = 3
    per = -(-table.num_rows // n)
    names = []
    for i in range(n):
        name = f"data-{i:05d}-of-{n:05d}.arrow"
        names.append(name)
        _write_stream(table.slice(i * per, per), shards_dir / name)
    (shards_dir / "dataset_info.json").write_text(json.dumps({"description": "", "features": {}}))
    (shards_dir / "state.json").write_text(
        json.dumps({"_data_files": [{"filename": name} for name in names], "_split": "train"})
    )
    print(f"Generated: {shards_dir}")

    # A `datasets` cache directory: a file per split, the train split in two shards,
    # and the `cache-*.arrow` files `map()` writes beside them with columns of their own.
    # Rows 0-599 are train, 600-799 test, 800-999 validation.
    cache_dir = OUTPUT_DIR / "hf_cache"
    cache_dir.mkdir(exist_ok=True)
    _write_stream(table.slice(0, 300), cache_dir / "people-train-00000-of-00002.arrow")
    _write_stream(table.slice(300, 300), cache_dir / "people-train-00001-of-00002.arrow")
    _write_stream(table.slice(600, 200), cache_dir / "people-test.arrow")
    _write_stream(table.slice(800, 200), cache_dir / "people-validation.arrow")
    mapped = table.slice(0, 5).select([table.column_names[0]])
    _write_stream(mapped, cache_dir / "cache-0f3c2a1b9d8e7f60.arrow")
    _write_stream(mapped, cache_dir / "cache-5e4d3c2b1a09f8e7.arrow")
    (cache_dir / "dataset_info.json").write_text(
        json.dumps({"builder_name": "people", "splits": {"train": {}, "test": {}, "validation": {}}})
    )
    print(f"Generated: {cache_dir}")

    # A DatasetDict as `save_to_disk` writes it: `dataset_dict.json` naming a
    # subdirectory per split, each of `data-*` shards beside its own JSON.
    # Rows 0-699 are train, 700-999 test.
    dict_dir = OUTPUT_DIR / "hf_dict"
    for split, rows in [("train", table.slice(0, 700)), ("test", table.slice(700))]:
        split_dir = dict_dir / split
        split_dir.mkdir(parents=True, exist_ok=True)
        _write_stream(rows, split_dir / "data-00000-of-00001.arrow")
        (split_dir / "dataset_info.json").write_text(json.dumps({"builder_name": "people"}))
        (split_dir / "state.json").write_text(
            json.dumps({"_data_files": [{"filename": "data-00000-of-00001.arrow"}]})
        )
    (dict_dir / "dataset_dict.json").write_text(json.dumps({"splits": ["train", "test"]}))
    print(f"Generated: {dict_dir}")

    # IPC files with a stream among them, which is not first.
    import pyarrow as pa

    mixed_dir = OUTPUT_DIR / "arrow_mixed"
    mixed_dir.mkdir(exist_ok=True)
    with pa.OSFile(str(mixed_dir / "a.arrow"), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table.slice(0, 500), max_chunksize=7)
    _write_stream(table.slice(500), mixed_dir / "b.arrow")
    print(f"Generated: {mixed_dir}")


def _polars_dtype_to_avro(dtype):
    """Map Polars dtype to Avro schema (nullable union). Date/Datetime use logical types."""
    if dtype == pl.Int64:
        return ["null", "long"]
    if dtype == pl.Float64:
        return ["null", "double"]
    if dtype == pl.Utf8:
        return ["null", "string"]
    if dtype == pl.Boolean:
        return ["null", "boolean"]
    if dtype == pl.Date:
        return ["null", {"type": "int", "logicalType": "date"}]
    if dtype == pl.Datetime("us") or dtype == pl.Datetime("ms"):
        return ["null", {"type": "long", "logicalType": "timestamp-micros"}]
    # fallback
    return ["null", "string"]


def save_avro(df, filename):
    """Save DataFrame as Avro (requires fastavro)."""
    if fastavro is None:
        print("Skipping Avro (fastavro not installed):", filename)
        return
    filepath = OUTPUT_DIR / filename
    fields = []
    for name in df.columns:
        dtype = df.schema[name]
        avro_type = _polars_dtype_to_avro(dtype)
        fields.append({"name": name, "type": avro_type})
    schema = {"type": "record", "name": "Record", "fields": fields}
    parsed = fastavro.parse_schema(schema)

    epoch_date = date(1970, 1, 1)

    def row_to_record(row):
        record = {}
        for i, name in enumerate(df.columns):
            val = row[i]
            dtype = df.schema[name]
            if val is None:
                record[name] = None
            elif dtype == pl.Date and isinstance(val, date):
                record[name] = (val - epoch_date).days
            elif dtype in (pl.Datetime("us"), pl.Datetime("ms")) and hasattr(val, "timestamp"):
                record[name] = int(val.timestamp() * 1_000_000)
            elif hasattr(val, "isoformat"):
                record[name] = val.isoformat()
            else:
                record[name] = val
        return record

    records = [row_to_record(row) for row in df.iter_rows()]
    with open(filepath, "wb") as out:
        fastavro.writer(out, parsed, records, codec="deflate")
    print(f"Generated: {filepath}")


def save_excel(df, filename):
    """Save DataFrame as Excel .xlsx (requires openpyxl)."""
    if openpyxl is None:
        print("Skipping Excel (openpyxl not installed):", filename)
        return
    filepath = OUTPUT_DIR / filename
    wb = openpyxl.Workbook()
    ws = wb.active
    if ws is None:
        return
    for c, name in enumerate(df.columns, 1):
        ws.cell(row=1, column=c, value=name)
    for r in range(df.height):
        row = df.row(r)
        for c, val in enumerate(row, 1):
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            ws.cell(row=r + 2, column=c, value=val)
    wb.save(filepath)
    print(f"Generated: {filepath}")


def write_safetensors(path, tensors, metadata=None):
    """A SafeTensors file: the header length, the JSON header, then the data.

    Written by hand rather than with the `safetensors` package, which the tests
    otherwise have no use for. `tensors` maps a name to a NumPy array.
    """
    dtypes = {np.dtype("float32"): "F32", np.dtype("float16"): "F16", np.dtype("int64"): "I64"}
    header = {}
    if metadata:
        header["__metadata__"] = metadata
    data = b""
    for name, array in tensors.items():
        raw = np.ascontiguousarray(array).tobytes()
        header[name] = {
            "dtype": dtypes[array.dtype],
            "shape": list(array.shape),
            "data_offsets": [len(data), len(data) + len(raw)],
        }
        data += raw
    text = json.dumps(header, separators=(",", ":")).encode()
    # Padded with spaces to eight bytes, as the reference writer does.
    text += b" " * (-len(text) % 8)
    with open(path, "wb") as f:
        f.write(struct.pack("<Q", len(text)))
        f.write(text)
        f.write(data)
    print(f"Generated: {path}")


def _gguf_str(s):
    raw = s.encode()
    return struct.pack("<Q", len(raw)) + raw


def write_gguf(path):
    """A GGUF v3 file with llama-style metadata, a vocabulary and three tensors."""
    STRING, ARRAY, UINT32, FLOAT32 = 8, 9, 4, 6
    kvs = [
        ("general.architecture", STRING, "llama"),
        ("general.name", STRING, "tiny"),
        ("llama.context_length", UINT32, 2048),
        ("llama.rope.freq_base", FLOAT32, 10000.0),
        ("tokenizer.ggml.model", STRING, "llama"),
        ("tokenizer.ggml.tokens", ARRAY, [f"tok{i}" for i in range(100)]),
        (
            "tokenizer.chat_template",
            STRING,
            "{% for message in messages %}\n{{ message['role'] }}: {{ message['content'] }}\n{% endfor %}",
        ),
    ]
    # name, dims (fastest first, as GGML writes them), GGML type
    tensors = [
        ("token_embd.weight", [256, 100], 12),  # Q4_K
        ("blk.0.attn_q.weight", [256, 256], 8),  # Q8_0
        ("output_norm.weight", [256], 0),  # F32
    ]
    sizes = {12: (256, 144), 8: (32, 34), 0: (1, 4)}
    out = b"GGUF" + struct.pack("<IQQ", 3, len(tensors), len(kvs))
    for key, kind, value in kvs:
        out += _gguf_str(key) + struct.pack("<I", kind)
        if kind == STRING:
            out += _gguf_str(value)
        elif kind == UINT32:
            out += struct.pack("<I", value)
        elif kind == FLOAT32:
            out += struct.pack("<f", value)
        else:
            out += struct.pack("<IQ", STRING, len(value))
            out += b"".join(_gguf_str(v) for v in value)
    offset = 0
    data_sizes = []
    for name, dims, ggml_type in tensors:
        out += _gguf_str(name) + struct.pack("<I", len(dims))
        out += b"".join(struct.pack("<Q", d) for d in dims)
        out += struct.pack("<IQ", ggml_type, offset)
        block, size = sizes[ggml_type]
        n = int(np.prod(dims)) // block * size
        data_sizes.append(n)
        offset += n + (-n % 32)
    out += b"\0" * (-len(out) % 32)
    out += b"\0" * offset
    with open(path, "wb") as f:
        f.write(out)
    print(f"Generated: {path}")


def generate_model_files():
    """Tiny model files: one SafeTensors file, a sharded checkpoint with its index,
    and a GGUF."""
    models = OUTPUT_DIR / "models"
    models.mkdir(exist_ok=True)
    rng = np.random.default_rng(42)
    write_safetensors(
        models / "tiny.safetensors",
        {
            "embed.weight": rng.standard_normal((16, 8)).astype(np.float32),
            "layer.0.weight": rng.standard_normal((8, 8)).astype(np.float16),
            "layer.0.bias": np.zeros(8, dtype=np.float32),
            "step": np.array(7, dtype=np.int64),
        },
        metadata={"format": "pt", "note": "tiny test model"},
    )
    sharded = models / "sharded"
    sharded.mkdir(exist_ok=True)
    shards = {
        "model-00001-of-00002.safetensors": {
            "embed.weight": rng.standard_normal((16, 8)).astype(np.float32),
        },
        "model-00002-of-00002.safetensors": {
            "layer.0.weight": rng.standard_normal((8, 8)).astype(np.float16),
            "layer.0.bias": np.zeros(8, dtype=np.float32),
        },
    }
    weight_map = {}
    total = 0
    for shard, tensors in shards.items():
        write_safetensors(sharded / shard, tensors, metadata={"format": "pt"})
        for name, array in tensors.items():
            weight_map[name] = shard
            total += array.nbytes
    with open(sharded / "model.safetensors.index.json", "w") as f:
        json.dump({"metadata": {"total_size": total}, "weight_map": weight_map}, f, indent=2)
    with open(sharded / "config.json", "w") as f:
        json.dump({"model_type": "tiny", "hidden_size": 8}, f)
    write_gguf(models / "tiny.gguf")
def _nmea(body):
    """A sentence with its checksum: the XOR of the bytes between `$` and `*`."""
    total = 0
    for ch in body.encode():
        total ^= ch
    return f"${body}*{total:02X}"


def _nmea_coord(value, positive, negative, width):
    """Decimal degrees as NMEA writes them: `ddmm.mmmm` and a hemisphere."""
    hemi = positive if value >= 0 else negative
    value = abs(value)
    deg = int(value)
    minutes = (value - deg) * 60
    return f"{deg:0{width}d}{minutes:07.4f}", hemi


def generate_nmea_drive(path):
    """A 1 Hz drive across midnight UTC: GGA, GSA, GSV, RMC and VTG each second.

    The first GGA comes before any RMC, so its date is learned from the next line. At
    second 150 the receiver drops out for 20 s, at second 200 the speed spikes, and a
    junk line, a sentence with a bad checksum and a vendor sentence are mixed in.
    """
    rng = random.Random(595)
    start = datetime(2024, 3, 9, 23, 58, 0)
    lat, lon = 47.3769, 8.5417
    lines = []
    t = start
    for i in range(300):
        if i == 150:
            t += timedelta(seconds=20)
        hhmmss = t.strftime("%H%M%S") + ".00"
        ddmmyy = t.strftime("%d%m%y")
        knots = 150.0 if i == 200 else 20.0 + rng.uniform(-1, 1)
        course = 45.0 + rng.uniform(-2, 2)
        lat += 0.00008
        lon += 0.00011
        la, ns = _nmea_coord(lat, "N", "S", 2)
        lo, ew = _nmea_coord(lon, "E", "W", 3)
        alt = 410.0 + i * 0.1
        quality = 4 if i % 50 == 0 else 1
        lines.append(_nmea(f"GPGGA,{hhmmss},{la},{ns},{lo},{ew},{quality},08,0.9,{alt:.1f},M,47.3,M,,"))
        lines.append(_nmea("GPGSA,A,3,01,03,07,08,11,17,19,28,,,,,1.6,0.9,1.3"))
        sats = [(1, 40, 83, 46), (3, 17, 308, 41), (7, 7, 344, 39), (8, 22, 228, 45),
                (11, 61, 120, 47), (17, 33, 45, 42), (19, 12, 270, 35), (28, 55, 190, 44)]
        for msg in range(2):
            blocks = ",".join(f"{p:02d},{e:02d},{a:03d},{snr + rng.randint(-2, 2):02d}"
                              for p, e, a, snr in sats[msg * 4:msg * 4 + 4])
            lines.append(_nmea(f"GPGSV,2,{msg + 1},08,{blocks}"))
        if i > 0:
            lines.append(_nmea(f"GPRMC,{hhmmss},A,{la},{ns},{lo},{ew},{knots:.1f},{course:.1f},{ddmmyy},,,A"))
        lines.append(_nmea(f"GPVTG,{course:.1f},T,,M,{knots:.1f},N,{knots * 1.852:.1f},K,A"))
        if i == 10:
            lines.append("logger: buffer flushed")
        if i == 20:
            good = _nmea(f"GPVTG,{course:.1f},T,,M,{knots:.1f},N,{knots * 1.852:.1f},K,A")
            lines[-1] = good[:-2] + ("00" if good[-2:] != "00" else "01")
        if i == 30:
            lines.append(_nmea("PGRME,15.0,M,45.0,M,25.0,M"))
        t += timedelta(seconds=1)
    with open(path, "w", newline="") as f:
        f.write("\r\n".join(lines) + "\r\n")
    print(f"Generated: {path}")


def generate_gpx_ride(path):
    """A GPX ride: two waypoints, a track of two segments with heart rate and cadence
    extensions (temperature only in the second), and a route of three points."""
    rng = random.Random(1595)
    start = datetime(2024, 5, 1, 6, 0, 0)
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<gpx version="1.1" creator="datui tests" xmlns="http://www.topografix.com/GPX/1/1"'
        ' xmlns:gpxtpx="http://www.garmin.com/xmlschemas/TrackPointExtension/v1">',
        '  <metadata><name>Morning ride</name><time>2024-05-01T06:00:00Z</time></metadata>',
        '  <wpt lat="47.37690" lon="8.54170"><ele>408</ele><name>Start &amp; finish</name><sym>Flag</sym></wpt>',
        '  <wpt lat="47.40000" lon="8.60000"><ele>520</ele><name>Summit</name><sym>Summit</sym></wpt>',
        '  <trk>',
        '    <name>Morning ride</name>',
    ]
    lat, lon, ele = 47.3769, 8.5417, 408.0
    t = start
    for seg, count in enumerate((100, 50)):
        out.append('    <trkseg>')
        for i in range(count):
            lat += 0.0002
            lon += 0.0003
            ele += rng.uniform(-0.5, 1.5)
            t += timedelta(seconds=1 if seg == 0 else 2)
            hr = 120 + i % 40
            cad = 80 + rng.randint(-5, 5)
            temp = f"<gpxtpx:atemp>{18 + i % 3}</gpxtpx:atemp>" if seg == 1 else ""
            out.append(
                f'      <trkpt lat="{lat:.6f}" lon="{lon:.6f}"><ele>{ele:.1f}</ele>'
                f'<time>{t.strftime("%Y-%m-%dT%H:%M:%SZ")}</time>'
                f'<extensions><gpxtpx:TrackPointExtension><gpxtpx:hr>{hr}</gpxtpx:hr>'
                f'<gpxtpx:cad>{cad}</gpxtpx:cad>{temp}</gpxtpx:TrackPointExtension></extensions></trkpt>'
            )
        out.append('    </trkseg>')
        t += timedelta(minutes=5)
    out += [
        '  </trk>',
        '  <rte><name>Way home</name>',
        '    <rtept lat="47.41" lon="8.61"><name>Turn</name></rtept>',
        '    <rtept lat="47.39" lon="8.58"/>',
        '    <rtept lat="47.3769" lon="8.5417"><name>Home</name></rtept>',
        '  </rte>',
        '</gpx>',
    ]
    with open(path, "w") as f:
        f.write("\n".join(out) + "\n")
    print(f"Generated: {path}")


def generate_gps():
    """GPS logs: an NMEA drive and a GPX ride."""
    gps = OUTPUT_DIR / "gps"
    gps.mkdir(exist_ok=True)
    generate_nmea_drive(gps / "drive.nmea")
    generate_gpx_ride(gps / "ride.gpx")


def generate_csv_dialect_files():
    """Text files laid out as instrument and logger exports are, for --comment-char,
    --header-rows and --skip-initial-space. Written as text: their layout is the point.
    """
    # A padded log: two comment lines, the second of them units, then the names, then
    # values padded to the width of their column, some cells only spaces.
    widths = [10, 9, 8, 13, 10, 8]
    names = ["Lcl Date", "Lcl Time", "UTCOfst", "Latitude", "bus1volts", "E1 CHT1"]
    lines = [
        '#device_info, log_version="1.03", model="X", serial="123"',
        "#yyyy-mm-dd, hh:mm:ss, hh:mm, degrees, volts, deg F",
        ",".join(n.rjust(w) for n, w in zip(names, widths)),
        ",".join(["".rjust(w) for w in widths[:4]] + ["25.1".rjust(10), "187.2".rjust(8)]),
    ]
    for i in range(20):
        cells = [
            "2024-03-01",
            f"10:00:{i:02d}",
            "-05:00",
            f"{40.1 + i / 1000:.6f}",
            f"{25.0 + (i % 5) / 10:.1f}",
            f"{180 + i * 0.5:.1f}",
        ]
        lines.append(",".join(c.rjust(w) for c, w in zip(cells, widths)))
    text = "\n".join(lines) + "\n"
    padded = OUTPUT_DIR / "dialect_padded_log.csv"
    padded.write_text(text)
    with gzip.open(OUTPUT_DIR / "dialect_padded_log.csv.gz", "wt", compresslevel=6) as f:
        f.write(text)
    print(f"Generated: {padded} (and .gz)")

    # Two header lines, names over units, that join into one name per column.
    two = OUTPUT_DIR / "dialect_two_header_rows.csv"
    two.write_text(
        "station,temp,pressure\n"
        ",degC,hPa\n"
        "A,12.5,1013.2\n"
        "B,13.0,1012.8\n"
        "C,11.75,1014.0\n"
    )
    print(f"Generated: {two}")

    # Comment lines before the header and among the data.
    mid = OUTPUT_DIR / "dialect_mid_comments.csv"
    mid.write_text(
        "# exported 2024-03-01\n"
        "id,value\n"
        "1,10\n"
        "# sensor reset\n"
        "2,20\n"
        "# sensor reset\n"
        "3,30\n"
    )
    print(f"Generated: {mid}")


def _riff_chunk(cid, body):
    """A RIFF chunk: id, little-endian size, body, padded to an even length."""
    return cid + struct.pack("<I", len(body)) + body + (b"\0" if len(body) % 2 else b"")


def _write_riff(path, chunks):
    body = b"WAVE" + b"".join(chunks)
    with open(path, "wb") as f:
        f.write(b"RIFF" + struct.pack("<I", len(body)) + body)
    print(f"Generated: {path}")


def _fmt(tag, channels, rate, bits, extensible=None):
    align = channels * bits // 8
    body = struct.pack("<HHIIHH", tag, channels, rate, rate * align, align, bits)
    if extensible is not None:
        valid_bits, mask, subformat = extensible
        guid = struct.pack("<H", subformat) + bytes.fromhex("000000001000800000aa00389b71")
        body += struct.pack("<HHI", 22, valid_bits, mask) + guid
    return body


def _int24(value):
    return struct.pack("<i", value)[:3]


def generate_audio_files():
    """Short audio files: a 16-bit WAV with a clipped run, a run of silence and a DC
    offset on its second channel; a 24-bit Broadcast WAV with bext, iXML, markers and
    INFO; a six-channel float WAV whose mask names its speakers; and an AIFF with a
    marker."""
    audio = OUTPUT_DIR / "audio"
    audio.mkdir(exist_ok=True)

    # tone.wav: 8 kHz stereo, 4,000 frames. ch1 a 440 Hz sine at half scale; ch2 the
    # same sine plus an offset of 1,000, clipped at full scale for frames 1000-1019
    # and silent (exact zeros) for frames 2000-2499.
    rate, frames = 8000, 4000
    out = bytearray()
    for i in range(frames):
        sine = int(16384 * math.sin(2 * math.pi * 440 * i / rate))
        right = sine + 1000
        if 1000 <= i < 1020:
            right = 32767
        elif 2000 <= i < 2500:
            right = 0
        out += struct.pack("<hh", sine, right)
    with wave.open(str(audio / "tone.wav"), "wb") as w:
        w.setnchannels(2)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(bytes(out))
    print(f"Generated: {audio / 'tone.wav'}")

    # take.wav: a 24-bit Broadcast WAV, 48 kHz stereo, 4,800 frames (0.1 s).
    rate, frames = 48000, 4800
    data = bytearray()
    for i in range(frames):
        v = int(4_000_000 * math.sin(2 * math.pi * 1000 * i / rate))
        data += _int24(v) + _int24(-v)
    bext = bytearray(602)
    for at, size, text in [
        (0, 256, b"Scene 12A, take 3"),
        (256, 32, b"Field recorder"),
        (288, 32, b"REF-0042"),
        (320, 10, b"2026-10-02"),
        (330, 8, b"14:30:00"),
    ]:
        bext[at : at + len(text)] = text[:size]
    # Time reference: one hour past midnight, in samples.
    struct.pack_into("<II", bext, 338, 3600 * rate, 0)
    struct.pack_into("<H", bext, 346, 2)
    bext += b"A=PCM,F=48000,W=24,M=stereo\r\n"
    ixml = (
        b"<?xml version=\"1.0\"?><BWFXML><PROJECT>Short film</PROJECT>"
        b"<SCENE>12A</SCENE><TAKE>3</TAKE><TAPE>Day 2</TAPE></BWFXML>"
    )
    cue = struct.pack("<I", 2)
    for cue_id, at in [(1, 0), (2, 2400)]:
        cue += struct.pack("<II4sIII", cue_id, at, b"data", 0, 0, at)
    adtl = b"adtl" + _riff_chunk(b"labl", struct.pack("<I", 1) + b"Slate\0")
    adtl += _riff_chunk(b"labl", struct.pack("<I", 2) + b"Action\0")
    adtl += _riff_chunk(b"ltxt", struct.pack("<IIIHHHH", 2, 1200, 0, 0, 0, 0, 0))
    info = b"INFO" + _riff_chunk(b"INAM", b"Scene 12A take 3\0")
    info += _riff_chunk(b"ISFT", b"datui fixtures\0")
    _write_riff(
        audio / "take.wav",
        [
            _riff_chunk(b"bext", bytes(bext)),
            _riff_chunk(b"iXML", ixml),
            _riff_chunk(b"fmt ", _fmt(1, 2, rate, 24)),
            _riff_chunk(b"data", bytes(data)),
            _riff_chunk(b"cue ", cue),
            _riff_chunk(b"LIST", adtl),
            _riff_chunk(b"LIST", info),
        ],
    )

    # surround.wav: 32-bit float, extensible, 5.1 (L R C LFE BL BR), 480 frames.
    rate, frames = 48000, 480
    data = b"".join(
        struct.pack("<6f", *[0.1 * (c + 1) * math.sin(2 * math.pi * 100 * i / rate) for c in range(6)])
        for i in range(frames)
    )
    _write_riff(
        audio / "surround.wav",
        [
            _riff_chunk(b"fmt ", _fmt(0xFFFE, 6, rate, 32, extensible=(32, 0x3F, 3))),
            _riff_chunk(b"data", data),
        ],
    )

    # loop.aiff: AIFF, 16-bit mono at 44.1 kHz, 441 frames, one marker.
    frames = 441
    samples = b"".join(
        struct.pack(">h", int(8000 * math.sin(2 * math.pi * 441 * i / 44100))) for i in range(frames)
    )
    # 44100 as an 80-bit extended float.
    rate80 = bytes([0x40, 0x0E, 0xAC, 0x44, 0, 0, 0, 0, 0, 0])
    comm = struct.pack(">hIh", 1, frames, 16) + rate80
    mark = struct.pack(">HHI", 1, 1, 220) + b"\x04Loop\x00"
    ssnd = struct.pack(">II", 0, 0) + samples

    def be_chunk(cid, body):
        return cid + struct.pack(">I", len(body)) + body + (b"\0" if len(body) % 2 else b"")

    body = b"AIFF" + be_chunk(b"COMM", comm) + be_chunk(b"MARK", mark) + be_chunk(b"SSND", ssnd)
    with open(audio / "loop.aiff", "wb") as f:
        f.write(b"FORM" + struct.pack(">I", len(body)) + body)
    print(f"Generated: {audio / 'loop.aiff'}")
def generate_sqlite():
    """SQLite databases: `shop.db` of several tables (a view, an AUTOINCREMENT table
    and so `sqlite_sequence`, a column of mixed types) and `one.sqlite` of one."""
    out = OUTPUT_DIR / "sqlite"
    out.mkdir(exist_ok=True)
    shop = out / "shop.db"
    shop.unlink(missing_ok=True)
    conn = sqlite3.connect(shop)
    conn.executescript(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            city VARCHAR(40),
            joined DATE
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER REFERENCES customers(id),
            amount REAL,
            note,
            receipt BLOB
        );
        CREATE VIEW big_orders AS SELECT * FROM orders WHERE amount > 100;
        CREATE INDEX orders_by_customer ON orders(customer_id);
        CREATE TRIGGER no_negative BEFORE INSERT ON orders
            WHEN NEW.amount < 0 BEGIN SELECT RAISE(ABORT, 'negative'); END;
        """
    )
    cities = ["Lisbon", "Oslo", "Quito", "Perth", None]
    conn.executemany(
        "INSERT INTO customers (name, city, joined) VALUES (?, ?, ?)",
        [
            (f"customer {i}", cities[i % len(cities)], f"2024-01-{i % 28 + 1:02d}")
            for i in range(1, 51)
        ],
    )
    notes = [None, "gift", 7, 2.5, b"\x00\x01"]
    conn.executemany(
        "INSERT INTO orders (customer_id, amount, note, receipt) VALUES (?, ?, ?, ?)",
        [
            (i % 50 + 1, round(i * 3.75, 2), notes[i % len(notes)], bytes([i % 256]) if i % 3 else None)
            for i in range(1, 201)
        ],
    )
    conn.commit()
    conn.close()
    print(f"Generated: {shop}")

    one = out / "one.sqlite"
    one.unlink(missing_ok=True)
    conn = sqlite3.connect(one)
    conn.executescript(
        """
        CREATE TABLE readings (station TEXT, at TEXT, celsius REAL, ok BOOLEAN);
        """
    )
    conn.executemany(
        "INSERT INTO readings VALUES (?, ?, ?, ?)",
        [
            (["north", "south"][i % 2], f"2024-05-01T{i % 24:02d}:00:00", 10 + i / 10, i % 7 != 0)
            for i in range(120)
        ],
    )
    conn.commit()
    conn.close()
    print(f"Generated: {one}")


def generate_numpy():
    """NumPy `.npy` files of each kind datui reads (and two it refuses), and `.npz`
    archives stored and compressed, of one array and of several."""
    out = OUTPUT_DIR / "numpy"
    out.mkdir(exist_ok=True)
    n = 500
    np.save(out / "vector.npy", np.arange(n, dtype="<f8") / 4)
    trades = np.zeros(n, dtype=[("ts", "<u8"), ("px", "<f8"), ("qty", "<i4")])
    trades["ts"] = 1_700_000_000_000 + np.arange(n) * 250
    trades["px"] = 100 + np.arange(n) / 100
    trades["qty"] = np.arange(n) % 7 - 3
    np.save(out / "trades.npy", trades)
    grid = np.arange(400, dtype="<f4").reshape(100, 4)
    np.save(out / "grid.npy", grid)
    np.save(out / "grid_fortran.npy", np.asfortranarray(grid))
    dtypes = np.dtype(
        [
            ("b1", "?"),
            ("i1", "i1"),
            ("i2", "<i2"),
            ("i4", "<i4"),
            ("i8", "<i8"),
            ("u1", "u1"),
            ("u2", "<u2"),
            ("u4", "<u4"),
            ("u8", "<u8"),
            ("f2", "<f2"),
            ("f4", "<f4"),
            ("f8", "<f8"),
            ("c8", "<c8"),
            ("c16", "<c16"),
            ("big", ">i4"),
            ("bytes", "S5"),
            ("text", "<U4"),
            ("at", "<M8[ns]"),
            ("day", "<M8[D]"),
            ("second", "<M8[s]"),
            ("took", "<m8[us]"),
        ]
    )
    each = np.zeros(3, dtype=dtypes)
    for name in ["i1", "i2", "i4", "i8", "u1", "u2", "u4", "u8", "big"]:
        each[name] = [1, 2, 3]
    for name in ["f2", "f4", "f8"]:
        each[name] = [0.5, 1.5, 2.5]
    each["b1"] = [True, False, True]
    each["c8"] = [1 + 2j, 3 - 4j, 0j]
    each["c16"] = [1 + 2j, 3 - 4j, 0j]
    each["bytes"] = [b"ab", b"cdefg", b""]
    each["text"] = ["h\u00e9", "\u65e5\u672c", ""]
    each["at"] = np.array(["2024-01-02T03:04:05.000000006", "NaT", "1970-01-01"], dtype="M8[ns]")
    each["day"] = np.array(["2024-02-29", "1969-12-31", "NaT"], dtype="M8[D]")
    each["second"] = np.array(["2024-01-01T00:00:01", "NaT", "2000-01-01"], dtype="M8[s]")
    each["took"] = np.array([1500, 0, -2], dtype="m8[us]")
    np.save(out / "dtypes.npy", each)
    aligned = np.zeros(4, dtype=np.dtype([("flag", "u1"), ("value", "<f8"), ("id", "<i2")], align=True))
    aligned["flag"] = [1, 0, 1, 0]
    aligned["value"] = [1.25, 2.5, 3.75, 5.0]
    aligned["id"] = [10, 20, 30, 40]
    np.save(out / "aligned.npy", aligned)
    offsets = np.zeros(
        3,
        dtype=np.dtype({"names": ["a", "b"], "formats": ["<i4", "<f4"], "offsets": [4, 12], "itemsize": 20}),
    )
    offsets["a"] = [7, 8, 9]
    offsets["b"] = [0.5, 0.25, 0.125]
    np.save(out / "offsets.npy", offsets)
    sub = np.zeros(5, dtype=[("id", "<i4"), ("px", "<f8", (10,))])
    sub["id"] = np.arange(5)
    sub["px"] = np.arange(50).reshape(5, 10) / 2
    np.save(out / "subarray.npy", sub)
    np.save(out / "cube.npy", np.zeros((2, 3, 4), dtype="<i2"))
    np.save(out / "objects.npy", np.array([{"a": 1}, None], dtype=object), allow_pickle=True)
    np.savez(out / "run.npz", prices=np.arange(n, dtype="<f8") / 4, grid=grid, trades=trades)
    np.savez_compressed(out / "packed.npz", prices=np.arange(n, dtype="<f8") / 4, grid=grid, trades=trades)
    np.savez(out / "single.npz", values=np.arange(10, dtype="<i8"))
    np.savez_compressed(out / "single_packed.npz", values=np.arange(10, dtype="<i8"))
    print(f"Generated: {out}")


def generate_elf():
    """A tiny 64-bit ELF executable written by hand: `.text`, `.rodata`, `.data` and
    `.bss`, with symbols in each and one mangled Rust name."""
    out = OUTPUT_DIR / "elf"
    out.mkdir(exist_ok=True)
    shstr = b"\0.text\0.rodata\0.data\0.bss\0.symtab\0.strtab\0.shstrtab\0"
    strtab = b"\0main\0TABLE\0counter\0buffer\0_ZN4core3fmt5write17h0123456789abcdefE\0weak_hook\0"
    sym_names = [b"main", b"TABLE", b"counter", b"buffer", b"_ZN4core3fmt5write17h0123456789abcdefE", b"weak_hook"]
    # (name, value, size, info, section index)
    symbols = [(0, 0, 0, 0, 0)] + [
        (strtab.index(n + b"\0"), value, size, info, shndx)
        for n, (value, size, info, shndx) in zip(
            sym_names,
            [(0x1000, 64, 0x12, 1), (0x2000, 256, 0x11, 2), (0x3000, 4, 0x11, 3),
             (0x3010, 1024, 0x01, 4), (0x1040, 128, 0x12, 1), (0x10C0, 8, 0x22, 1)],
        )
    ]
    symtab = b"".join(struct.pack("<IBBHQQ", n, i, 0, x, v, z) for n, v, z, i, x in symbols)
    body = bytearray()

    def place(data):
        at = 64 + len(body)
        body.extend(data)
        while len(body) % 8:
            body.append(0)
        return at

    text = b"\xc3" * 0xC8
    text_at = place(text)
    rodata_at = place(b"\x01" * 256)
    data_at = place(b"\x02" * 16)
    symtab_at = place(symtab)
    strtab_at = place(strtab)
    shstr_at = place(shstr)
    shoff = 64 + len(body)
    name = lambda n: shstr.index(n + b"\0")
    sections = [
        (0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        (name(b".text"), 1, 0x6, 0x1000, text_at, len(text), 0, 0, 16, 0),
        (name(b".rodata"), 1, 0x2, 0x2000, rodata_at, 256, 0, 0, 8, 0),
        (name(b".data"), 1, 0x3, 0x3000, data_at, 16, 0, 0, 8, 0),
        (name(b".bss"), 8, 0x3, 0x3010, data_at + 16, 1024, 0, 0, 8, 0),
        (name(b".symtab"), 2, 0, 0, symtab_at, len(symtab), 6, 1, 8, 24),
        (name(b".strtab"), 3, 0, 0, strtab_at, len(strtab), 0, 0, 1, 0),
        (name(b".shstrtab"), 3, 0, 0, shstr_at, len(shstr), 0, 0, 1, 0),
    ]
    header = b"\x7fELF" + bytes([2, 1, 1, 0]) + bytes(8)
    header += struct.pack("<HHIQQQIHHHHHH", 2, 62, 1, 0x1000, 0, shoff, 0, 64, 56, 0, 64, len(sections), 7)
    shdrs = b"".join(struct.pack("<IIQQQQIIQQ", *sec) for sec in sections)
    (out / "tiny.elf").write_bytes(header + bytes(body) + shdrs)
    print(f"Generated: {out}")


def _ulog_message(kind, payload):
    return struct.pack("<HB", len(payload), ord(kind)) + payload


def _ulog_key(key, value):
    key = key.encode()
    return bytes([len(key)]) + key + value


def generate_ulog(path):
    """A small PX4 ULog: a nested format, a topic with two instances and one with one,
    info, parameters (one changed in flight), logged text, a dropout, a damaged stretch
    before a sync marker, and data cut off at the end."""
    sync = bytes([0x2F, 0x73, 0x13, 0x20, 0x25, 0x0C, 0xBB, 0x12])
    log = b"ULog\x01\x12\x35" + bytes([1]) + struct.pack("<Q", 1_000)
    log += _ulog_message("B", bytes(40))
    log += _ulog_message("F", b"vec3:float x;float y;float z;")
    log += _ulog_message(
        "F",
        b"sensor_accel:uint64_t timestamp;uint32_t device_id;vec3 accel;float temperature;"
        b"int16_t[3] raw;uint8_t[2] _padding0;",
    )
    log += _ulog_message(
        "F", b"vehicle_status:uint64_t timestamp;uint8_t arming_state;bool failsafe;char[8] mode;"
    )
    log += _ulog_message("I", _ulog_key("char[3] sys_name", b"PX4"))
    log += _ulog_message("I", _ulog_key("char[7] ver_hw", b"SITL_V1"))
    log += _ulog_message("P", _ulog_key("float MPC_XY_VEL_MAX", struct.pack("<f", 12.0)))
    log += _ulog_message("P", _ulog_key("int32_t COM_ARM_WO_GPS", struct.pack("<i", 1)))
    for multi, msg_id, name in [(0, 1, b"sensor_accel"), (1, 2, b"sensor_accel"), (0, 3, b"vehicle_status")]:
        log += _ulog_message("A", struct.pack("<BH", multi, msg_id) + name)

    def accel(msg_id, t, i, padded):
        body = struct.pack("<HQI", msg_id, t, 100 + msg_id)
        body += struct.pack("<ffff", i * 0.5, -i * 0.25, 9.81, 30.0 + msg_id)
        body += struct.pack("<hhh", i, -i, 1000)
        if padded:
            body += bytes(2)
        return _ulog_message("D", body)

    for i in range(100):
        t = 10_000 + i * 1_000
        log += accel(1, t, i, i % 2 == 0)
        if i % 2 == 0:
            log += accel(2, t + 500, i, False)
        if i % 10 == 0:
            mode = (b"MANUAL" if i < 50 else b"MISSION").ljust(8, b"\0")
            log += _ulog_message("D", struct.pack("<HQB?", 3, t, 2 if i >= 20 else 1, i == 70) + mode)
        if i == 20:
            log += _ulog_message("L", b"6" + struct.pack("<Q", t) + b"Armed by RC")
        if i == 40:
            log += _ulog_message("C", b"4" + struct.pack("<HQ", 7, t) + b"Low battery")
            log += _ulog_message("P", _ulog_key("float MPC_XY_VEL_MAX", struct.pack("<f", 8.0)))
        if i == 60:
            log += _ulog_message("O", struct.pack("<H", 25))
        if i == 80:
            log += bytes([0xEE] * 9) + _ulog_message("S", sync)
    cut = accel(1, 999_999, 1, False)
    log += cut[:-6]
    path.write_bytes(log)


def _df_fmt(type_id, name, fmt, labels):
    sizes = {"Q": 8, "q": 8, "B": 1, "b": 1, "h": 2, "H": 2, "i": 4, "I": 4, "f": 4,
             "d": 8, "n": 4, "N": 16, "Z": 64, "c": 2, "C": 2, "e": 4, "E": 4, "L": 4, "M": 1}
    length = 89 if type_id == 0x80 else 3 + sum(sizes[c] for c in fmt)
    return (bytes([0xA3, 0x95, 0x80, type_id, length]) + name.encode().ljust(4, b"\0")
            + fmt.encode().ljust(16, b"\0") + labels.encode().ljust(64, b"\0"))


def generate_dataflash(path):
    """A small ArduPilot DataFlash log: FMT, UNIT, MULT and FMTU, attitude and GPS
    records interleaved, parameters and messages, a stray byte, and a record cut off at
    the end."""
    log = _df_fmt(0x80, "FMT", "BBnNZ", "Type,Length,Name,Format,Columns")
    log += _df_fmt(129, "UNIT", "QbZ", "TimeUS,Id,Label")
    log += _df_fmt(130, "MULT", "Qbd", "TimeUS,Id,Mult")
    log += _df_fmt(131, "FMTU", "QBNN", "TimeUS,FmtType,UnitIds,MultIds")
    log += _df_fmt(132, "PARM", "QNf", "TimeUS,Name,Value")
    log += _df_fmt(133, "MSG", "QZ", "TimeUS,Message")
    log += _df_fmt(140, "ATT", "QccC", "TimeUS,Roll,Pitch,Yaw")
    log += _df_fmt(141, "GPS", "QBLLeI", "TimeUS,Status,Lat,Lng,Alt,Ms")
    head = lambda t: bytes([0xA3, 0x95, t])
    for uid, label in [(b"s", "s"), (b"d", "deg"), (b"D", "deglatitude"), (b"U", "deglongitude"), (b"m", "m")]:
        log += head(129) + struct.pack("<Q", 0) + uid + label.encode().ljust(64, b"\0")
    for mid, mult in [(b"-", 0.0), (b"0", 1.0), (b"B", 0.01), (b"C", 0.001)]:
        log += head(130) + struct.pack("<Q", 0) + mid + struct.pack("<d", mult)
    fmtu = lambda t, units, mults: head(131) + struct.pack("<QB", 0, t) + units.encode().ljust(16, b"\0") + mults.encode().ljust(16, b"\0")
    log += fmtu(140, "sddd", "F000")
    log += fmtu(141, "s-DUm-", "F-GGB-")
    log += head(132) + struct.pack("<Q", 0) + b"ARMING_CHECK".ljust(16, b"\0") + struct.pack("<f", 1.0)
    for i in range(200):
        t = 1_000_000 + i * 20_000
        log += head(140) + struct.pack("<QhhH", t, 150 + i, -250, (i * 100) % 36000)
        if i % 4 == 0:
            log += head(141) + struct.pack("<QBiiiI", t + 5, 3, 473977418 + i, 85455939 - i, 48850 + i, 1000 * i)
        if i == 50:
            log += head(133) + struct.pack("<Q", t) + b"Mission: 1 WP".ljust(64, b"\0")
        if i == 120:
            log += b"\x00"
    log += head(140) + struct.pack("<Q", 9_999_999)
    path.write_bytes(log)


def generate_flight_logs():
    out = OUTPUT_DIR / "flight"
    out.mkdir(exist_ok=True)
    generate_ulog(out / "flight.ulg")
    generate_dataflash(out / "00000042.BIN")
    print(f"Generated: {out}")


CAN_DBC = """VERSION ""

NS_ :
	NS_DESC_
	CM_
	BA_DEF_
	VAL_

BS_:

BU_: ECU GW BODY

BO_ 291 ENGINE: 8 ECU
 SG_ Speed : 0|16@1+ (0.125,0) [0|8191.875] "rpm" GW
 SG_ Temp : 16|8@1- (1,-40) [-168|87] "degC" GW
 SG_ Gear : 24|4@1+ (1,0) [0|15] "" GW
 SG_ Throttle : 32|8@1+ (0.4,0) [0|102] "%" GW

BO_ 2566844926 BRAKES: 8 GW
 SG_ Pressure : 7|16@0+ (0.1,0) [0|6553.5] "kPa" ECU
 SG_ Balance : 23|12@0- (1,0) [-2048|2047] "" ECU

BO_ 512 BATTERY: 8 ECU
 SG_ Page M : 0|8@1+ (1,0) [0|255] "" GW
 SG_ Volts m1 : 8|16@1+ (0.01,0) [0|655.35] "V" GW
 SG_ Amps m2 : 8|16@1- (0.1,0) [-3276.8|3276.7] "A" GW
 SG_ Ratio : 24|32@1- (1,0) [0|0] "" GW

CM_ BO_ 291 "Engine status, every 10 ms";
CM_ SG_ 291 Speed "Crankshaft speed";
VAL_ 291 Gear 0 "Neutral" 1 "First" 2 "Second" 3 "Third" ;
SIG_VALTYPE_ 512 Ratio : 1;
"""

BODY_DBC = """BO_ 1024 DOORS: 1 BODY
 SG_ Open : 0|4@1+ (1,0) [0|15] "" GW
"""


def generate_can():
    """A candump log of 300 frames on two interfaces, with classic, extended, CAN FD,
    remote and error frames and a comment line; the same frames as candump prints
    them; and DBC files: one for every interface, and one a TOML file names for can1."""
    out = OUTPUT_DIR / "can"
    out.mkdir(exist_ok=True)
    lines = []
    printed = []
    t0 = 1_706_689_000.0
    for i in range(300):
        ts = f"({t0 + i * 0.01:.6f})"
        kind = i % 6
        if kind == 0:
            data = struct.pack("<HbBB", 8000 + i, -10 + i % 20, i % 4, 100) + bytes(3)
            frame = ("can0", "123", data)
        elif kind == 1:
            data = struct.pack(">HH", 4660 + i, 0xFFF0) + bytes(4)
            frame = ("can0", "18FEF1FE", data)
        elif kind == 2:
            data = bytes([1]) + struct.pack("<H", 10000 + i) + struct.pack("<f", 1.5) + bytes(1)
            frame = ("can0", "200", data)
        elif kind == 3:
            data = bytes([2]) + struct.pack("<h", -10 - i) + struct.pack("<f", -0.5) + bytes(1)
            frame = ("can0", "200", data)
        elif kind == 4:
            frame = ("can1", "400", bytes([i % 16]))
        else:
            frame = ("can1", "7DF", b"\x02\x01\x0c")
        iface, cid, data = frame
        lines.append(f"{ts} {iface} {cid}#{data.hex().upper()}")
        printed.append(f" {ts}  {iface}  {cid:>8}   [{len(data)}]  " + " ".join(f"{b:02X}" for b in data))
        if i == 100:
            lines.append("# a comment")
            lines.append(f"{ts} can0 123##1" + (bytes(range(12)).hex().upper()))
            lines.append(f"{ts} can1 7DF#R")
            lines.append(f"{ts} can0 20000080#0000000000000000")
    (out / "candump-2024-01-31_081640.log").write_text("\n".join(lines) + "\n")
    (out / "printed.txt").write_text("\n".join(printed) + "\n")
    dbc = out / "dbc"
    dbc.mkdir(exist_ok=True)
    (dbc / "car.dbc").write_text(CAN_DBC)
    (dbc / "body").mkdir(exist_ok=True)
    (dbc / "body" / "body.dbc").write_text(BODY_DBC)
    (dbc / "body.toml").write_text('kind = "dbc"\nfile = "body/body.dbc"\n[match]\ninterface = "can1"\n')
    print(f"Generated: {out}")


def _vlq(n):
    """A MIDI variable-length quantity: seven bits a byte, high bit on all but the last."""
    out = [n & 0x7F]
    n >>= 7
    while n:
        out.insert(0, (n & 0x7F) | 0x80)
        n >>= 7
    return bytes(out)


def _meta(kind, data):
    if isinstance(data, str):
        data = data.encode("latin-1")
    return bytes([0xFF, kind]) + _vlq(len(data)) + data


def _smf(fmt, division, tracks):
    """A Standard MIDI File: `tracks` are lists of (delta, event bytes)."""
    out = b"MThd" + struct.pack(">IHHH", 6, fmt, len(tracks), division)
    for events in tracks:
        body = b"".join(_vlq(delta) + event for delta, event in events)
        out += b"MTrk" + struct.pack(">I", len(body)) + body
    return out


END = _meta(0x2F, b"")


def generate_midi_files():
    """MIDI files written by hand: a format 1 song with a tempo change, running status,
    a note that never ends and a sysex; a format 0 drum loop; SMPTE timing; a RIFF MIDI
    wrapper; a directory of songs with one broken file; and a file cut short.
    """
    midi = OUTPUT_DIR / "midi"
    midi.mkdir(exist_ok=True)
    conductor = [
        (0, _meta(0x03, "Song")),
        (0, _meta(0x02, "(c) datui tests")),
        (0, _meta(0x58, bytes([4, 2, 24, 8]))),
        (0, _meta(0x59, bytes([1, 0]))),  # one sharp, major: G
        (0, _meta(0x51, (500_000).to_bytes(3, "big"))),  # 120 bpm
        (0, bytes([0xF0, 5, 0x7E, 0x7F, 0x09, 0x01, 0xF7])),
        (3840, _meta(0x51, (666_667).to_bytes(3, "big"))),  # 90 bpm from bar 3
        (0, _meta(0x06, "Chorus")),
        (0, END),
    ]
    piano = [
        (0, _meta(0x03, "Piano")),
        (0, _meta(0x04, "Acoustic Grand")),
        (0, bytes([0xC0, 0])),
        (0, bytes([0xB0, 64, 127])),  # sustain on
        (0, bytes([0x90, 60, 100])),  # C4, E4 and G4, the last two by running status
        (0, bytes([64, 80])),
        (0, bytes([67, 90])),
        (480, bytes([0x80, 60, 64])),
        (0, bytes([64, 64])),
        (0, bytes([67, 64])),
        (0, bytes([0xB0, 64, 0])),
        (0, bytes([0xE0, 0x00, 0x40])),  # pitch bend at center
        (480, bytes([0x90, 72, 127])),  # C5, never released
        (0, _meta(0x05, "la")),
        (0, END),
    ]
    bass = [
        (0, _meta(0x03, "Bass")),
        (0, bytes([0x91, 36, 112])),
        (960, bytes([36, 0])),  # a note on at velocity 0 is a note off
        (0, bytes([36, 96])),
        (960, bytes([36, 0])),
        (0, END),
    ]
    song = _smf(1, 480, [conductor, piano, bass])
    (midi / "song.mid").write_bytes(song)
    drums = _smf(
        0,
        96,
        [[
            (0, _meta(0x51, (600_000).to_bytes(3, "big"))),  # 100 bpm
            (0, bytes([0x99, 36, 100])),
            (96, bytes([0x89, 36, 0])),
            (0, END),
        ]],
    )
    (midi / "drums.mid").write_bytes(drums)
    # 25 fps and 40 ticks a frame: a thousand ticks a second, whatever the tempo.
    smpte = _smf(
        0,
        ((256 - 25) << 8) | 40,
        [[(0, bytes([0x90, 60, 100])), (1000, bytes([0x80, 60, 0])), (0, END)]],
    )
    (midi / "smpte.mid").write_bytes(smpte)
    riff = b"RMID" + b"data" + struct.pack("<I", len(song)) + song
    (midi / "song.rmi").write_bytes(b"RIFF" + struct.pack("<I", len(riff)) + riff)
    (midi / "cut_short.mid").write_bytes(song[:-5])
    corpus = midi / "corpus"
    corpus.mkdir(exist_ok=True)
    (corpus / "drums.mid").write_bytes(drums)
    (corpus / "song.mid").write_bytes(song)
    (corpus / "broken.mid").write_bytes(song[:-5])
    print(f"Generated: {midi}")


def main():
    print("Generating sample data files...")
    print(f"Output directory: {OUTPUT_DIR}")

    # People data for grouping
    print("\n1. Generating people data...")
    people_df = generate_people_data()
    save_csv(people_df, "people.csv")
    save_parquet(people_df, "people.parquet")
    save_ipc(people_df, "people.arrow")
    save_ipc_streams(people_df)
    save_avro(people_df, "people.avro")
    save_excel(people_df, "people.xlsx")

    # Sales data for aggregates
    print("\n2. Generating sales data...")
    sales_df = generate_sales_data()
    save_csv(sales_df, "sales.csv")
    save_parquet(sales_df, "sales.parquet")
    save_ipc(sales_df, "sales.arrow")
    save_avro(sales_df, "sales.avro")
    save_excel(sales_df, "sales.xlsx")

    # Mixed types
    print("\n3. Generating mixed types data...")
    mixed_df = generate_mixed_types()
    save_csv(mixed_df, "mixed_types.csv")
    save_parquet(mixed_df, "mixed_types.parquet")
    save_ipc(mixed_df, "mixed_types.arrow")
    save_avro(mixed_df, "mixed_types.avro")
    save_excel(mixed_df, "mixed_types.xlsx")

    # Generate a small uncompressed CSV for testing (3 columns, good coverage)
    print("\n3a. Generating small test CSV (uncompressed)...")
    test_df = generate_mixed_types()  # Reuse mixed_types as it has good coverage
    # Save uncompressed version
    test_filepath = OUTPUT_DIR / "3-sfd-header.csv"
    test_df.write_csv(test_filepath)
    print(f"Generated: {test_filepath}")

    # Quoted strings
    print("\n4. Generating quoted strings data...")
    quoted_df = generate_quoted_strings()
    save_csv(quoted_df, "quoted_strings.csv")
    # For unquoted, we'll just save without special quoting (Polars handles this)
    save_csv(quoted_df, "unquoted_strings.csv")

    # Empty table
    print("\n5. Generating empty table...")
    empty_df = generate_empty_table()
    save_csv(empty_df, "empty.csv")
    save_parquet(empty_df, "empty.parquet")
    save_ipc(empty_df, "empty.arrow")
    save_avro(empty_df, "empty.avro")
    save_excel(empty_df, "empty.xlsx")

    # Single row
    print("\n6. Generating single row table...")
    single_df = generate_single_row()
    save_csv(single_df, "single_row.csv")
    save_parquet(single_df, "single_row.parquet")
    save_ipc(single_df, "single_row.arrow")
    save_avro(single_df, "single_row.avro")
    save_excel(single_df, "single_row.xlsx")

    # Large dataset
    print("\n7. Generating large dataset...")
    large_df = generate_large_dataset()
    save_csv(large_df, "large_dataset.csv")
    save_parquet(large_df, "large_dataset.parquet")

    # Error cases
    print("\n8. Generating error case files...")
    error_cases = generate_error_cases()
    for name, df in error_cases.items():
        save_csv(df, f"error_{name}.csv")
        # Skip parquet for inconsistent_types as it can't handle mixed types
        if name != "inconsistent_types":
            save_parquet(df, f"error_{name}.parquet")

    # Pivot and Melt testing
    print("\n9. Generating pivot and melt testing data...")
    pivot_long_df = generate_pivot_long()
    save_csv(pivot_long_df, "pivot_long.csv")
    save_parquet(pivot_long_df, "pivot_long.parquet")
    save_ipc(pivot_long_df, "pivot_long.arrow")
    save_avro(pivot_long_df, "pivot_long.avro")
    save_excel(pivot_long_df, "pivot_long.xlsx")
    pivot_long_string_df = generate_pivot_long_string()
    save_csv(pivot_long_string_df, "pivot_long_string.csv")
    save_parquet(pivot_long_string_df, "pivot_long_string.parquet")
    melt_wide_df = generate_melt_wide()
    save_csv(melt_wide_df, "melt_wide.csv")
    save_parquet(melt_wide_df, "melt_wide.parquet")
    save_ipc(melt_wide_df, "melt_wide.arrow")
    save_avro(melt_wide_df, "melt_wide.avro")
    save_excel(melt_wide_df, "melt_wide.xlsx")
    melt_wide_many_df = generate_melt_wide_many()
    save_csv(melt_wide_many_df, "melt_wide_many.csv")
    save_parquet(melt_wide_many_df, "melt_wide_many.parquet")

    # Charting demo (10 years daily time series)
    print("\n10. Generating charting demo data...")
    chart_df = generate_charting_demo()
    save_parquet(chart_df, "charting_demo.parquet")

    # Correlation matrix demo (Parquet only: 100k rows, 10 numeric columns)
    print("\n11. Generating correlation matrix demo data...")
    corr_df = generate_correlation_matrix_data()
    save_parquet(corr_df, "correlation_matrix_demo.parquet")

    # Infer schema length demo (CSV only: 200 rows, 100 ints, then "N/A", then 100 more ints)
    print("\n12. Generating infer schema length demo data...")
    infer_schema_length_data = generate_infer_schema_length_data()
    save_infer_schema_length_data(infer_schema_length_data, "infer_schema_length_data.csv")

    # Model files: SafeTensors (one file and a sharded checkpoint) and GGUF
    print("\n13. Generating model files...")
    generate_model_files()
    print("\n14. Generating CSV dialect files...")
    generate_csv_dialect_files()

    # GPS logs: NMEA 0183 and GPX
    print("\n15. Generating GPS logs...")
    generate_gps()
    # Audio: WAV, Broadcast WAV, extensible float and AIFF
    print("\n16. Generating audio files...")
    generate_audio_files()
    print("\n17. Generating MIDI files...")
    generate_midi_files()

    # SQLite databases
    print("\n18. Generating SQLite databases...")
    generate_sqlite()

    print("\n19. Generating NumPy arrays...")
    generate_numpy()

    print("\n20. Generating an ELF file...")
    generate_elf()

    print("\n21. Generating flight logs...")
    generate_flight_logs()

    print("\n22. Generating CAN logs...")
    generate_can()

    print("\nSample data generation complete!")

if __name__ == "__main__":
    main()
