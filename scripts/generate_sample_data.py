#!/usr/bin/env python3
"""
Generate sample data files for datui testing.

This script generates various CSV and Parquet files with different characteristics:
- Different data types
- Missing data (nulls)
- Empty tables
- Quoted and unquoted strings
- Files for grouping operations
- Files for aggregate calculations
- Large and small datasets
- Error case testing

Uses Polars instead of Pandas.
"""

import os
import sys
from pathlib import Path
import polars as pl
import numpy as np
from datetime import datetime, timedelta
import random
import gzip

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

def main():
    print("Generating sample data files...")
    print(f"Output directory: {OUTPUT_DIR}")
    
    # People data for grouping
    print("\n1. Generating people data...")
    people_df = generate_people_data()
    save_csv(people_df, "people.csv")
    save_parquet(people_df, "people.parquet")
    
    # Sales data for aggregates
    print("\n2. Generating sales data...")
    sales_df = generate_sales_data()
    save_csv(sales_df, "sales.csv")
    save_parquet(sales_df, "sales.parquet")
    
    # Mixed types
    print("\n3. Generating mixed types data...")
    mixed_df = generate_mixed_types()
    save_csv(mixed_df, "mixed_types.csv")
    save_parquet(mixed_df, "mixed_types.parquet")
    
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
    
    # Single row
    print("\n6. Generating single row table...")
    single_df = generate_single_row()
    save_csv(single_df, "single_row.csv")
    save_parquet(single_df, "single_row.parquet")
    
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
    
    print("\n✅ Sample data generation complete!")

if __name__ == "__main__":
    main()
