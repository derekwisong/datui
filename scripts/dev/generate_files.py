from operator import ge
import polars as pl
import numpy as np
import os
from datetime import datetime, timedelta

def generate_stress_test_parquet(target_mb, file_name):
    n_rows = 250_000

    # 1. Use pl.select to build the DataFrame correctly from the start
    df_chunk = pl.select([
        pl.lit(np.random.randint(0, 1e9, n_rows, dtype=np.int64)).alias("id"),
        pl.lit(np.random.choice(["Alpha", "Bravo", "Charlie", "Delta", "Echo"], n_rows)).alias("label"),
        pl.lit(np.random.randn(n_rows).astype(np.float64)).alias("value_float"),
        pl.lit(np.random.poisson(50, n_rows).astype(np.int64)).alias("value_int"),
        # Use pl.repeat for the long string column
        pl.repeat("Standardized long string for testing data density " * 5, n_rows).alias("description"),
        # Native Boolean generation
        pl.lit(np.random.randint(0, 2, n_rows)).cast(pl.Boolean).alias("is_active"),
        # Native Null generation
        pl.when(pl.lit(np.random.rand(n_rows)) > 0.3)
          .then(pl.lit(np.random.randint(1, 100, n_rows)))
          .otherwise(None)
          .alias("null_heavy_col"),
        # Native Datetime range
        pl.datetime_range(
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 1) + timedelta(minutes=n_rows - 1),
            interval="1m",
            eager=True
        ).alias("timestamp")
    ])

    print(f"Generating {file_name} (Target: {target_mb}MB)...")

    full_df = df_chunk
    current_size_mb = 0

    # Start writing and growing
    while current_size_mb < target_mb:
        # row_group_size is key for S3 tool parallelization testing
        full_df.write_parquet(file_name, compression="snappy", row_group_size=100_000)
        current_size_mb = os.path.getsize(file_name) / (1024 * 1024)

        if current_size_mb >= target_mb:
            break

        # Double the dataframe size each iteration for speed
        full_df = pl.concat([full_df, full_df])

        # Stop if we accidentally go way over (e.g. 2GB)
        if current_size_mb > target_mb * 2:
            break

    print(f"Success: {file_name}")
    print(f"   Size: {current_size_mb:.2f} MB | Rows: {full_df.height:,}\n")


# # Generate a 10MB file
# generate_stress_test_parquet(10, "stress_test_10mb.parquet")

# # Generate a 100MB file
# generate_stress_test_parquet(100, "stress_test_100mb.parquet")

# # Generate a 500MB file
# generate_stress_test_parquet(500, "stress_test_500mb.parquet")

# # Generate the 1GB file
# generate_stress_test_parquet(1024, "stress_test_1gb.parquet")

# # Generate a 5GB file
# generate_stress_test_parquet(5000, "stress_test_5gb.parquet")

# # Generate a 10GB file
# generate_stress_test_parquet(10000, "stress_test_10gb.parquet")

# Generate a 100GB file
generate_stress_test_parquet(100000, "stress_test_100gb.parquet")