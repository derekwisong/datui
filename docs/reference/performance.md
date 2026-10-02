# Performance

Time to first rows and peak memory, measured with `scripts/bench/startup.py` on a
release build.

| Measure | Meaning |
|---|---|
| First rows | From launch to the first drawn frame that shows the file's rows |
| Peak RSS | The process's resident high-water mark (`VmHWM`), from launch until 2 s after the first rows |
| Warm | The file is already in the page cache |
| Cold | The file is dropped from the page cache first (`posix_fadvise` `DONTNEED`) |

## Results

datui 0.4.0-dev, median of 5 runs (remote: 3). Terminal 120×30.

| File | Cache | First rows | Peak RSS |
|---|---|---:|---:|
| Parquet, 1M rows (13 MB) | warm | 8 ms | 55 MiB |
| Parquet, 1M rows (13 MB) | cold | 11 ms | 55 MiB |
| CSV, 1M rows (58 MB) | warm | 12 ms | 173 MiB |
| CSV, 1M rows (58 MB) | cold | 14 ms | 172 MiB |
| Parquet, 10M rows (132 MB) | warm | 8 ms | 56 MiB |
| Parquet, 10M rows (132 MB) | cold | 11 ms | 55 MiB |
| CSV, 10M rows (586 MB) | warm | 12 ms | 688 MiB |
| CSV, 10M rows (586 MB) | cold | 14 ms | 688 MiB |
| Parquet, 30M rows (397 MB) | warm | 8 ms | 55 MiB |
| Parquet, 30M rows (397 MB) | cold | 10 ms | 55 MiB |
| CSV, 30M rows (1,779 MB) | warm | 10 ms | 1.43 GiB |
| CSV, 30M rows (1,779 MB) | cold | 11 ms | 1.38 GiB |
| [NYC yellow taxis, Jan 2025](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet) (HTTPS, 59 MB Parquet) | remote | 667 ms | 71 MiB |
| [NOAA GHCN-D 2023 TMAX](https://registry.opendata.aws/noaa-ghcn/) (`s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/ELEMENT=TMAX/`, 9 files) | remote | 540 ms | 72 MiB |

- Only the rows on screen are read before the first frame, so first rows does
  not grow with the file.
- CSV peak RSS grows with the file: the row count in the status bar reads the
  whole CSV in the 2 s after the first rows. A Parquet footer holds the count.
- The HTTPS row includes downloading the file and answering the download
  question. The S3 row reads the footers and the first row group in place.
- Remote rows depend on the network and the server.

VisiData and tabiew were not installed on the machine, so they have no column.
The script adds them when `vd` or `tw` is on the `PATH`, timed by the first row's
cell appearing in the terminal output.

## Machine

| | |
|---|---|
| CPU | AMD Ryzen 7 9800X3D, 8 cores, 16 threads |
| Memory | 62 GiB |
| Disk | Samsung 990 PRO NVMe, btrfs with zstd compression |
| OS | Arch Linux, kernel 7.2.7 |
| Date | October 2, 2026 |

## Reproduce

```bash
./scripts/dev/setup-test-data.sh        # once: .venv with Polars and NumPy
cargo build --release --locked -p datui
.venv/bin/python scripts/bench/startup.py table \
  --datui target/release/datui --data ~/tmp/datui-bench --remote --compare
```

| Option | Default | Effect |
|---|---|---|
| `--rows` | `1M,10M,30M` | File sizes to generate; each writes one Parquet and one CSV file |
| `--runs` | 5 | Runs per viewer and case; remote cases run at most 3 |
| `--remote` | off | Add the two public-catalog files above |
| `--compare` | off | Add VisiData and tabiew if installed; never installs them |
| `--settle` | 2 | Seconds kept open after the first rows, inside the peak RSS window |
| `--json FILE` | | Write every run |

The generated files are seeded (seed 561) and written once under `--data`. The
default sizes take about 3 GB of disk. Linux only. On a platform without
`posix_fadvise` the table has warm rows only.

## The first-rows hook

```bash
DATUI_TRACE_FIRST_ROWS=/path/to/file datui data.parquet
```

datui writes the wall-clock time, in Unix nanoseconds, to the file as one line
right after the first frame that shows rows is drawn, then never again. Unset,
it does nothing. The script compares it with the time it started datui.

## Regression check

The Nightly workflow's **Startup guard** job runs:

```bash
.venv/bin/python scripts/bench/startup.py guard \
  --baseline BASELINE/datui --candidate target/release/datui --data DIR
```

| Rule | Value |
|---|---|
| Files | Generated 5M-row Parquet and CSV, warm cache |
| Runs | 7 per build, baseline and candidate interleaved |
| Baseline | The build from the last Nightly on `main` whose guard passed; the latest release before there is one |
| Fails when | The candidate's median first rows is 2× the baseline's and 100 ms slower, or its median peak RSS is 2× and 128 MiB larger, or the hook writes nothing |

Both builds run on the same runner in the same job, so runner speed cancels out.
Both are timed from the terminal output, because the baseline may predate the
hook.
