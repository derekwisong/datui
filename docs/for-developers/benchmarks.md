# Run benchmarks

Measure time to first rows and peak memory, as [Performance](../reference/performance.md) reports them.

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
| Runs | 7 per build, baseline and candidate interleaved, alternating which goes first; peak RSS through 3 s after the first rows |
| Baseline | The build from the last Nightly on `main` whose guard passed and whose build is still kept; the latest release before there is one |
| Fails when | The candidate's median first rows is 2× the baseline's and 100 ms slower, or its median peak RSS is 2× and 128 MiB larger, or the hook writes nothing |

Both builds run on the same runner in the same job, so runner speed cancels out.
Both are timed from the terminal output, because the baseline may predate the
hook.

### Accept a new baseline

A failing night is never a baseline, so an intended slowdown keeps the guard
failing. Accept it by hand: run Nightly on `main` with `accept_baseline` set.
On any other branch the input is ignored, since only `main` holds baselines.

```bash
gh workflow run nightly.yml --ref main -f accept_baseline=true
```

| | |
|---|---|
| Measures | As usual; the table is in the run's summary |
| Passes | Despite a slower or larger candidate, which is reported as a warning. A candidate that shows no rows, or whose hook writes nothing, still fails |
| Becomes | The baseline for the following nights |
| Record | The run's log: a notice naming who accepted it, and the summary's last line. Nothing is checked in |
