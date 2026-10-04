# Run benchmarks

<a id="reproduce"></a>

`scripts/bench/startup.py` measures time to first rows and peak memory, as
[Performance](../reference/performance.md) reports them.

```bash,repo
./scripts/dev/setup-test-data.sh
cargo build --release --locked -p datui
.venv/bin/python scripts/bench/startup.py table --datui target/release/datui --data ~/tmp/datui-bench --remote --compare
```

`setup-test-data.sh` makes the `.venv` with Polars and NumPy, once.

| Option | Default | Effect |
|---|---|---|
| `--rows` | `1M,10M,30M` | File sizes to generate; each writes one Parquet and one CSV file |
| `--runs` | 5 | Runs per viewer and case; remote cases run at most 3 |
| `--remote` | off | Add the two public-catalog files on the Performance page |
| `--compare` | off | Add VisiData and tabiew when installed; never installs them |
| `--settle` | 2 | Seconds kept open after the first rows, inside the peak RSS window |
| `--json FILE` | | Write every run |

The generated files are seeded (seed 561) and written once under `--data`. The
default sizes take about 3 GB of disk. Linux only. On a platform without
`posix_fadvise` the table has warm rows only.

## The first-rows hook

`DATUI_TRACE_FIRST_ROWS=FILE` makes datui write the wall-clock time, in Unix
nanoseconds, to FILE as one line, right after the first frame that shows rows
is drawn, then never again. Unset, it does nothing. The script compares it with
the time it started datui; the [doc-example runner](documentation.md#run-the-checks)
uses it to know an example opened.

## Regression check

The Nightly workflow's **Startup guard** job runs:

```bash,repo
.venv/bin/python scripts/bench/startup.py guard --baseline baseline/datui --candidate target/release/datui --data ~/tmp/datui-bench
```

| Rule | Value |
|---|---|
| Files | Generated 5M-row Parquet and CSV, warm cache |
| Runs | 7 per build, baseline and candidate interleaved, alternating which goes first; peak RSS through 3 s after the first rows |
| Baseline | The build from the last Nightly on `main` whose guard passed and whose build is still kept; the latest release before there is one |
| Fails when | The candidate's median first rows is 2× the baseline's and 100 ms slower, or its median peak RSS is 2× and 128 MiB larger, or the hook writes nothing |

Both builds run on the same runner in the same job, so the runner's speed
cancels out. Both are timed from the terminal output, because the baseline may
predate the hook.

### Accept a new baseline

A failing night is never a baseline, so an intended slowdown keeps the guard
failing. Accept it by running Nightly on `main` with `accept_baseline`; on any
other branch the input is ignored.

```bash,repo
gh workflow run nightly.yml --ref main -f accept_baseline=true
```

| | |
|---|---|
| Measures | As usual; the table is in the run's summary |
| Passes | Despite a slower or larger candidate, which is reported as a warning. A candidate that shows no rows, or whose hook writes nothing, still fails |
| Becomes | The baseline for the following nights |
| Record | The run's log: a notice naming who accepted it, and the summary's last line. Nothing is checked in |
