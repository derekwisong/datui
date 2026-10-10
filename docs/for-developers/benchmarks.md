# Run benchmarks

<a id="reproduce"></a>

`scripts/bench/startup.py` measures time to first rows and peak memory, as
[Performance](../reference/performance.md) reports them.

```bash,repo
./scripts/dev/test.sh setup
cargo build --release --locked -p datui
.venv/bin/python scripts/bench/startup.py table --datui target/release/datui --data ~/tmp/datui-bench --remote --compare
```

`setup` makes the `.venv` with Polars and NumPy, once.

| Option | Default | Effect |
|---|---|---|
| `--rows` | `1M,10M,30M` | File sizes to generate; each writes one Parquet and one CSV file |
| `--runs` | 5 | Runs per viewer and case; remote cases run at most 3 |
| `--remote` | off | Add the two public-catalog files on the Performance page |
| `--compare` | off | Add VisiData and tabiew when installed; never installs them |
| `--settle` | 2 | Seconds datui stays open after the first rows; peak RSS covers this time |
| `--json FILE` | | Write every run |

The generated files are seeded (seed 561) and written once under `--data`. The
default sizes take about 2.8 GiB of disk. The script needs Linux (it uses a pty
and `/proc`). Where Python lacks `posix_fadvise`, the table has warm-cache rows
only.

## The first-rows hook

With `DATUI_TRACE_FIRST_ROWS=FILE` set, datui writes the wall-clock time, in
Unix nanoseconds, to FILE as one line, right after it draws the first frame that
shows rows. It writes only once. The script compares that time with the time it
started datui; the [doc-example runner](documentation.md#run-the-checks)
uses it to know an example opened.

## Regression check

The Nightly workflow's **Startup guard** job runs:

```bash,repo
.venv/bin/python scripts/bench/startup.py guard --baseline baseline/datui --candidate target/release/datui --data ~/tmp/datui-bench
```

| Rule | Value |
|---|---|
| Files | Generated 5M-row Parquet and CSV, warm cache |
| Runs | 7 per build, baseline and candidate interleaved, alternating which goes first; peak RSS measured until 3 s after the first rows |
| Baseline | The build from the last Nightly on `main` whose guard passed and whose build is still kept; until there is one, the latest release |
| Fails when | The candidate's median time to first rows is at least 2× the baseline's and 100 ms slower, or its median peak RSS is at least 2× and 128 MiB larger, or the hook writes nothing |

Both builds run on the same runner in the same job, so the runner's speed
cancels out. Both are timed from the terminal output, because the baseline may
predate the hook.

### Accept a new baseline

A failing night never becomes the baseline, so an intended slowdown keeps
failing the guard. Accept it by running Nightly on `main` with `accept_baseline`; on any
other branch the input is ignored.

```bash,repo
gh workflow run nightly.yml --ref main -f accept_baseline=true
```

| | |
|---|---|
| Measures | As usual; the table is in the run's summary |
| Passes | Even when the candidate is slower or larger, which is reported as a warning. A candidate that shows no rows, or whose hook writes nothing, still fails |
| Becomes | The baseline for the following nights |
| Record | The run's log: a notice naming who accepted it, and the summary's last line. Nothing is checked in |

## Microbenchmarks

`crates/datui-lib/benches/numfmt.rs` is a [Criterion](https://github.com/bheisler/criterion.rs)
benchmark of number formatting. Criterion keeps the last run in
`target/criterion` and reports the change against it.

```bash,repo
cargo bench -p datui-lib --bench numfmt
```

## Profile

The release profile keeps symbol names but strips debug info. For a profile
with source lines, build with line tables and without stripping, then record with
[samply](https://github.com/mstange/samply) or
[cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph). Replace
`<FILE>` with the dataset to open:

```bash,template
CARGO_PROFILE_RELEASE_DEBUG=line-tables-only CARGO_PROFILE_RELEASE_STRIP=none \
    cargo build --release --locked -p datui
samply record target/release/datui <FILE>
```

| Tool | Shows |
|---|---|
| `samply record` | Where CPU time goes, in the Firefox Profiler, per thread |
| `cargo flamegraph --release -p datui -- <FILE>`, with the same variables | The same as an SVG flame graph (Linux `perf`, macOS `dtrace`) |
| `heaptrack target/release/datui <FILE>` | Allocations and peak heap, by call site (Linux) |

Polars runs its work on its own thread pool, so look at those threads, not
only the UI thread.

## Link faster

Every integration test executable links Polars, so linking takes up much of
each edit-and-test cycle. On x86_64 Linux, Rust links with LLD already;
[mold](https://github.com/rui314/mold) is often faster still. With `clang` and
`mold` installed:

```bash,repo
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang \
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=-fuse-ld=mold" \
cargo build
```

To keep it, set `linker = "clang"` and
`rustflags = ["-C", "link-arg=-fuse-ld=mold"]` under
`[target.x86_64-unknown-linux-gnu]` in your own `~/.cargo/config.toml`, not in
the repository. Changing the linker or flags rebuilds everything once.
