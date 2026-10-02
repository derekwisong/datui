# Tests

```bash
./scripts/dev/setup-test-data.sh   # once: creates .venv and generates the fixtures
./scripts/dev/test.sh full         # cargo test --workspace --locked --no-fail-fast
./scripts/dev/test.sh --help       # the scoped commands
```

`cargo test` alone runs only the root package. `--workspace` adds `datui-lib`
and `datui-cli`. CI runs the same tests with
`cargo nextest run --workspace --locked --no-fail-fast`, one process per test,
then `cargo test --doc --workspace --locked`. The Python bindings are tested
separately; see [Python Bindings](python-bindings.md#testing).

## Select the checks

Run `./scripts/dev/test.sh check` while editing, then select the relevant test
target. A name filter selects tests to execute; it does not by itself restrict
the executables Cargo builds.

| Command | Scope |
|---|---|
| `./scripts/dev/test.sh check` | Check `datui-lib` without linking |
| `./scripts/dev/test.sh unit data_quality::` | Library test executable; only data-quality tests execute |
| `./scripts/dev/test.sh integration integration_test test_data_quality` | App integration executable; matching quality tests execute |
| `./scripts/dev/test.sh integration home_test` | Home integration executable |
| `./scripts/dev/test.sh integration statistics_test` | Statistics integration executable |
| `./scripts/dev/test.sh cli` | CLI library tests |
| `./scripts/dev/test.sh preflight` | Formatting (workspace and fuzz targets) and workspace clippy with all targets |
| `./scripts/dev/test.sh features` | Clippy on `datui` and `datui-lib`, all targets, with no default features and then each feature alone |
| `./scripts/dev/test.sh features none sql` | Only the listed combinations; `none` is no features |
| `./scripts/dev/test.sh features --test` | The same, then `datui-lib`'s library tests in each combination |
| `./scripts/dev/test.sh full` | Full workspace tests, including doctests; ignored tests remain opt-in |
| `./scripts/dev/test.sh --print full` | Print the command without running it |

The script works from any directory and returns the underlying command's exit
status. Apart from `features`, it keeps the current feature set. It does not
install dependencies or prepare fixtures ahead of tests, and leaves ignored
tests opt-in. Existing tests can still generate missing fixtures through their
fallback helper. Clippy checks all targets, but does
not execute tests or link their executables. The existing pre-commit hooks
still run formatting and clippy.

Run `features` after gating code or tests on a feature, and `features --test`
after changing behavior a feature decides. Each combination is a separate
Polars build, so the first run is slow. CI runs `features none` (clippy only)
on every pull request; the Nightly workflow runs `features --test`, then the
root crate's tests with `cargo test --no-default-features`.

During an edit, run the changed behavior's regression and related tests. Before
submission, broaden to related targets and run formatting/clippy for Rust
changes. Run the full suite for cross-cutting App/event-loop, LazyFrame,
loading/schema, shared configuration, dependency/feature, and harness/layout
changes. For isolated changes, CI supplies full-workspace coverage; report
which checks were local. Documentation-only changes do not require Rust tests.
Replay the fuzz corpus for parser or matcher changes
(`./scripts/dev/test.sh integration fuzz_corpus_test`). Do not rerun an unchanged
broad check merely because another small scoped check finished.

Select multiple affected targets explicitly when needed:

```bash
cargo test --locked -p datui --test statistics_test --test distribution_detection_test
```

For changes to the binary itself, also run `cargo check --locked -p datui` and
exercise the changed CLI behavior. CLI definition tests do not replace this.

Keep existing build artifacts for the edit loop. Changing compiler flags,
toolchains or features can cause rebuilds; `cargo clean` is not a routine test
step. See the [test organization review](test-organization-review.md) for the
structural changes proposed to reduce linking and harness overhead.

## Heavy runs queue

```text
Waiting for another heavy test run to finish (/run/user/1000/datui-test-heavy.lock)...
```

`unit`, `integration`, `preflight`, `features`, `full`, and any command given
`--release` take one lock, shared by all of the user's checkouts and worktrees
on the machine, so they run one at a time instead of exhausting memory
together. A run that has to wait prints the line above once, then starts when
the other finishes. `check`, `cli` and `--print` do not take it.

| Case | Behavior |
|---|---|
| Lock file | `$XDG_RUNTIME_DIR/datui-test-heavy.lock`, or `/tmp/datui-test-heavy-<uid>.lock` without `XDG_RUNTIME_DIR` |
| Held | Until the command exits, by Ctrl-C or a crash too; never by a daemon it starts, such as sccache's server |
| `test.sh` inside a heavy run | Runs under the outer run's lock (`DATUI_TEST_LOCK_HELD` is set) |
| No `flock` (macOS without util-linux) | Runs unlocked and says so |

When several agents or people share a machine, run full suites, workspace
clippy and release builds through `test.sh` rather than `cargo` directly, so
they queue.

## Fixtures

The statistics, distribution-detection and pivot/melt tests read sample files
that are too large to commit. `scripts/dev/setup-test-data.sh` creates `.venv`,
installs `scripts/requirements.txt` (which pins Polars, NumPy, pyarrow, fastavro
and openpyxl in `scripts/requirements-fixtures.txt`) and generates them, using [uv](https://github.com/astral-sh/uv) when
it is installed and `python -m venv` otherwise. It is safe to re-run;
`--force` regenerates from scratch.

The test harness looks for `.venv/bin/python` (`.venv\Scripts\python.exe` on
Windows) and falls back to the system Python, so the environment does not need
to be activated. If the fixtures are missing when the tests start, they run the
generator themselves.

To regenerate by hand:

```bash
.venv/bin/python scripts/generate_sample_data.py
```

The fixtures are not regenerated automatically once they exist.

CI's `linux` job caches `tests/sample-data` under a key built from every input
to the generator:

| Key part | Input |
|---|---|
| `scripts/generate_sample_data.py` | The generator; it reads no other file |
| `scripts/requirements-fixtures.txt` | Every package it imports, and their dependencies, at exact versions |
| Python version | As `setup-python` resolved it |
| Runner OS and arch | |
| `sample-data-v1` | Schema version; bump it in `ci.yml` to discard every entry |

A restored copy is checked against the SHA-256 manifest saved with it, and
regenerated if anything differs. Only runs on `main` save an entry. If the
generator starts reading another file or importing another package, add the
file to the key or the package to `requirements-fixtures.txt`.

Tests only read `tests/sample-data`. Another test process may have its files
memory-mapped, and rewriting one kills that process with SIGBUS. A test that
writes its own data writes it elsewhere:

| Tests | Write to |
|---|---|
| Integration tests | `common::fixture_dir()`: a fresh directory, removed when the process exits |
| Unit tests | `tempfile::tempdir()` |

`scripts/dev/test.sh` fails a test run that wrote into `tests/sample-data`,
unless that run generated the fixtures. The generator rewrites every fixture in
place, so do not run it while tests are running.

## Cache and config isolation

Tests never read or write the developer's own cache or config. Each test
process points `DATUI_CACHE_DIR` and `DATUI_CONFIG_DIR` at scratch directories,
removed when it exits.

| Tests | Isolation |
|---|---|
| Unit tests | Automatic: `CacheManager::new` and `ConfigManager::new` call `cache::isolate_cache()` under `cfg(test)` |
| Integration tests | Take the runtime from `common::test_runtime()`, or call `common::isolate_cache()`, before building an `App`, a `CacheManager` or a `ConfigManager` |

A test binary that reaches either manager without the variables panics with
`DATUI_CACHE_DIR is not set` or `DATUI_CONFIG_DIR is not set`. Under
`cargo test` a test that forgot can still pass, because an earlier test in the
same process set them. `cargo nextest run --workspace` runs each test in its
own process, so it fails any test that depends on another having run first.
Run it after adding tests that build an `App` or touch the cache or config.

## Layout

| Path | Tests |
|---|---|
| `tests/integration_test.rs` | Load, query, display, end to end; `remote_quality::` (in `tests/quality/remote.rs`) counts Data Quality's requests at an in-process S3 bucket (`tests/common/fake_s3.rs`) |
| `tests/quality_spill_test.rs` | What a full Data Quality scan leaves on disk. Its own process: it sets Polars' spill directory before Polars reads it |
| `tests/quality_bench_test.rs` | Data Quality's cost: time, requests, bytes, peak memory and spill. Ignored; `scripts/dev/quality_bench.py BEFORE_REF` runs it here and at an earlier commit |
| `tests/statistics_test.rs`, `tests/distribution_detection_test.rs` | Analysis |
| `tests/pivot_melt_backend_test.rs` | Reshaping |
| `tests/template_test.rs` | Templates and their scoring |
| `tests/home_test.rs`, `tests/search_test.rs`, `tests/locality_test.rs` | Home screen, recursive search, filesystem detection |
| `tests/config_test.rs`, `tests/config_integration_test.rs`, `tests/theme_application_test.rs` | Configuration and themes |
| `tests/startup_test.rs` | The binary in a pseudo-terminal (Linux): a silent terminal, stalled settings, keys typed before the app exists, startup errors |
| `tests/fuzz_corpus_test.rs` | Every committed fuzz corpus input through its target's body in `fuzz/src/`; see [Fuzzing](fuzzing.md) |
| `tests/cloud_live_test.rs` | Against a real object store. Ignored by default; run with `DATUI_LIVE_GCS=1` or `DATUI_LIVE_S3=<endpoint>` and `--ignored` |
| `tests/common/` | Shared helpers |

Unit tests live beside the code they test.

## Wait for completion

Tests that drive an `App` wait on the work, never on a quiet channel. The
shared helpers in `tests/common/`:

| Helper | Waits for |
|---|---|
| `pump_open_until_loaded(app, rx, paths, options)` | An open's whole event chain, then `work_pending` to clear |
| `drain_events(app, rx)` | Every queued event and each event it chains to, until `work_pending` clears |
| `next_event(app, rx)` | One queued event, or one that pending work still owes; `None` once nothing is owed |
| `work_pending(app)` | `is_busy()`, `row_count_pending()`, or a footer pass still reading the schema |

A wait returns as soon as the work is done. One that runs past `HANG_GUARD`
(300 s) fails the test, naming the wait's location and what was still owed,
rather than falling through to asserts on the previous state.

`work_pending` ignores abandoned work: a cancelled analysis or a stale worker
can keep running after the app stops waiting on it. Tests about those
(cancellation, stale results, chart preparation, background discovery) wait on
their own condition, as `integration_test.rs` does with `pump_until` and
`ticks()`. Library unit tests use `crate::tests::work_pending`, which also
covers the buffer collect.

## Startup timing

```bash
cargo build --release
scripts/dev/first_frame_probe.py before=/path/to/old/datui after=target/release/datui --runs 20
```

| Column | Meaning |
|---|---|
| first frame | Spawn to the first output that draws a screen |
| first rows | Spawn to the first output holding the fixture's first row |
| idle CPU, wakeups/s, bytes | All threads, over a window after the rows are drawn |

It runs each binary in a 120×30 pseudo-terminal with isolated config and cache,
on 1,000-row CSV and Parquet fixtures, and prints p50/p95 as a Markdown table.
`--silent` never answers the keyboard-protocol query and `--reply-delay MS`
answers it late; a build that never asks is unaffected. Linux only. The
numbers depend on the machine: they belong in a PR description, not in a test.
