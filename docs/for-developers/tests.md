# Run tests

```bash,repo
./scripts/dev/test.sh setup   # once: creates .venv and generates the fixtures
./scripts/dev/test.sh full    # cargo test --workspace --locked --no-fail-fast
./scripts/dev/test.sh ci      # what CI's linux job runs: nextest, then doctests
./scripts/dev/test.sh --help  # every command
```

`cargo test` alone runs only the root package. `--workspace` adds `datui-lib`
and `datui-cli`. CI runs the same tests with
`cargo nextest run --workspace --locked --no-fail-fast`, one process per test,
then `cargo test --doc --workspace --locked`; `test.sh ci` runs exactly that
(with nextest's `ci` profile from `.config/nextest.toml`), or `cargo test` when
nextest is not installed. The Python bindings are tested separately
(`test.sh python`); see [Build Python bindings](python-bindings.md#build-and-test).

## Select the checks

Run `./scripts/dev/test.sh check` while editing, then select the relevant test
target. A name filter selects tests to execute; it does not by itself restrict
the executables Cargo builds.

| Command | Scope |
|---|---|
| `./scripts/dev/test.sh check` | Check `datui-lib` without linking |
| `./scripts/dev/test.sh unit data_quality::` | Library test executable; only data-quality tests execute |
| `./scripts/dev/test.sh integration app data_quality::` | App integration executable; only its Data Quality module executes |
| `./scripts/dev/test.sh integration home` | Home integration executable |
| `./scripts/dev/test.sh integration data statistics::` | Data integration executable; only its statistics module executes |
| `./scripts/dev/test.sh cli` | CLI library tests |
| `./scripts/dev/test.sh fmt` | Format the workspace, the fuzz targets and `crates/datui-pyo3` (`--check` only checks) |
| `./scripts/dev/test.sh lint` | Formatting check and clippy with all targets, in the workspace, the fuzz targets and `crates/datui-pyo3`; `preflight` is the same |
| `./scripts/dev/test.sh clippy` | Workspace clippy alone, as the pre-commit hook runs it |
| `./scripts/dev/test.sh msrv` | `cargo +<rust-version> check --workspace`, with the version from `Cargo.toml` |
| `./scripts/dev/test.sh docs` | The [documentation checks](documentation.md#run-the-checks) and the docs and demo scripts' tests |
| `./scripts/dev/test.sh features` | Clippy on `datui` and `datui-lib`, all targets, with no default features and then each feature alone |
| `./scripts/dev/test.sh features none sql` | Only the listed combinations; `none` is no features |
| `./scripts/dev/test.sh features --test` | The same, then `datui-lib`'s library tests in each combination |
| `./scripts/dev/test.sh full` | Full workspace tests, including doctests; ignored tests remain opt-in |
| `./scripts/dev/test.sh ci` | The same tests as CI's `linux` job: nextest, one process per test, then doctests |
| `./scripts/dev/test.sh --print full` | Print the command without running it |

The script works from any directory and returns the underlying command's exit
status. Apart from `features`, it keeps the current feature set. Only `setup`
installs anything, and ignored tests stay opt-in. Clippy checks all targets, but
does not execute tests or link their executables. The pre-commit hooks run
`test.sh fmt --check` and `test.sh clippy`.

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
which checks were local. Documentation-only changes need the
[documentation checks](documentation.md#run-the-checks), not Rust tests.
Replay the fuzz corpus for parser or matcher changes
(`./scripts/dev/test.sh integration fuzz_corpus_test`). Do not rerun an unchanged
broad check merely because another small scoped check finished.

Select multiple affected targets explicitly when needed:

```bash,repo
cargo test --locked -p datui --test data --test config
```

For changes to the binary itself, also run `cargo check --locked -p datui` and
exercise the changed CLI behavior. CLI definition tests do not replace this.

Keep existing build artifacts for the edit loop. Changing compiler flags,
toolchains or features can cause rebuilds; `cargo clean` is not a routine test
step. `tests/ORGANIZATION.md` in the repository proposes structural changes to
reduce linking and harness overhead.

## Shared machines

```text
Waiting for one of 2 heavy test runs to finish (/run/user/1000/datui-test-heavy*.lock)...
```

On your own machine you can ignore this. The maintainer runs several checkouts
and agents on one machine, so `unit`, `integration`, `lint`, `clippy`, `msrv`,
`ci`, `python`, `features`, `full`, and any command given `--release` take one
of `DATUI_TEST_HEAVY_SLOTS` slots (default 2) shared by all of the user's
checkouts, and wait, saying so once, when none is free. `DATUI_TEST_HEAVY_SLOTS=0`
turns this off.

| Case | Behavior |
|---|---|
| Lock files | `$XDG_RUNTIME_DIR/datui-test-heavy*.lock`, or `/tmp/datui-test-heavy-<uid>*.lock` without `XDG_RUNTIME_DIR` |
| Held | Until the command exits, by Ctrl-C or a crash too; never by a daemon it starts, such as sccache's server |
| `test.sh` inside a heavy run | Runs under the outer run's slot (`DATUI_TEST_LOCK_HELD` is set) |
| No `flock` (macOS without util-linux) | Runs without it and says so |

On such a machine, run full suites, workspace clippy and release builds through
`test.sh` rather than `cargo` directly, so they queue.

## Fixtures

The statistics, distribution-detection and pivot/melt tests read sample files
that are too large to commit. `./scripts/dev/test.sh setup`
(`scripts/setup_dev.py`) creates `.venv`, installs `scripts/requirements.txt`
(which pins Polars, NumPy, pyarrow, fastavro and openpyxl in
`scripts/requirements-fixtures.txt`) and generates them, using
[uv](https://github.com/astral-sh/uv) when it is installed and `python -m venv`
otherwise. It is safe to re-run; `--force` regenerates even current fixtures.

The generator writes `tests/sample-data/.generated`, a SHA-256 of
`scripts/generate_sample_data.py` and `scripts/requirements-fixtures.txt`. When
a test starts and that stamp is missing or holds another digest, the test
harness regenerates the fixtures, so pulling a generator change needs no manual
step. It runs `.venv/bin/python` (`.venv\Scripts\python.exe` on Windows),
falling back to the system Python, so the environment does not need to be
activated. Test processes take the lock file `tests/.sample-data.lock` first,
so only one generates and the rest wait for it. If generation fails, the test
panics with the generator's error and the setup command.

To regenerate by hand:

```bash,repo
.venv/bin/python scripts/generate_sample_data.py
```

The generator writes into a scratch directory beside the output and then
renames each file into place, so a test process that has the old file mapped
keeps reading it. On Windows a rename over a file another process has open
fails, so generate while no tests run there. `--out DIR` writes somewhere
else, and `--if-stale` does nothing when the stamp is current. It exits with an
error if any of its packages is missing.

CI's `linux` job caches `tests/sample-data` under a key built from every input
to the generator:

| Key part | Input |
|---|---|
| `scripts/generate_sample_data.py` | The generator; it reads no other file but the next one, for the stamp |
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
unless that run generated the fixtures.

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

Each directory under `tests/` with a `main.rs` is one test executable, and its
modules are what a filter selects (`scripts/dev/test.sh integration app loading::`).
Every executable links the app, so a new test goes into the module that fits, not
into a new top-level file.

| Target | Tests |
|---|---|
| `tests/app/` | The App end to end. Helpers in `main.rs`; tests by area in `loading`, `query`, `export`, `data_quality`, `home_screen`, `inspector`, `chart`, `analysis`, `views`, `table_keys` and `harness`; formats in `formats/` (`formats_open::`, `formats_follow::`, …); `cloud_download::` and `cloud_parity::` in `cloud/`; `remote_quality::` (in `quality/remote.rs`) counts Data Quality's requests at an in-process S3 bucket (`tests/common/fake_s3.rs`); also `capture`, `terminal_escape`, `catalog`, `public_datasets`, `quality_export` |
| `tests/home/` | The home screen: discovery, roots, filtering, and the App driving it, with modules such as `coming_back::` and `cloud_level_paging::`; `search` (recursive search) and `locality` (filesystem detection) |
| `tests/data/` | `statistics`, `distribution` (analysis), `reshape` (pivot and melt), `excel` |
| `tests/config/` | `settings`, `flags`, `themes`, `colors`, `indexed_colors`, `views` (the Views surface), `view_store` (saved views on disk and their scoring) |
| `tests/repo/` | `desktop_entry`, `release_notes`, and `wording`: retired words ([glossary](../reference/glossary.md)) and "opens anything" claims, in the UI strings, the key registry, docs, `--help` and the manpages. A real use goes in its `ALLOWED` list |
| `tests/startup_test.rs` | The binary in a pseudo-terminal (Linux): a silent terminal, stalled settings, keys typed before the app exists, startup errors |
| `tests/stderr_log_test.rs` | Stderr goes to the log while the TUI runs; in a child process, since it points fd 2 away |
| `tests/quality_spill_test.rs` | What a full Data Quality scan leaves on disk. Sets Polars' spill directory before Polars reads it |
| `tests/quality_bench_test.rs` | Data Quality's cost: time, requests, bytes, peak memory and spill. Ignored; `scripts/dev/quality_bench.py BEFORE_REF` runs it here and at an earlier commit |
| `tests/aws_profiles_test.rs`, `tests/cloud_home_test.rs`, `tests/cloud_list_on_enter_test.rs` | Cloud sources and credentials, which they set in the environment |
| `tests/cloud_live_test.rs` | Against a real object store. Ignored by default; run with `DATUI_LIVE_GCS=1` or `DATUI_LIVE_S3=<endpoint>` and `--ignored` |
| `tests/fuzz_corpus_test.rs` | Every committed fuzz corpus input through its target's body in `fuzz/src/`; see [Fuzzing](fuzzing.md) |
| `crates/datui-cli/src/docgen.rs` | `the_generated_docs_are_current`: the generated pages match the code ([Build documentation](documentation.md#generated-pages)) |
| `crates/datui-lib/src/tests/doc_queries_tests.rs` | The docs' `q` blocks parse, and their `sql` and `q` blocks run on the datasets they name |
| `tests/common/` | Shared helpers |

A test that changes the process (an environment variable another test reads, fd 2,
Polars' configuration) keeps a target of its own; one executable shares all of it.
Better still, give the code the setting directly, as the `NO_COLOR` unit test gives
the color parser `no_color`. Tests in `config` only ever remove `NO_COLOR`. A test
that counts something process-wide counts its own thread instead
(`/proc/thread-self/io`, as `locality` does).

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
| `wait_for_event(tx, rx)` | For a loop that draws a frame each pass: the next event, or `FRAME_WAIT` (5 ms) when none comes, as the run loop sleeps on its channel. The event stays queued |

A wait returns as soon as the work is done. One that runs past `HANG_GUARD`
(300 s) fails the test, naming the wait's location and what was still owed,
rather than falling through to asserts on the previous state.

`work_pending` ignores abandoned work: a cancelled analysis or a stale worker
can keep running after the app stops waiting on it. Tests about those
(cancellation, stale results, chart preparation, background discovery) wait on
their own condition, as `tests/app/main.rs` does with `pump_until` and
`ticks()`. Library unit tests use `crate::tests::work_pending`, which also
covers the buffer collect.

No test sleeps to let work happen. Wait for the work, the event, or the frame's
content (wait until the screen shows the row, not for a while after the key). A
sleep stays only where a real timer is the subject: a lock deadline, a server that
stalls, a frame rate over a second, or proving that something does not happen.
Such a sleep says why in a comment.

## Size the expensive tests

Statistical and large-data tests set the run time of their executable. Use the
smallest input that still makes the assertion: a size test needs blocks bigger than
anything else allocated, not 200,000 rows; a band test needs bands that do and do
not divide the rows. Keep what is the subject (the seeds of a fit, a cap that is a
constant), and say in the test why its size is what it is.

## Startup timing

```bash,repo
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
