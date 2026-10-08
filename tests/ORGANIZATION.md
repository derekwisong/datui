# Test organization

`scripts/dev/test.sh` selects a target before a filter;
[Run tests](../docs/for-developers/tests.md) has the commands and the layout. This
file records why the targets are what they are, and what it cost and saved.

## Where a test goes

Every test executable links the app, about 400 MiB in a debug build, so the root
targets are a few by domain, each a directory with a `main.rs` that declares its
modules:

| Target | Holds |
|---|---|
| `app` | The App end to end: helpers in `main.rs`, tests by area (`loading`, `query`, `export`, `data_quality`, `home_screen`, `inspector`, `chart`, `analysis`, `views`, `table_keys`, `harness`), formats in `formats/`, cloud in `cloud/`, `remote_quality` in `quality/`, and `capture`, `terminal_escape`, `catalog`, `public_datasets`, `quality_export`; `alloc_budget` holds per-key allocation budgets, counted by a pass-through allocator on the test's own thread |
| `home` | The home screen, with `search` and `locality` |
| `data` | `statistics`, `distribution`, `reshape`, `excel` |
| `config` | `settings`, `flags`, `themes`, `colors`, `indexed_colors`, `views`, `view_store` |
| `repo` | `desktop_entry`, `release_notes`, `wording` |

A new test goes into the module that fits; a new area is a new module, not a new
top-level file. Module-qualified names keep filters narrow (`app loading::`).

A target of its own is for a test that changes the process for everyone in it:

| Target | Why apart |
|---|---|
| `startup_test`, `stderr_log_test` | Run the binary, or this test binary, as a child process |
| `quality_spill_test`, `quality_bench_test` | Set Polars' spill directory and `TMPDIR` before Polars reads them, once per process |
| `aws_profiles_test`, `cloud_home_test`, `cloud_list_on_enter_test` | Set cloud credentials in the environment |
| `cloud_live_test` | Live stores; ignored by default |
| `fuzz_corpus_test` | The fuzz targets' bodies; run by name for parser changes |

Before giving a test its own process, repair it instead when that is possible:
the one test that set `NO_COLOR` now gives the color parser `no_color` directly,
and `locality` counts its own thread's reads rather than the process's. A mutex
helps only if every reader and writer shares it.

## Phase E, before and after

Code-review Phase E, October 7, 2026: 16 cores, sccache, mold, incremental,
`CARGO_BUILD_JOBS=6`, `RUST_TEST_THREADS=6`, with other agents building on the
machine, so wall times are approximate.

| Measure | Before (6596f3be) | After |
|---|---|---|
| Test executables in the workspace | 36; 22 over 200 MiB | 18; 12 over 200 MiB |
| Their size | 8.2 GiB | 4.7 GiB |
| Root test executables' compile and link after a one-line `datui-lib` edit | 33 units, 35.9 s CPU | 15 units, 16.8–17.9 s CPU |
| Wall time of that rebuild | 11.4–12.0 s | 7.8–8.2 s |
| Root test executables in a fresh target dir | 33 units, 70.1 s CPU | 15 units, 26.5 s CPU |
| Rebuild after a one-line test edit | under 1 s | under 1 s (`app`, the largest, 1.0 s) |
| `scripts/dev/test.sh full`, built | 38.8 s wall | 23.4 s wall |
| Tests | 4,002 passed, 32 ignored | 3,977 passed, 31 ignored |
| `sleep` calls in test code | 49 | 28, none waiting for work: 11 poll a condition no event reports (a file on disk, a flag), 15 have a real timer as the subject (a slow disk or server, a sampler, a frame rate, a wait that must not end), 2 in `src/tests` that set an order (views sent after an open starts, a future that outlasts the runtime's shutdown) |

The library's own test executable (about 9.5 s after an edit) is the critical path
after a library edit; the root executables link beside it. The fresh-target-dir
build after the change missed sccache and ran under a load average of 13, so only
its root test units are compared. 29 tests were folded into table-driven or exact
neighbors that cover the same cases; two were added
(a followed file replaced before its watcher opens it, and `NO_COLOR` given to the
color parser, which replaced an ignored test), and other work added two.

## Make the cheapest tests independent

Move pure logic cases beside their implementation when they do not need the
App/public integration boundary. This reduces integration executables, but
does not remove the library's heavy dependencies. A new file inside
`datui-lib` is not a lightweight compilation boundary.

After the consolidation pilot, measure whether coherent independent code
(formatting, matching, or discovery rules) warrants a small crate without
Polars. Keep APIs narrow and preserve behavior. Do not split the whole app or
move configuration indiscriminately: config currently exposes Polars types.
The existing `datui-cli` crate is already a cheap independent check target.

Use in-memory frames and temporary files for focused tests. Keep real-format
and end-to-end cases where they protect loading/schema or event behavior.
Fixture generation is stamped, locked across processes and installed by
rename (`crates/datui-lib/src/tests/shared.rs`); a failure names the setup command. Do not silently
download/install tooling in a targeted test invocation.

## Measure each delivery

| Scenario | Measure |
|---|---|
| Existing binaries, selected tests | Actual execution, including harness waits |
| Small implementation edit, selected target | Warm compile + link + execution |
| Small implementation edit, full suite | Cost of relinking all affected targets |
| One test-only edit | Cost introduced by larger consolidated targets |
| Full build in a separate temporary target directory | Cold compilation/linking, without deleting the developer's normal artifacts |
| CLI or independent pure crate | Whether a genuinely lightweight loop exists |

Keep compiler, feature set, profile, linker, and fixture state identical for
before/after measurements. Record wall time, target count, aggregate executable
size, peak memory where available, and discovered/passed/ignored cases.
`cargo test --no-run --timings` separates build costs from execution. If the
last timing data is reused, label it; do not call it a new benchmark.

Aim for seconds on warmed targeted loops; use measured limits for the first
pilot rather than promising all library edits will rebuild in seconds. Heavy
library code generation remains even after reducing links. Retain incremental
artifacts, avoid concurrent competing Cargo jobs, and do not change feature
sets as the default shortcut for ordinary testing.

Nextest can schedule execution across binaries and improve reports, but
[builds test binaries first](https://nexte.st/docs/design/how-it-works/).
It complements target reorganization; it does not replace it. Format/clippy
remain required before Rust submission. Broaden tests based on change scope,
with full workspace and platform coverage retained in CI.

## Record: baseline, October 7, 2026

Before folding any target (code-review Phase E, milestone 1), on `code-review-plan`
at 6596f3be: 16 cores, sccache, mold, incremental, `CARGO_BUILD_JOBS=6`,
`RUST_TEST_THREADS=6`, a fresh target directory, other agents building on the
machine. sccache was warm, so "cold" is a fresh target directory, not a fresh
compiler cache.

| Measure | Before |
|---|---|
| `cargo test --workspace --no-run --timings`, fresh target dir | 58.5 s wall, 693 units; `datui-lib` lib tests 45.9 s, `datui-lib` 21.5 s, `integration_test` 11.8 s, `home_test` 4.9 s |
| The same after a one-line `datui-lib` edit | 12.0 s, 38 units: lib tests 9.7 s, `datui-lib` 3.9 s, `integration_test` 3.0 s, every other test binary about 1.5 s |
| Test executables | 36; 22 over 200 MiB; 8.2 GiB together (14 GiB target dir) |
| `scripts/dev/test.sh full` | 38.8 s wall, built; 26.2 s summed over 38 test binaries |
| Tests | 4,002 passed, 0 failed, 32 ignored |

`full`, per binary (the slowest test in a binary sets its time):

| Binary | Tests | Time | Its slowest test |
|---|---|---|---|
| `integration_test` | 668 | 6.0 s | `test_abandoned_load_never_installs_itself_afterwards` 2.3 s |
| `statistics_test` | 11 | 5.3 s | `distribution_of_a_wide_integer_range_finishes` 4.2 s alone |
| `datui-lib` lib | 2,699 | 4.9 s | `data_quality::tests::a_count_past_a_million_keys_gives_up_and_keeps_the_rows` 3.3 s |
| `distribution_detection_test` | 14 | 3.1 s | each family about 1 s alone: three seeds of one fit |
| `cloud_list_on_enter_test` | 1 | 2.0 s | two one-second quiet waits |
| `config_test` | 131 | 2.0 s | `test_history_update_is_dropped_rather_than_blocking`: a 2 s lock deadline |
| every other binary | | under 1 s | |

### First pass: slow tests, sleeps, duplicates

Without folding targets. Times are nextest's, `-j 6`, over the lib,
`integration_test`, `statistics_test` and `distribution_detection_test`: 21.2 s
before, 16.9 s after.

| Test | Before | After | Change |
|---|---|---|---|
| `statistics_test::correlation_allocates_per_column_not_per_pair` | 5.05 s | 1.28 s | 50,003 rows, not 200,003: a column-sized block is still far above anything else |
| `statistics::tests::a_matrix_in_bands_is_the_matrix_in_one` | 1.53 s | 0.05 s | 200 rows; bands of 1, 2, 7, 67, 199, 200 and 1,000, dividing and not |
| `cloud_list_on_enter_test` (binary) | 2.02 s | 0.01 s | waits for no source being listed, not one second each time |
| `download::tests::a_refused_write_stops_the_stream` | 0.32 s | 0.11 s | waits for the stream to be let go |

`full` after: 29.9 s wall, 28.5 s summed (`integration_test` measured 10.4 s
under other agents' load; its tests' nextest times are unchanged), 3,981
passed, 32 ignored: 21 tests folded into neighbors that cover their cases.

Left as they are, each for a reason:

| Test | Time | Why |
|---|---|---|
| `distribution_of_a_wide_integer_range_finishes` | 4–6 s | `incomplete_gamma` on values in the millions, in a debug build; 500 values instead of 2,000 saved 0.5 s |
| `distribution_detection_test::*` | 1–2 s each | one fit per seed; the three seeds are the assertion |
| `a_count_past_a_million_keys_gives_up_and_keeps_the_rows` | 2–3 s | the limit is the constant `MAX_COUNTED_KEYS` |
| `sqlite::tests::a_preview_of_a_view_that_never_ends_gives_up`, `a_read_stops_when_the_dataset_lets_go` | 2 s | the product's 2 s budgets are the subject |
| `event_pump::tests::a_background_count_redraws_at_the_idle_cadence` | 2 s | frames counted over a real second |
| `config_test::test_history_update_is_dropped_rather_than_blocking` | 2 s | the lock deadline is the subject |

### Pilot: the `data` target

`tests/data/main.rs` declares `statistics`, `distribution`, `reshape` (pivot and
melt) and `excel`, which were four executables. They share one `common`, and the
counting allocator `statistics` installs covers the whole executable. Select
them as `scripts/dev/test.sh integration data statistics::`. The same 49 tests
pass under `cargo test` and under nextest.

| Measure | Four targets | `data` |
|---|---|---|
| Executables in the workspace | 36 | 33 |
| Their size | 1,204 MiB (236 + 237 + 400 + 390) | 382 MiB |
| Rebuild after a one-line `datui-lib` edit, all test targets | 11.4 s wall, 38 units, 35.9 s across the root test units | 11.1 s wall, 35 units, 33.2 s |
| Of that, the data tests | 4.3 s across four links | 1.5 s, one link |
| Rebuild after a one-line edit of one data test file | 0.5 s | 0.5 s |
| Run under `cargo test` | 8.3 s, one after another | 7.8 s |

Wall time after a library edit hardly moves: the library's own test executable
(about 9.5 s) is the critical path, and the root targets link alongside it. What
folding saves is link work (2.8 s of CPU for these four), disk (820 MiB), and the
cold build's link queue. A test edit costs the same. No cold build was measured
for the pilot.

### `config`, `home` and `repo`

| Target | Modules (were) |
|---|---|
| `config` | `settings` (config_test), `flags` (config_integration_test), `themes` (theme_application_test), `colors` (color_parser_test), `indexed_colors` (indexed_color_test), `views` (views_test), `view_store` (view_store_test) |
| `home` | `search` (search_test), `locality` (locality_test); `home_test.rs` stays apart for now |
| `repo` | `desktop_entry`, `release_notes`, `wording` |

The only test that set `NO_COLOR` (ignored, for that reason) is now a unit test
that gives the parser `no_color` directly. The rest only remove it, and
`DATUI_TEST_IMPORT_DIR` is read by the one test that sets it, so `config` runs
them in one process. `stderr_log_test` and `startup_test` run child processes
and stay apart, as do the cloud, live and AWS targets.

| Measure | Before (after the data pilot) | After |
|---|---|---|
| Executables in the workspace | 33 | 24 (18 over 200 MiB), 6.8 GiB |
| These twelve | 1,285 MiB (config group 944, home 316, repo 25) | 663 MiB (389 + 263 + 11) |
| Rebuild after a one-line `datui-lib` edit | 11.1 s wall, 35 units, 33.2 s across the root test units | 10.1 s wall, 26 units, 24.7 s |
| Rebuild after a one-line edit of one `config` module | 0.5 s | 0.6 s |
| `scripts/dev/test.sh full`, built | 29.9 s wall, 38 binaries | 23.7 s wall, 26 binaries |

The same 219 tests pass in the three targets, under `cargo test`, under nextest,
and with `NO_COLOR=1` set for the run.

### `app`, apart from `integration_test`

`tests/app/main.rs` declares `capture`, `terminal_escape`, `catalog` and
`public_datasets` (both with the `cloud` and `http` features), and
`quality_export` (was quality_intent_export_test). `table_sample.rs` in the same
directory is still a module of `integration_test`. `quality_spill_test` and
`quality_bench_test` stay apart: each sets Polars' spill directory and `TMPDIR`
before Polars reads them, once per process.

| Measure | Before | After |
|---|---|---|
| Executables in the workspace | 24, 6.8 GiB | 20, 5.3 GiB |
| These five | 1,900 MiB, about 375–390 each | 389 MiB |
| Rebuild after a one-line `datui-lib` edit | 8.9 s wall, 26 units, 22.8 s across the root test units; these five 5.6 s | 8.4–9.5 s wall, 22 units, 19.4–24.4 s; `app` 1.4–2.8 s |
| Rebuild after a one-line edit of one module | 0.6 s | 0.5 s |
| `scripts/dev/test.sh full`, built | 23.7 s wall | 23.4 s wall; 3,983 passed, 31 ignored |

Other agents kept the load average near 10 during these runs, so the rebuild
times are ranges over two runs.

### `app` and `home` whole

`integration_test.rs` (26,166 lines) became `app`'s root: its helpers stay in
`main.rs` and its 434 tests went to eleven modules by area; `formats/`, `cloud/`
and `quality/` moved under `tests/app/`. `home_test.rs` became `home`'s root, so
its tests and modules (`coming_back::`, …) keep their names. Waits between drawn
frames are now on the channel (`common::wait_for_event`), which took
`test_startup_buffer_race_does_not_lose_rows` from 2.05 s to 0.65 s with its 50
iterations kept; `test_abandoned_load_never_installs_itself_afterwards` (2.3 s to
2.0 s) keeps 40 quiet ticks per abandon point, which are what let a late row count
show itself.

| Measure | Before | After |
|---|---|---|
| Executables in the workspace | 20, 5.3 GiB | 18, 4.7 GiB |
| `integration_test`, `home_test`, `app`, `home` | 1,444 MiB in four | 796 MiB in two |
| Rebuild after a one-line `datui-lib` edit | 8.4–9.5 s wall, 22 units, 19.4–24.4 s | 7.8–8.2 s wall, 20 units, 16.8–17.9 s |
| Rebuild after a one-line edit of an `app` module | 0.5 s (a small module) | 1.0 s (the whole of `app`) |
| `scripts/dev/test.sh full`, built | 23.4 s | 23.4 s; 3,977 passed, 31 ignored |
