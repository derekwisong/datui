# Reduce test iteration time

Use `./scripts/dev/test.sh` to select a target before filtering tests. The
[test policy](tests.md#select-the-checks) and `AGENTS.md` describe the current
workflow; the target consolidation below is a proposal, not an available
command set.

## Findings

Measured on September 30, 2026, on a debug build:

| Observation | Consequence |
|---|---|
| 26 root integration targets, about 720 test functions | A library edit relinks every target that uses the app |
| More than half the test executables link the app at 650–800 MiB each; the rest are under 45 MiB | Linking dominates a warm rebuild, even when the tests run in seconds |
| `integration_test.rs`: 16,400 lines and 276 tests | Many unrelated behaviors share one large target |
| `home_test.rs`: 5,400 lines and 171 tests | Another large target mixes discovery, UI and configuration |
| About 1,400 library tests across 84 source files | `--lib FILTER` limits execution, but still compiles the library test executable |
| Environment-mutating config, color and cloud tests | Combining executables can introduce new process-global races |
| Python fixtures initialized with a per-executable `Once` | Different processes can independently notice missing fixtures and start generation |

Waiting is no longer a cost: every App-driving wait returns when the work it
waits on is done (see [Wait for completion](tests.md#wait-for-completion)).
What remains is build and link time.

Cargo compiles each top-level integration test as a separate crate. Its own
[target documentation](https://doc.rust-lang.org/cargo/reference/cargo-targets.html#integration-tests)
suggests grouping tests into modules to reduce executable overhead. Target
selection and dependency boundaries are the useful levers here; deleting
regression cases is not required.

## Delivered first steps

| Change | Effect |
|---|---|
| `scripts/dev/test.sh` and the selection policy | Runs the relevant target instead of building and running the whole suite |
| Completion waits in `tests/common/` | One `work_pending`, `next_event`, `drain_events` and `pump_open_until_loaded` replace a copy in each target |
| Footer passes count as pending work | Table assertions see the schema the footer pass brings back |
| Whole event chains | Every returned event is handled, with no depth limit |
| Deadline diagnostics | A wait that runs out fails, naming its location and what was still owed |
| Harness regression tests | Queued events, an owed result and an expired guard each have a test |

## Consolidate by domain, preserving isolation

Pilot the data target before changing the entire suite:

```text
tests/
  common/mod.rs
  data/
    main.rs
    statistics.rs
    distribution.rs
    reshape.rs
    excel.rs
  app/
    main.rs
    analysis.rs
    chart.rs
    query_filter.rs
    loading_schema.rs
    capture.rs
    views.rs
    terminal.rs
  home/
    main.rs
    discovery.rs
    search.rs
    locality.rs
  config/
    main.rs
    settings.rs
    views.rs
    themes.rs
```

These are candidate groups, not a mandate to create four new monoliths.
`main.rs` declares modules; module-qualified filters preserve narrow execution.
Move existing test bodies without altering their assertions in the layout PR.
Extract shared helpers once per target, rather than including `mod common`
separately in each child module. Update relative paths and commands deliberately.

| Current cases | Suggested destination |
|---|---|
| Statistics, distribution, pivot/melt, Excel | First consolidation pilot: `data` |
| App analysis/chart/query/loading, capture, views, terminal | Split the big file into `app` modules without adding an executable per module |
| Home, search, locality | `home`, after separating environment-dependent cases |
| Config, views, themes | `config`; keep process-global environment cases isolated until repaired |
| Live cloud, AWS profiles, credential discovery | Separate process-isolated targets initially; retain ignored/live behavior |
| Desktop entry and release-note wiring | Retain inexpensive targets or move to a lightweight repository-check package if measurements justify it |

Pilot acceptance: identical discovered cases after accounting for module-name
changes, unchanged ignored status, full-suite pass, fewer heavy executables,
and measured improvements to a library-edit rebuild. Also measure edits to
one test module: consolidation trades fewer links for recompiling more test
code within that target. Keep a separate target if the measurements favor it.

Do not blindly combine environment-mutating tests. Cloud targets set AWS
credential/profile variables; color cases set/remove `NO_COLOR`. Prefer
explicit configuration injection or subprocess isolation. A mutex helps only
if every writer and reader shares it; locking selected tests alone is not
isolation. Nextest's per-test process execution is another option, but native
`cargo test --workspace` must remain reliable unless CI and policy are changed
explicitly.

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
Generate large Python fixtures explicitly before the full suite; eventually
make a missing-fixture error name the setup command, or protect fallback
generation with an interprocess lock and atomic installation. Do not silently
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

## Baseline, October 7, 2026

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
passed, 32 ignored: 21 tests folded, listed in the Phase E report.

Left as they are, each for a reason:

| Test | Time | Why |
|---|---|---|
| `distribution_of_a_wide_integer_range_finishes` | 4–6 s | `incomplete_gamma` on values in the millions, in a debug build; 500 values instead of 2,000 saved 0.5 s |
| `distribution_detection_test::*` | 1–2 s each | one fit per seed; the three seeds are the assertion |
| `a_count_past_a_million_keys_gives_up_and_keeps_the_rows` | 2–3 s | the limit is the constant `MAX_COUNTED_KEYS` |
| `sqlite::tests::a_preview_of_a_view_that_never_ends_gives_up`, `a_read_stops_when_the_dataset_lets_go` | 2 s | the product's 2 s budgets are the subject |
| `event_pump::tests::a_background_count_redraws_at_the_idle_cadence` | 2 s | frames counted over a real second |
| `config_test::test_history_update_is_dropped_rather_than_blocking` | 2 s | the lock deadline is the subject |
