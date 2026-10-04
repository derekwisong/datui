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
