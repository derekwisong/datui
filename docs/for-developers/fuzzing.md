# Fuzzing

Datui fuzzes the hand-written parsers and matchers that run on untrusted input, using
[cargo-fuzz][cargo-fuzz] and libFuzzer. The targets live in `fuzz/`.

## What is fuzzed

| Target | Surface | What it checks |
| --- | --- | --- |
| `parse_query` | `query::parse_query` | A tokenizer and recursive-descent parser that slices token vectors by index. Malformed input must return `Err`, never panic. |
| `sql_group_plan` | `sql_group::plan` | Reads every SQL statement to decide whether a `GROUP BY` result drills: resolves keys by ordinal, alias and expression and writes a statement of its own. Any text must give a plan or `None`, never panic. |
| `number_format` | `numfmt::NumberFormat` | `width_*` computes a display width arithmetically, `write_*` renders into a fixed 64-byte stack buffer and returns the width it produced. The two must agree, and both must equal the characters actually appended. Table columns are sized from these numbers, so a disagreement corrupts the layout instead of failing visibly. |
| `fuzzy_match` | `fuzzy::best_match` | Returned positions must be valid, strictly ascending character indices into the haystack, one per needle character. The home screen highlights matches by indexing with them. |
| `glob_match` | `numfmt::Glob` | A backtracking wildcard matcher, checked for hangs and for its wildcard-free fast path agreeing with equality. |
| `config_parse` | `config::AppConfig`, `config::ColorParser` | Validation and merging of user TOML, and color strings that get sliced by byte offset after a byte-length check. |

## Running

Install the tool once:

```bash
cargo install cargo-fuzz --locked
```

Then, from the repository root:

```bash
./scripts/code/fuzz.sh list                       # the target names
./scripts/code/fuzz.sh build                      # build them all
./scripts/code/fuzz.sh replay                     # replay the committed corpus and exit
./scripts/code/fuzz.sh run parse_query            # fuzz until interrupted
./scripts/code/fuzz.sh run parse_query -- -max_total_time=60
```

`replay` is the quick one. It loads every committed corpus input, runs each once, and
generates no new test inputs. Use it to check known cases after editing a parser
or matcher.

## What CI does

**Every pull request** runs `replay` in the `CI` workflow. It is a regression gate: it
re-runs the inputs already known to be interesting and fails if one of them starts
crashing again. It does not look for new bugs.

**The Nightly workflow** runs each target for ten minutes against fresh input, with
AddressSanitizer on, as a matrix so one slow target does not consume another's budget.
Crashing inputs are uploaded as build artifacts.

Each target restores the previous coverage corpus before running and minimizes
it with `cargo fuzz cmin` afterward. Separate cache restore/save steps preserve
new inputs even when a target crashes.

## The corpus

`fuzz/corpus/` is committed, but it is a *seed* corpus, not the full coverage corpus.

The three targets that take text are seeded with inputs a person can read: `parse_query`
from the parser's own unit tests and the query examples throughout `docs/`,
`sql_group_plan` from the planner's unit tests, `config_parse` from the TOML blocks in
`docs/`. Anything named `regression-*` is an
input that once crashed a target, kept so the replay job notices if it ever crashes
again.

The other three take structured input that `arbitrary` decodes from raw bytes, so a
hand-written seed would mean nothing. Those directories hold a bounded sample of
minimized inputs from a real run, capped at 64 files each.

Commit a `regression-*` input for each fixed crash. Keep routine coverage inputs
in the fuzzing cache. Minimize any additional seeds before committing them:

```bash
./scripts/code/fuzz.sh cmin parse_query
```

## When a target fails

libFuzzer writes the offending input to `fuzz/artifacts/<target>/`. Reproduce it by
passing that file instead of a corpus directory:

```bash
./scripts/code/fuzz.sh run parse_query fuzz/artifacts/parse_query/crash-<hash>
```

Fix the bug, then copy the input into `fuzz/corpus/<target>/` so the replay job keeps it
fixed. A minimal reproducer usually deserves a unit test next to the code as well.

## Sanitizer

Local runs omit the sanitizer by default. Enable AddressSanitizer when
checking dependency memory errors or running a longer fuzzing session:

```bash
DATUI_FUZZ_SANITIZER=address ./scripts/code/fuzz.sh run parse_query
```

The Nightly workflow runs this configuration; the pull request job does not.

Sanitizer builds use substantial memory. The Nightly job limits parallel
compilation to two jobs; use the same limit if your build is killed:

```bash
CARGO_BUILD_JOBS=2 DATUI_FUZZ_SANITIZER=address ./scripts/code/fuzz.sh run parse_query
```

## Why these run on stable

cargo-fuzz reaches for `-Z sanitizer`, which is normally nightly-only, and
`scripts/code/fuzz.sh` sets `RUSTC_BOOTSTRAP=1` to allow it on stable instead.

Polars currently enables an incompatible internal code path on nightly.
The wrapper uses stable with `RUSTC_BOOTSTRAP=1` to compile the fuzz targets.

If a future Polars release fixes the nightly path, the flag can be dropped and the
scripts switched to `cargo +nightly fuzz`.

[cargo-fuzz]: https://github.com/rust-fuzz/cargo-fuzz
