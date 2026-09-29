# Fuzzing

Datui fuzzes the hand-written parsers and matchers that run on untrusted input, using
[cargo-fuzz][cargo-fuzz] and libFuzzer. The targets live in `fuzz/`.

## What is fuzzed

| Target | Surface | What it checks |
| --- | --- | --- |
| `parse_query` | `query::parse_query` | A tokenizer and recursive-descent parser that slices token vectors by index. Malformed input must return `Err`, never panic. |
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

The two targets that take text are seeded with inputs a person can read: `parse_query`
from the parser's own unit tests and the query examples throughout `docs/`,
`config_parse` from the TOML blocks in `docs/`. Anything named `regression-*` is an
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

The targets build without a sanitizer by default. `datui-lib` contains no `unsafe`, so
for this code AddressSanitizer has very little to find that a panic would not already
report: the bugs here are index arithmetic, unbounded recursion and disagreeing width
calculations, and the fuzz profile turns on debug assertions and overflow checks so all
of them trap. It costs about double the build output, 12 GB against 6.0 GB as measured
on Linux, and runs the target itself noticeably slower, so it buys less coverage per
minute of CI.

It is still worth it for a long run, where it can reach the `unsafe` code inside Polars
and Arrow that these targets feed. Turn it on with:

```bash
DATUI_FUZZ_SANITIZER=address ./scripts/code/fuzz.sh run parse_query
```

The Nightly workflow runs this configuration; the pull request job does not.

Memory limits a sanitizer build more than disk does. cargo-fuzz compiles every crate as a
single codegen unit, and instrumented that way `polars-core` alone peaks at about 8.5 GB,
with `polars-expr`, `polars-ops` and `arrow-cast` at 2–3.4 GB each compiling alongside
it. At four parallel jobs that is more than a 16 GB machine has, and the build is killed
rather than failing with an error. The Nightly workflow builds with `CARGO_BUILD_JOBS=2`
for this reason; do the same locally if the build disappears partway through:

```bash
CARGO_BUILD_JOBS=2 DATUI_FUZZ_SANITIZER=address ./scripts/code/fuzz.sh run parse_query
```

## Why these run on stable

cargo-fuzz reaches for `-Z sanitizer`, which is normally nightly-only, and
`scripts/code/fuzz.sh` sets `RUSTC_BOOTSTRAP=1` to allow it on stable instead.

That is a deliberate choice rather than a shortcut. `polars-ops` has a build script that
enables its own `nightly` feature whenever it detects a nightly compiler, and that code
path uses `core::unicode` internals which current nightly no longer exposes. The
dependency tree therefore does not compile on nightly at all, for reasons unrelated to
datui. Building on stable sidesteps that and keeps the fuzzers on the same pinned
toolchain as every other job.

If a future Polars release fixes the nightly path, the flag can be dropped and the
scripts switched to `cargo +nightly fuzz`.

[cargo-fuzz]: https://github.com/rust-fuzz/cargo-fuzz
