# Fuzzing datui

Coverage-guided fuzzing with [cargo-fuzz] and libFuzzer. The targets cover the
hand-written parsers and matchers that run on untrusted input: the query language, the
number renderer, the fuzzy matcher, the config glob matcher, and config loading.

## Setup

```sh
cargo install cargo-fuzz --locked
```

No nightly toolchain is needed. See [Why not nightly](#why-not-nightly) below.

## Running

From the repository root:

```sh
scripts/code/fuzz.sh run parse_query          # fuzz until you stop it
scripts/code/fuzz.sh run parse_query -- -max_total_time=60
scripts/code/fuzz.sh replay                   # replay every committed corpus, no new input
scripts/code/fuzz.sh build                    # build all targets
```

`replay` is what CI runs on every pull request. It is deterministic and finishes in
seconds, and it fails if a previously fixed crash comes back.

## Targets

| Target | Surface | What it checks |
| --- | --- | --- |
| `parse_query` | `query::parse_query` | A hand-written tokeniser and recursive-descent parser that slices token vectors by index. Malformed input must return `Err`, never panic. |
| `number_format` | `numfmt::NumberFormat` | `width_*` computes a display width arithmetically; `write_*` renders into a fixed 64-byte stack buffer and returns the width it produced. The two must agree, and both must equal the characters actually appended. Table columns are sized from these numbers. |
| `fuzzy_match` | `fuzzy::best_match` | Returned positions must be valid, strictly ascending character indices into the haystack, one per needle character. The home screen highlights by indexing with them. |
| `glob_match` | `numfmt::Glob` | A backtracking wildcard matcher. Checked for hangs and for the wildcard-free fast path agreeing with equality. |
| `config_parse` | `config::AppConfig`, `config::ColorParser` | Validation and merging of user TOML, and colour strings that are sliced by byte offset after a byte-length check. |

## Corpus

`corpus/` is committed as a *seed* corpus, not a full coverage corpus. `parse_query` and
`config_parse` are seeded from the unit tests and the examples in `docs/`, so their
entries are readable. The three targets taking `arbitrary`-decoded input hold a bounded
sample of minimised inputs, capped at 64 files each — a few minutes of fuzzing yields
thousands, and carrying them buys little when none of them is reviewable.

Files named `regression-*` are inputs that once crashed a target. Add one whenever you
fix a crash; leave general coverage to the fuzzer.

## When a target fails

libFuzzer writes the offending input to `fuzz/artifacts/<target>/`. Reproduce it with:

```sh
scripts/code/fuzz.sh run parse_query fuzz/artifacts/parse_query/crash-<hash>
```

Fix the bug, then add the input to `corpus/<target>/` so the replay job keeps it fixed.
A minimal reproducer usually deserves a unit test next to the code as well.

## Sanitizer

Off by default. `datui-lib` has no `unsafe`, so ASan finds little here that a panic would
not, and it roughly doubles the build (12 GB against 6.0 GB) while running slower. Turn
it on for a long run, where it reaches the `unsafe` inside Polars and Arrow:

```sh
DATUI_FUZZ_SANITIZER=address scripts/code/fuzz.sh run parse_query
```

## Why not nightly

cargo-fuzz normally wants a nightly compiler for `-Z sanitizer`. datui builds these
targets on stable with `RUSTC_BOOTSTRAP=1` instead, which `scripts/code/fuzz.sh` sets.

That is not just a preference. `polars-ops` has a build script that turns on its own
`nightly` feature whenever it detects a nightly compiler, and that code path uses
`core::unicode` internals which current nightly no longer exposes. Any nightly build of
the dependency tree therefore fails to compile, for reasons that have nothing to do
with datui. Building on stable keeps the fuzzers on the same pinned toolchain as the
rest of CI and sidesteps the problem.

[cargo-fuzz]: https://github.com/rust-fuzz/cargo-fuzz
