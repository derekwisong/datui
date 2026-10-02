# Fuzzing datui

Coverage-guided fuzzing with [cargo-fuzz] and libFuzzer. The targets cover the
hand-written parsers and matchers that run on untrusted input: the query language, the
number renderer, the fuzzy matcher, the config glob matcher, config loading, the
SafeTensors and GGUF header readers, and the NMEA and GPX readers.

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
scripts/code/fuzz.sh cmin parse_query         # drop inputs that add no coverage
```

The Nightly workflow caches each target's corpus between runs, so a night's fuzzing
starts where the last one finished rather than from these seeds. `cmin` runs before it is
stored, to stop it growing without bound.

Each target's body lives in `src/<target>.rs`; `fuzz_targets/<target>.rs` only decodes
the input and calls it. The main workspace's `tests/fuzz_corpus_test.rs` replays every
committed corpus through the same bodies as an ordinary test, without this build, so a
previously fixed crash fails the test suite if it comes back:

```sh
scripts/dev/test.sh integration fuzz_corpus_test
```

## Targets

| Target | Surface | What it checks |
| --- | --- | --- |
| `parse_query` | `query::parse_query` | A hand-written tokeniser and recursive-descent parser that slices token vectors by index. Malformed input must return `Err`, never panic. |
| `number_format` | `numfmt::NumberFormat` | `width_*` computes a display width arithmetically; `write_*` renders into a fixed 64-byte stack buffer and returns the width it produced. The two must agree, and both must equal the characters actually appended. Table columns are sized from these numbers. |
| `fuzzy_match` | `fuzzy::best_match` | Returned positions must be valid, strictly ascending character indices into the haystack, one per needle character. The home screen highlights by indexing with them. |
| `glob_match` | `numfmt::Glob` | A backtracking wildcard matcher. Checked for hangs and for the wildcard-free fast path agreeing with equality. |
| `config_parse` | `config::AppConfig`, `config::ColorParser` | Validation and merging of user TOML, and colour strings that are sliced by byte offset after a byte-length check. |
| `model_header` | `model_files::read_safetensors`, `model_files::read_gguf` | Model file headers read by lengths the file states. A corrupt header must be an error, never a panic or an allocation sized by the file. |
| `gps_parse` | `gps::nmea::NmeaReader`, `gps::gpx::GpxReader` | GPS logs read a piece at a time, with line, markup, text and depth bounds. The first byte picks the NMEA table and the piece size. Frames must keep their schema and every coordinate must be on the globe. |

## Corpus

`corpus/` is committed as a *seed* corpus, not a full coverage corpus. `parse_query` and
`config_parse` are seeded from the unit tests and the examples in `docs/`, so their
entries are readable. The three targets taking `arbitrary`-decoded input hold a bounded
sample of minimised inputs, capped at 64 files each — a few minutes of fuzzing yields
thousands, and carrying them buys little when none of them is reviewable.
`gps_parse` takes the bytes as they are; its seeds are small NMEA logs and GPX files
behind a first byte that picks the table and the piece size.

Files named `regression-*` are inputs that once crashed a target. Add one whenever you
fix a crash; leave general coverage to the fuzzer.

## When a target fails

libFuzzer writes the offending input to `fuzz/artifacts/<target>/`. Reproduce it with:

```sh
scripts/code/fuzz.sh run parse_query fuzz/artifacts/parse_query/crash-<hash>
```

Fix the bug, then add the input to `corpus/<target>/` so the replay test keeps it fixed.
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
