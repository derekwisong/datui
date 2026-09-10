# Fuzzing

Datui fuzzes the hand-written parsers and matchers that run on untrusted input, using
[cargo-fuzz][cargo-fuzz] and libFuzzer. The targets live in `fuzz/`.

Everything datui parses arrives from somewhere the user does not fully control: a query
typed into the query bar, a config file written by a theme generator, column names that
came out of a data file. Serde and Polars handle their own inputs; what is fuzzed here
is the code datui wrote itself, which is where the index arithmetic lives.

## What is fuzzed

| Target | Surface | What it checks |
| --- | --- | --- |
| `parse_query` | `query::parse_query` | A tokeniser and recursive-descent parser that slices token vectors by index. Malformed input must return `Err`, never panic. |
| `number_format` | `numfmt::NumberFormat` | `width_*` computes a display width arithmetically, `write_*` renders into a fixed 64-byte stack buffer and returns the width it produced. The two must agree, and both must equal the characters actually appended. Table columns are sized from these numbers, so a disagreement corrupts the layout instead of failing visibly. |
| `fuzzy_match` | `fuzzy::best_match` | Returned positions must be valid, strictly ascending character indices into the haystack, one per needle character. The home screen highlights matches by indexing with them. |
| `glob_match` | `numfmt::Glob` | A backtracking wildcard matcher, checked for hangs and for its wildcard-free fast path agreeing with equality. |
| `config_parse` | `config::AppConfig`, `config::ColorParser` | Validation and merging of user TOML, and colour strings that get sliced by byte offset after a byte-length check. |

Three of these check an invariant rather than merely the absence of a panic. A fuzzer
that only asks "did it crash" finds far less than one that can also ask "did it produce
an answer that contradicts the other implementation of the same thing".

### What the first run found

Two real bugs, both fixed in the change that added these targets, and both kept fixed by
the corpus:

- **The query parser died on the stack.** Parsing is recursive descent, so nesting in
  the query became nesting on the stack. Roughly two hundred nested parentheses, or the
  same number of leading minus signs, exhausted a worker thread's stack and killed the
  process. Anyone could reach it by pasting into the query bar. `parse_expr` now refuses
  to nest past `MAX_EXPR_DEPTH` and returns an ordinary error.
- **The colour parser sliced a character in half.** It checked that a hex colour was
  seven *bytes* and then indexed at fixed byte offsets. `#` followed by a four-byte emoji
  and two more characters is also seven bytes, so the slice landed mid-character and
  panicked. It now requires the body to be ASCII before indexing.

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
generates nothing new, so it finishes in seconds and never reports something different
on two runs.

## What CI does

**Every pull request** runs `replay` in the `CI` workflow. It is a regression gate: it
re-runs the inputs already known to be interesting and fails if one of them starts
crashing again. It does not look for new bugs.

**The Nightly workflow** runs each target for ten minutes against fresh input, with
AddressSanitizer on, as a matrix so one slow target does not consume another's budget.
Crashing inputs are uploaded as build artifacts.

Each target's corpus is cached between runs, and this matters more than the ten minutes
does. Fuzzing is cumulative: reaching a bug often takes a chain of discoveries, where one
input gets as far as the tokeniser, a mutation of it reaches the parser, and a mutation
of *that* crashes. Starting from the seed corpus every night caps the search at whatever
is reachable in one sitting, so the deep chains never form. With the corpus restored,
each night begins where the last one left off.

A cache entry cannot be updated in place, so the key carries the run id and
`restore-keys` picks up the most recent previous entry. Restore and save are separate
steps because the combined action only writes its cache when the job succeeds, and the
night a target crashes is the night its corpus is most worth keeping. `cargo fuzz cmin` runs before the
corpus is stored, dropping inputs that no longer reach anything the rest does — otherwise
it grows until restoring it costs more than the fuzzing.

## The corpus

`fuzz/corpus/` is committed, but it is a *seed* corpus, not the full coverage corpus.

The two targets that take text are seeded with inputs a person can read: `parse_query`
from the parser's own unit tests and the query examples throughout `docs/`,
`config_parse` from the TOML blocks in `docs/`. Anything named `regression-*` is an
input that once crashed a target, kept so the replay job notices if it ever crashes
again.

The other three take structured input that `arbitrary` decodes from raw bytes, so a
hand-written seed would mean nothing. Those directories hold a bounded sample of
minimised inputs from a real run, capped at 64 files each.

The cap is deliberate. A few minutes of fuzzing produces thousands of inputs — around 27
MB across 6,900 files even after `cargo fuzz cmin` — and carrying that in the repository
buys far less than it costs, since nothing in it is reviewable. A run started from these
seeds rediscovers the rest within minutes, and the Nightly workflow uploads whatever it
finds as an artifact.

So: contribute a `regression-*` file for anything that crashed, and leave the rest to the
fuzzer. If you do want to add coverage seeds, minimise first:

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
