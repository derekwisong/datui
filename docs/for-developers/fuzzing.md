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
| `ipc_stream_head` | `ipc_stream::is_stream_head`, `stdin::sniff` | Reads a length from a file's first bytes and checks the flatbuffer it names is an Arrow schema message, on any file being opened and any pipe. Any bytes must give an answer, never a panic (Polars' own schema reader panics on some column types), and a pipe that is a stream must be read as one. |
| `config_parse` | `config::AppConfig`, `config::ColorParser` | Validation and merging of user TOML, and color strings that get sliced by byte offset after a byte-length check. |
| `audio_header` | `audio::read_header`, `audio::AudioSource` | The WAV, RF64 and AIFF chunk walker, which slices by sizes, counts and offsets the file states, and the sample decoder, which reads at offsets worked out from the header. A corrupt header must be an error, never a panic or an allocation sized by the file; a header that parses must give frames inside the file, and decoding them must work. |
| `midi_file` | `midi::parse`, `midi::build` | A hand-written Standard MIDI File parser that slices by chunk lengths, variable-length deltas and event lengths read from the file, and keeps running status between events. A corrupt file must be an error, never a panic or an allocation sized by the file; a file that parses must build its table, one row per event. |
| `model_header` | `model_files::read_safetensors`, `model_files::read_gguf`, `model_files::read_header_ranged_from` | Hand-written readers for model file headers that allocate and skip by lengths read from the file. Every input goes to both; a corrupt header must be an error, never a panic or an allocation sized by the file, and a header that parses must build its table. Read again by range, in ranges of a few bytes, each finds the same header or fails as the file reader does. |
| `format_spec` | `formats::Spec`, `fixed_records` | A binary format spec and a file it reads, split at the first NUL byte. A spec parses or fails with a line and column; a file reads or fails; every row the reader counts decodes, and a window of the rows matches the same rows read from the start. |
| `gps_parse` | `gps::nmea::NmeaReader`, `gps::gpx::GpxReader` | Hand-written readers for GPS logs that take the file a piece at a time. The first byte picks the NMEA table and the size of the pieces, so every line, tag and entity is cut somewhere. Never a panic, a frame of another schema, or a coordinate off the globe; every length is bounded by the reader. |

## Layout

| Path | What |
| --- | --- |
| `fuzz/src/<target>.rs` | The target's body: `run`, its input type and every check |
| `fuzz/fuzz_targets/<target>.rs` | libfuzzer-sys decodes the input and calls `run` |
| `tests/fuzz_corpus_test.rs` | Replays `fuzz/corpus/<target>/` through the same `run`, as an ordinary test |

The test decodes each input as libfuzzer-sys does (`Arbitrary::arbitrary_take_rest`
over `Unstructured`), runs the empty input as libFuzzer does, and fails on any panic,
even one the code catches, as the fuzzer's panic hook does. Its decoding matches the fuzzer's only while `Cargo.lock` and `fuzz/Cargo.lock` resolve the
same `arbitrary`; the test checks that too. Put new checks in `run`. A new target needs
a line in the test, which fails until its corpus is replayed.

## Running

Replay every committed corpus input, without building the fuzzers:

```bash
./scripts/dev/test.sh integration fuzz_corpus_test
```

To fuzz, install the tool once:

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

`replay` loads every committed corpus input into the instrumented binaries, runs each
once, and generates no new test inputs. It checks the same inputs as the test above,
after a much longer build.

## What CI does

**Every pull request** replays the corpus through `tests/fuzz_corpus_test.rs`, as part
of the test suite. It is a regression gate: it re-runs the inputs already known to be
interesting and fails if one of them starts crashing again. It does not look for new
bugs. The `Fuzz targets` job runs `cargo check --manifest-path fuzz/Cargo.toml --locked`
so the fuzz crate keeps compiling.

**The Nightly workflow** runs each target for ten minutes against fresh input, with
AddressSanitizer on, as a matrix so one slow target does not consume another's budget.
Crashing inputs are uploaded as build artifacts.

Each target restores the previous coverage corpus before running and minimizes
it with `cargo fuzz cmin` afterward. Separate cache restore/save steps preserve
new inputs even when a target crashes.

**Every release** runs `DATUI_FUZZ_SANITIZER=address ./scripts/code/fuzz.sh replay`
over all committed corpora from the tag. Publishing requires this job to pass,
independently of Nightly. Failed replays upload crashing inputs as build artifacts.

## The corpus

`fuzz/corpus/` is committed, but it is a *seed* corpus, not the full coverage corpus.

The four targets that take text are seeded with inputs a person can read: `parse_query`
from the parser's own unit tests and the query examples throughout `docs/`,
`sql_group_plan` from the planner's unit tests, `config_parse` from the TOML blocks in
`docs/`, and `format_spec` from the specs in the user guide, each with a file after it. Anything named `regression-*` is an
input that once crashed a target, kept so the replay test notices if it ever crashes
again.

The other three take structured input that `arbitrary` decodes from raw bytes, so a
hand-written seed would mean nothing. Those directories hold a bounded sample of
minimized inputs from a real run, capped at 64 files each.

`midi_file` takes the bytes as they are. Its seeds are small files: format 0, 1 and
2 with running status, sysex and meta events, SMPTE timing, a RIFF MIDI wrapper, and
a track cut short.

`model_header` takes the bytes as they are. Its seeds are small model file headers:
SafeTensors with and without `__metadata__`, GGUF v3 in both byte orders with strings,
arrays and tensors of several types, and GGUF v2.
`gps_parse` takes the bytes as they are. Its seeds are a short NMEA log (every sentence
type it reads, a prefixed line, a vendor sentence, out-of-range coordinates) and a GPX
file (a DOCTYPE, CDATA, entities, namespaced extensions), each behind several first
bytes, and a nesting past the depth bound.

`audio_header` takes the bytes as they are too. Its seeds are tiny audio files: 16-bit
PCM, a Broadcast WAV with `bext`, iXML, `cue ` and `LIST` chunks, extensible float with
a channel mask, RF64 with `ds64`, 8-bit with a placeholder data size, and AIFF and
AIFF-C (`sowt`, `fl32`) with a marker.

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

Fix the bug, then copy the input into `fuzz/corpus/<target>/` so the replay test keeps it
fixed. A minimal reproducer usually deserves a unit test next to the code as well.

## Sanitizer

Local runs omit the sanitizer by default. Enable AddressSanitizer when
checking dependency memory errors or running a longer fuzzing session:

```bash
DATUI_FUZZ_SANITIZER=address ./scripts/code/fuzz.sh run parse_query
```

The Nightly and Release workflows run this configuration; pull requests do not.

Sanitizer builds use substantial memory. Nightly and Release limit parallel
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
