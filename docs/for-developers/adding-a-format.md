# Add a format

A format has two parts: a descriptor in `datui-cli`, with the facts about it
that need no file, and a reader in `datui-lib`, with the code that reads it.
Both are matched exhaustively, so a format missing either part does not compile.

| Step | Where |
|---|---|
| A variant | `FileFormat` in `crates/datui-cli/src/formats.rs`, and its place in `FileFormat::ALL` |
| A descriptor | A `const` beside the others, spread from `BASE`, `DELIMITED`, `READ_INTO` or `MODEL`, and its line in `FileFormat::descriptor` |
| A parser and a `READER` | A module of its own in `crates/datui-lib/src/formats/`; a format Polars reads goes in `formats/readers/polars.rs` |
| Its line in the registry | `readers::of` in `crates/datui-lib/src/formats/readers/mod.rs` |
| Its docs page | A heading for it on a family page in `docs/formats/`, and its line in `format_page` in `crates/datui-cli/src/docgen.rs` |
| Generated docs | `cargo run -p datui-cli --bin gen_docs -- write`: the format table, the format count, `--format`'s and `--table`'s help |

The open, the home screen, `--help` and the docs all read the descriptor:

```rust
const ELF: Descriptor = Descriptor {
    name: "elf",
    title: "ELF",
    extensions: &["elf", "axf"],
    tables: Some(Tables {
        opens: Some("its symbols"),
        by_name: false,
        help: "symbols (default) or sections",
    }),
    summary: Summary::Tab("ELF"),
    ..BASE
};
```

| Field | Says |
|---|---|
| `name` | What `--format` takes and the home screen counts (`12 parquet`). The dataset cache stores it, so never rename one |
| `title` | Its name in a sentence and in the docs |
| `extensions`, `name_endings` | The file names that identify it. An extension may belong to only one format |
| `read`, `compressed`, `stream` | How a local file is read: `Lazy`, `Converted` or `InMemory`, and how a compressed file is read |
| `http`, `bucket_object`, `bucket_prefix` | How a remote file, object or prefix is read |
| `many_files` | Whether several files of it are one table |
| `lines` | For text read a line at a time: what `--follow` reads |
| `tables` | The tables a file of it holds, and what `--table` takes |
| `summary` | Its Info panel tab, or why it has none |
| `declares_types`, `conversion` | Whether a file declares its column types; what the loading screen says while it converts |

The reader starts from `readers::BASE`:

```rust
pub(crate) const READER: crate::formats::readers::Reader = crate::formats::readers::Reader {
    scan,
    signatures: &[crate::formats::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::formats::readers::Kind::Magic,
        trusted: crate::formats::readers::EVERYWHERE,
    }],
    tables: Some(|_| Ok(tables())),
    ..crate::formats::readers::BASE
};
```

| Field | Holds |
|---|---|
| `scan` | Opens the files and returns a frame, or a `Scan` that the load turns into one |
| `convert` | Converts a file into files datui can scan, when the scan returns `Scan::ReadInto` |
| `signatures` | The leading bytes that identify it, how they match (`Magic`, `Structure`, `Text`), and where they are trusted (`Trusted`: pipes, unnamed files, listings, files of tables) |
| `tables`, `table_schema` | The tables, and their columns, that the home screen lists |
| `facts`, `preview` | Cheap reads for the Info panel and the home screen's preview |
| `python`, `export` | The Polars call that Copy as Python writes; the default export format |

The module docs of `formats/readers/mod.rs` list the places outside a format's
own module that still name it, and why.

## Tests that catch a miss

| Test | Fails when |
|---|---|
| `every_format_is_listed` (`datui-cli`) | A variant is missing from `ALL` |
| `names_and_extensions_say_one_format` | Two formats claim an extension or a name |
| `help_and_refusals_come_from_the_descriptors` | `--format`'s or `--table`'s help leaves it out |
| `conversions_say_what_they_read` | A converted format has no loading-screen text, or shares another format's |
| `read_mode_by_format_and_storage` (`datui-cli`) | Its read modes differ from the test's list; update the list on purpose |
| `readers_agree_with_their_descriptors` (`datui-lib`) | The reader lists tables, converts or scans a prefix where the descriptor says otherwise |
| `the_docs_tab_table_agrees_with_the_descriptors` | `docs/user-guide/dataset-info.md`'s table of tabs has no row for it |
| `the_copy_docs_name_each_format_s_reader` | `docs/user-guide/copying.md`'s reader table has no row for it, in `--format`'s order |
| `the_generated_docs_are_current` | `gen_docs write` was not run |

A hand-written parser of untrusted bytes also gets a fuzz target and a seed
corpus; see [Fuzzing](fuzzing.md). Give the format's docs page an example that
opens a public file or a file the block creates, so the
[doc-example runner](documentation.md#code-blocks) tests it.

```bash,repo
scripts/dev/test.sh cli
scripts/dev/test.sh unit readers::
```
