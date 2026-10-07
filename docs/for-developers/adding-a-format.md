# Add a format

A format is a descriptor in `datui-cli`, which says what is true of it without a
file, and a reader in `datui-lib`, which holds the code. Both are matched
exhaustively, so a format missing one does not compile.

| Step | Where |
|---|---|
| A variant | `FileFormat` in `crates/datui-cli/src/formats.rs`, and its place in `FileFormat::ALL` |
| A descriptor | A `const` beside the others, spread from `BASE`, `DELIMITED`, `READ_INTO` or `MODEL`, and its line in `FileFormat::descriptor` |
| A parser and a `READER` | A module of its own in `crates/datui-lib/src/`; a format Polars reads goes in `readers/polars.rs` |
| Its line in the registry | `readers::of` in `crates/datui-lib/src/formats/readers/mod.rs` |
| Its docs page | A heading for it on a family page in `docs/formats/`, and its line in `format_page` in `crates/datui-cli/src/docgen.rs` |
| Generated docs | `cargo run -p datui-cli --bin gen_docs -- write`: the format table, the format count, `--format`'s and `--table`'s help |

A descriptor, read by the open, the home screen, `--help` and the docs:

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
| `name` | What `--format` takes and the home screen counts (`12 parquet`). The dataset cache stores it: never rename one |
| `title` | Its name in a sentence and in the docs |
| `extensions`, `name_endings` | The names that say it. An extension may say one format only |
| `read`, `compressed`, `stream` | How a local file is read: `Lazy`, `Converted` or `InMemory`; what a compressed one does |
| `http`, `bucket_object`, `bucket_prefix` | How a remote file, object or prefix is read |
| `many_files` | Whether several files of it are one table |
| `lines` | For text read a line at a time: what `--follow` reads |
| `tables` | The tables a file of it holds, and what `--table` takes |
| `summary` | Its Info panel tab, or why it has none |
| `declares_types`, `conversion` | Whether its columns' types are known; the loading screen's words while it converts |

The reader, which starts from `readers::BASE`:

```rust
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan,
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::readers::Kind::Magic,
        trusted: crate::readers::EVERYWHERE,
    }],
    tables: Some(|_| Ok(tables())),
    ..crate::readers::BASE
};
```

| Field | Holds |
|---|---|
| `scan` | Opens the files: a frame, or a `Scan` the load turns into one |
| `convert` | Reads a format the scan answers with `Scan::ReadInto` into files of its own |
| `signatures` | The first bytes that say it, how (`Magic`, `Structure`, `Text`), and where they are believed (`Trusted`: pipes, unnamed files, listings, files of tables) |
| `tables`, `table_schema` | The tables and their columns the home screen lists |
| `facts`, `preview` | What the Info panel and the home preview read cheaply |
| `python`, `export` | Copy as Python's Polars call; the export default |

The module docs of `readers/mod.rs` list where a format is still named outside
its own module, and why.

## Tests that catch a miss

| Test | Fails when |
|---|---|
| `every_format_is_listed` (`datui-cli`) | A variant is missing from `ALL` |
| `names_and_extensions_say_one_format` | Two formats claim an extension or a name |
| `help_and_refusals_come_from_the_descriptors` | `--format`'s or `--table`'s help leaves it out |
| `conversions_say_what_they_read` | A format read into files of its own has no words of its own |
| `read_mode_by_format_and_storage` (`datui-cli`) | Its read modes are not the ones the test lists; update the list deliberately |
| `readers_agree_with_their_descriptors` (`datui-lib`) | The reader lists tables, converts or scans a prefix where the descriptor says otherwise |
| `the_docs_tab_table_agrees_with_the_descriptors` | `docs/user-guide/dataset-info.md`'s table of tabs has no row for it |
| `the_copy_docs_name_each_format_s_reader` | `docs/user-guide/copying.md`'s reader table has no row for it, in `--format`'s order |
| `the_generated_docs_are_current` | `gen_docs write` was not run |

A hand-written parser of untrusted bytes also gets a fuzz target and a seed
corpus; see [Fuzzing](fuzzing.md). Give the format's page an example on a
public file or one the block makes, so the [doc-example runner](documentation.md#code-blocks)
opens it.

```bash,repo
scripts/dev/test.sh cli
scripts/dev/test.sh unit readers::
```
