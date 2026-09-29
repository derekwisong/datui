# Loading Data

```bash
datui data.parquet                             # a file
datui jan.csv feb.csv mar.csv                  # files of the same shape, as one table
datui /data/events/                            # a directory, read the way Enter reads its row
datui --hive "/data/events/**/*.parquet"       # a glob (quote it)
datui s3://bucket/path/file.parquet            # S3, GCS (gs://) or HTTP(S)
datui --format csv https://example.com/export  # force the format when the name gives no hint
```

## Directories

`datui <directory>` does what <kbd>Enter</kbd> on that directory's row does on
the [home screen](home-screen.md), and needs no flag:

| The directory | What happens |
|---|---|
| A hive tree, or files that are one table | Opens as one table |
| Separate tables, more than one format, or no data directly inside | The home screen, browsed into it — one keystroke from either a file or the union |
| A Delta, Iceberg or Hudi root | The home screen, browsed into it, saying datui does not read the table itself yet |

`--hive` means: read this as partitioned, which is the answer for a glob and
for a layout that does not say so itself.

Working out which of those a directory is means reading footers, or the front
of a spread of its files, and datui reads them the way it is about to read the
whole directory — `--no-header`, `--skip-rows`, `--skip-lines`,
`--infer-schema-length` and the rest all apply. So `datui --no-header exports/`
judges the directory as headerless, finds its files stack, and opens it;
without the flag the same directory is judged with a header, its files do not
agree, and the home screen opens on it instead. datui draws the screen first,
and the loading screen keeps the way out visible: <kbd>Ctrl</kbd>+<kbd>O</kbd>,
<kbd>q</kbd> and <kbd>?</kbd> stay in the bar beside the progress and act at
once (<kbd>Ctrl</kbd>+<kbd>C</kbd> and <kbd>Ctrl</kbd>+<kbd>Q</kbd> too). Keys
are not held during a load — the allowed keys act, the rest are dropped — and
abandoning a cloud load stops its reads instead of letting them run on.

Every option is listed in [Command Line Options](../reference/command-line-options.md).
Defaults for most of them can be set once in the
[configuration file](configuration.md#file-loading).

## Formats

The format is taken from the extension, or from `--format` when there is none.

| Format | Extensions | Lazy | Hive partitions |
|---|---|---|---|
| Parquet | `.parquet` | yes | yes |
| CSV and other delimited text | `.csv`, `.tsv`, `.psv` | yes | |
| NDJSON | `.jsonl` | yes | |
| Arrow IPC, Feather v2 | `.arrow`, `.ipc`, `.feather` | yes | |
| JSON | `.json` | | |
| Avro | `.avro` | | |
| Excel | `.xlsx`, `.xlsm`, `.xlsb`, `.xls` | | |
| ORC | `.orc` | | |

**Lazy** formats are scanned as needed. Browsing reads a buffer of rows;
queries, sorting and analysis may read the full input. The other formats are
loaded in full before the table appears.

**Excel** opens the first sheet unless `--sheet` names another, by index
(`--sheet 0`) or name (`--sheet Sales`).

### CSV options

They apply to `.tsv` and `.psv` files too. A directory of CSVs in a bucket takes
all of them except `--parse-strings`.

| Option | Config key | What it does |
|---|---|---|
| `--delimiter 9` | | Column separator as an ASCII code (`59` for `;`, `124` for `\|`). Default `,` for `.csv`, tab for `.tsv`, `\|` for `.psv` |
| `--no-header` | | The first row is data, not names. <kbd>H</kbd> does the same, or undoes it, on the file on screen |
| `--skip-lines N`, `--skip-rows N` | | Ignore a preamble |
| `--skip-tail-rows N` | | Ignore a footer. Counts every row first: on a directory in a bucket, that downloads every file before the table opens |
| `--null-value NA`, `--null-value amount=` | | Values to read as null, for every column or one (`COL=VAL`). Repeatable |
| `--infer-schema-length 10000` | `infer_schema_length` | Rows used to infer column types (default 1000). Raise it when a column turns from integer to text late in the file |
| `--ignore-errors` | `ignore_errors` | Skip rows that fail to parse instead of failing the load |
| `--parse-dates=false` | `parse_dates` | Stop parsing date-looking strings as Date and Datetime |
| `--parse-strings=COL`, `--no-parse-strings` | | Trim and type-infer string columns; limit it to named columns, or turn it off |

## Compression

Files ending in `.gz`, `.zst`, `.bz2` or `.xz` are decompressed before loading.
Use `--compression gzip|zstd|bzip2|xz` when the extension is missing or wrong.

Compressed CSV is decompressed to a temporary file so it can still be scanned
lazily. `--temp-dir` chooses where; `--decompress-in-memory` skips the
file and reads the whole thing into memory instead.

## Hive-partitioned data

A directory tree whose segments are `key=value` (`year=2024/month=01/...`)
opens as one table. Pass the root directory, which needs no flag, or a glob with
`--hive`; a glob usually needs quoting so your shell leaves it alone. Only
Parquet is supported — for a hive tree of anything else, open one partition.

Partition columns appear first in the table and on the **Partitions** tab of the
[Info panel](dataset-info.md). On disk, a directory is faster to open than a glob:
a local glob is handed to Polars, while a directory is walked by datui and gets the
schema union, the row count and the notes. In a bucket both are datui's — it lists
the prefix and matches the pattern itself — so a remote glob opens the same way a
remote prefix does.

### Files that disagree

By default, datui combines schemas from Parquet footers. It does not need to
scan data values to find the columns.

| Across the files | In the table |
|---|---|
| A column appears in only some files | Shown, with nulls for files missing the column |
| Compatible types, such as `Int32` and `Int64` | Widened to a shared type |
| Incompatible types, such as numbers and text | Uses the type with the most rows; values of incompatible types are not read |
| An unreadable footer | Skips that file |

The [Info panel](dataset-info.md) reports the metadata scope. Above 20,000
files, datui samples evenly across the file list. A cloud dataset may also
start with a partial schema while the remaining footers load.

When all file row counts are known, empty cells distinguish three cases:

| Cell | Meaning |
|---|---|
| `∅` | A null value |
| `·` | The source file has no such column |
| `≠` | The source file stores an incompatible type, so the value was not read |

Column-name markers also identify missing or conflicting fields. When row
counts are incomplete, empty cells all display as `∅`, though the column
markers remain. Queries, pivots and other transformations create new rows
without this file-level distinction. Exports write all three cases as null.

To recover conflicting values, open the column's note in **Info → Notes**
and apply **read as text**, if offered. This reuses the existing metadata.
Lists, arrays, durations, binary and unknown types cannot use this action.
Filters and sorting then compare strings: `"10"` sorts before `"2"`.
Compatible numeric types still widen as usual; their mixed-type note remains.

**Sidebar filters and sorting can exclude conflicting rows.** When every
file's row count is known, using a conflicting column removes rows from files
that store its incompatible type. A note reports the affected count. Clearing
that filter or sort restores the rows. With incomplete row counts, those rows
remain as nulls instead.

This exclusion applies even to an OR filter: `id = 3 OR n = 0` still removes
rows from files where `n` has an incompatible type. Queries in the
[query bar](querying-data.md) create a separate result and do not apply this
file-level rule. Missing-column (`·`) rows remain during sorting; filters
handle missing values as nulls.

### How large remote datasets open

Cloud directories with more than 64 files open using the first and last files
by name, then read the remaining footers in the background. New columns join
the end of the table as they are found. Until then:

- The total row count is unavailable and empty cells display as `∅`.
- Notes state the partial metadata scope.
- A query, pivot or drill-down defers the new columns until you return to the original data.

Local directories read metadata before opening. Above 20,000 files, both
routes use a sample. The control bar reports footer-reading progress.

The Notes tab also flags storage layouts that may explain a slow open:

| Finding | Why it matters |
|---|---|
| Median row-group size above 64 MiB | Reading a page may require a large row group |
| More than 10,000 files, median size below 1 MiB | Many metadata reads before data can be displayed |
| Different partition keys, such as `date` and `dt` | Partition columns vary across the dataset; key order alone is fine |

`--single-spine-schema=false` skips datui's footer union and uses Polars'
single-file schema inference. That route also omits the partition-key check.

### Opening it again

Remote schema metadata is cached by URL. Datui still lists the files to check
for changes to names, sizes, timestamps or etags. A changed listing triggers
fresh metadata reads.

`--clear-cache` clears this metadata along with other cached state, including
query history. See [cache contents](home-screen.md#what-datui-remembers).

## Binary columns

A binary column shows a dim `‹binary›` placeholder instead of its bytes, so
scrolling past large blobs stays fast. The bytes are still read for exports and
analysis. The placeholder color is `binary_col` in the
[theme](configuration.md#colors).

## Remote data

```bash
datui s3://bucket/events/
datui gs://bucket/data.parquet
datui abfss://container@account.dfs.core.windows.net/data.parquet
datui https://example.com/data.csv
```

[Remote data](remote-data.md) explains credentials, public access and what gets
downloaded. Use the [cloud browser](cloud-browser.md) to find data without
typing a URL.

| Connect to | Setup |
|---|---|
| AWS | [S3](remote-data.md#amazon-s3) · [Profiles and SSO](remote-data.md#aws-profiles) |
| S3-compatible storage | [Custom endpoint](remote-data.md#s3-compatible-storage-minio-r2-ceph) · [Multiple stores](remote-data.md#several-stores-at-once) |
| Google Cloud | [GCS](remote-data.md#google-cloud-storage) |
| Azure | [Blob Storage](remote-data.md#azure-blob-storage) |
| Public data | [No-login access](remote-data.md#public-data) |
| Web URL | [HTTP and HTTPS](remote-data.md#http-and-https) |

<a id="amazon-s3"></a>
<a id="aws-profiles"></a>
<a id="s3-compatible-storage-minio-r2-ceph"></a>
<a id="several-stores-at-once"></a>
<a id="google-cloud-storage"></a>
<a id="azure-blob-storage"></a>
<a id="public-data"></a>
<a id="http-and-https"></a>
<a id="building-without-cloud-support"></a>
