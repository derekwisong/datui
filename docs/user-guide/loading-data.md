# Files and formats

```bash
datui data.parquet                             # a file
datui jan.csv feb.csv mar.csv                  # files of the same shape, as one table
datui /data/events/                            # a directory, read the way Enter reads its row
datui --hive "/data/events/**/*.parquet"       # a glob (quote it)
datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/   # public S3; gs:// and abfss:// too
datui https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv
datui --format csv https://example.com/export  # force the format when the name gives no hint
cat data.csv | datui                           # data piped in
datui - < events.parquet                       # `-` reads standard input
```

## Standard input

`datui -` reads the data piped to it, and so does `datui` with no path when
something is piped in. Keys still come from the terminal.

```bash
xsv select id,amount sales.csv | datui
curl -s https://example.com/export.csv.gz | datui
datui --no-header - < raw.txt
```

The data is written to a temporary file as it arrives, in `--temp-dir` or the
`temp_dir` setting when given, then read like any file. The loading screen counts the bytes read;
<kbd>Ctrl</kbd>+<kbd>O</kbd> stops the read and removes the file. The file is
removed when datui exits.

The format comes from the first bytes, unless `--format` or `--compression`
names it:

| First bytes | Read as |
|---|---|
| Parquet, Arrow IPC or Avro magic number | that format |
| gzip, zstd, bzip2 or xz magic number | compressed CSV |
| `[` | JSON |
| `{`, the first line a whole object | NDJSON |
| `{`, the object open past the first line | JSON |
| a first line with tabs and no commas | TSV |
| anything else | CSV |

With `--format csv`, `tsv` or `psv` and no `--compression`, compression still
comes from the first bytes.

The CSV options below apply. The dataset is named `stdin`. It is not added to
recent datasets, and [views](views.md) match it by its columns only.

## Directories

`datui <directory>` does what <kbd>Enter</kbd> on that directory's row does on
the [home screen](home-screen.md), and needs no flag:

| The directory | What happens |
|---|---|
| A hive tree, or files that are one table | Opens as one table |
| Separate tables, more than one format, or no data directly inside | Opens the directory browser; choose a file or the first row, which reads them all |
| A Delta, Iceberg or Hudi root | Opens the directory browser with a warning that transaction logs are not applied |

`--hive` means: read this as partitioned, which is the answer for a glob and
for a layout that does not say so itself.

Reader options also affect directory detection. For headerless CSVs, use
`datui --no-header exports/`; otherwise datui may treat the first data rows as
headers and decide the files are separate tables.

During loading, <kbd>Ctrl</kbd>+<kbd>O</kbd> cancels and returns home.
<kbd>Ctrl</kbd>+<kbd>Q</kbd> quits. Other editing keys are not queued during a load.

Every option is listed in [Command Line Options](../reference/command-line-options.md).
Defaults for most of them can be set once in the
[configuration file](../reference/settings.md#file-loading).

## Formats

The format is taken from the extension, or from `--format` when there is none.

| Format | Extensions | Lazy | Hive partitions |
|---|---|---|---|
| Parquet | `.parquet` | yes | yes |
| CSV and other delimited text | `.csv`, `.tsv`, `.psv` | yes | |
| Arrow IPC, Feather v2 | `.arrow`, `.ipc`, `.feather` | yes | |
| NDJSON | `.jsonl` | | |
| JSON | `.json` | | |
| Avro | `.avro` | | |
| Excel | `.xlsx`, `.xlsm`, `.xlsb`, `.xls` | | |
| ORC | `.orc` | | |

**Lazy** formats are scanned as needed. Browsing reads a buffer of rows;
queries, sorting and analysis may read the full input. The other formats are
loaded in full before the table appears, except a directory of NDJSON files in
a bucket, which is scanned.

**Excel** opens the first sheet unless `--sheet` names another, by index
(`--sheet 0`) or name (`--sheet Sales`).

### CSV options

They apply to `.tsv` and `.psv` files too. A directory of CSVs in a bucket takes
all of them except `--parse-strings` and `--parse-dates`.

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

### Dates and timestamps

String columns in CSV and JSON become dates when every value in the first 1000
rows (`parse_strings_sample_rows`) parses the same way. `--parse-dates=false`
turns this off.

| Value | Type |
|---|---|
| `2024-01-31` | `date` |
| `2024-01-31 10:00:00`, `2024-01-31T10:00:00.250` | `datetime[μs]` |
| `2024-01-31T10:00:00Z`, `2024-01-31T10:00:00.250+00:00`, `2024-01-31 05:00:00-05:00` | `datetime[μs, UTC]`, converted to UTC |

A column whose values disagree, such as an offset on some and none on others,
stays text. A value past those rows that does not parse is null. JSON strings
become dates or times, never numbers.

With `--no-parse-strings`, Polars decides from the rows it reads for the schema,
and a value it cannot parse fails the read. A directory of CSV or NDJSON files
in a bucket keeps them as text.

## Compression

Files ending in `.gz`, `.zst`, `.bz2` or `.xz` are decompressed before loading.
Use `--compression gzip|zstd|bzip2|xz` when the extension is missing or wrong.

Compressed CSV, TSV or PSV is decompressed to a temporary file so it can still
be scanned lazily. `--temp-dir` chooses where; `--decompress-in-memory` skips
the file and reads the whole thing into memory instead.

### Temporary files

A decompressed text file or a downloaded file lives in the temp directory while datui
uses it.

| Exit | Temporary files |
|---|---|
| `q`, Ctrl+Q, Ctrl+C, an error | Removed, including a partial file mid-download or mid-decompression |
| SIGTERM, SIGHUP (closing the terminal) | Removed by the `datui` command, which quits as for `q` and exits with status 128 + the signal. Left by `datui.view()` in Python, which leaves signals to Python |
| SIGKILL | Left in the temp directory |

## Hive-partitioned data

A directory tree whose segments are `key=value` (`year=2024/month=01/...`)
opens as one table. Pass the root directory, which needs no flag, or a glob with
`--hive`; a glob usually needs quoting so your shell leaves it alone. Only
Parquet is supported — for a hive tree of anything else, open one partition.

Partition columns appear first in the table and on the **Partitions** tab of the
[Info panel](dataset-info.md). Local directories use datui's schema union, counts and notes; local globs are
delegated to Polars. Remote prefixes and globs both use datui's metadata reader.

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
[theme](../reference/settings.md#colors).

## Remote data

```bash
datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/
datui gs://cloud-samples-data/bigquery/us-states/us-states.parquet
datui abfss://release@overturemapswestus2.dfs.core.windows.net/
datui https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv
```

Each of these is public and opens with no login.

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
