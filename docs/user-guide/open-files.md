# Open files and directories

Pass a file, several files, a directory, a glob or a URL.

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
datui -f app.log.ndjson                        # follow a file as it grows
datui app.log                                  # text: a row per line
SYSTEMD_PAGER=datui journalctl -u nginx        # datui as a pager
```

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
[configuration file](../reference/settings.md#csv).

## Compression

Files ending in `.gz`, `.zst`, `.bz2` or `.xz` are decompressed before loading.
Use `--compression gzip|zstd|bzip2|xz` when the extension is missing or wrong.

Compressed CSV, TSV or PSV is decompressed to a temporary file so it can still
be scanned lazily. `--temp-dir` chooses where; `-c read.decompress_in_memory=true`
skips the file and reads the whole thing into memory instead.

### Temporary files

A decompressed text file, a converted Arrow stream or GPS log, or a downloaded file
lives in the temp directory while datui uses it.

| Exit | Temporary files |
|---|---|
| `q`, Ctrl+Q, Ctrl+C, an error | Removed, including a partial file mid-download, mid-decompression or mid-conversion |
| SIGTERM, SIGHUP (closing the terminal) | Removed by the `datui` command, which quits as for `q` and exits with status 128 + the signal. Left by `datui.view()` in Python, which leaves signals to Python |
| Windows: closing the console window, signing out, shutting down | Removed by the `datui` command, which quits as for `q` |
| SIGKILL, ending the task in Task Manager | Left in the temp directory |

On Windows a file cannot be removed while datui still reads it through a memory map.
One that would not go is tried again as datui quits.

## Hive-partitioned data

A directory tree whose segments are `key=value` (`year=2024/month=01/...`)
opens as one table. Pass the root directory, which needs no flag, or a glob with
`--hive`; a glob usually needs quoting so your shell leaves it alone. A path
that exists is never a glob: `d[1].parquet` opens that file, not `d1.parquet`. Only
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
files, datui samples evenly across the file list. A dataset of more than 64
files may also start with a partial schema while the remaining footers load.

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
downloaded. Use a [cloud source](home-screen.md) on the home screen to find data without
