# Open files and directories

Pass datui a file, several files, a directory, a glob or a URL. Files of the
same shape open as one table.

```bash
printf 'id,amount\n1,9.50\n2,3.25\n' > jan.csv
printf 'id,amount\n3,4.00\n' > feb.csv
datui jan.csv
datui jan.csv feb.csv
mkdir -p exports && cp jan.csv feb.csv exports/ && datui exports/
```

| Command | Opens |
|---|---|
| `datui FILE` | One file, in the format its extension or first bytes say ([Formats](../formats/index.md)) |
| `datui FILE FILE...` | Files of the same shape as one table |
| `datui DIR/` | A directory, as <kbd>Enter</kbd> on its row on the [home screen](home-screen.md) does |
| `datui --hive 'GLOB'` | The files a glob matches, as one partitioned table. Quote the glob |
| `datui URL` | An `s3://`, `gs://`, `abfss://` or `https://` URL: [Connect to cloud storage](remote-data.md) |
| `datui -` | Standard input: [Pipes and growing files](pipes-and-follow.md) |
| `datui --format FMT FILE` | A file whose name does not say its format |

During a load, <kbd>Ctrl</kbd>+<kbd>O</kbd> cancels it and returns home, and
<kbd>Ctrl</kbd>+<kbd>Q</kbd> quits.
[Command-line options](../reference/command-line-options.md) lists every flag;
most flags also have a [setting](../reference/settings.md) for their default.

## Directories

`datui DIR/` needs no flag:

| The directory holds | What opens |
|---|---|
| A hive tree, or files that are one table | One table |
| Separate tables, several formats, or no data directly inside | The directory on the home screen; its first row reads everything as one table |
| A Delta, Iceberg or Hudi root | The directory on the home screen, with a warning that the transaction log is not applied |

`--hive` reads a glob, or a layout datui would not recognize on its own, as
partitioned. Reading flags also affect the decision: with
`datui --no-header exports/`, the first row of each headerless CSV is not taken
as a header, which would make the files look like separate tables.

## Hive-partitioned data

A tree of `key=value` directories (`year=2024/month=01/...`) opens as one
table, its partition columns first. Pass the root, or a glob with `--hive`:

```bash,network
datui --hive 's3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=T*/*.parquet'
```

- Only Parquet is read as a hive tree; for anything else, open one partition.
- A path that exists is never a glob: `d[1].parquet` opens that file.
- The **Partitions** tab of the [Info panel](dataset-info.md) lists the keys
  and values.
- For local and remote directories and remote globs, datui reads the schema
  from the Parquet footers; a local glob is passed to Polars.

### Files that disagree

The schema is the union of the files' Parquet footers; no data is scanned to
find it.

| Across the files | In the table |
|---|---|
| A column in only some files | Shown; null in the files that lack it |
| Compatible types (`Int32`, `Int64`) | Widened to one type |
| Incompatible types (numbers and text) | The type of the most rows; values of the other type are not read |
| An unreadable footer | That file is skipped |

Above 64 files, the table may open on a partial schema while the other
footers load. Above 20,000, the footers are sampled evenly across the list. The Info panel
says what was read; [Large datasets](large-datasets.md#how-large-datasets-open)
has the details.

When every file's row count is known, an empty cell says why:

| Cell | Means |
|---|---|
| `∅` | A null value |
| `·` | The file has no such column |
| `≠` | The file holds the column in an incompatible type; the value was not read |

Without every row count, all three show as `∅`, but the column name's `*`
marker still shows. Queries, pivots and exports write all three as null.

| To | Do |
|---|---|
| Read the conflicting values | In **Info → Notes**, choose **read as text** on the column's note. Not offered for lists, arrays, durations, binary or unknown types. Sorting and filtering then compare text, so `"10"` comes before `"2"` |
| Keep every row while filtering on a conflicting column | Use a [query](querying-data.md). A sidebar filter or sort on the column drops the rows of files that hold the other type, even inside an OR (`id = 3 OR n = 0`). A note counts the dropped rows, and clearing the filter brings them back |

## Compression

`.gz`, `.zst`, `.bz2` and `.xz` files are decompressed as they open. When the
extension does not say, name the compression with `--compression gzip|zstd|bzip2|xz`.

```bash
printf 'id,amount\n1,9.50\n2,3.25\n' | gzip > sales.csv.gz
datui sales.csv.gz
```

Compressed CSV, TSV and PSV are decompressed once to a temporary file in
`--temp-dir`, then scanned; `-c read.decompress_in_memory=true` reads them
into memory instead. The
[formats table](../formats/index.md#how-each-format-is-read) lists which
formats open compressed.

### Temporary files

Decompressed, converted and downloaded files live in the temp directory while
datui uses them.

| How datui ends | Its temporary files |
|---|---|
| `q`, <kbd>Ctrl</kbd>+<kbd>Q</kbd>, <kbd>Ctrl</kbd>+<kbd>C</kbd>, an error | Removed, including a partial download, decompression or conversion |
| SIGTERM, SIGHUP (closing the terminal) | Removed: the `datui` command quits as for `q` and exits with 128 + the signal number. `datui.view()` in Python leaves signals, and so the files, to Python |
| Windows: closing the console, signing out, shutting down | Removed, as for `q` |
| SIGKILL, ending the task in Task Manager | Left behind |

On Windows, a file that is still mapped cannot be removed; datui tries again
as it quits.

## Binary columns

A binary column shows a dim `‹binary›` instead of its bytes. Exports and
analysis still read the bytes, and the [inspector](inspecting-rows.md) shows
them. The color is `binary_col` in the [theme](../reference/settings.md#colors).

## Remote data

These are public and open with no login:

```bash,network
datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/
datui https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv
```

[Connect to cloud storage](remote-data.md) covers logins and what is
downloaded; the home screen's [cloud sources](home-screen.md#cloud-sources)
let you find data without typing a URL.
