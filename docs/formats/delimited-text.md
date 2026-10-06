# Delimited text

CSV, TSV and PSV files are scanned where they are, and a `.log` or `.txt` file
is read a row per line.

```bash
printf 'id;amount\n1;9.50\n2;3.25\n' > sales.csv
datui --delimiter ';' sales.csv
```

## CSV, TSV and PSV

| | CSV | TSV | PSV |
|---|---|---|---|
| Extensions | `.csv` | `.tsv` | `.psv` |
| Delimiter | `,` | tab | `\|` |
| Read | [lazy scan](index.md#how-each-format-is-read); compressed, decompressed copy | the same | the same |
| A bucket prefix | read in place, as one table | one object at a time | one object at a time |
| `--follow` | yes | yes | yes |
| Info tab | none | none | none |

A file with another extension, or none, opens with `--format csv` (or `tsv`,
`psv`); text piped in is [detected by content](index.md#detected-by-content).
<kbd>H</kbd> on the [Info panel](../user-guide/dataset-info.md)'s Schema tab
reads the first row as data, or as column names again.

## CSV options

The options apply to TSV and PSV too. Set defaults under
[`[csv]`](../reference/settings.md#csv); a flag wins for one run.

| Option | Config key | What it does |
|---|---|---|
| `--delimiter ';'` | | Column separator: one character, `tab`, `\t` or a code such as `0x1f` |
| `--no-header` | | The first row is data; columns are `column_1`, `column_2`, … |
| `--skip-lines N` | | Skip N raw lines at the top, split on newlines alone |
| `--skip-rows N` | | Skip N rows at the top, quote-aware; the header is read after them |
| `--footer-rows N` | | Skip N rows at the end. Counts every row first: on a directory in a bucket, that downloads every file before the table opens |
| `--null NA`, `--null amount=` | `csv.null_values` | Values read as null, in every column or in one (`COL=VAL`). Repeatable; replaces the config's list |
| `--comment '#'` | `csv.comment` | Skip lines that start with it, before the header and among the data |
| `--header-rows 3`, `--header-rows 3,2` | `csv.header_join` | The line or lines holding the header, counted from 1 at the top. Several are joined per column with `header_join` (a space) |
| `--skip-initial-space` | `csv.skip_initial_space` | Ignore the spaces after a delimiter: padded numbers are numbers, a cell of spaces is null |
| `--infer-rows 10000` | `csv.infer_rows` | Rows read to infer column types (1,000). Raise it when a column turns from integer to text late in the file |
| `--ignore-errors` | `csv.ignore_errors` | Skip rows that do not parse instead of failing the read |
| `--infer-types=COL`, `--infer-types=off` | `read.infer_types` | Type string columns (dates, times, numbers) only in the named columns, or in none |

Column names are trimmed: `"     Latitude"` reads as `Latitude`. A blank name
reads as `column_N`, and a repeated one gets `_duplicated_0`. A directory of
CSV files in a bucket takes every option but `--infer-types` and
`--header-rows`, and keeps its values as text.

## Instrument and logger exports

Loggers often write comments and a units line above a padded header:

**`log.csv`**

```csv,file=log.csv
#device_info, log_version="1.03", model="X", serial="123"
#yyyy-mm-dd, hh:mm:ss, hh:mm, degrees, volts, deg F
  Lcl Date, Lcl Time, UTCOfst,     Latitude, bus1volts, E1 CHT1
          ,         ,        ,             ,      25.1,   187.2
2024-03-01, 10:00:00,  -05:00,    40.100000,      25.0,   180.0
```

```bash
datui --comment '#' log.csv
datui --comment '#' --header-rows 3,2 log.csv
datui --header-rows 3 log.csv
```

| Command | Columns |
|---|---|
| `datui --comment '#' log.csv` | `Lcl Date`, `Latitude`, …; `#` lines anywhere are skipped |
| `datui --comment '#' --header-rows 3,2 log.csv` | `Lcl Date yyyy-mm-dd`, `Latitude degrees`, … |
| `datui --header-rows 3 log.csv` | `Lcl Date`, `Latitude`, …; lines 1 and 2 are passed over |

- `--header-rows` counts lines before anything is skipped, and a named line
  that starts with the comment character loses it. `--skip-lines` counts from
  the same top; `--skip-rows` counts data rows after the header.
- Padded numbers are numbers with or without `--skip-initial-space`, unless
  `--infer-types=off`. With the flag, cells of spaces are null in text columns
  too, and `--null` matches a value without its padding.
- A file with nothing after its header lines opens with its columns and no rows.
- A run of NUL bytes at the end of a file ends it. Loggers that preallocate a
  file at a fixed size leave one. A NUL inside the text is kept.
- A byte that is not UTF-8 reads as `�` where it stands, rather than failing
  the read.
- In a directory, a file with no header (empty, blank, or only NULs) is
  skipped, and the [Notes tab](../user-guide/dataset-info.md) names it.

A [delimited format spec](format-specs.md#delimited-text) keeps these options
for a family of files, with their units and metadata line, so they open with
no flags. A flag on the command line still wins over the spec.

## Dates and timestamps

String columns in CSV and JSON become dates when every value in the first
1,000 rows (`--infer-rows`) parses the same way. `--infer-types=off` turns
this off.

| Value | Type |
|---|---|
| `2024-01-31` | `date` |
| `2024-01-31 10:00:00`, `2024-01-31T10:00:00.250` | `datetime[μs]` |
| `2024-01-31T10:00:00Z`, `2024-01-31T10:00:00.250+00:00`, `2024-01-31 05:00:00-05:00` | `datetime[μs, UTC]`, converted to UTC |

- A column whose values disagree, such as an offset on some and none on
  others, stays text.
- A value past the rows read for types that does not parse is null. The
  first time the Info panel opens, one pass counts them, and the Notes tab
  says how many per column: `volts: 1 value not f64, read as null`.
- A column with a number that starts with a zero another digit follows
  (`02134`, `007`) stays text: a ZIP code or an ID. `0`, `0.5` and `-0.5` are
  numbers.
- JSON strings become dates or times, never numbers.
- With `--infer-types=off`, Polars types the columns from the rows it reads,
  and a value it cannot parse fails the read.

## Text and logs

A `.log` or `.txt` file, and text no format claims (a pipe, a file with no
extension), is a row per line.

```bash
printf 'start\nerror: disk full\n\nstop\n' > app.log
datui app.log
gzip -k app.log
datui app.log.gz
seq 1 1000 | datui
```

| Column | Holds |
|---|---|
| `file` | The file the line is from, when several are read as one table |
| `line` | The line as written, without its line ending |

- Every line is a row, blank lines included. `\r\n` is a line ending.
- `#` is on for text: it numbers each row by its line in the file, as
  `less -N` does, and the number stays with the row through a sort or a
  filter. With several files it is the line in the row's own file, beside the
  `file` column.
- Bytes that are not UTF-8 show as `�`; the Info panel counts the lines that
  hold them. Control characters are escaped on screen and kept in the value.
- A `.log` whose bytes say a format (a candump log, a FIX log) is read as that
  format.
- In a directory, text files beside other data are left out of its table: a
  README beside Parquet files is passed over.
- Find, the query and filters work on `line`: `select where line like "*error*"`.
- Lines are indexed in one pass and read where they are shown. A file over
  8 MiB shows its first rows at once and is indexed behind them: the footer
  says `lines` and how much is read, the row count waits for the last line, and
  End, <kbd>:</kbd> to a row past it, a sort or an analysis wait for it too.
  Home pauses the indexing until you are back. A file that shrinks meanwhile
  (a log rotated with copytruncate) stops it with a note: open it again. Past
  67,108,864 lines, the first that many show and the Info panel counts the rest.
- `--follow` reads lines as they are appended; see
  [Pipes and growing files](../user-guide/pipes-and-follow.md).
- `SYSTEMD_PAGER=datui journalctl -u nginx` makes datui journalctl's pager. For
  a column per journal field, read `journalctl -o json`: see
  [systemd journal](signals-and-logs.md#systemd-journal).
