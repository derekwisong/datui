# Delimited text

## CSV options

Read: [lazy, or converted once when compressed](index.md#how-each-format-is-read).

They apply to `.tsv` and `.psv` files too. A directory of CSVs in a bucket takes
all of them except `--infer-types` and `--header-rows`, and
`--skip-initial-space` only removes the padding there: values stay text.

| Option | Config key | What it does |
|---|---|---|
| `--delimiter ';'` | | Column separator: one character, `tab`, `\t` or a code such as `0x1f`. Default `,` for `.csv`, tab for `.tsv`, `\|` for `.psv` |
| `--no-header` | | The first row is data, not names. <kbd>H</kbd> does the same, or undoes it, on the file on screen |
| `--skip-lines N`, `--skip-rows N` | | Ignore a preamble |
| `--footer-rows N` | | Ignore a footer. Counts every row first, and the loading screen says `Counting rows to skip the footer` meanwhile: on a directory in a bucket, that downloads every file before the table opens |
| `--null NA`, `--null amount=` | `csv.null_values` | Values to read as null, for every column or one (`COL=VAL`, the name as shown). Repeatable; replaces the config's list |
| `--comment '#'` | `csv.comment` | Skip lines that start with it, before the header and among the data. The header is the first line that is not a comment |
| `--header-rows 3`, `--header-rows 3,2` | `csv.header_join` | The line or lines holding the header, counted from 1 at the top of the file. Several are joined per column, in the order given, with `header_join` (default a space) |
| `--skip-initial-space` | `csv.skip_initial_space` | Ignore the spaces after a delimiter: padded numbers are numbers and a cell of spaces is null |
| `--infer-rows 10000` | `csv.infer_rows` | Rows used to infer column types (default 1000). Raise it when a column turns from integer to text late in the file |
| `--ignore-errors` | `csv.ignore_errors` | Skip rows that fail to parse instead of failing the load |
| `--infer-types=COL`, `--infer-types=off` | `read.infer_types` | Trim and type-infer string columns, dates included; limit it to named columns, or turn it off |

Column names are always trimmed: `"     Latitude"` reads as `Latitude`. A blank
name reads as `column_N`, and a repeated one gets `_duplicated_0`.

## Instrument and logger exports

Loggers often write comments and a units line above a padded header:

```
#device_info, log_version="1.03", model="X", serial="123"
#yyyy-mm-dd, hh:mm:ss, hh:mm, degrees, volts, deg F
  Lcl Date, Lcl Time, UTCOfst,     Latitude, bus1volts, E1 CHT1
          ,         ,        ,             ,      25.1,   187.2
```

| Command | Columns |
|---|---|
| `datui --comment '#' log.csv` | `Lcl Date`, `Latitude`, …; `#` lines anywhere are skipped |
| `datui --comment '#' --header-rows 3,2 log.csv` | `Lcl Date yyyy-mm-dd`, `Latitude degrees`, … |
| `datui --header-rows 3 log.csv` | `Lcl Date`, `Latitude`, …; lines 1 and 2 are passed over |

`--header-rows` counts lines before anything is skipped, and a named line that
starts with the comment character loses it. `--skip-lines` counts from the same
top; `--skip-rows` counts data rows after the header. <kbd>H</kbd> reads the
named lines as data. A file with nothing after its header lines opens with its
columns and no rows.

A [delimited format spec](format-specs.md#delimited-text) holds these options
for a family of files, with their units and metadata line, so they open with no
flags. A flag typed on the command line still wins over the spec.

Padded numbers become numbers with or without `--skip-initial-space`, as long
as string parsing is on (the default). With the flag, cells of spaces are null
in text columns too, and `--null` matches the value without its padding.
Typing follows `--infer-types`: with `--infer-types=off` the padding goes but
the columns stay text, and `--infer-types=COL` types only the columns named.

## Dates and timestamps

String columns in CSV and JSON become dates when every value in the first 1000
rows (`--infer-rows`) parses the same way. `--infer-types=off` turns this off.

| Value | Type |
|---|---|
| `2024-01-31` | `date` |
| `2024-01-31 10:00:00`, `2024-01-31T10:00:00.250` | `datetime[μs]` |
| `2024-01-31T10:00:00Z`, `2024-01-31T10:00:00.250+00:00`, `2024-01-31 05:00:00-05:00` | `datetime[μs, UTC]`, converted to UTC |

A column whose values disagree, such as an offset on some and none on others,
stays text. A value past those rows that does not parse is null. JSON strings
become dates or times, never numbers.

With `--infer-types=off`, Polars decides from the rows it reads for the schema,
and a value it cannot parse fails the read. A directory of CSV or NDJSON files
in a bucket keeps them as text.

## Text and logs

A `.log` or `.txt` file, and text no format claims (a pipe, a file with no
extension), is read as lines:

```bash
datui app.log
datui app.log.gz
datui /var/log/nginx/                         # several files: a file column first
journalctl -u nginx | datui
SYSTEMD_PAGER=datui journalctl -u nginx
```

| Column | What |
|---|---|
| `file` | The file the line is from, when there are several |
| `line_no` | Its number, from 1, as `less -N` numbers it |
| `line` | The line as written, without its line ending |

- Every line is a row, blank lines included. `\r\n` is a line ending.
- Bytes that are not UTF-8 are shown as `�`; the Info panel says how many lines
  hold them. Control characters are escaped on screen and kept in the value.
- A `.log` whose bytes say a format (a candump log, a FIX log) is read as that format.
- In a directory, text files beside other data are not part of the table: a
  README beside Parquet files is passed over.
- Find, the query and filters work on `line` as on any column:
  `select where line like "*error*"`.
- Lines are indexed in one pass and read where they are shown. A file of more
  than 67,108,864 lines shows the first that many; the Info panel says how many
  were left out.
