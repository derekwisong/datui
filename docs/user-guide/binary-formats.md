# Binary formats

```bash
datui day.l2                           # a spec on the search path matches it
datui --format acme.l2feed capture.bin # read it as that spec
datui --spec l2feed.toml capture.bin   # read it with this spec file
datui formats                          # list the specs datui finds
datui formats check acme.l2feed day.l2 # check a spec and print a file's first rows
```

A file of fixed-size records, such as a tick capture, a sensor log or a struct
dump, opens as a table once a spec describes it. So does a family of CSV-like
text files with lines above their header: see [delimited text](#delimited-text).
A spec is one TOML file per format. Specs are data: no scripts or expressions.
Every size read from a file is bounded.

## A spec

```toml
name = "acme.l2feed"
description = "Level 2 capture"
match = { glob = ["*.l2"], magic = "L2FD" }
endian = "le"

[header]
fields = [{ name = "magic", type = "str", size = 4 }, { name = "count", type = "u8" }]

[records]
count = "header.count"
fields = [
  { name = "ts",     type = "u8", time = "ns" },
  { name = "symbol", type = "str", size = 8 },
  { name = "side",   type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
  { name = "price",  type = "u4", scale = 4, null = "max" },
]
```

The table has the columns `ts` (datetime), `symbol`, `side` and `price` (a
decimal with four places), one row per record.

| Key | What it says |
|---|---|
| `name` | The format's name, namespaced: `acme.l2feed`. `--format` takes it |
| `description` | Shown by `datui formats` |
| `match` | Which files are this format: `glob` (a pattern or a list), `magic` (a string or a list of bytes) at `magic_offset` (default 0), `where` (header values) |
| `endian` | `le` (default) or `be`, for fields without their own suffix |
| `layout` | `rows` (default): one file of records. `columns`: a directory with one file per field |
| `[header] fields` | Fields read once from the start of the file. Later parts refer to them |
| `[header] size` | The header's size when it is more than its fields; a number or a header field |
| `[records] fields` | The fields of one record, in order |
| `[records] size` | The record's size, at least the fields' sum (the default); the rest is skipped |
| `[records] count` | How many records there are, such as `"header.count"` |
| `[records] framing` | `fixed` (default). Other framings are not yet supported |

## Field types

Type names follow [Kaitai Struct](https://kaitai.io). Widths are in bytes.

| Type | Value |
|---|---|
| `u1` to `u8`, `s1` to `s8` | Unsigned and signed integers of 1 to 8 bytes, `u3` and `s6` included |
| `f4`, `f8` | Floats |
| `bool` | One byte, nonzero is true |
| `str` | Text of `size` bytes, its NUL and space padding trimmed |
| `bytes` | Raw bytes of `size` |
| `pad` | `size` bytes skipped, no column |

A `le` or `be` suffix (`u4be`, `s2le`) overrides the spec's `endian`.

## Field keys

| Key | Example | What it does |
|---|---|---|
| `name` | `"price"` | The column's name. Every field but `pad` has one |
| `size` | `8`, `"header.len"` | Bytes of a `str`, `bytes` or `pad`: a number or a header field |
| `size_adjust` | `-4` | Added to a size read from a field |
| `count` | `10` | That many values side by side: one Array column |
| `flatten` | `true` | With `count` (up to 1024), columns `name_0` to `name_9` instead of an Array |
| `null` | `"min"`, `"max"`, `"nan"`, `-1` | A stored value that means no value: the type's smallest or largest value, a NaN, or this number, which the type must be able to hold |
| `scale` | `4` | Implied decimal places: the integer becomes a Decimal |
| `factor`, `offset` | `0.1`, `-40.0` | `value * factor + offset`, as a float |
| `enum` | `{ 1 = "BUY", 2 = "SELL" }` | Codes and labels; an unlisted code reads as its number |
| `time` | `"ns"` | A count of `days`, `s`, `ms`, `us` or `ns`: a datetime, or a date for `days`. A float counts fractions too |
| `epoch` | `2000-01-01` | What the count is since. Default 1970-01-01 |
| `date` | `"yyyymmdd"` | An integer such as 20240102, as a date |
| `of_day` | `true` | With `time`, a count since midnight: a time of day |
| `date` with `of_day` | `"header.trade_date"` | The day those times are on, from a header field that reads as a date (`date = "yyyymmdd"`, `time = "days"`), a datetime, or text such as `2024-01-02`: a datetime |
| `file` | `"px.dat"` | In the columns layout, the file in the directory holding the field. Default: its name |

A field takes at most one of `time` (or `date`), `scale`, `factor` and `enum`.

A field refers to an earlier one by name, never by an expression. In the header,
`size = "len"` reads an earlier header field; anywhere, `header.NAME` does. A
record's own fields cannot size it: that is `length_prefixed` framing, which is
not yet supported.

## Where specs live

Datui reads every `*.toml` in these places, in order. The first spec of each name
wins, as with `PATH`.

| Place | |
|---|---|
| `~/.config/datui/formats/` | Your own specs |
| `$DATUI_FORMATS_PATH` | Directories separated by `:` (`;` on Windows), such as a checked-out repository of a team's specs |
| `formats_path` in [config](../reference/settings.md#binary-formats) | More directories. Lists add up across imported config files |

`datui formats` lists each spec, what it matches, the file it came from, any copy
of the same name it overrides, and the files that could not be read, with the
line and column of each problem. The same places hold
[FIX dictionaries](loading-data.md#fix-dictionaries): QuickFIX XML files and TOML
files of `kind = "fix"`, listed after the specs.

`datui formats check SPEC [FILE]` checks one spec, by name or file. With a file,
it prints the warnings and the first ten rows. Given a FIX dictionary, it checks
that, and with a FIX log says how many messages it matches and which of its tags
they hold. It exits non-zero on an error, so
a repository of specs can run it in CI.

## Which spec reads a file

| First that applies | |
|---|---|
| `--spec FILE` | That spec, whatever the file is called |
| `--format NAME` | The spec of that name |
| A name datui already reads (`.csv`, `.parquet`) | Read as it is, as before, unless a [delimited spec](#delimited-text) matches a `.csv`, `.tsv` or `.psv` |
| A `glob` matches | That spec |
| A `magic` matches, in a file whose bytes are no format datui reads (such as Parquet) | That spec |

A spec with `match.where` matches only a file whose header holds those values,
so one spec per version can share a glob and a magic:

```toml
match = { glob = "*.l2", magic = "L2FD", where = { "header.version" = 3 } }
```

When two specs match the same way, the first on the search path reads the file.
The bar shows `2 formats match`, and the Notes tab names the others. A file no
spec matches opens as it does without specs.

<kbd>b</kbd> on the table picks another spec and reads the file again with it.
The Notes tab of <kbd>i</kbd> says which spec read the file and why, the header's
values, and any bytes left out.

## Delimited text

```bash
datui flight.csv                                 # a delimited spec's magic matches
datui logs/                                      # a directory of them, as one table
datui formats check acme.instrument-log flight.csv
```

Loggers and instruments write a metadata line and a units line above a padded
header:

```
#device_info, log_version="1.03", model="Unit 7, rev B", serial="123"
#yyyy-mm-dd, hh:mm:ss, hh:mm, degrees, volts, deg F
  Lcl Date, Lcl Time, UTCOfst,     Latitude, bus1volts, T1 Temp
          ,         ,        ,             ,      25.1,   187.2
2024-03-01, 10:00:00,  -05:00,    40.100000,      25.0,   180.0
```

A spec of `kind = "delimited"` holds the [CSV options](loading-data.md#csv-options)
for such a family of files, so they open with no flags: from the command line,
from the home screen, compressed, or as a directory.

```toml
name = "acme.instrument-log"
kind = "delimited"
match = { magic = "#device_info" }       # or glob = ["**/logs/log_*.csv"]

comment_char = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }     # a list, such as [3] or [3, 2], also works
metadata_line = 1

[columns]
time = { from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }
```

| Key | What it says |
|---|---|
| `kind` | `delimited`. Default `binary` |
| `match` | `glob` and `magic`, as for binary specs. `magic` compares the start of the first line |
| `delimiter` | One character, such as `";"` or `"\t"`. Default `,`, or the one the file's name implies |
| `comment_char` | Lines that start with it are skipped wherever they are |
| `skip_initial_space` | `true`: ignore the spaces after a delimiter |
| `header_rows` | `{ name = N, unit = M }`: the line that names the columns and the line that gives their units. `name` may be a list of lines, joined with `header_join` (default a space). A number or a list is `name` alone. A header line is never data |
| `header_join` | What joins the pieces of a name from several lines |
| `metadata_line` | A line of `key="value"` or `key=value` pairs, separated by commas, for the Info panel. It must not be data: above the last header line, within `skip_lines`, or a comment line |
| `null_value` | A value, or a list, read as null: `"NA"`, or `"COL=-999"` for one column |
| `skip_lines` | Lines to pass over before the header |
| `[columns]` | Derived columns, below |

Lines count from 1 at the top of the file. Each option the spec sets replaces
the config's; a flag typed on the command line (`--delimiter`,
`--comment-char`, `--skip-initial-space`, `--header-rows`, `--skip-lines`)
wins over the spec. The options the spec does not set keep theirs.
`datui --delimiter 44 formats check SPEC FILE` reads the file as an open with
those flags would, and names the flags that override the spec. The header lines
and the metadata line are the only lines read apart from the CSV reader.

### Units

A unit sits beside its column's type on the table's type row
(`f64 · deg F`), in a **Unit** column on the Info panel's Schema tab, and in
chart axis titles (`T1 Temp (deg F)`). A filter, sort or drill keeps them, and
so does a query, pivot or melt for each column it carries unchanged, renamed or
not. A column a query computes has no unit, even under the name of one that had.

### Metadata

The Info panel's **Metadata** tab lists the metadata line's pairs, under its
leading item when it has one (`device_info`). A line that is not pairs is shown
as it is. For a directory, the first file's line is shown.

### Derived columns

| `as` | `from` | Column |
|---|---|---|
| `datetime` | a date and a time, and optionally a UTC offset such as `-05:00`, `+0530` or `-5`; or one column of text | A datetime. With an offset it is in UTC |
| `date` | one column | A date |
| `time` | one column | A time of day |

`format = "%Y-%m-%d %H:%M:%S"` gives the strftime format of the text, a date
and a time joined with a space; without it the format is inferred. A value that
does not parse is null. The column goes before the first column it is made
from, which stays; one named after a column it is made from replaces that
column, and its unit. There is no expression language: anything more is a
[query](querying-data.md).

### Matching

A delimited spec matches a file whose name says no format datui reads, or says
`.csv`, `.tsv` or `.psv`, compressed or not. A directory is read through the
spec its first file matches. <kbd>H</kbd> reads the file without a header, and
without its derived columns.

## Checks

| Problem | What happens |
|---|---|
| A field, size or key the spec gets wrong | The spec is refused, with its line and column |
| The magic does not match | The open fails, showing the bytes found |
| A file that ends partway through a record | The whole records open; a note shows the bytes left over |
| A header count larger than the file | The whole records open, with a note |
| `--spec` or `--format NAME` on an `s3://`, `gs://` or Azure path | Refused: specs read local files, so download it first |

## Columns layout

```toml
name = "kdb.trades"
layout = "columns"
endian = "be"

[records]
fields = [{ name = "price", type = "f8" }, { name = "size", type = "s8" }]
```

`datui --format kdb.trades db/trades/` reads `db/trades/price` and
`db/trades/size` as two columns of one table, as kdb+ splays a table. A
`[header]` describes the start of each file. A glob in a columns spec matches
the directory.

## Compressed files

`day.l2.zst`, `.gz`, `.bz2` and `.xz` are decompressed to a temporary file before
they are read. The glob matches the name without the compression suffix, and
magic is read from the decompressed bytes.

## Large files

Read: [lazy, or converted once when compressed](loading-data.md#how-each-format-is-read).

A file is memory-mapped, and only the columns and rows on screen are decoded.
Scrolling to the last row of a gigabyte file reads only the rows shown. A sort,
filter, query, chart or analysis reads every row of the columns it uses, a batch
at a time on the streaming engine (`[performance] polars_streaming`, on by
default).
A table holds at most 4,294,967,295 rows; records past that are not shown, and
the dataset's notes say so.

A file that grows while it is open keeps the rows it had; open it again to read
the rest. A file cut short by another program is refused at the next read
rather than read past its end.
