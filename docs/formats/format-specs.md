# Format specs

```bash
datui day.l2                           # a spec on the search path matches it
datui --format acme.l2feed capture.bin # read it as that spec
datui --format ./l2feed.toml capture.bin  # read it with this spec file
datui --format s3://team/l2feed.toml day.bin  # or a spec at a URL
datui formats                          # list the specs datui finds
datui formats check acme.l2feed day.l2 # check a spec and print a file's first rows
```

A file of records, such as a tick capture, a sensor log or a struct dump, opens
as a table once a spec describes it: fixed-size records, records that carry
their length, several message types in one stream, or compressed blocks. So
does a family of CSV-like text files with lines above their header: see
[delimited text](#delimited-text).
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
| `endian` | `le` (default), `be`, or `auto`: big-endian when the magic (at least two bytes) reads reversed. For fields without their own suffix |
| `layout` | `rows` (default): one file of records. `columns`: a directory with one file per field |
| `[header] fields` | Fields read once from the start of the file. Later parts refer to them |
| `[header] size` | The header's size when it is more than its fields; a number or a header field |
| `[records] fields` | The fields of one record, in order |
| `[records] size` | The record's size, at least the fields' sum (the default); the rest is skipped |
| `[records] count` | How many records there are, such as `"header.count"` or `"footer.n"` |
| `[records] framing` | `fixed` (default), `length_prefixed`, `variant` or `sync`: see [records of different sizes](../reference/format-specs.md#records-of-different-sizes) |
| `[records] ring` | A ring buffer of fixed records: the oldest record's index, such as `"header.write_idx"`; rows start there and wrap |
| `[records] checksum` | A checksum in each record: see [checks](#checks) |
| `[[variants]]` | Record layouts a type field picks: see [variants](../reference/format-specs.md#variants) |
| `[footer]` | Fields at the end of the file: see [footer](../reference/format-specs.md#footer) |
| `[blocks]` | Records in blocks, each compressed on its own: see [blocks](../reference/format-specs.md#blocks) |
| `[sections.NAME]` | A part of the file, `offset` and `size` from the header, that `string_at` fields point into |
| `[capture]` | Records in the UDP payloads of a pcap or pcapng capture: see [captures](../reference/format-specs.md#captures) |
| `[files]` | A directory tree of the format's files as one table: see [a tree of files](../reference/format-specs.md#a-tree-of-files) |

## Where specs live

Datui reads every `*.toml` in these places, in order. The first spec of each name
wins, as with `PATH`.

| Place | |
|---|---|
| `~/.config/datui/formats/` | Your own specs |
| `$DATUI_FORMATS_PATH` | Directories separated by `:` (`;` on Windows), such as a checked-out repository of a team's specs |
| `[formats] path` in [config](../reference/settings.md#formats) | More directories. Lists add up across imported config files |

`datui formats` lists each spec, what it matches, the file it came from, any copy
of the same name it overrides, and the files that could not be read, with the
line and column of each problem. The same places hold
[FIX log dictionaries](signals-and-logs.md#fix-log-dictionaries): QuickFIX XML files and TOML
files of `kind = "fix"`, listed after the specs.

`datui formats check SPEC [FILE]` checks one spec, by name or file. With a file,
it prints the warnings and the first ten rows. Given a QuickFIX dictionary, it checks
that, and with a FIX log says how many messages it matches and which of its tags
they hold. It exits non-zero on an error, so
a repository of specs can run it in CI.

## Which spec reads a file

| First that applies | |
|---|---|
| `--format FILE`: a path (it has a `/` or ends `.toml`), or an `http(s)://`, `s3://`, `gs://` or `az://` URL fetched once as the open starts | That spec, whatever the file is called |
| `--format NAME` | The spec of that name |
| A name datui already reads (`.csv`, `.parquet`) | Read as it is, as before, unless a [delimited spec](#delimited-text) matches a `.csv`, `.tsv` or `.psv` |
| A `glob` matches | That spec |
| A `magic` matches, in a file whose bytes are no format datui reads (such as Parquet) | That spec |

A spec with `match.where` matches only a file whose header holds those values,
so one spec per version can share a glob and a magic:

```toml
match = { glob = "*.l2", magic = "L2FD", where = { "header.version" = 3 } }
```

A spec file is at most 1 MiB.

When two specs match the same way, the first on the search path reads the file.
The bar shows `2 formats match`, and the Notes tab names the others. A file no
spec matches opens as it does without specs; a local file no reader takes
either opens in the [hex view](../user-guide/hex-view.md), where <kbd>B</kbd> reads it with a
spec and <kbd>r</kbd> lines the bytes up in records while you write one.

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

A spec of `kind = "delimited"` holds the [CSV options](delimited-text.md#csv-options)
for such a family of files, so they open with no flags: from the command line,
from the home screen, compressed, or as a directory. It is a `[csv]` block of
the [config](../reference/settings.md#csv), with the same keys, plus `match`,
`kind`, the layout keys and `[columns]`.

```toml
name = "acme.instrument-log"
kind = "delimited"
match = { magic = "#device_info" }       # or glob = ["**/logs/log_*.csv"]

comment = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }     # a list, such as [3] or [3, 2], also works
metadata_line = 1

[columns]
time = { from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }
```

| Key | What it says |
|---|---|
| `kind` | `delimited`. Default `binary` |
| `match` | `glob` and `magic`, as for other format specs. `magic` compares the start of the first line |
| `delimiter` | One character, `"tab"`, `"\t"` or a code such as `"0x1f"`, as `--delimiter` takes. Default `,`, or the one the file's name implies |
| `comment` | Lines that start with it are skipped wherever they are |
| `skip_initial_space` | `true`: ignore the spaces after a delimiter |
| `header_rows` | `{ name = N, unit = M }`: the line that names the columns and the line that gives their units. `name` may be a list of lines, joined with `header_join` (default a space). A number or a list is `name` alone. A header line is never data |
| `header_join` | What joins the pieces of a name from several lines |
| `metadata_line` | A line of `key="value"` or `key=value` pairs, separated by commas, for the Info panel. It must not be data: above the last header line, within `skip_lines`, or a comment line |
| `null_values` | A value, or a list, read as null: `"NA"`, or `"COL=-999"` for one column |
| `skip_lines` | Lines to pass over before the header |
| `[columns]` | Derived columns, below |

Lines count from 1 at the top of the file. Each option the spec sets replaces
the config's; a flag typed on the command line (`--delimiter`,
`--comment`, `--skip-initial-space`, `--header-rows`, `--skip-lines`)
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
[query](../user-guide/querying-data.md).

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
| A record whose length or type cannot be read | The records before it open; a note says where the rest was left out |
| `checksum` in `[records]` | A `checksum_ok` column, true or false for each record, rather than an error |
| `--format` with a spec on an `s3://`, `gs://` or Azure path | Refused: specs read local files, so download it first |

```toml
checksum = { algo = "crc16-ccitt", field = "crc", from = "len", to = "crc" }
```

A record checksum covers the bytes from the `from` field (default: the record's
start) up to the `to` field (default: the checksum's own field). `algo` is
`crc16-ccitt`, `crc16-xmodem`, `crc16-modbus`, `crc16-arc`, `crc32`, `crc32c`,
`sum8` or `xor8`; a footer checksum takes the same names.

## Compressed files

`day.l2.zst`, `.gz`, `.bz2` and `.xz` are decompressed to a temporary file before
they are read: the loading screen says `Decompressing`, then `Reading records`,
and Esc stops either. The glob matches the name without the compression suffix,
and magic is read from the decompressed bytes.

## Large files

Read: [lazy, or converted once when compressed](index.md#how-each-format-is-read).

A file is memory-mapped, and only the columns and rows on screen are decoded.
Records that are not all one size, and blocks, are indexed by one pass when the
file opens. That pass keeps where each record starts (5 bytes a record, up to
64M records), so a query reads every column from there rather than walking the
records again, and a file opened again with the same spec is not walked again.
Scrolling to the last row of a gigabyte file reads only the rows shown. A sort,
filter, query, chart or analysis reads every row of the columns it uses, a batch
at a time on the streaming engine (`[performance] streaming`, on by
default).
A table holds at most 4,294,967,295 rows; records past that are not shown, and
the dataset's notes say so.

A file that grows while it is open keeps the rows it had; open it again to read
the rest. A file cut short by another program is refused at the next read
rather than read past its end.
