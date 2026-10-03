# Binary formats

```bash
datui day.l2                           # a spec on the search path matches it
datui --format acme.l2feed capture.bin # read it as that spec
datui --spec l2feed.toml capture.bin   # read it with this spec file
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
| `[records] framing` | `fixed` (default), `length_prefixed`, `variant` or `sync`: see [records of different sizes](#records-of-different-sizes) |
| `[records] ring` | A ring buffer of fixed records: the oldest record's index, such as `"header.write_idx"`; rows start there and wrap |
| `[records] checksum` | A checksum in each record: see [checks](#checks) |
| `[[variants]]` | Record layouts a type field picks: see [variants](#variants) |
| `[footer]` | Fields at the end of the file: see [footer](#footer) |
| `[blocks]` | Records in blocks, each compressed on its own: see [blocks](#blocks) |
| `[sections.NAME]` | A part of the file, `offset` and `size` from the header, that `string_at` fields point into |
| `[capture]` | Records in the UDP payloads of a pcap or pcapng capture: see [captures](#captures) |
| `[files]` | A directory tree of the format's files as one table: see [a tree of files](#a-tree-of-files) |

## Field types

Type names follow [Kaitai Struct](https://kaitai.io). Widths are in bytes.

| Type | Value |
|---|---|
| `u1` to `u8`, `s1` to `s8` | Unsigned and signed integers of 1 to 8 bytes, `u3` and `s6` included |
| `f2`, `f4`, `f8` | Floats; `f2` is a half float |
| `bf2` | A bfloat16 |
| `vu`, `vs` | LEB128 varints, `vs` zigzag-encoded. Records only |
| `bool` | One byte, nonzero is true |
| `str` | Text of `size` bytes, its NUL and space padding trimmed |
| `strz` | Text up to a NUL, at most `size` bytes when given. Records only |
| `bytes` | Raw bytes of `size` |
| `pad` | `size` bytes skipped, no column |

A `le` or `be` suffix (`u4be`, `s2le`, `bf2be`) overrides the spec's `endian`.

## Field keys

| Key | Example | What it does |
|---|---|---|
| `name` | `"price"` | The column's name. Every field but `pad` has one |
| `size` | `8`, `"header.len"`, `"len"`, `"rest"` | Bytes of a `str`, `strz`, `bytes` or `pad`: a number, a header or footer field, an earlier field of the record, or `rest`, what is left of the record |
| `size_adjust` | `-4` | Added to a size read from a field |
| `encoding` | `"latin1"` | Of a `str` or `strz`: `utf8` (default), `latin1`, `utf16le` or `utf16be` |
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
| `offset` | `"header.px_off"` | In the columns layout, where the column starts in one file: see [columns layout](#columns-layout) |
| `lookup` | `{ file = "../sym", format = "lines" }` | An integer indexes a list of symbols in a file beside the data: a categorical. `format` is `lines` (default), `nul` or `str:N` |

These keys are for record fields only:

| Key | Example | What it does |
|---|---|---|
| `delta` | `true`, `"block"` | Each value is the change from the record before; the running sum is shown. `"block"` starts the sum again in each block |
| `bits` | `[{ name = "valid", bit = 0 }, { name = "mode", bit = 4, width = 3, enum = { 0 = "IDLE" } }]` | Bit fields of an integer, each its own column. A width of 1 is a bool |
| `group` | `{ count = "n_levels", fields = [...] }` | A counted run of items: a List of Structs. Takes no `type` |
| `string_at` | `"strings"` | An unsigned offset into a `[sections.strings]` part of the file, where NUL-terminated text is |

A field takes at most one of `time` (or `date`), `scale`, `factor` and `enum`.

A field refers to an earlier one by name, never by an expression. In the header,
`size = "len"` reads an earlier header field; anywhere, `header.NAME` and
`footer.NAME` do. In a record, `"len"` reads an earlier field of the same
record. A record whose own field gives its size is `length_prefixed`.

## Records of different sizes

```toml
[records]
framing = "length_prefixed"
size = "len"          # the field that holds each record's length
size_adjust = 2       # the length leaves out its own two bytes
fields = [{ name = "len", type = "u2" }, { name = "msg", type = "str", size = "rest" }]
```

| `framing` | How one record is told from the next |
|---|---|
| `fixed` | Every record takes `size`, or what its fields take |
| `length_prefixed` | A field of the record gives its size: `size = "len"`, with `size_adjust` |
| `variant` | The variant the type field picks gives the size: its `size`, or what its fields take |
| `sync` | Each record starts with the `sync` marker (`"1ACFFC1D"`, `"0xEB90"` or a list of bytes); bytes between records are skipped and counted in a note |

| Key | What it does |
|---|---|
| `length_suffix` | `true`: the length is written again after the record, as Fortran unformatted files do. Needs `size = "len"` |
| `align` | Each record starts at a multiple of this (1 to 65536), counted from the first: `align = 2` for IFF and RIFF chunks |

Records that are not all one size are walked once when the file opens, and the
start of every 1024th is kept, so a scroll anywhere reads from the nearest one.

### Variants

```toml
[records]
framing = "length_prefixed"
size = "len"
size_adjust = 2
fields = [{ name = "len", type = "u2" }, { name = "kind", type = "str", size = 1 }]
type = "kind"

[[variants]]
name = "add"
when = "A"
fields = [{ name = "ref", type = "u8" }, { name = "shares", type = "u4" }, { name = "price", type = "u4", scale = 4 }]

[[variants]]
name = "exec"
when = ["E", "C"]
fields = [{ name = "ref", type = "u8" }, { name = "shares", type = "u4" }]
```

`fields` (or `[records.common] fields`) are the fields every record starts with.
`type` names the common field that picks the variant: an integer, or text,
compared with its padding trimmed. `type = { field = "kind", type = "u1" }`
declares it in place.

| Variant key | What it says |
|---|---|
| `name` | Shown in the `type` column |
| `when` | The type value, or a list of them, that picks it |
| `fields` | The fields after the common ones |
| `size`, `size_adjust` | The whole record's size, when more than its fields take |

All records make one table, with a `type` column naming each one's variant. A
column of a field one variant lacks is null in that variant's rows; a field two
variants share is one column, so it must be the same field in both. A record of
a type no variant names shows as `?X` when its size is known
(`length_prefixed`); otherwise the read stops there, with a note.

`datui --variant add capture.bin` opens one variant as its own table: only its
records and its columns.

## Footer

```toml
[records]
count = "footer.n"
fields = [{ name = "v", type = "u2" }]

[footer]
fields = [{ name = "n", type = "u4" }, { name = "crc", type = "u4" }]
checksum = { algo = "crc32", field = "crc" }
```

The footer is read from the end of the file, so its fields' sizes are written in
the spec, or it gives `size`. Later parts refer to its fields as `footer.NAME`:
a record count, or a block index's offset. `checksum` checks the bytes before
the footer against a footer field; a mismatch is a note.

## Blocks

```toml
[blocks]
header = [{ name = "clen", type = "u4" }, { name = "rawlen", type = "u4" }]
size = "clen"
compression = "zstd"
uncompressed = "rawlen"

[records]
fields = [{ name = "v", type = "u4", delta = "block" }]
```

The data after the file's header is a run of blocks: a block header, then
`size` bytes of records. The records' framing applies inside each block.

| Key | What it says |
|---|---|
| `header` | The fields at the start of each block |
| `size`, `size_adjust` | The bytes after the block header: a number or a block header field |
| `compression` | `none` (default), `gzip`, `deflate`, `zlib`, `zstd`, `lz4`, `lz4_block`, `snappy`, `snappy_framed`, `brotli`, `bzip2` or `xz`. Or a code in the block header: `{ field = "codec", values = { 0 = "none", 1 = "zstd" } }` |
| `uncompressed` | The block header field with the decompressed size. Required for `lz4_block` |
| `records` | The block header field counting its records; missing records are null |
| `index` | `{ at = "footer.index_off", count = "footer.n_blocks", fields = [...] }`: a block index read instead of walking the blocks. Its entries need an integer `offset`, and may give `rows` |

Only the block headers are read when the file opens. A block is decompressed
when its rows are first read, up to 256 MiB each, and the last few are kept. A
block that will not decompress is left out, with a note.

## Captures

```toml
[capture]
header = [{ name = "session", type = "str", size = 10 }, { name = "seq", type = "u8" }, { name = "count", type = "u2" }]
count = "count"
time = "captured"
```

The file is a pcap or pcapng capture, told apart by its magic. Each UDP
payload holds the records, after the payload `header`; `count` names the
header field counting them, and `time` adds a column with each packet's capture
time. Packets that are not UDP are left out and counted in a note. A capture
spec has no `[header]`, `[footer]` or `[blocks]`.

## A tree of files

```toml
[files]
path = "{date:%Y%m%d}/{venue}/trades.bin"
```

`datui --format acme.trades store/` reads every file under `store/` that the
pattern matches as one table, with a column for each part: a date for a part
with a format, text otherwise. A file whose part does not parse as its date is
left out, with a note.

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
spec matches opens as it does without specs; a local file no reader takes
either opens in the [hex view](hex-view.md), where <kbd>B</kbd> reads it with a
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
| A record whose length or type cannot be read | The records before it open; a note says where the rest was left out |
| `checksum` in `[records]` | A `checksum_ok` column, true or false for each record, rather than an error |
| `--spec` or `--format NAME` on an `s3://`, `gs://` or Azure path | Refused: specs read local files, so download it first |

```toml
checksum = { algo = "crc16-ccitt", field = "crc", from = "len", to = "crc" }
```

A record checksum covers the bytes from the `from` field (default: the record's
start) up to the `to` field (default: the checksum's own field). `algo` is
`crc16-ccitt`, `crc16-xmodem`, `crc16-modbus`, `crc16-arc`, `crc32`, `crc32c`,
`sum8` or `xor8`; a footer checksum takes the same names.

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

When the header lists where each column starts in one file, every field gives
`offset` and the spec reads that file:

```toml
layout = "columns"
[header]
fields = [{ name = "n", type = "u4" }, { name = "px_off", type = "u4" }]
[records]
count = "header.n"
fields = [{ name = "px", type = "f8", offset = "header.px_off" }]
```

## Compressed files

`day.l2.zst`, `.gz`, `.bz2` and `.xz` are decompressed to a temporary file before
they are read. The glob matches the name without the compression suffix, and
magic is read from the decompressed bytes.

## Large files

Read: [lazy, or converted once when compressed](loading-data.md#how-each-format-is-read).

A file is memory-mapped, and only the columns and rows on screen are decoded.
Records that are not all one size, and blocks, are indexed by one pass when the
file opens.
Scrolling to the last row of a gigabyte file reads only the rows shown. A sort,
filter, query, chart or analysis reads every row of the columns it uses, a batch
at a time on the streaming engine (`[performance] polars_streaming`, on by
default).
A table holds at most 4,294,967,295 rows; records past that are not shown, and
the dataset's notes say so.

A file that grows while it is open keeps the rows it had; open it again to read
the rest. A file cut short by another program is refused at the next read
rather than read past its end.
