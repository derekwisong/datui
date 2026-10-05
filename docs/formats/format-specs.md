# Format specs

A format spec is a TOML file that describes a binary format, or a family of
delimited text files, so datui opens it as a table.

**`l2feed.toml`**

```toml,file=l2feed.toml
name = "acme.l2feed"
description = "Level 2 capture"
match = { glob = ["*.l2"], magic = "L2FD" }
endian = "le"

[header]
fields = [{ name = "magic", type = "str", size = 4 }, { name = "count", type = "u1" }]

[records]
count = "header.count"
fields = [
  { name = "ts",     type = "u8", time = "ns" },
  { name = "symbol", type = "str", size = 8 },
  { name = "side",   type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
  { name = "price",  type = "u4", scale = 4, null = "max" },
]
```

**`make_day_l2.py`** writes `day.l2`, a file in that format:

```python,file=make_day_l2.py
import ctypes


class Header(ctypes.LittleEndianStructure):
    _layout_ = "ms"
    _pack_ = 1  # no padding between fields
    _fields_ = [
        ("magic", ctypes.c_char * 4),
        ("count", ctypes.c_uint8),
    ]


class Record(ctypes.LittleEndianStructure):
    _layout_ = "ms"
    _pack_ = 1
    _fields_ = [
        ("ts", ctypes.c_uint64),  # nanoseconds since 1970
        ("symbol", ctypes.c_char * 8),
        ("side", ctypes.c_uint8),  # 1 BUY, 2 SELL
        ("price", ctypes.c_uint32),  # in ten-thousandths; the largest value is null
    ]


records = [
    Record(1709294400000000000, b"MSFT", 1, 4105000),
    Record(1709294400000500000, b"AAPL", 2, 0xFFFFFFFF),
]
with open("day.l2", "wb") as f:
    f.write(bytes(Header(b"L2FD", len(records))))
    for record in records:
        f.write(bytes(record))
```

```bash
python3 make_day_l2.py
datui formats check ./l2feed.toml day.l2
datui --format ./l2feed.toml day.l2
mkdir -p formats
cp l2feed.toml formats/
DATUI_FORMATS_PATH=formats datui day.l2
DATUI_FORMATS_PATH=formats datui formats
```

| Command | Does |
|---|---|
| `datui formats check ./l2feed.toml day.l2` | Checks the spec and prints the file's header and first rows |
| `datui --format ./l2feed.toml day.l2` | Reads the file with that spec file |
| `datui --format acme.l2feed day.l2` | Reads it with the spec of that name, from the search path |
| `datui day.l2` | Reads it with the spec whose `match` it fits, from the search path |
| `datui formats` | Lists the specs and dictionaries on the search path |

A spec reads fixed-size records, records that carry their length, several
message types in one stream, or compressed blocks; a `kind = "delimited"` spec
reads [CSV-like text with lines above its header](#delimited-text). Specs are
data: no scripts or expressions, and every size read from a file is bounded.
`--format` also takes a spec's `http(s)://`, `s3://`, `gs://` or `az://` URL,
fetched once as the open starts; a spec file is at most 1 MiB.

## A spec

`l2feed.toml` above is a whole spec.
Its table has the columns `ts` (a datetime), `symbol`, `side` and `price` (a
decimal with four places, null where the field holds its largest value), one
row per record. [Format spec reference](../reference/format-specs.md) lists
every field type and key.

| Key | What it says |
|---|---|
| `name` | The format's name, namespaced: `acme.l2feed`. `--format` takes it |
| `description` | Shown by `datui formats` and the [Documentation view](../reference/format-specs.md#documentation) |
| `documentation` | An `https://` link to the format's own documentation, for the Documentation view |
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

`datui formats` lists each spec, what it matches (`magic L2FD · version 3 · glob
*.l2`, or `no match`), the file it came from, any copy of the same name it
overrides, and the files that could not be read, with the
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
so one spec per version can share a glob and a magic. In a spec, in place of
its `match` line:

```toml,template
match = { glob = "*.l2", magic = "L2FD", where = { "header.version" = 3 } }
```


When two specs match the same way, the first on the search path reads the file.
The bar shows `2 formats match`, and the Notes tab names the others. A file no
spec matches opens as it does without specs; a local file no reader takes
either opens in the [hex view](../user-guide/hex-view.md), where <kbd>B</kbd> reads it with a
spec and <kbd>r</kbd> lines the bytes up in records while you write one.

<kbd>b</kbd> on the table picks another spec and reads the file again with it,
clearing the query, filters and sort. The list starts with the spec the file
was read with, then the others that matched it the same way, then every other
spec on the search path for a file (or for a directory of column files).
The Notes tab of <kbd>i</kbd> says which spec read the file and why (`matched by
magic L2FD · version 3`), the header's values, and any bytes left out.

The home screen and its search name a file the same way. A file whose name
says nothing (no extension, or `.bin`) is matched by magic and `where` against
the first 4 KiB the listing reads from it anyway, up to 256 files a directory;
nothing more is read. Its row reads the spec's name, and its details:

| Field | Says |
|---|---|
| `kind` | `acme.l2feed file` |
| `spec` | The spec's file, cut in the middle to fit: `~/…/formats/l2feed.toml` |
| `match` | What named the file, a chip per condition: `[magic L2FD] [version 3]` when the magic and the header did, `[glob *.l2]` when the glob did |
| `schema` | `3 columns (spec)` and each column's type, when the spec alone says them (fixed records, no size from the header); otherwise `on open` |
| `records` | For a spec with variants: `2 types (spec)` and each record type's column count (`add 5 · cancel 3`). → lists the record types |

A chip with several values (`[glob *.l2 *.lvl2]`) takes any of them. The
listing names a file by its glob without reading it, so a glob-named row shows
no `where` values; the open still checks them. Chips are drawn without brackets
where the header tint shows. Text values are quoted where it does not
(`[kind "A"]`), and a magic that is not text is hex (`7f 45 4c 46`).

## Delimited text

Loggers and instruments write a metadata line and a units line above a padded
header. A spec of `kind = "delimited"` holds the [CSV options](delimited-text.md#csv-options)
for such a family of files, so they open with no flags: from the command line,
from the home screen, compressed, or as a directory.

**`instrument.toml`**

```toml,file=instrument.toml
name = "acme.instrument-log"
kind = "delimited"
match = { magic = "#device_info" }
comment = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }
metadata_line = 1

[columns]
time = { from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }
bus1volts = { description = "Main bus voltage" }
```

**`flight.csv`**

```csv,file=flight.csv
#device_info, log_version="1.03", model="Unit 7, rev B", serial="123"
#yyyy-mm-dd, hh:mm:ss, hh:mm, degrees, volts, deg F
  Lcl Date, Lcl Time, UTCOfst,     Latitude, bus1volts, T1 Temp
          ,         ,        ,             ,      25.1,   187.2
2024-03-01, 10:00:00,  -05:00,    40.100000,      25.0,   180.0
```

```bash
datui formats check ./instrument.toml flight.csv
datui --format ./instrument.toml flight.csv
```

A delimited spec takes the keys of the config's [`[csv]`](../reference/settings.md#csv),
plus `match`, `kind`, the layout keys, `[columns]`, `description` and
`documentation`.

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
| `[columns]` | Column types and derived columns, below, and what columns mean: `description` and `unit` |

Lines count from 1 at the top of the file. Each option the spec sets replaces
the config's; a flag typed on the command line (`--delimiter`,
`--comment`, `--skip-initial-space`, `--header-rows`, `--skip-lines`)
wins over the spec. The options the spec does not set keep theirs.
`datui --delimiter ';' formats check SPEC FILE` reads the file as an open with
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

### Column types

A column of the file takes a `type`, beside its `unit` and `description`:

**`typed.toml`**

```toml,file=typed.toml
name = "acme.typed-log"
kind = "delimited"
match = { magic = "#device_info" }
comment = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }
metadata_line = 1

[columns]
"Lcl Date" = { type = "date", format = "%Y-%m-%d" }
Latitude = { type = "f64", description = "GPS latitude" }
bus1volts = { type = "f64", unit = "V" }
```

**`typed.csv`**

```csv,file=typed.csv
#device_info, log_version="1.03"
#yyyy-mm-dd, degrees, volts
  Lcl Date,     Latitude, bus1volts
2024-03-01,    40.100000,      25.0
2024-03-01,             ,      n/a
```

```bash
datui formats check ./typed.toml typed.csv
```

| `type` | Reads |
|---|---|
| `str` | Text as it is, never typed by `read.infer_types`: `02134` keeps its zero |
| `bool` | `true`/`false` or `1`/`0`, in any case |
| `i8` `i16` `i32` `i64` | Signed integers |
| `u8` `u16` `u32` `u64` | Unsigned integers |
| `f32` `f64` | Decimals. `f32` keeps about 7 significant digits |
| `date` `time` `datetime` | With `format`, a strftime format; without, the format is inferred |
| `duration` | `1d`, `2h30m`, `-1w2d` |

Use `i64` and `f64` unless a narrower type is wanted for an export or to hold
values to a range. A value is trimmed first, and one that does not fit the type,
or is out of an integer type's range, is null. The first time the Info panel
opens, one pass counts them, and the Notes tab says how many per column:
`RPM: 2 values out of range for u8, read as null`. A typed column the file does
not have is a note, not an error, since the files of a family differ. A typed
column is the same type in every file read together, and `read.infer_types`
leaves it alone. `type` beside `from` or `as` is refused: a derived column
takes its type from `as`.

### Derived columns

| `as` | `from` | Column |
|---|---|---|
| `datetime` | a date and a time, and optionally a UTC offset such as `-05:00`, `+0530` or `-5`; or one column of text | A datetime. With an offset it is in UTC |
| `date` | one column | A date |
| `time` | one column | A time of day |

A column of the file takes `description` and `unit` alone
(`bus1volts = { description = "Main bus voltage" }`), and a derived one takes
them beside `from` and `as`. They show in the
[Documentation view](../reference/format-specs.md#documentation). A `unit`
there is documentation only: the type row shows the units line's.

`format = "%Y-%m-%d %H:%M:%S"` gives the strftime format of the text, a date
and a time joined with a space; without it the format is inferred. A value that
does not parse is null. The column goes before the first column it is made
from, which stays; one named after a column it is made from replaces that
column, and its unit. There is no expression language: anything more is a
[query](../user-guide/querying-data.md).

### Matching

A delimited spec matches a file whose name says no format datui reads, or says
`.csv`, `.tsv` or `.psv`, compressed or not. A directory, or a glob such as
`'logs/log_*.csv'`, is read through the spec its first file with text matches.
<kbd>H</kbd> on the Info panel's Schema tab reads the file without a header,
and without its derived columns.

### Several files

Files read together through a spec are matched by column name, so logs from
different writer versions stack:

| When | Then |
|---|---|
| A file lacks a column | The column is null in its rows |
| A column is blank in the first rows a file's types are inferred from | It takes the type the other files give it. A value further on that is not of that type stops the read, naming the file and the column |
| One file's column holds integers and another's decimals | The column is `f64` |
| A file's column holds text where another's holds numbers | The column is text |
| Files give a column different units | The first file's unit; a note lists the units seen |

The Notes tab lists the columns not every file has. Columns keep the order
the files first have them in.

## Garmin TXi logs

Garmin and TXi are trademarks of Garmin Ltd. or its subsidiaries; datui is not
affiliated with or endorsed by Garmin.

The repository's `contrib/formats/garmin-txi.toml` reads the data logs a Garmin
TXi writes: the airframe line as metadata, the units line, `time` in UTC, and
each column typed. Copy it into `~/.config/datui/formats/` to open the logs, or
a directory of them, with no flags. A twin fills the `E2` columns and a single
leaves them blank. A log written before a GPS fix has blank date and GPS cells.

**`garmin-log.csv`**

```csv,file=garmin-log.csv
#airframe_info, log_version="1.03", airframe_name="Example 182", tail_number="N12345", system_id="0000EXAMPLE", unit="GDU1",
#yyy-mm-dd, hh:mm:ss,   hh:mm,  ident,      degrees,      degrees,  ft msl,     kt,    rpm,   deg F,   deg F,  bool,      #
  Lcl Date, Lcl Time, UTCOfst, AtvWpt,     Latitude,    Longitude,  AltMSL,    IAS, E1 RPM, E1 CHT1, E1 EGT1, OnGrnd, LogIdx
          ,         ,        ,       ,             ,             ,        ,    0.0,  980.0,   210.0,  1105.0,      1,      1
2024-05-04, 09:12:01,  -04:00,   KXYZ,   41.0000000,  -74.0000000,   350.0,    0.0, 1000.0,   215.0,  1120.0,      1,      2
2024-05-04, 09:12:02,  -04:00,   KXYZ,   41.0000100,  -74.0000100,   350.0,   12.5, 1800.0,   230.0,  1250.0,      0,      3
```

```bash
datui formats check contrib/formats/garmin-txi.toml garmin-log.csv
```

## Checks

| Problem | What happens |
|---|---|
| A field, size or key the spec gets wrong | The spec is refused, with its line and column |
| The magic does not match | The open fails, showing the bytes found |
| A file that ends partway through a record | The whole records open; a note shows the bytes left over |
| A header count larger than the file | The whole records open, with a note |
| A record whose length or type cannot be read | The records before it open; a note says where the rest was left out |
| `checksum` in `[records]` | A `checksum_ok` column, true or false for each record, rather than an error |
| A spec for a file in a bucket or at a URL | The file is downloaded first, then read |

In a spec's `[records]`:

```toml,template
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

A spec's file is read as a [lazy scan](index.md#how-each-format-is-read), or
from a decompressed copy when compressed. It is memory-mapped, and only the columns and rows on screen are decoded.
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
