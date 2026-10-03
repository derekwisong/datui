# Binary formats

```bash
datui day.l2                           # a spec on the search path matches it
datui --format acme.l2feed capture.bin # read it as that spec
datui --spec l2feed.toml capture.bin   # read it with this spec file
datui formats                          # list the specs datui finds
datui formats check acme.l2feed day.l2 # check a spec and print a file's first rows
```

A file of fixed-size records, such as a tick capture, a sensor log or a struct
dump, opens as a table once a spec describes it. A spec is one TOML file per
format. Specs are data: no scripts or expressions. Every size read from a file is
bounded.

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
line and column of each problem.

`datui formats check SPEC [FILE]` checks one spec, by name or file. With a file,
it prints the warnings and the first ten rows. It exits non-zero on an error, so
a repository of specs can run it in CI.

## Which spec reads a file

| First that applies | |
|---|---|
| `--spec FILE` | That spec, whatever the file is called |
| `--format NAME` | The spec of that name |
| A name datui already reads (`.csv`, `.parquet`) | Read as it is, as before |
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

A file is memory-mapped, and only the columns and rows on screen are decoded.
Scrolling to the last row of a gigabyte file reads only the rows shown. A sort,
filter, query, chart or analysis reads every row of the columns it uses, in
memory.

A file that grows while it is open keeps the rows it had; open it again to read
the rest. A file cut short by another program is refused at the next read
rather than read past its end.
