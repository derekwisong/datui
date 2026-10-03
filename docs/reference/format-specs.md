# Format spec reference

Every key a [format spec](../formats/format-specs.md) takes.

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

`datui --table add capture.bin` opens one variant as its own table: only its
records and its columns.

On the home screen, a file a spec's glob names that holds several variants
counts them as tables ("2 tables" in its details). Enter opens every record; →
lists the tables inside it, one row each at `capture.bin/add`, and Enter on one
opens it alone. That path opens the table on the command line too, and is
what recents record.

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
