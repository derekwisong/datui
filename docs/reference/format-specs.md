# Format spec reference

Every field type and key a [format spec](../formats/format-specs.md) takes.
Each example below is a whole spec; `datui formats check ./spec.toml` checks one.

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
| `description` | `"Limit price"` | What the column means, for the [Documentation view](#documentation). Not read |
| `unit` | `"USD"` | The column's unit, for the Documentation view. Not read |

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

```toml,spec
name = "acme.messages"
match = { glob = "*.msg" }

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

```toml,spec
name = "acme.orders"
match = { glob = "*.ord" }

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
| `description` | What a record of this type is, for the [Documentation view](#documentation) |

All records make one table, with a `type` column naming each one's variant. A
column of a field one variant lacks is null in that variant's rows. A record of
a type no variant names shows as `?X` when its size is known
(`length_prefixed`); otherwise the read stops there, with a note.

A field name two variants share is one column, so it must be the same field in
both: the same type, count, encoding, bits and group, and the same `time`,
`scale`, `factor` or `enum`. Otherwise the spec is refused: ``variants: `px` is
a different field in two variants; one column has one type, so name them
apart``. A variant's field cannot take a common field's name either (``a second
field named `kind` ``), nor be named `type`.

`datui --table add day.ord` opens one variant as its own table: only its
records and its columns.

On the home screen, each variant of a file a spec reads is a record type: its
details read `records  2 types (spec)` and each type's column count
(`add 5 · exec 4`). Enter opens every record; → lists the record types, one row
each at `day.ord/add`, and Enter on one opens it alone. That path also opens the
record type from the command line, and it is what the recents list keeps.

## Documentation

A spec says what its files mean with the words a
[catalog](catalogs.md#documentation) uses. <kbd>Ctrl</kbd>+<kbd>E</kbd> on a
file the spec reads, and the Info panel's Documentation tab once it is open,
show it in the [Documentation view](../user-guide/home-screen.md#documentation-view).
None of it changes how a file is read.

```toml,spec
name = "acme.quotes"
description = "Quotes and trades from the Acme feed"
documentation = "https://example.com/acme-feed.pdf"
match = { glob = "*.acq" }

[records]
framing = "variant"
type = "kind"
fields = [{ name = "kind", type = "u1" }, { name = "ts", type = "u8", time = "ns", description = "When the exchange sent it" }]

[[variants]]
name = "quote"
when = 1
description = "The best bid and offer"
fields = [{ name = "bid", type = "u4", scale = 4, unit = "USD" }, { name = "ask", type = "u4", scale = 4, unit = "USD" }]

[[variants]]
name = "trade"
when = 2
description = "A trade on the book"
fields = [
  { name = "px", type = "u4", scale = 4, description = "Trade price", unit = "USD" },
  { name = "side", type = "u1", enum = { 1 = "BUY", 2 = "SELL" }, description = "The aggressor's side" },
]
```

| Key | Where | Says |
|---|---|---|
| `description` | The spec, a variant, a field | What the format, the record type, the column or the header or footer field is |
| `documentation` | The spec | An `https://` link to the format's own documentation |
| `unit` | A field | The column's, or the header or footer field's, unit |
| `enum` | A record field | Its codes and labels are the column's value legend |

A flattened field's description and unit go to each of its columns, `bid_0`,
`bid_1` and on.
Named `[header]` and `[footer]` fields with a `description` or `unit` are listed
in sections of their own.

A [delimited spec](../formats/format-specs.md#delimited-text) takes
`description` and `unit` in `[columns]`, for a column of the file or a derived one:
`temp = { description = "Air temperature", unit = "deg F" }`. A column's declared
`type` shows beside its unit: `Latitude  f64 · deg  GPS latitude`.

Each text is trimmed, and an empty one is refused. Where a catalog lists the
same file, its description and its `documentation` link stand over the spec's.
Where both note a column, each of the catalog's `description`, `unit` and
`values` stands over the spec's when the catalog gives it, and the spec's fills
the rest. The spec's other notes and its record types stay.

## Footer

```toml,spec
name = "acme.counted"
match = { glob = "*.cnt" }

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

```toml,spec
name = "acme.blocks"
match = { glob = "*.blk" }

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

```toml,spec
name = "acme.multicast"
match = { glob = "*.pcap" }

[capture]
header = [{ name = "session", type = "str", size = 10 }, { name = "seq", type = "u8" }, { name = "count", type = "u2" }]
count = "count"
time = "captured"

[records]
fields = [{ name = "price", type = "u4" }]
```

The file is a pcap or pcapng capture, told apart by its magic. Each UDP
payload holds the records, after the payload `header`; `count` names the
header field counting them, and `time` adds a column with each packet's capture
time. Packets that are not UDP are left out and counted in a note. A capture
spec has no `[header]`, `[footer]` or `[blocks]`.

## A tree of files

```toml,spec
name = "acme.trades"
match = { glob = "*.bin" }

[files]
path = "{date:%Y%m%d}/{venue}/trades.bin"

[records]
fields = [{ name = "price", type = "f8" }]
```

`datui --format acme.trades store/` reads every file under `store/` that the
pattern matches as one table, with a column for each part: a date for a part
with a format, text otherwise. A file whose part does not parse as its date is
left out, with a note.

## Columns layout

```toml,spec
name = "kdb.trades"
layout = "columns"
endian = "be"

[records]
fields = [{ name = "price", type = "f8" }, { name = "size", type = "s8" }]
```

With this spec, `datui --format kdb.trades db/trades/` reads `db/trades/price` and
`db/trades/size` as two columns of one table, as kdb+ splays a table. A
`[header]` describes the start of each file. A glob in a columns spec matches
the directory.

When the header lists where each column starts in one file, every field gives
`offset` and the spec reads that one file:

```toml,spec
name = "acme.packed"
layout = "columns"

[header]
fields = [{ name = "n", type = "u4" }, { name = "px_off", type = "u4" }]

[records]
count = "header.n"
fields = [{ name = "px", type = "f8", offset = "header.px_off" }]
```
