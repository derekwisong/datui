# Hex view

The hex view shows a file as its bytes: the offset, the bytes in hex in groups
of four, and the same bytes as ASCII. Use it to look inside a file datui has no
reader for, and to work out the layout of a [format spec](../formats/format-specs.md).

| Opens it | |
|---|---|
| A local file that no reader or format spec handles | Opens here instead of failing |
| <kbd>Enter</kbd> on a `binary` row of the home screen | Rows shown with <kbd>Ctrl</kbd>+<kbd>A</kbd> |
| <kbd>Ctrl</kbd>+<kbd>X</kbd> on the home screen | Any local file under the cursor |
| <kbd>x</kbd> in the Info panel | The dataset's file, when it is one local file |
| `datui --hex FILE` | Any local file |

The file is memory-mapped, never read whole: drawing reads only the rows on
screen, and a find reads the file in the background. A file of many gigabytes opens
at once.

## Find a record's length

This script makes `feed.bin`, 500 records of 25 bytes that each start with
`SYNC`, and the commands after it open the file:

**`make_feed.py`**

```python,file=make_feed.py
import ctypes


class Record(ctypes.LittleEndianStructure):
    _layout_ = "ms"
    _pack_ = 1  # no padding: 25 bytes a record
    _fields_ = [
        ("sync", ctypes.c_char * 4),
        ("seq", ctypes.c_uint64),
        ("price", ctypes.c_uint32),
        ("change", ctypes.c_int16),
        ("size", ctypes.c_int16),
        ("flags", ctypes.c_int32),
        ("check", ctypes.c_uint8),
    ]


with open("feed.bin", "wb") as f:
    for i in range(500):
        f.write(bytes(Record(b"SYNC", i, 3 * i, -i, i, 0, i % 256)))
```

```bash
python3 make_feed.py
datui --hex feed.bin
```

Press <kbd>f</kbd>, type `SYNC`, <kbd>Enter</kbd>. The status line says the
matches are 25 bytes apart; <kbd>R</kbd> makes that the bytes per row, and
the records line up:

```text
Hex · feed.bin · 12,500 bytes · 25 bytes/row (fixed)
offset    00 01 02 03  04 05 06 07   08 09 0a 0b  0c 0d 0e 0f   10 11 12 13  14 15 16 17   18
00000000  53 59 4e 43  00 00 00 00   00 00 00 00  00 00 00 00   00 00 00 00  00 00 00 00   00  SYNC·····················
00000019  53 59 4e 43  01 00 00 00   00 00 00 00  03 00 00 00   ff ff 01 00  00 00 00 00   01  SYNC·····················
00000032  53 59 4e 43  02 00 00 00   00 00 00 00  06 00 00 00   fe ff 02 00  00 00 00 00   02  SYNC·····················
0x19 of 0x30d4 · 0.2% · found SYNC · every 25 bytes · format unknown
```

Bytes 4 to 11 count up in each record: a little-endian `u64` field, in a
[format spec](../formats/format-specs.md)'s types. The byte inspector reads
the bytes at the cursor in every way at once; that is how you work out the
rest of the layout.

## Layout

| Width | Bytes per row |
|---|---|
| 60 columns | 8 |
| 80 columns | 16 |
| About 150 columns | 32 |
| About 300 columns | 64 |
| Under 50 columns | As many as fit, without the ASCII column |

The byte inspector sits beside the bytes when there is room for it alongside
16 bytes per row. Otherwise <kbd>i</kbd> opens it under the bytes, in at most
half the screen's rows, with a count of the readings that do not fit. <kbd>r</kbd> or
`--hex-width N` fixes the bytes per row (1 to 4096) so that records line up;
a row wider than the screen shows the part the cursor is in.

Bytes are colored by class: 0x00, printable ASCII, whitespace, other control
bytes, 0x80 to 0xFE, and 0xFF. The colors are the `hex_*` slots in
[the color settings](../reference/settings.md#colors). In the ASCII column a
byte that is not printable is `·` (`.` on a terminal without Unicode).

## Keys

| Key | Action |
|---|---|
| <kbd>f</kbd>, <kbd>n</kbd> <kbd>N</kbd> | [Find](#find); the next and previous match, round the end of the file |
| <kbd>:</kbd> | [Go to an offset](#go-to-an-offset) |
| <kbd>R</kbd> | Make the distance between matches the bytes per row |
| <kbd>r</kbd> | Set the bytes per row; leave it empty for as many as fit |
| <kbd>i</kbd> <kbd>Enter</kbd> | Show or hide the [byte inspector](#the-byte-inspector) |
| <kbd>v</kbd> | Mark a range from the cursor; the status line counts it |
| <kbd>B</kbd> | Read the file with a format spec |
| <kbd>Esc</kbd> | Stop a find, close the byte inspector, or clear the mark; otherwise go back to where the hex view was opened from |

Movement uses vim's keys (`h` `j` `k` `l`, `w` `b`, `0` `$`, `g` `G`); the [keyboard reference](../reference/keyboard-shortcuts.md#hex-view)
has every key.

## Go to an offset

| Typed | Goes to |
|---|---|
| `4096` | Byte 4096 |
| `0x1000` | Byte 4096 |
| `+16`, `-16` | 16 bytes after or before the cursor |
| `e-8` | The eighth byte from the end; `e-1` is the last |

## Find

| Typed | Finds |
|---|---|
| `PAR1` | The text, as UTF-8 bytes |
| `0x1acffc1d` | Those bytes |
| `de ad be ef` | Those bytes: two or more hex pairs |
| `de ?? be ef` | `??` matches any byte |
| `"de ad"` | The text in quotes, even when it looks like hex |

<kbd>Ctrl</kbd>+<kbd>U</kbd> in the prompt finds text as UTF-16 little-endian.
A match may span rows. Every match on screen is marked, and <kbd>Esc</kbd> stops
a find still reading a large file.

## The byte inspector

The byte inspector reads the bytes at the cursor, little-endian and big-endian side by side:

| Reading | |
|---|---|
| `u8` to `u64`, `i8` to `i64` | With the 3-, 5- and 6-byte widths (`u24`, `u40`, `u48`) |
| `f16`, `f32`, `f64` | Floats |
| `bits` | The byte in binary |
| `varint`, `zigzag` | A LEB128 varint, its length, and its zigzag value |
| `unix s`, `ms`, `us`, `ns` | A Unix time, when it lands between 1980 and 2100 |
| `yyyymmdd` | A date written as an integer |
| `days 1970`, `days 2000` | A count of days since either date |
| `text` | The text up to the first NUL |
| `null?` | The null sentinels the bytes hold: an int's min, a uint's max, NaN |

A marked range (<kbd>v</kbd>) shows its length under the readings.
