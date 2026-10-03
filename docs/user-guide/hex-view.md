# Hex view

The hex view shows a file as its bytes: the offset, the bytes in hex in groups
of four, and the same bytes as ASCII. Use it to look inside a file datui has no
reader for, and to work out the layout of a [binary format spec](binary-formats.md).

| Opens it | |
|---|---|
| A local file no reader and no spec takes | Opens here instead of failing |
| <kbd>Enter</kbd> on a `binary` row of the home screen | Rows shown with <kbd>Ctrl</kbd>+<kbd>A</kbd> |
| <kbd>Ctrl</kbd>+<kbd>X</kbd> on the home screen | Any local file under the cursor |
| <kbd>x</kbd> in the Info panel | The dataset's file, when it is one local file |
| `datui --hex FILE` | Any local file |

The file is memory-mapped, never read whole: drawing reads only the rows on
screen, and a find reads the file on a worker. A file of many gigabytes opens
at once.

## Find a record's length

`feed.bin` holds records that each start with `SYNC`. Open it, press
<kbd>f</kbd>, type `SYNC`, <kbd>Enter</kbd>. The status line says the matches
are 25 bytes apart; <kbd>R</kbd> makes that the bytes per row, and the records
line up:

```text
Hex · feed.bin · 12,500 bytes · 25 a row (fixed)
offset h  00 01 02 03  04 05 06 07   08 09 0a 0b  0c 0d 0e 0f   10 11 12 13  14 15 16 17   18
00000000  53 59 4e 43  00 00 2a 36   fe 9c 97 17  00 00 00 00   00 00 44 20  82 3c fd e6   f1  SYNC··*6··········D ·<···
00000019  53 59 4e 43  01 00 2a 36   fe 9c 97 17  03 00 00 00   ff ff c2 6b  30 f9 0e c7   dd  SYNC··*6···········k0····
00000032  53 59 4e 43  02 00 2a 36   fe 9c 97 17  06 00 00 00   fe ff 01 e4  88 75 34 a2   0f  SYNC··*6·············u4··
0x19 of 0x30d4 · 0.2% · found SYNC · every 25 bytes · No reader matched this file
```

Bytes 4 to 11 count up in each record: a little-endian `u8` field. The byte
inspector reads the bytes at the cursor every way at once, which is how the
rest of the layout is found.

## Layout

| Width | Bytes a row |
|---|---|
| 60 columns | 8 |
| 80 columns | 16 |
| About 150 columns | 32 |
| About 300 columns | 64 |
| Under 50 columns | As many as fit, without the ASCII column |

The inspector sits beside the bytes when there is room for it and 16 bytes a
row; elsewhere <kbd>i</kbd> opens it under them. <kbd>r</kbd> or
`--record-size N` fixes the bytes per row (1 to 4096) so that records line up;
a row wider than the screen shows the part the cursor is in.

Bytes are colored by class: 0x00, printable ASCII, whitespace, other control
bytes, 0x80 to 0xFE, and 0xFF. The colors are the `hex_*` slots in
[the color settings](../reference/settings.md#colors). In the ASCII column a
byte that is not printable is `·` (`.` on a terminal without Unicode).

## Keys

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>h</kbd> <kbd>l</kbd> <kbd>k</kbd> <kbd>j</kbd> | A byte, or a row |
| <kbd>w</kbd> <kbd>b</kbd> | The next group of four, or back one |
| <kbd>0</kbd> <kbd>$</kbd> | The start or end of the row |
| <kbd>g</kbd> <kbd>G</kbd> or <kbd>Home</kbd> <kbd>End</kbd> | The start or end of the file |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page (<kbd>Ctrl</kbd>+<kbd>B</kbd> <kbd>Ctrl</kbd>+<kbd>F</kbd>; <kbd>Ctrl</kbd>+<kbd>U</kbd> <kbd>Ctrl</kbd>+<kbd>D</kbd> half a page) |
| <kbd>:</kbd> | Go to an offset |
| <kbd>f</kbd> | Find |
| <kbd>n</kbd> <kbd>N</kbd> | Next and previous match, round the end of the file |
| <kbd>R</kbd> | Make the distance between matches the bytes per row |
| <kbd>r</kbd> | Bytes per row; empty for as many as fit |
| <kbd>v</kbd> | Mark a range from the cursor; the status line counts it |
| <kbd>i</kbd> <kbd>Enter</kbd> | Show or hide the inspector |
| <kbd>#</kbd> | Offsets in decimal or hex |
| <kbd>B</kbd> | Read the file with a format spec |
| <kbd>Esc</kbd> | Stop a find; close the inspector or the mark; then back to the table or home screen it came from |
| <kbd>q</kbd> | Home, when opened from there; otherwise quit |

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

## The inspector

At the cursor, little-endian and big-endian side by side:

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
