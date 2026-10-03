# Signals and logs

## Audio files

Read: [lazy](index.md#how-each-format-is-read).

```bash
datui take.wav
datui -c read.audio_float=true take.wav   # integer samples as float in [-1, 1]
```

An uncompressed audio file opens as a table with one row per sample frame:
`frame`, `seconds` from the start, and one column per channel.
The file is mapped and only the frames on screen are decoded, so a recording
of many gigabytes opens at once and scrolls to any point as fast as to the
first. The row count comes from the file's size.

| Containers | Samples |
|---|---|
| WAV, Broadcast WAV, RF64/BW64, `WAVE_FORMAT_EXTENSIBLE` | 8-, 16-, 24- and 32-bit integer; 32- and 64-bit float |
| AIFF, AIFF-C (`NONE`, `twos`, `sowt`, `fl32`, `fl64`, `in24`, `in32`) | The same |

- Channels are `ch1`, `ch2`, ... An extensible file's channel mask names them
  instead: `L`, `R`, `C`, `LFE`, `BL`, `BR`, `SL`, `SR`, and so on.
- Integer samples stay integer: 24-bit is `i32`, and 8-bit WAV, stored
  unsigned, is shown signed. `[read] audio_float` shows them as `f32` in [-1, 1];
  float samples are never rescaled.
- A data size of 0 or a placeholder, as a recorder writes until it stops, is
  read as everything to the end of the file. A size past the end of the file
  is cut to what the file holds, and the Audio tab says so. A plain WAV past
  4 GiB, whose 32-bit data size wrapped, is read to its whole length.
- Files are recognized by their first bytes too, so a WAV or AIFF with any
  name opens.
- Compressed audio (A-law, mu-law, ADPCM, MP3, FLAC) is refused with its name.

Press <kbd>i</kbd> for the [Audio tab](../user-guide/dataset-info.md#audio): the format,
sample rate, length, the Broadcast WAV (`bext`), iXML and `LIST INFO`
fields, and the `cue `/`MARK` markers with their labels.

A line chart of a long recording draws each step's lowest and highest sample
([Charting](../user-guide/charting.md#large-tables)), and a full
[Data Quality](../user-guide/data-quality.md) run reports clipping, runs of zeros and DC
offset.

## MIDI files

Read: [in memory](index.md#how-each-format-is-read).

```bash
datui song.mid
datui path/to/midi/          # a directory of songs, as one table with a file column
```

A Standard MIDI File opens as a table with one row per event, track by track in
file order.

| Column | Holds |
|---|---|
| `track` | The track, from 1 |
| `tick` | Ticks from the start of the track |
| `seconds` | Seconds from the start, through the tempo map |
| `kind` | `note_on`, `note_off`, `cc`, `program`, `pitch_bend`, `poly_aftertouch`, `channel_aftertouch`, `sysex`, `sysex_escape`, or a meta event: `tempo`, `time_signature`, `key_signature`, `track_name`, `instrument`, `lyric`, `marker`, `cue`, `text`, `copyright`, `end_of_track`, ...; or a system message a file should not hold but some do: `clock`, `start`, `stop`, `song_position`, ... |
| `channel` | 1-16, as a sequencer numbers them |
| `note`, `note_name` | The note number and its name, middle C (60) as `C4` |
| `velocity` | For `note_on` and `note_off` |
| `controller` | The controller number of a `cc` |
| `value` | The `cc` value, program, pressure, pitch bend (-8192 to 8191), tempo in microseconds per quarter, key signature in sharps (negative for flats), a sysex's length, or a system message's data |
| `length` | On a `note_on`, the seconds until its `note_off`; null for a note that never ends |
| `text` | Meta text, tempo as `120 bpm`, `6/8`, `D major`, or sysex bytes in hex |

- A `note_on` at velocity 0 is a `note_off`, as the specification says.
- Formats 0 and 1 share one tempo map, from tempo events in any track; each
  format 2 track keeps its own. With SMPTE timing, `seconds` follows the frame
  rate and tempo events do not change it.
- Meta text is read as UTF-8, or as Latin-1 when it is not.
- Files are recognized by their first bytes too, so a MIDI file with any name
  opens, and so does one in a RIFF MIDI (`.rmi`) wrapper.
- A track that runs past the end of the file, an event cut short, or fewer
  tracks than the header says is refused with an error. Of a directory, a file
  that cannot be read is left out; the Notes tab says so and the MIDI tab lists
  each one with why.
- `channel`, `note`, `velocity` and `controller` are `u8`, `track` is `u16`,
  and `value` is `i32`.
- A file over 64 MiB is refused, or left out of a directory. An open of more
  than 10 million events in all is refused.
- A real-time byte in a track keeps running status, as on the wire; a sysex,
  meta or system common message cancels it, as the specification says.

Press <kbd>i</kbd> for the [MIDI tab](../user-guide/dataset-info.md#midi): format, timing,
length, tempo, meter, key and each track's name, events, notes and channels.
Notes that never end are counted on the Notes tab.

## VCD value change dumps

Read: [converted once](index.md#how-each-format-is-read).

```bash
datui waves.vcd
datui waves.vcd.gz
```

A VCD file from an HDL simulator or logic analyzer opens as a long table, one row
per value change of each signal, read once into a temporary Arrow IPC file.

| Column | Holds |
|---|---|
| `time` | The change's time: a Duration in nanoseconds for a timescale of `1 ns` or coarser; for `ps` and `fs`, an integer count of them (a Notes line says which) |
| `signal` | The dotted scope path and name with its bit range: `tb.dut.count[3:0]` |
| `value` | The value as written, a short vector padded to the signal's width (`b1` of a 4-bit signal is `0001`; `bx` is `xxxx`); a real's text |
| `int` | The value as an integer, when it is binary with no `x` or `z` and fits 64 bits |
| `width` | The signal's width from its `$var` |

- A file with another name opens when it starts with a VCD section, such as
  `$date` or `$timescale`.
- An identifier declared at two paths (an alias) gives a row for each.
- Press <kbd>i</kbd> for the VCD tab: timescale, date, version, comments, the
  number of value changes and their time span, and each signal's type, width
  and identifier. The Notes tab counts tokens that are not VCD and changes to
  undeclared identifiers.
- A token is at most 1 MiB, and a header holds at most 1,048,576 signals, 256
  scopes deep.

The wide table, one row per time and one column per signal, each carried
forward from its last change, is this SQL query (list the signals you want):

```sql
SELECT time,
       MAX(clk) OVER (PARTITION BY clk_n) AS clk,
       MAX(count) OVER (PARTITION BY count_n) AS count
FROM (
  SELECT *, COUNT(clk) OVER (ORDER BY time) AS clk_n,
            COUNT(count) OVER (ORDER BY time) AS count_n
  FROM (
    SELECT time,
           MAX(CASE WHEN signal = 'tb.clk' THEN value END) AS clk,
           MAX(CASE WHEN signal = 'tb.count[3:0]' THEN value END) AS count
    FROM df GROUP BY time
  )
)
ORDER BY time
```

The inner `GROUP BY` is the pivot: one row per time, null where a signal did not
change. Each `COUNT(...) OVER` numbers the runs between changes, and `MAX` over a
run fills it with the change that starts it. Without the fill,
[Pivot](../user-guide/reshaping.md#pivot) (<kbd>p</kbd>) with Index `time`, Columns `signal`,
Values `value` and Aggregate `last` gives the same table with nulls between
changes.

## GPS logs

Read: [converted once](index.md#how-each-format-is-read).

```bash
datui drive.nmea
datui --table GSV drive.nmea         # one row per satellite in view
head -n 3000 /dev/ttyACM0 | datui    # NMEA from standard input
datui ride.gpx
datui activities/                    # a directory of GPX files, as one table
```

An NMEA 0183 log or a GPX file is read once, start to end, into a temporary
Arrow IPC file, which is then scanned like any other: memory stays at one batch
of rows however long the log. Several logs, named together or as a directory of
them, open as one table with a `file` column first; a column one file lacks is
null in its rows, and the Notes tab counts across the files. A file with another name, such as `capture.log`,
opens when its first complete line is an NMEA sentence or its first element is `<gpx`.
`.nmea.gz` and the other compressions are read as they are decompressed.

**NMEA** opens as one row per fix, merged from each second's GGA, RMC, VTG and
GLL sentences:

| Column | Holds |
|---|---|
| `time` | UTC. NMEA dates only RMC and ZDA; every other time of day takes the last date, a day on when it passes midnight |
| `lat`, `lon` | Decimal degrees, negative south and west |
| `alt` | Meters above mean sea level (GGA) |
| `speed`, `course` | Meters per second; degrees true |
| `sats`, `hdop` | Satellites used and horizontal dilution (GGA) |
| `fix` | `none`, `gps`, `dgps`, `pps`, `rtk`, `rtk float`, `estimated`, `manual` or `simulated` |
| `gap` | Seconds since the fix before; see [the gap column](#the-gap-column) |
| `checksum_ok` | Every sentence of the fix matched its checksum; null when none had one |

`--table` opens one sentence type instead, with all its fields: `GGA`, `RMC`,
`VTG`, `GSA`, `GSV` (a row per satellite), `GLL`, `ZDA`, or `sentences` (every
sentence as written, with its line number, vendor sentences included).
On the home screen, <kbd>Enter</kbd> on a log opens its fixes and <kbd>→</kbd> lists
these tables (`drive.nmea/GSV`). The Info panel's [GPS tab](../user-guide/dataset-info.md#file-format-tabs)
gives the time span, the bounds and the count of each sentence type.
Lines that are not NMEA are skipped; the Info panel's Notes tab counts them,
and the sentences that fail their checksum. When the log has sentence types the
table on screen does not show, such as GSV beside the fixes, the Schema tab
names the other tables with how many sentences each has.

A time of day is dated by the last RMC or ZDA before it. Rows read before the
first one are dated back from it when it comes within the first 65,536 rows;
when it comes later, those rows keep a null `time`. A log with neither sentence
has no dates, and `time` is null throughout.

`--table` (`-t`) is for any file that holds several tables: an NMEA log's
sentence types, a [SQLite database](databases-and-arrays.md#sqlite-databases)'s tables, an Excel
workbook's worksheets, a format spec's record types, or a Hugging Face cache directory's
splits ([Arrow IPC streams](columnar-and-json.md#arrow-ipc-streams)). Any other file opened with it
is refused.

**GPX** opens as one row per `trkpt`, `rtept` and `wpt`:

| Column | Holds |
|---|---|
| `time`, `lat`, `lon`, `ele` | The point's time (UTC), position and elevation |
| `kind` | `track`, `route` or `waypoint` |
| `track`, `track_name` | The track or route, numbered from 0 in each kind, and its name |
| `segment` | The track segment, numbered from 0 in its track |
| `gap` | Seconds since the point before in the same track segment; see [the gap column](#the-gap-column) |
| the rest | The point's other fields (`name`, `sym`, `sat`, `hdop`...) and each leaf of its `<extensions>` by its name without the namespace (`hr`, `cad`, `atemp`), as numbers when every value is one |

A file cut off mid-element opens with the points before the cut, and says so in
Notes.

### The gap column

`gap` is not in the file: datui adds it so a dropout can be sorted and checked.
It is the seconds from the row before (the fix before, or the point before in
the same GPX track segment) to this one.

| `gap` is | When |
|---|---|
| null | The first fix or point; one without a time |
| null | Time steps back more than 5 seconds: a receiver reset, or logs joined together |
| null | NMEA not yet dated and more than an hour passed: whole days could be hidden in it |
| negative, down to -5 | Time steps back a little, as a receiver's clock settles |
| across midnight | An undated NMEA time earlier than the one before, within the hour, is taken as past midnight |

To look at a track:

| To see | Do |
|---|---|
| A rough map | Chart, XY, Scatter, X axis `lon`, Y series `lat` |
| Dropouts | Sort by `gap`, largest first; or in [Data Quality](../user-guide/data-quality.md#declare-what-a-column-must-hold) declare a range for `gap`, such as at most 2, and each dropout is **Out of range** |
| Speed spikes | The same for `speed`; Analysis also counts its outliers |

## Flight logs

Read: [lazy](index.md#how-each-format-is-read): one pass indexes the log, then each
table is decoded from a map of the file where it is shown.

```bash
datui flight.ulg                         # the list of its tables
datui flight.ulg --table vehicle_status  # one topic
datui 00000042.BIN/GPS                   # one DataFlash message type
```

Both formats describe their own messages; no spec is needed. A log of several
tables opens the home screen inside it, a row per table, like a directory.
<kbd>Enter</kbd> opens one; <kbd>q</kbd> comes back to the list without reading
the log again. A log downloaded or piped in is refused with the names of its
tables; `--table` picks one.

| PX4 ULog (`.ulg`) | |
|---|---|
| A table per topic | Named for the topic; `sensor_accel.0`, `sensor_accel.1` when it has several instances. `timestamp` is a duration since boot; nested types are `outer.inner`, `outer[0].inner` for an array of them; a number array is an Array column, a `char` array text. `_padding` fields are left out |
| `logged_messages` | `timestamp`, `level` (`error`, `warning`, `info`, ...), `tag`, `message` |
| `parameters` | `name`, `type`, `value`, and the `timestamp` of a change made in flight (null for the value the log started with) |
| Info tab | The version, dropouts, info messages (`sys_name`, `ver_hw`, ...) and each parameter's starting value |

| ArduPilot DataFlash (`.bin`) | |
|---|---|
| A table per message type | Named for the type (`GPS`, `ATT`, `PARM`, ...), a column per label, typed by its format character |
| `TimeUS`, `TimeMS` | A duration since boot |
| `c`, `C`, `e`, `E` | Hundredths, as a float |
| `L` | Degrees (latitude, longitude), as a float |
| `a` | An Array of 32 `i16` |
| Units | From `FMTU` and `UNIT`, on the Info panel's Schema tab; an integer field `FMTU` gives a multiplier (`MULT`) is scaled by it |
| Info tab | Message types and their record counts, formats and lengths |

- A ULog file is known by its first bytes. A DataFlash log is known by its first
  record, an `FMT` that defines `FMT`, whatever it is called.
- A damaged stretch is passed over to the next ULog sync marker or DataFlash
  record header; a log cut off mid-message keeps what it holds. The Notes tab
  says how many bytes were passed over.
- ULog appended data (written after a crash) is read with the rest.
- At most 67,108,864 messages are indexed in one log.

## CAN logs

Read: [lazy](index.md#how-each-format-is-read): one pass indexes the log, then each
frame is read from its line where it is shown.

```bash
datui candump-2024-01-31_081500.log                # the frames
datui candump.log --dict vehicle.dbc                # a table per message
datui candump.log --dict vehicle.dbc --table EEC1   # one message
```

A `candump` log opens by its content, whatever it is called:
`(1706689000.123456) can0 123#DEADBEEF` as `candump -l` and `-L` write it (`##`
for CAN FD, `#R` for a remote request), or `can0  123   [4]  DE AD BE EF` as
`candump` prints it, with or without a timestamp in front.

| `frames` | |
|---|---|
| `ts` | The timestamp: a datetime for wall-clock time (`-l`, `-ta`), a duration for time since the start; null when the line has none |
| `iface` | The interface: `can0`, `vcan0` |
| `id` | The id in hex: three digits standard, eight extended |
| `ext` | Whether the id is extended |
| `dlc` | The data length code |
| `data` | The data bytes |
| `fd`, `flags` | Whether it is a CAN FD frame, and its flags (BRS, ESI) |
| `kind` | `data`, `remote` or `error` |

With a dictionary that names the log's messages, the log opens the home screen
inside it, like a directory: a table per message with frames, `frames`, and
`signals`.

| Table | Columns |
|---|---|
| A message, by its DBC name | `ts` and a column per signal: factor and offset applied, an integer while they keep it one; value names (`VAL_`) as text; a multiplexed signal null in the frames its multiplexer does not select. Units are on the Info panel's Schema tab |
| `signals` | One row per decoded value: `ts`, `message`, `signal`, `value` (a float) and `unit`, in time order |

Signals in Intel and Motorola byte order, signed and unsigned, and floats
(`SIG_VALTYPE_`) are read; a signal past the end of a short frame is null.
Extended multiplexing (`SG_MUL_VAL_`) is not; the Notes tab says so.

### CAN log dictionaries

DBC dictionaries are found where [format specs](format-specs.md) are: the `formats`
directory of the config directory, `$DATUI_FORMATS_PATH`, and `[formats] path`.
A `.dbc` file there applies to every interface. A TOML file names one for an
interface:

```toml
kind = "dbc"
file = "powertrain.dbc"     # beside this file, or a full path
[match]
interface = "can1"
```

They are read in that order, then `--dict FILE`; where two name a message of the
same id, the later one is read. Press <kbd>i</kbd> for the CAN tab: frames,
interfaces, the dictionaries read and the frames none of them names, and each
message's id, frames, signals and comment.

## FIX logs

Read: [converted once](index.md#how-each-format-is-read).

```bash
datui session.log                       # known by its content
datui --format fix capture.bin
datui --dict broker.toml session.log
```

A log of FIX `tag=value` messages, delimited by SOH, `|` or `^A`, opens as one row
per message, read once into a temporary Arrow IPC file. A file of any name opens
when a line in its first 4 KiB holds `8=FIX`, a delimiter and `9=`; messages may
be one per line or back to back.

| Column | Holds |
|---|---|
| `prefix` | The text before `8=FIX` on the line, such as a log timestamp; only when a line has one |
| `direction` | `in` or `out`, from a word in the prefix: `IN`, `OUT`, `<`, `>`, `RECV`, `SENT` and the like |
| `session` | A session in the prefix: `FIX.4.4:SENDER->TARGET` |
| a column per tag | Named from the dictionary (`35` is `MsgType`, `55` is `Symbol`), in the order the tags first appear; a tag no dictionary names keeps its number |
| `<name>_code` | Beside an enumerated tag: the code, where the tag's column shows its name (`54=1` is `Buy`) |
| `<name>_rest` | Beside a tag repeated within a message, as a repeating group's tags are: a list of its later values; the tag's column keeps the first |
| `body_length_ok` | Tag 9 matches the message's length; null for a message cut short of tag 10 |
| `checksum_ok` | Tag 10 matches the message's checksum; null for a message cut short |

- Prices, quantities and amounts are numbers, integers and sequence numbers
  `i64`, UTC timestamps (`52`, `60`) datetimes, dates dates and `Y`/`N` booleans,
  when every value of the tag reads as one; otherwise text.
- A length-tagged value (`95`/`96` RawData, `90`/`91`, `93`/`89`, `212`/`213`
  XmlData and the encoded text fields) is read by its length, so it may hold
  the delimiter or a newline.
- A bad message stays: its checks are false, and the Notes tab counts them, the
  lines with no message, and messages cut short.
- A message is at most 1 MiB and holds at most 4,096 fields; at most 4,096 tags
  become columns.
- Press <kbd>i</kbd> for the FIX tab: messages per BeginString, the dictionaries
  read with the log, and each column's tag number and the names the dictionaries
  give it.
- Binary FIX encodings (SBE, FAST) are not read.

### FIX log dictionaries

The built-in dictionary is FIX 4.2, 4.4 and 5.0 SP2 together, the newest
version's names winning. Venues and brokers add their own tags (5000-9999 and
10000 up), so dictionaries can be added: on the
[format search path](format-specs.md#where-specs-live), or with
`--dict FILE`.

| Form | |
|---|---|
| QuickFIX XML (`.xml`) | A QuickFIX or QuickFIX/J data dictionary, read as it is: its fields, types and enums. It applies to the messages of its version's BeginString |
| TOML (`.toml`, `kind = "fix"`) | As below |

```toml
name = "acme.fix.broker-x"
kind = "fix"
match = { sender = "BROKERX", begin_string = "FIX.4.4" }   # optional
tags = { 9001 = "AlgoName", 9002 = { name = "Urgency", type = "int", enum = { 1 = "Low", 2 = "High" } } }
```

| Key | |
|---|---|
| `name` | A namespaced name, such as `acme.fix.broker-x` |
| `match` | `sender` (49), `target` (56), `begin_string` (8): the dictionary applies only to messages with these values |
| `tags` | Tag number to a name, or to `name`, `type` (`int`, `float`, `price`, `qty`, `string`, `char`, `timestamp`, `date`, `bool`, `length`, `data`), `enum` (code to name) and, for a length tag, `data` (the tag it sizes) |

The built-in dictionary comes first, then each matching dictionary on the search
path in order, then `--dict`; a later one renames a tag or adds to its enums.
One log can hold two counterparties that name tag 9001 differently: each
message is read with its own, the column falls back to the tag number, and the
FIX tab shows both names. `datui formats` lists dictionaries beside the
format specs, and `datui formats check NAME [LOG]` checks one, and with a log
says how many messages it matches and which of its tags they hold.

The built-in dictionary is generated from QuickFIX's data dictionaries. This
product includes software developed by quickfixengine.org
(http://www.quickfixengine.org/).

## SDF compound files

Read: [converted once](index.md#how-each-format-is-read).

```bash
datui compounds.sdf
datui compounds.sdf.gz
datui https://example.com/library.sdf.gz
```

An SDF (structure-data) file of molecules, as PubChem, ChEMBL and screening
libraries publish them, opens as one row per record (`$$$$`), read once into a
temporary Arrow IPC file. The atom and bond blocks are passed over, never held.

| Column | Holds |
|---|---|
| `name` | The molecule's name, the record's first line; null when blank |
| `atoms`, `bonds` | From the counts line, or a V3000 `COUNTS` line |
| a column per data item | Each `> <FIELD>` (also `>  <FIELD>`, `> <FIELD> (ID)`, `> 25 <FIELD>`, `> DT12`), in the order first seen; null in a record without it. Integers or floats when every value is one, text otherwise |

- A value of several lines keeps them, joined by newlines.
- A record that names a field twice keeps the first; the Notes tab counts the rest.
- A line or value is at most 1 MiB, and a file has at most 4,096 fields.
- Press <kbd>i</kbd> for the SDF tab: the record count, and each field's type and
  how many records hold it.
- **Aqueous solubility (SDF)** in the home screen's
  [public datasets](../user-guide/home-screen.md#public-datasets) is one to try: 1,025
  molecules with `SOL` as a float and `SOL_classification` as text. Sort by
  `SOL`, or filter `SOL_classification` to `(C) high`.

## ELF symbol tables

Read: [in memory](index.md#how-each-format-is-read): the symbol and section tables,
from a map of the file.

```bash
datui firmware.elf                    # one row per symbol
datui firmware.elf --table sections   # one row per section
datui firmware.elf/sections           # the same
```

On the home screen, <kbd>Enter</kbd> on an ELF file opens its symbols and <kbd>→</kbd>
lists both tables.

| Column | Holds |
|---|---|
| `name` | The symbol's name; a Rust name demangled, without its hash. C++ names stay mangled |
| `addr` | Its address (`u64`) |
| `size` | Its size in bytes |
| `kind` | `func`, `object`, `section`, `file`, `common`, `tls`, `ifunc` or `notype` |
| `bind` | `local`, `global`, `weak` or `unique` |
| `section` | The section it is in; `UND` for undefined, `ABS` for absolute, `COMMON` |
| `region` | `flash` when its section is loaded and not written (code, constants), `ram` when it is written (`.data`, `.bss`); null for what is not loaded |

The `sections` table has `name`, `addr`, `size`, `flags` (as `readelf` writes
them: `W` write, `A` alloc, `X` execute, ...), `kind` and `region`.

- Sort by `size` and group by `section` or `region` to see what fills flash and RAM.
- The symbol table is `.symtab`, or `.dynsym` for a stripped library.
- `.elf` and `.axf` files open by name; any file that starts with `\x7fELF`
  opens too when named on the command line.
- At most 10 million symbols are read; the Notes tab says how many more there are.

Press <kbd>i</kbd> for the ELF tab: class, machine, type, entry point, the bytes
in flash and in RAM, and each section's address, size and flags.

## systemd journal

`journalctl -o json` output is read as the journal, from a pipe or a file:

```bash
journalctl -o json -u nginx --since today | datui
journalctl -o json -b -p warning | datui
journalctl -o json -f | datui -f -            # live
jd() { journalctl -o json "$@" | datui; }     # jd -u nginx -b
```

| Column | What |
|---|---|
| `time` | `__REALTIME_TIMESTAMP` as a UTC datetime |
| `level` | `PRIORITY` as `emerg`, `alert`, `crit`, `err`, `warning`, `notice`, `info`, `debug`, ordered by severity: `select where level <= "err"` keeps errors and worse, and a sort puts `emerg` first |
| `_SYSTEMD_UNIT` | The unit, or `SYSLOG_IDENTIFIER` when no entry has one |
| `_PID`, `MESSAGE` | Then the rest of the fields as they came, and bookkeeping (`__CURSOR`, `__SEQNUM`, `_BOOT_ID`, ...) last |

- Every field is a column, one first seen late in the journal included. Values
  stay text as journalctl writes them; `PRIORITY` is kept beside `level`.
- A `MESSAGE` journalctl wrote as bytes (not UTF-8, or with control
  characters) is shown as text, lossily; the Info panel says how many.
- The Info panel's Journal tab gives the time span, the entries, and the units,
  boots and hosts, with the entries per unit.
- The entries are read whole into memory. Narrow a large journal with
  `--since`, `-u` or `-b` before it is piped.
- Copy as Python reads a journal file with `pl.scan_ndjson` and derives the same columns.
