# Files and formats

```bash
datui data.parquet                             # a file
datui jan.csv feb.csv mar.csv                  # files of the same shape, as one table
datui /data/events/                            # a directory, read the way Enter reads its row
datui --hive "/data/events/**/*.parquet"       # a glob (quote it)
datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/   # public S3; gs:// and abfss:// too
datui https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv
datui --format csv https://example.com/export  # force the format when the name gives no hint
cat data.csv | datui                           # data piped in
datui - < events.parquet                       # `-` reads standard input
datui -f app.log.ndjson                        # follow a file as it grows
datui app.log                                  # text: a row per line
SYSTEMD_PAGER=datui journalctl -u nginx        # datui as a pager
```

## Standard input

`datui -` reads the data piped to it, and so does `datui` with no path when
something is piped in. Keys still come from the terminal.

```bash
xsv select id,amount sales.csv | datui
curl -s https://example.com/export.csv.gz | datui
datui --no-header - < raw.txt
```

The data is written to a temporary file as it arrives, in `--temp-dir` or the
`temp_dir` setting when given, then read like any file. The loading screen counts the bytes read;
<kbd>Ctrl</kbd>+<kbd>O</kbd> stops the read and removes the file. The file is
removed when datui exits.

The format comes from the first bytes, unless `--format` or `--compression`
names it:

| First bytes | Read as |
|---|---|
| Parquet, Arrow IPC or Avro magic number, or an Arrow IPC stream's schema message | that format |
| `SQLite format 3` | SQLite; a database of several tables needs `--table` |
| gzip, zstd, bzip2 or xz magic number | decompressed, then read by what is inside, as below: a text format, CSV or TSV, or lines |
| `{`, the first line an object with `__CURSOR` and `__REALTIME_TIMESTAMP` | [systemd journal](#systemd-journal) |
| `[`, then JSON | JSON |
| `{`, the first line a whole object | NDJSON |
| `{`, the object open past the first line, JSON so far | JSON |
| an NMEA sentence (`$GPGGA,`) with a checksum that matches, or of a type receivers write | NMEA |
| XML whose first element is `<gpx` | GPX |
| a line with `8=FIX`, a delimiter and `9=` | FIX |
| `$date`, `$version`, `$timescale`, `$comment`, `$scope` or `$var`, with an `$end` | VCD |
| a `V2000` or `V3000` counts line, or `M  END` with a data item or `$$$$` | SDF |
| several lines with one field count, two or more, split at tabs | TSV |
| several lines with one field count, two or more, split at commas, quotes where CSV allows them | CSV |
| anything else | [lines](#text-and-logs) |

A comma or a tab alone is not a table: a log line with a comma in it stays a
line. An unnamed CSV the first lines do not show as one opens with `--format
csv`; the Info panel's notes say so. With `--format csv`, `tsv` or `psv` and no
`--compression`, compression still comes from the first bytes.

The CSV options below apply. The dataset is named `stdin`. It is not added to
recent datasets, and [views](views.md) match it by its columns only.

## Following a growing file

`--follow` (`-f`) shows rows as they are appended to a local CSV, TSV, PSV,
NDJSON or text file, or record batches to an Arrow IPC stream, as `tail -f` does:
a logger's output, a test rig's results, an app's event log. With `-`, it shows
standard input as it arrives rather than waiting for it to end. Text is a row per
line, blank lines included.

```bash
datui -f readings.csv
datui -f /var/log/app.log
cat /dev/ttyUSB0 | datui -f -
while sleep 0.1; do echo "$(date +%s.%N),$RANDOM"; done | datui -f --no-header -
```

| Key | Action |
|---|---|
| <kbd>t</kbd> | Pause and resume. On a file opened without `-f`, follow it: it is read again, as <kbd>H</kbd> reads it, so the query, filters and sort are cleared |
| <kbd>Esc</kbd> | Stop following; the rows read stay |

The control bar says `following` and how long ago rows last arrived, or
`paused` and how many have arrived since.

- A follow starts on the last row. On the last row, the cursor stays on the
  last row as rows arrive; anywhere else it stays where you put it, and the
  bar counts the rows that came in below.
- Only complete lines are read. A partial last line waits for its newline.
- The query, filters, sort, hidden columns and column cursor apply to the new
  rows. [Value counts](value-counts.md), [analysis](analysis-features.md) and
  [charts](charting.md) keep the rows they were opened on; the bar says how many
  have arrived since, and <kbd>t</kbd> there reads them.
- The column types come from the first rows, as for any file. A later row
  that does not fit (a field too many, a word in a number column) is counted on
  the bar in the warning color and its values are read as null; it never stops
  the follow.
- A refresh reads only the rows on screen, from a mark near them, so it costs
  the same on a 10 MB file as on a 10 GB one. Under a filter, only the new rows
  are counted.
- A file that shrinks or is replaced (truncation, rotation) is read again from
  its start, and the bar says so. A deleted file stops the follow; its rows
  stay readable.
- On Linux, datui hears of an append as it lands and reads new rows at most
  every 250 ms; elsewhere, and on a network file system, it checks the file's
  size every 250 ms. A burst of appends is one refresh. Set
  `follow_interval_ms` under [`[file_loading]`](../reference/settings.md#file-loading)
  to change it.
- Standard input keeps being written to its temporary file after the first
  rows show, until it ends or <kbd>Esc</kbd>, <kbd>Ctrl</kbd>+<kbd>O</kbd> or
  quitting stops it. The bar says when it ends.

An Arrow IPC stream shows a record batch once its message is whole; one with
dictionary-encoded columns cannot be followed. Parquet, Arrow IPC files, Excel
and other formats written with a footer cannot be read before they are
finished, nor can a compressed file be read from the middle: `--follow`
refuses them, and remote data, with a message.

### Recording standard input

`--tee FILE` records standard input to FILE while you view it: the bytes
exactly as they arrive, in any format. The table reads FILE itself; there is
no second copy.

```bash
some_logger | datui -f --tee run1.csv -
arecord -f S16_LE -r 48000 -c 2 -t wav - | datui -f --tee take1.wav -
```

| When | What happens |
|---|---|
| The bar | `rec` with the bytes written and the rate; `saved` with the size, length and file once the stream ends; `stopped` and why, in the warning color, if writing failed (a full disk), which the error dialog says too |
| An existing FILE | Refused; `--force` overwrites it |
| Quit, <kbd>Ctrl</kbd>+<kbd>O</kbd> while the producer still sends | Asks: Stop recording, or Keep recording until the stream ends (after a quit, datui waits for it with the terminal handed back). <kbd>Esc</kbd> stays |
| <kbd>Esc</kbd> at the table | Stops following; the recording goes on |
| SIGTERM, SIGHUP | The file is finished and closed, and datui exits |
| WAV | A producer writing to a pipe cannot fill in the RIFF and `data` sizes; they are filled in when the stream ends, as RF64 over 4 GB when the producer reserved a `JUNK` chunk for it. `--tee-raw` leaves FILE exactly as it came |

`--tee -` passes the stream on to standard output instead, as `tee` does, and
draws on the terminal (`/dev/tty`, or the console on Windows): datui sits in
the middle of a pipeline and shows what goes through it. The table reads a
temporary copy in `--temp-dir`. Standard output has to go to a pipe or a file;
the bar says `sent` once the stream ends, and a reader downstream that stops
reading stops the copy, saying so.

```bash
some_logger | datui -f --tee - | gzip > run1.csv.gz
```

Without `-f`, the whole stream is recorded before the table opens. With
`-f`, a format that cannot be followed (a WAV file) shows what had arrived
when it opened, and the recording goes on. The copy holds one megabyte however
long the stream runs; a producer faster than the disk waits on the pipe.
FILE is never removed, whatever happens.

## Directories

`datui <directory>` does what <kbd>Enter</kbd> on that directory's row does on
the [home screen](home-screen.md), and needs no flag:

| The directory | What happens |
|---|---|
| A hive tree, or files that are one table | Opens as one table |
| Separate tables, more than one format, or no data directly inside | Opens the directory browser; choose a file or the first row, which reads them all |
| A Delta, Iceberg or Hudi root | Opens the directory browser with a warning that transaction logs are not applied |

`--hive` means: read this as partitioned, which is the answer for a glob and
for a layout that does not say so itself.

Reader options also affect directory detection. For headerless CSVs, use
`datui --no-header exports/`; otherwise datui may treat the first data rows as
headers and decide the files are separate tables.

During loading, <kbd>Ctrl</kbd>+<kbd>O</kbd> cancels and returns home.
<kbd>Ctrl</kbd>+<kbd>Q</kbd> quits. Other editing keys are not queued during a load.

Every option is listed in [Command Line Options](../reference/command-line-options.md).
Defaults for most of them can be set once in the
[configuration file](../reference/settings.md#file-loading).

## Formats

The format is taken from the extension, or from `--format` when there is none.

### How each format is read

| Format | Extensions | Read | Compressed | HTTP(S) | In a bucket | Bucket prefix |
|---|---|---|---|---|---|---|
| Parquet | `.parquet` | lazy | no | downloaded | in place | in place |
| CSV | `.csv` | lazy | converted once | downloaded | downloaded | in place |
| [Text](#text-and-logs) | `.log`, `.txt` | lazy | converted once | downloaded | downloaded | no |
| TSV, PSV | `.tsv`, `.psv` | lazy | converted once | downloaded | downloaded | no |
| Arrow IPC file, Feather v2 | `.arrow`, `.arrows`, `.ipc`, `.feather` | lazy | no | downloaded | in place | in place |
| [Arrow IPC stream](#arrow-ipc-streams) | `.arrow`, `.arrows`, `.ipc`, `.feather` | converted once | no | downloaded | downloaded | downloaded |
| NDJSON | `.jsonl`, `.ndjson` | in memory | no | downloaded | downloaded | in place |
| JSON | `.json` | in memory | no | downloaded | downloaded | no |
| Avro | `.avro` | in memory | no | downloaded | downloaded | no |
| Excel | `.xlsx`, `.xlsm`, `.xlsb`, `.xls` | in memory | no | downloaded | downloaded | no |
| ORC | `.orc` | in memory | no | downloaded | downloaded | no |
| [SafeTensors](#model-files) | `.safetensors`, `model.safetensors.index.json` | in memory | no | in place | in place | in place |
| [GGUF](#model-files) | `.gguf` | in memory | no | in place | in place | in place |
| [NMEA 0183](#gps-logs) | `.nmea` | converted once | converted once | downloaded | downloaded | no |
| [GPX](#gps-logs) | `.gpx` | converted once | converted once | downloaded | downloaded | no |
| [WAV, BWF, RF64, AIFF](#audio-files) | `.wav`, `.wave`, `.bwf`, `.rf64`, `.aif`, `.aiff`, `.aifc` | lazy | no | downloaded | downloaded | no |
| [MIDI](#midi-files) | `.mid`, `.midi`, `.smf`, `.kar`, `.rmi` | in memory | no | downloaded | downloaded | no |
| [VCD](#vcd-value-change-dumps) | `.vcd` | converted once | converted once | downloaded | downloaded | no |
| [FIX logs](#fix-logs) | any, by content (`8=FIX`), or `--format fix` | converted once | converted once | downloaded | downloaded | no |
| [SDF](#sdf-compound-files) | `.sdf`, `.sd` | converted once | converted once | downloaded | downloaded | no |
| [SQLite](#sqlite-databases) | `.db`, `.sqlite`, `.sqlite3`, `.db3` | lazy | no | downloaded | downloaded | no |
| [NumPy](#numpy-arrays) | `.npy`, `.npz` | lazy | no | downloaded | downloaded | no |
| [ELF](#elf-symbol-tables) | `.elf`, `.axf` | in memory | no | downloaded | downloaded | no |
| [ULog](#flight-logs) | `.ulg` | lazy | no | downloaded | downloaded | no |
| [DataFlash](#flight-logs) | any, by content, or `--format dataflash` | lazy | no | downloaded | downloaded | no |
| [candump](#can-logs) | any, by content, or `--format candump` | lazy | no | downloaded | downloaded | no |
| [systemd journal](#systemd-journal) | any, by content, or `--format journal` | in memory | no | downloaded | downloaded | no |
| [Binary records](binary-formats.md) | any, through a format spec | lazy | converted once | downloaded | downloaded | no |

| Read | What it means |
|---|---|
| lazy | Scanned where it is. Browsing reads a buffer of rows; queries, sorting and analysis may read the whole input |
| converted once | Read through once into a temporary file in the temp directory (`--temp-dir`), which is then scanned lazily. The file is removed on quit ([temporary files](#temporary-files)) |
| in memory | Read whole into memory before the table appears. Past `memory_warning_mb` in `[file_loading]` (1024 MB by default; 0 never asks), datui asks first: `big.json: JSON reads 2.10 GB into memory`. A model file's table is one row per tensor, from the header, so it is small however large the model, and is never asked about; a MIDI file is at most 64 MiB |

- **Compressed** is a `.gz`, `.zst`, `.bz2` or `.xz` file; `no` means it does not
  open. `-c file_loading.decompress_in_memory=true` reads compressed CSV, TSV, PSV
  and text in memory instead.
- **HTTP(S)** is one file at an `http://` or `https://` URL. `downloaded` copies
  it to the temp directory first, then reads it as **Read** says. A model file's
  header is fetched by range; a server that sends no ranges gets the download
  question.
- **In a bucket** is one S3, GCS or Azure object. `in place` reads only what is
  needed with ranged requests: a Parquet or Arrow IPC file's footer and the rows
  shown, or a model file's header. `downloaded` copies the object to the temp
  directory first, after asking, then reads it as **Read** says; an Arrow stream
  is converted as it downloads, with no copy of the stream kept.
- **Bucket prefix** is a prefix or glob read as one table. Only Parquet reads
  hive partitions; the model files directly under a prefix are read by their
  headers. An Arrow prefix scans its IPC files in place and downloads its
  streams, one split of a Hugging Face cache as on disk; a glob of Arrow reads
  IPC files only. A prefix marked `no` opens one object at a time from the
  [cloud browser](cloud-browser.md).
- [Standard input](#standard-input) is written to a temporary file first, then
  read as **Read** says.

The home screen marks a file row that is not read lazily where it is:
`converts`, `in memory` or `downloads`. The details pane and the Info panel's
Resources tab say how it is read.

### Text and logs

A `.log` or `.txt` file, and text no format claims (a pipe, a file with no
extension), is read as lines:

```bash
datui app.log
datui app.log.gz
datui /var/log/nginx/                         # several files: a file column first
journalctl -u nginx | datui
SYSTEMD_PAGER=datui journalctl -u nginx
```

| Column | What |
|---|---|
| `file` | The file the line is from, when there are several |
| `line_no` | Its number, from 1, as `less -N` numbers it |
| `line` | The line as written, without its line ending |

- Every line is a row, blank lines included. `\r\n` is a line ending.
- Bytes that are not UTF-8 are shown as `�`; the Info panel says how many lines
  hold them. Control characters are escaped on screen and kept in the value.
- A `.log` whose bytes say a format (a candump log, a FIX log) is read as that format.
- In a directory, text files beside other data are not part of the table: a
  README beside Parquet files is passed over.
- Find, the query and filters work on `line` as on any column:
  `select where line like "*error*"`.
- Lines are indexed in one pass and read where they are shown. A file of more
  than 67,108,864 lines shows the first that many; the Info panel says how many
  were left out.

### systemd journal

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

### Arrow IPC streams

Read: [converted once](#how-each-format-is-read).

Arrow IPC streams, the format of a Hugging Face `datasets` cache, are told
from IPC files by their first bytes: a `.arrow`, `.arrows`, `.ipc` or `.feather`
file, one with no extension, or one read with `--format arrow`. A stream has no
index of its rows, so it is converted once to an IPC file in the temp directory,
then scanned lazily like any other:

```bash
datui ~/.cache/huggingface/datasets/imdb/plain_text/0.0.0/abc123/imdb-train.arrow
datui ~/.cache/huggingface/datasets/imdb/plain_text/0.0.0/abc123/   # the train split
datui --table test ~/.cache/huggingface/datasets/imdb/plain_text/0.0.0/abc123/
datui my_dataset/                 # save_to_disk shards: data-00000-of-00004.arrow ...
datui --table test my_dataset_dict/   # save_to_disk of a DatasetDict: one split
```

| What | How it opens |
|---|---|
| One stream | Converted, then scanned |
| A directory of stream shards | Converted together into one file, in name order; stream shards with different columns fail |
| IPC files among the streams | Scanned in place, not copied, and stacked with the streams in name order |
| A `datasets` cache directory (`name-train.arrow`, `name-test-00000-of-00002.arrow`, ...) | One split: the one `--table` names, else `train`, `validation`, `test`, then the first by name. The Schema tab lists the others |
| A DatasetDict saved with `save_to_disk` (`dataset_dict.json` and a directory per split) | One split's directory, chosen the same way |
| `cache-*.arrow` files `map()` wrote in a cache directory | Left out; a note on the Notes tab counts them |
| `dataset_info.json`, `state.json` beside `.arrow` files | Left aside as the dataset's metadata; either one marks a cache directory |
| LZ4 or ZSTD buffers | Read; written out uncompressed, so the copy can be larger than the stream |

The loading screen shows how far the conversion has got;
<kbd>Ctrl</kbd>+<kbd>O</kbd> stops it and removes the partial file. `--temp-dir`
chooses where the copy goes, and it is removed with the dataset. A stream larger
than the temp directory's free space is refused before it is written.

### Excel

Read: [in memory](#how-each-format-is-read).

Excel opens the first sheet unless `--table` names another, by index
(`--table 0`) or name (`--table Sales`).

### Model files

Read: [in memory](#how-each-format-is-read), from the header.

```bash
datui model.safetensors
datui Llama-3-8B-Q4_K_M.gguf
datui model.safetensors.index.json     # a sharded checkpoint, as one table
datui path/to/checkpoint/              # the same, from its directory
datui https://huggingface.co/ORG/MODEL/resolve/main/model.safetensors.index.json
datui s3://bucket/checkpoints/llama/   # a prefix of shards, read where it is
```

A SafeTensors or GGUF file opens as a table with one row per tensor. Only the
header is read, so a 70 GB model opens as fast as a small one.

| Format | Columns |
|---|---|
| SafeTensors | `name`, `dtype`, `shape`, `params`, `bytes`, `offset_start`, `offset_end` |
| GGUF | `name`, `type`, `shape`, `params`, `bytes`, `offset` |

- `shape` is a list; `params` is its product.
- GGUF `type` is the quantization type (`Q4_K`, `Q8_0`, `F16`). `bytes` is
  null for a type datui does not know the size of.
- GGUF `shape` lists dimensions as the file does, fastest-varying first.
- Offsets are as the file records them, from the start of the tensor data.
  SafeTensors rows are in data order; GGUF rows are in file order.
- A sharded checkpoint opens from its `model.safetensors.index.json` or its
  directory, with a `file` column first. Several model files named together
  open the same way.
- Files are recognized by their first bytes too, so a `.bin` or a file with
  no extension opens when it is SafeTensors or GGUF. GGUF versions 2 and 3
  are read, in either byte order.

Press <kbd>i</kbd> for the [Model tab](dataset-info.md#model): parameter
count, size, the dtype or quantization mix, and the header's metadata
(`__metadata__`, or GGUF's key/value pairs).

A header that is corrupt, or a tensor that reaches past the end of the file
(a download cut short), is refused with an error.

Remote model files are not downloaded. Only their headers are fetched, with
ranged requests:

| Source | Read |
|---|---|
| HTTP(S), S3, GCS, Azure file | SafeTensors: the first 64 KiB, which holds most headers whole, then the rest of a longer header. GGUF: growing ranges until the tensor list ends |
| `model.safetensors.index.json` | The index, then each shard's header, beside the index's URL, eight shards at a time |
| S3, GCS or Azure prefix | Every SafeTensors or GGUF file directly under it, as a directory on disk. The listing stops at 10,000 objects; the Notes tab says when it did |

A server that does not send byte ranges gets the download question instead,
which says why, and the file is downloaded whole. Shards named by an index
cannot be downloaded this way, and the open says so.

### GPS logs

Read: [converted once](#how-each-format-is-read).

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
Lines that are not NMEA are skipped; the Info panel's Notes tab counts them,
and the sentences that fail their checksum. When the log has sentence types the
table on screen does not show, such as GSV beside the fixes, the Schema tab
names the other tables with how many sentences each has.

A time of day is dated by the last RMC or ZDA before it. Rows read before the
first one are dated back from it when it comes within the first 65,536 rows;
when it comes later, those rows keep a null `time`. A log with neither sentence
has no dates, and `time` is null throughout.

`--table` (`-t`) is for any file that holds several tables: an NMEA log's
sentence types, a [SQLite database](#sqlite-databases)'s tables, an Excel
workbook's sheets, a format spec's variants, or a Hugging Face cache directory's
splits ([Arrow IPC streams](#arrow-ipc-streams)). Any other file opened with it
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

#### The gap column

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
| Dropouts | Sort by `gap`, largest first; or in [Data Quality](data-quality.md#declare-what-a-column-must-hold) declare a range for `gap`, such as at most 2, and each dropout is **Out of range** |
| Speed spikes | The same for `speed`; Analysis also counts its outliers |

### Audio files

Read: [lazy](#how-each-format-is-read).

```bash
datui take.wav
datui -c file_loading.audio_float=true take.wav   # integer samples as float in [-1, 1]
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
  unsigned, is shown signed. `[file_loading] audio_float` shows them as `f32` in [-1, 1];
  float samples are never rescaled.
- A data size of 0 or a placeholder, as a recorder writes until it stops, is
  read as everything to the end of the file. A size past the end of the file
  is cut to what the file holds, and the Audio tab says so. A plain WAV past
  4 GiB, whose 32-bit data size wrapped, is read to its whole length.
- Files are recognized by their first bytes too, so a WAV or AIFF with any
  name opens.
- Compressed audio (A-law, mu-law, ADPCM, MP3, FLAC) is refused with its name.

Press <kbd>i</kbd> for the [Audio tab](dataset-info.md#audio): the format,
sample rate, length, the Broadcast WAV (`bext`), iXML and `LIST INFO`
fields, and the `cue `/`MARK` markers with their labels.

A line chart of a long recording draws each step's lowest and highest sample
([Charting](charting.md#large-tables)), and a full
[Data Quality](data-quality.md) run reports clipping, runs of zeros and DC
offset.

### MIDI files

Read: [in memory](#how-each-format-is-read).

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

Press <kbd>i</kbd> for the [MIDI tab](dataset-info.md#midi): format, timing,
length, tempo, meter, key and each track's name, events, notes and channels.
Notes that never end are counted on the Notes tab.

### SQLite databases

Read: [lazy, in place](#how-each-format-is-read); a database in a bucket or over
HTTP(S) is downloaded first.

```bash
datui shop.db                        # its one table, or the list of its tables
datui shop.db --table orders         # a table or view by name
datui shop.db/orders                 # the same
cat shop.db | datui --table orders   # from standard input
```

| The database | What happens |
|---|---|
| One table or view of its own | Opens it |
| Several | Opens the home screen inside the database: a row per table and view, like a directory of tables. <kbd>Enter</kbd> opens one; <kbd>q</kbd> comes back to the list |
| Several, downloaded or piped in | Refused with the names of its tables; `--table` picks one |

SQLite's own tables (`sqlite_master`, `sqlite_sequence`, the `sqlite_stat`
tables, a full-text index's shadow tables) are hidden until
<kbd>Ctrl</kbd>+<kbd>A</kbd>; `--table` opens them by name. A file is known by
its first bytes whatever it is called. The home screen labels a database with
its tables (`3 tables`).

A table is read in place; nothing is copied.

| | How |
|---|---|
| The rows on screen | Read from SQLite a page at a time, by the table's rowid (or primary key), so the first rows show at once and <kbd>End</kbd> costs what the top does |
| Row count | SQLite's `count(*)`, in the background |
| Sort and filters from the sidebar | Run in SQLite as `ORDER BY` and `WHERE`, so an index on the column serves them. Ties keep the table's order and nulls sort last, as for any other file |
| A query, analysis, Data Quality, a chart, an export | Read the columns they use from SQLite a batch at a time; what they hold is in memory, as for a JSON or Excel file. A query's simple comparisons run in SQLite |
| Leaving the table (<kbd>Ctrl</kbd>+<kbd>O</kbd>, quit) | Stops whatever SQLite is running for it |

A view is paged by position and cannot be reversed with <kbd>r</kbd> in SQLite
(Polars does it). A sort on a column without an index has SQLite sort the
rows for each page; SQLite may use temporary files in the temp directory to do
so.

Columns are typed by what they declare, as SQLite reads a declared type:

| Declared | Column |
|---|---|
| `INTEGER`, `INT`, `BIGINT`, anything with `INT` | `i64` |
| `REAL`, `FLOAT`, `DOUBLE` | `f64` |
| `TEXT`, `VARCHAR(n)`, `CLOB` | `str` |
| `BLOB` | `binary` |
| nothing, `NUMERIC`, `DECIMAL`, `BOOLEAN`, `DATE` | by the values in the first 1,000 rows: whole numbers `i64`, numbers `f64`, blobs `binary`, text `str` |

SQLite lets a column hold values of any type. A column whose first 1,000 rows
hold values of several types is read as text, numbers as SQLite writes them and
blobs as `X'0A1B'`. After the open, one pass over the table checks the rest: a
value further on that is not a number, in a number column, reads as null, and
the Info panel's Notes tab says how many. Dates stay text, as SQLite stores
them.

The database is only read:

| | |
|---|---|
| Opened | Read only, with `query_only` and defensive mode. Extensions cannot be loaded, and reading a table runs no trigger |
| A WAL database with a `-wal` file | Read through it, so what another program has committed is seen. SQLite creates the `-shm` index beside it if it is missing |
| A WAL database without a `-wal` file | Read as the file stands (SQLite's `immutable`), writing nothing and taking no lock. A program that starts writing it during the read can make the read fail or come out wrong |
| A `-wal` without its `-shm`, in a directory datui cannot write to | An error: read without the WAL, it would lack what was committed there |
| A `-journal` left by a program that stopped mid-write | An error: datui does not roll it back. Opening the database once with the `sqlite3` tool does |
| A program writing the database meanwhile | datui waits up to 2 seconds for its lock. Without WAL, the program cannot commit while datui reads, which is a page at a time except for a whole-table read |
| Not a SQLite database, or damaged | An error |

A compressed database (`shop.db.gz`) is not read; decompress it first.

### NumPy arrays

Read: [lazy](#how-each-format-is-read), from a map of the file; an array
compressed in an `.npz` archive is decompressed once to a temporary file first.

```bash
datui prices.npy
datui run.npz                    # its one array, or the list of its arrays
datui run.npz --table weights    # an array by name
datui run.npz/weights            # the same
```

| The array | Columns |
|---|---|
| 1-D | One, named for the file (`prices.npy` is `prices`) or the archive's array |
| 2-D | One per index, `0` to `n-1`; more than 1,024 make one Array column, `values` |
| Structured (`[('ts', '<u8'), ('px', '<f8')]`) | One per field; a nested field is `outer.inner`, a subarray field an Array column |
| 0-D | One row |
| 3-D or more | An error that gives the shape |

| `dtype` | Column |
|---|---|
| `b1` | `bool` |
| `i1` to `i8`, `u1` to `u8` | The integer of that width |
| `f2`, `f4` | `f32` |
| `f8` | `f64` |
| `c8`, `c16` | An Array of two floats: real, imaginary |
| `S` | `str`, NUL padding trimmed |
| `U` | `str`, from UTF-32 |
| `V` | `binary` |
| `M8[ns]`, `M8[us]`, `M8[ms]` | `datetime` in that unit |
| `M8[s]`, `M8[m]`, `M8[h]` | `datetime[ms]` |
| `M8[D]` | `date` |
| `m8[...]` | `duration`, by the same units |
| `M8` and `m8` in months, years or finer than `ns` | `i64`, the count as stored |
| `O` | An error: Python objects are pickled, and datui does not unpickle |

- `NaT` is null.
- Big-endian (`>i4`) and little-endian fields mix in one array.
- Fortran (column-major) order reads the same as C order.
- Padding fields (`align=True`) are left out, and fields at offsets
  (`offsets`, `itemsize`) are read where they are.
- A file shorter than its shape says shows the rows it holds; the Notes tab
  says so.
- A file named without `.npy` is known by its first bytes, `\x93NUMPY`.

An `.npz` archive of several arrays opens the home screen inside it, a row per
array in the order they were saved, like a directory. <kbd>Enter</kbd> opens
one; <kbd>q</kbd> comes back to the list. An archive downloaded or piped in is
refused with the names of its arrays; `--table` picks one. An array saved with
`np.savez` is read in place in the archive; one saved with
`np.savez_compressed` is decompressed to the temp directory and removed when
the dataset closes.

Press <kbd>i</kbd> for the NumPy tab: shape, type, order and format version,
and each field's type and offset.
### ELF symbol tables

Read: [in memory](#how-each-format-is-read): the symbol and section tables,
from a map of the file.

```bash
datui firmware.elf                    # one row per symbol
datui firmware.elf --table sections   # one row per section
datui firmware.elf/sections           # the same
```

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

### Flight logs

Read: [lazy](#how-each-format-is-read): one pass indexes the log, then each
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

### CAN logs

Read: [lazy](#how-each-format-is-read): one pass indexes the log, then each
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

With a DBC file that names the log's messages, the log opens the home screen
inside it, like a directory: a table per message with frames, `frames`, and
`signals`.

| Table | Columns |
|---|---|
| A message, by its DBC name | `ts` and a column per signal: factor and offset applied, an integer while they keep it one; value names (`VAL_`) as text; a multiplexed signal null in the frames its multiplexer does not select. Units are on the Info panel's Schema tab |
| `signals` | One row per decoded value: `ts`, `message`, `signal`, `value` (a float) and `unit`, in time order |

Signals in Intel and Motorola byte order, signed and unsigned, and floats
(`SIG_VALTYPE_`) are read; a signal past the end of a short frame is null.
Extended multiplexing (`SG_MUL_VAL_`) is not; the Notes tab says so.

#### DBC files

DBC files are found where [format specs](binary-formats.md) are: the `formats`
directory of the config directory, `$DATUI_FORMATS_PATH`, and `formats_path`.
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
interfaces, the DBC files read and the frames none of them names, and each
message's id, frames, signals and comment.

### VCD value change dumps

Read: [converted once](#how-each-format-is-read).

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
[Pivot](reshaping.md#pivot) (<kbd>p</kbd>) with Index `time`, Columns `signal`,
Values `value` and Aggregate `last` gives the same table with nulls between
changes.

### FIX logs

Read: [converted once](#how-each-format-is-read).

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

#### FIX dictionaries

The built-in dictionary is FIX 4.2, 4.4 and 5.0 SP2 together, the newest
version's names winning. Venues and brokers add their own tags (5000-9999 and
10000 up), so dictionaries can be added: on the
[format search path](binary-formats.md#where-specs-live), or with
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
FIX tab shows both names. `datui formats` lists FIX dictionaries beside the
binary specs, and `datui formats check NAME [LOG]` checks one, and with a log
says how many messages it matches and which of its tags they hold.

The built-in dictionary is generated from QuickFIX's data dictionaries. This
product includes software developed by quickfixengine.org
(http://www.quickfixengine.org/).

### SDF compound files

Read: [converted once](#how-each-format-is-read).

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
  [public datasets](home-screen.md#public-datasets) is one to try: 1,025
  molecules with `SOL` as a float and `SOL_classification` as text. Sort by
  `SOL`, or filter `SOL_classification` to `(C) high`.

### CSV options

Read: [lazy, or converted once when compressed](#how-each-format-is-read).

They apply to `.tsv` and `.psv` files too. A directory of CSVs in a bucket takes
all of them except `--infer-types` and `--header-rows`, and
`--skip-initial-space` only removes the padding there: values stay text.

| Option | Config key | What it does |
|---|---|---|
| `--delimiter ';'` | | Column separator: one character, `tab`, `\t` or a code such as `0x1f`. Default `,` for `.csv`, tab for `.tsv`, `\|` for `.psv` |
| `--no-header` | | The first row is data, not names. <kbd>H</kbd> does the same, or undoes it, on the file on screen |
| `--skip-lines N`, `--skip-rows N` | | Ignore a preamble |
| `--footer-rows N` | | Ignore a footer. Counts every row first, and the loading screen says `Counting rows to skip the footer` meanwhile: on a directory in a bucket, that downloads every file before the table opens |
| `--null NA`, `--null amount=` | `null_values` | Values to read as null, for every column or one (`COL=VAL`, the name as shown). Repeatable; replaces the config's list |
| `--comment '#'` | `comment_char` | Skip lines that start with it, before the header and among the data. The header is the first line that is not a comment |
| `--header-rows 3`, `--header-rows 3,2` | `header_join` | The line or lines holding the header, counted from 1 at the top of the file. Several are joined per column, in the order given, with `header_join` (default a space) |
| `--skip-initial-space` | `skip_initial_space` | Ignore the spaces after a delimiter: padded numbers are numbers and a cell of spaces is null |
| `--infer-rows 10000` | `infer_schema_length` | Rows used to infer column types (default 1000). Raise it when a column turns from integer to text late in the file |
| `--ignore-errors` | `ignore_errors` | Skip rows that fail to parse instead of failing the load |
| `--infer-types=COL`, `--infer-types=off` | `parse_strings`, `parse_dates` | Trim and type-infer string columns, dates included; limit it to named columns, or turn it off |

Column names are always trimmed: `"     Latitude"` reads as `Latitude`. A blank
name reads as `column_N`, and a repeated one gets `_duplicated_0`.

### Instrument and logger exports

Loggers often write comments and a units line above a padded header:

```
#device_info, log_version="1.03", model="X", serial="123"
#yyyy-mm-dd, hh:mm:ss, hh:mm, degrees, volts, deg F
  Lcl Date, Lcl Time, UTCOfst,     Latitude, bus1volts, E1 CHT1
          ,         ,        ,             ,      25.1,   187.2
```

| Command | Columns |
|---|---|
| `datui --comment '#' log.csv` | `Lcl Date`, `Latitude`, …; `#` lines anywhere are skipped |
| `datui --comment '#' --header-rows 3,2 log.csv` | `Lcl Date yyyy-mm-dd`, `Latitude degrees`, … |
| `datui --header-rows 3 log.csv` | `Lcl Date`, `Latitude`, …; lines 1 and 2 are passed over |

`--header-rows` counts lines before anything is skipped, and a named line that
starts with the comment character loses it. `--skip-lines` counts from the same
top; `--skip-rows` counts data rows after the header. <kbd>H</kbd> reads the
named lines as data. A file with nothing after its header lines opens with its
columns and no rows.

A [delimited format spec](binary-formats.md#delimited-text) holds these options
for a family of files, with their units and metadata line, so they open with no
flags. A flag typed on the command line still wins over the spec.

Padded numbers become numbers with or without `--skip-initial-space`, as long
as string parsing is on (the default). With the flag, cells of spaces are null
in text columns too, and `--null` matches the value without its padding.
Typing follows `--infer-types`: with `--infer-types=off` the padding goes but
the columns stay text, and `--infer-types=COL` types only the columns named.

### Dates and timestamps

String columns in CSV and JSON become dates when every value in the first 1000
rows (`--infer-rows`) parses the same way. `--infer-types=off` turns this off.

| Value | Type |
|---|---|
| `2024-01-31` | `date` |
| `2024-01-31 10:00:00`, `2024-01-31T10:00:00.250` | `datetime[μs]` |
| `2024-01-31T10:00:00Z`, `2024-01-31T10:00:00.250+00:00`, `2024-01-31 05:00:00-05:00` | `datetime[μs, UTC]`, converted to UTC |

A column whose values disagree, such as an offset on some and none on others,
stays text. A value past those rows that does not parse is null. JSON strings
become dates or times, never numbers.

With `--infer-types=off`, Polars decides from the rows it reads for the schema,
and a value it cannot parse fails the read. A directory of CSV or NDJSON files
in a bucket keeps them as text.

## Compression

Files ending in `.gz`, `.zst`, `.bz2` or `.xz` are decompressed before loading.
Use `--compression gzip|zstd|bzip2|xz` when the extension is missing or wrong.

Compressed CSV, TSV or PSV is decompressed to a temporary file so it can still
be scanned lazily. `--temp-dir` chooses where; `-c file_loading.decompress_in_memory=true`
skips the file and reads the whole thing into memory instead.

### Temporary files

A decompressed text file, a converted Arrow stream or GPS log, or a downloaded file
lives in the temp directory while datui uses it.

| Exit | Temporary files |
|---|---|
| `q`, Ctrl+Q, Ctrl+C, an error | Removed, including a partial file mid-download, mid-decompression or mid-conversion |
| SIGTERM, SIGHUP (closing the terminal) | Removed by the `datui` command, which quits as for `q` and exits with status 128 + the signal. Left by `datui.view()` in Python, which leaves signals to Python |
| Windows: closing the console window, signing out, shutting down | Removed by the `datui` command, which quits as for `q` |
| SIGKILL, ending the task in Task Manager | Left in the temp directory |

On Windows a file cannot be removed while datui still reads it through a memory map.
One that would not go is tried again as datui quits.

## Hive-partitioned data

A directory tree whose segments are `key=value` (`year=2024/month=01/...`)
opens as one table. Pass the root directory, which needs no flag, or a glob with
`--hive`; a glob usually needs quoting so your shell leaves it alone. A path
that exists is never a glob: `d[1].parquet` opens that file, not `d1.parquet`. Only
Parquet is supported — for a hive tree of anything else, open one partition.

Partition columns appear first in the table and on the **Partitions** tab of the
[Info panel](dataset-info.md). Local directories use datui's schema union, counts and notes; local globs are
delegated to Polars. Remote prefixes and globs both use datui's metadata reader.

### Files that disagree

By default, datui combines schemas from Parquet footers. It does not need to
scan data values to find the columns.

| Across the files | In the table |
|---|---|
| A column appears in only some files | Shown, with nulls for files missing the column |
| Compatible types, such as `Int32` and `Int64` | Widened to a shared type |
| Incompatible types, such as numbers and text | Uses the type with the most rows; values of incompatible types are not read |
| An unreadable footer | Skips that file |

The [Info panel](dataset-info.md) reports the metadata scope. Above 20,000
files, datui samples evenly across the file list. A dataset of more than 64
files may also start with a partial schema while the remaining footers load.

When all file row counts are known, empty cells distinguish three cases:

| Cell | Meaning |
|---|---|
| `∅` | A null value |
| `·` | The source file has no such column |
| `≠` | The source file stores an incompatible type, so the value was not read |

Column-name markers also identify missing or conflicting fields. When row
counts are incomplete, empty cells all display as `∅`, though the column
markers remain. Queries, pivots and other transformations create new rows
without this file-level distinction. Exports write all three cases as null.

To recover conflicting values, open the column's note in **Info → Notes**
and apply **read as text**, if offered. This reuses the existing metadata.
Lists, arrays, durations, binary and unknown types cannot use this action.
Filters and sorting then compare strings: `"10"` sorts before `"2"`.
Compatible numeric types still widen as usual; their mixed-type note remains.

**Sidebar filters and sorting can exclude conflicting rows.** When every
file's row count is known, using a conflicting column removes rows from files
that store its incompatible type. A note reports the affected count. Clearing
that filter or sort restores the rows. With incomplete row counts, those rows
remain as nulls instead.

This exclusion applies even to an OR filter: `id = 3 OR n = 0` still removes
rows from files where `n` has an incompatible type. Queries in the
[query bar](querying-data.md) create a separate result and do not apply this
file-level rule. Missing-column (`·`) rows remain during sorting; filters
handle missing values as nulls.

### How large datasets open

Directories with more than 64 Parquet files, local or in the cloud, open using
the first and last files by name, then read the remaining footers in the
background. New columns join the end of the table, and the total row count
arrives, when they land. Until then:

- The total row count is unavailable and empty cells display as `∅`.
- Notes state the partial metadata scope.
- A query, pivot or drill-down defers the new columns until you return to the original data.

Above 20,000 files the background pass reads a sample, and the row count reads
the footers the sample skipped. Once every footer is read, the dataset is cached
and a reopen reads none. The control bar reports footer-reading progress.

A large S3 or Google Cloud prefix is listed in parallel key ranges. While it is
listed, the loading screen counts the objects found: `Listing files: 412,000`.

The Notes tab also flags storage layouts that may explain a slow open:

| Finding | Why it matters |
|---|---|
| Median row-group size above 64 MiB | Reading a page may require a large row group |
| More than 10,000 files, median size below 1 MiB | Many metadata reads before data can be displayed |
| Different partition keys, such as `date` and `dt` | Partition columns vary across the dataset; key order alone is fine |

`-c file_loading.single_spine_schema=false` skips datui's footer union and uses
Polars' single-file schema inference. That route also omits the partition-key check.

### Opening it again

Schema metadata of a remote dataset, or of a local directory of more than 64
files, is cached by URL or path. Datui still lists the files to check for
changes to names, sizes, timestamps or etags. An unchanged listing opens with
no footer reads; a changed one triggers fresh metadata reads. The cache keeps
128 MiB of this metadata and drops the least recently opened dataset first.

`datui cache clear` clears this metadata along with other cached state, including
query history. See [cache contents](home-screen.md#what-datui-remembers).

## Binary columns

A binary column shows a dim `‹binary›` placeholder instead of its bytes, so
scrolling past large blobs stays fast. The bytes are still read for exports and
analysis. The placeholder color is `binary_col` in the
[theme](../reference/settings.md#colors).

## Remote data

```bash
datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/
datui gs://cloud-samples-data/bigquery/us-states/us-states.parquet
datui abfss://release@overturemapswestus2.dfs.core.windows.net/
datui https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv
```

Each of these is public and opens with no login.

[Remote data](remote-data.md) explains credentials, public access and what gets
downloaded. Use the [cloud browser](cloud-browser.md) to find data without
typing a URL.

| Connect to | Setup |
|---|---|
| AWS | [S3](remote-data.md#amazon-s3) · [Profiles and SSO](remote-data.md#aws-profiles) |
| S3-compatible storage | [Custom endpoint](remote-data.md#s3-compatible-storage-minio-r2-ceph) · [Multiple stores](remote-data.md#several-stores-at-once) |
| Google Cloud | [GCS](remote-data.md#google-cloud-storage) |
| Azure | [Blob Storage](remote-data.md#azure-blob-storage) |
| Public data | [No-login access](remote-data.md#public-data) |
| Web URL | [HTTP and HTTPS](remote-data.md#http-and-https) |

<a id="amazon-s3"></a>
<a id="aws-profiles"></a>
<a id="s3-compatible-storage-minio-r2-ceph"></a>
<a id="several-stores-at-once"></a>
<a id="google-cloud-storage"></a>
<a id="azure-blob-storage"></a>
<a id="public-data"></a>
<a id="http-and-https"></a>
<a id="building-without-cloud-support"></a>
