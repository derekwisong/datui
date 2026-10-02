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
| gzip, zstd, bzip2 or xz magic number | compressed CSV, or TSV or PSV with `--format` |
| `[` | JSON |
| `{`, the first line a whole object | NDJSON |
| `{`, the object open past the first line | JSON |
| an NMEA sentence (`$GPGGA,`) with a checksum that matches, or of a type receivers write | NMEA |
| XML whose first element is `<gpx` | GPX |
| a first line with tabs and no commas | TSV |
| anything else | CSV |

With `--format csv`, `tsv` or `psv` and no `--compression`, compression still
comes from the first bytes.

The CSV options below apply. The dataset is named `stdin`. It is not added to
recent datasets, and [views](views.md) match it by its columns only.

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

| Format | Extensions | Lazy | Hive partitions |
|---|---|---|---|
| Parquet | `.parquet` | yes | yes |
| CSV and other delimited text | `.csv`, `.tsv`, `.psv` | yes | |
| Arrow IPC, Feather v2 | `.arrow`, `.arrows`, `.ipc`, `.feather` | yes | |
| NDJSON | `.jsonl` | | |
| JSON | `.json` | | |
| Avro | `.avro` | | |
| Excel | `.xlsx`, `.xlsm`, `.xlsb`, `.xls` | | |
| ORC | `.orc` | | |
| SafeTensors | `.safetensors`, `model.safetensors.index.json` | header only | |
| GGUF | `.gguf` | header only | |
| NMEA 0183 | `.nmea` | read once to a temporary file | |
| GPX | `.gpx` | read once to a temporary file | |
| WAV, BWF, RF64, AIFF | `.wav`, `.wave`, `.bwf`, `.rf64`, `.aif`, `.aiff`, `.aifc` | yes | |
| MIDI | `.mid`, `.midi`, `.smf`, `.kar`, `.rmi` | | |
| [Binary records](binary-formats.md) | any, through a format spec | yes | |

**Lazy** formats are scanned as needed. Browsing reads a buffer of rows;
queries, sorting and analysis may read the full input. The other formats are
loaded in full before the table appears, except a directory of NDJSON files in
a bucket, which is scanned.

**Arrow IPC streams**, the format of a Hugging Face `datasets` cache, are told
from IPC files by their first bytes: a `.arrow`, `.arrows`, `.ipc` or `.feather`
file, one with no extension, or one read with `--format arrow`. A stream has no
index of its rows, so it is converted once to an IPC file in the temp directory,
then scanned lazily like any other:

```bash
datui ~/.cache/huggingface/datasets/imdb/plain_text/0.0.0/abc123/imdb-train.arrow
datui my_dataset/                 # save_to_disk shards: data-00000-of-00004.arrow ...
```

| What | How it opens |
|---|---|
| One stream | Converted, then scanned |
| A directory of stream shards | Converted together into one file, in name order; shards with different columns fail |
| `dataset_info.json`, `state.json` beside `.arrow` files | Left aside as the dataset's metadata |
| LZ4 or ZSTD buffers | Read; written out uncompressed, so the copy can be larger than the stream |

The loading screen shows how far the conversion has got;
<kbd>Ctrl</kbd>+<kbd>O</kbd> stops it and removes the partial file. `--temp-dir`
chooses where the copy goes, and it is removed with the dataset. A temp directory
with less free space than the streams take is refused before anything is written.

**Excel** opens the first sheet unless `--sheet` names another, by index
(`--sheet 0`) or name (`--sheet Sales`).

### Model files

```bash
datui model.safetensors
datui Llama-3-8B-Q4_K_M.gguf
datui model.safetensors.index.json     # a sharded checkpoint, as one table
datui path/to/checkpoint/              # the same, from its directory
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
(a download cut short), is refused with an error. Remote model files are downloaded whole before they open.

### GPS logs

```bash
datui drive.nmea
datui --table GSV drive.nmea         # one row per satellite in view
head -n 3000 /dev/ttyACM0 | datui    # NMEA from standard input
datui ride.gpx
```

An NMEA 0183 log or a GPX file is read once, start to end, into a temporary
Arrow IPC file, which is then scanned like any other: memory stays at one batch
of rows however long the log. A file with another name, such as `capture.log`,
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

`--table` is for any file that holds several tables. NMEA logs are the only
ones so far; any other file opened with it is refused. Excel workbooks take
`--sheet`.

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

```bash
datui take.wav
datui take.wav --normalize      # integer samples as float in [-1, 1]
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
  unsigned, is shown signed. `--normalize` shows them as `f32` in [-1, 1];
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
| `time` | The same as a duration, through the tempo map |
| `kind` | `note_on`, `note_off`, `cc`, `program`, `pitch_bend`, `poly_aftertouch`, `channel_aftertouch`, `sysex`, `sysex_escape`, or a meta event: `tempo`, `time_signature`, `key_signature`, `track_name`, `instrument`, `lyric`, `marker`, `cue`, `text`, `copyright`, `end_of_track`, ... |
| `channel` | 1-16, as a sequencer numbers them |
| `note`, `note_name` | The note number and its name, middle C (60) as `C4` |
| `velocity` | For `note_on` and `note_off` |
| `controller` | The controller number of a `cc` |
| `value` | The `cc` value, program, pressure, pitch bend (-8192 to 8191), tempo in microseconds per quarter, key signature in sharps (negative for flats), or a sysex's length |
| `length` | On a `note_on`, the time until its `note_off`; null for a note that never ends |
| `text` | Meta text, tempo as `120 bpm`, `6/8`, `D major`, or sysex bytes in hex |

- A `note_on` at velocity 0 is a `note_off`, as the specification says.
- Formats 0 and 1 share one tempo map, from tempo events in any track; each
  format 2 track keeps its own. With SMPTE timing, `time` follows the frame
  rate and tempo events do not change it.
- Meta text is read as UTF-8, or as Latin-1 when it is not.
- Files are recognized by their first bytes too, so a MIDI file with any name
  opens, and so does one in a RIFF MIDI (`.rmi`) wrapper.
- A track that runs past the end of the file, an event cut short, or fewer
  tracks than the header says is refused with an error. Of a directory, a file
  that cannot be read is left out; the Notes tab says so and the MIDI tab lists
  each one with why.
- Files over 64 MiB, or more than 10 million events in all, are refused.

Press <kbd>i</kbd> for the [MIDI tab](dataset-info.md#midi): format, timing,
length, tempo, meter, key and each track's name, events, notes and channels.
Notes that never end are counted on the Notes tab.

### CSV options

They apply to `.tsv` and `.psv` files too. A directory of CSVs in a bucket takes
all of them except `--parse-strings`, `--parse-dates` and `--header-rows`, and
`--skip-initial-space` only removes the padding there: values stay text.

| Option | Config key | What it does |
|---|---|---|
| `--delimiter 9` | | Column separator as an ASCII code (`59` for `;`, `124` for `\|`). Default `,` for `.csv`, tab for `.tsv`, `\|` for `.psv` |
| `--no-header` | | The first row is data, not names. <kbd>H</kbd> does the same, or undoes it, on the file on screen |
| `--skip-lines N`, `--skip-rows N` | | Ignore a preamble |
| `--skip-tail-rows N` | | Ignore a footer. Counts every row first: on a directory in a bucket, that downloads every file before the table opens |
| `--null-value NA`, `--null-value amount=` | | Values to read as null, for every column or one (`COL=VAL`, the name as shown). Repeatable |
| `--comment-char '#'` | `comment_char` | Skip lines that start with it, before the header and among the data. The header is the first line that is not a comment |
| `--header-rows 3`, `--header-rows 3,2` | `header_join` | The line or lines holding the header, counted from 1 at the top of the file. Several are joined per column, in the order given, with `header_join` (default a space) |
| `--skip-initial-space` | `skip_initial_space` | Ignore the spaces after a delimiter: padded numbers are numbers and a cell of spaces is null |
| `--infer-schema-length 10000` | `infer_schema_length` | Rows used to infer column types (default 1000). Raise it when a column turns from integer to text late in the file |
| `--ignore-errors` | `ignore_errors` | Skip rows that fail to parse instead of failing the load |
| `--parse-dates=false` | `parse_dates` | Stop parsing date-looking strings as Date and Datetime |
| `--parse-strings=COL`, `--no-parse-strings` | | Trim and type-infer string columns; limit it to named columns, or turn it off |

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
| `datui --comment-char '#' log.csv` | `Lcl Date`, `Latitude`, …; `#` lines anywhere are skipped |
| `datui --comment-char '#' --header-rows 3,2 log.csv` | `Lcl Date yyyy-mm-dd`, `Latitude degrees`, … |
| `datui --header-rows 3 log.csv` | `Lcl Date`, `Latitude`, …; lines 1 and 2 are passed over |

`--header-rows` counts lines before anything is skipped, and a named line that
starts with the comment character loses it. `--skip-lines` counts from the same
top; `--skip-rows` counts data rows after the header. <kbd>H</kbd> reads the
named lines as data. A file with nothing after its header lines opens with its
columns and no rows.

Padded numbers become numbers with or without `--skip-initial-space`, as long
as string parsing is on (the default). With the flag, cells of spaces are null
in text columns too, and `--null-value` matches the value without its padding.
Typing follows `--parse-strings`: with `--no-parse-strings` the padding goes but
the columns stay text, and `--parse-strings=COL` types only the columns named.

### Dates and timestamps

String columns in CSV and JSON become dates when every value in the first 1000
rows (`parse_strings_sample_rows`) parses the same way. `--parse-dates=false`
turns this off.

| Value | Type |
|---|---|
| `2024-01-31` | `date` |
| `2024-01-31 10:00:00`, `2024-01-31T10:00:00.250` | `datetime[μs]` |
| `2024-01-31T10:00:00Z`, `2024-01-31T10:00:00.250+00:00`, `2024-01-31 05:00:00-05:00` | `datetime[μs, UTC]`, converted to UTC |

A column whose values disagree, such as an offset on some and none on others,
stays text. A value past those rows that does not parse is null. JSON strings
become dates or times, never numbers.

With `--no-parse-strings`, Polars decides from the rows it reads for the schema,
and a value it cannot parse fails the read. A directory of CSV or NDJSON files
in a bucket keeps them as text.

## Compression

Files ending in `.gz`, `.zst`, `.bz2` or `.xz` are decompressed before loading.
Use `--compression gzip|zstd|bzip2|xz` when the extension is missing or wrong.

Compressed CSV, TSV or PSV is decompressed to a temporary file so it can still
be scanned lazily. `--temp-dir` chooses where; `--decompress-in-memory` skips
the file and reads the whole thing into memory instead.

### Temporary files

A decompressed text file, a converted Arrow stream or GPS log, or a downloaded file
lives in the temp directory while datui uses it.

| Exit | Temporary files |
|---|---|
| `q`, Ctrl+Q, Ctrl+C, an error | Removed, including a partial file mid-download, mid-decompression or mid-conversion |
| SIGTERM, SIGHUP (closing the terminal) | Removed by the `datui` command, which quits as for `q` and exits with status 128 + the signal. Left by `datui.view()` in Python, which leaves signals to Python |
| SIGKILL | Left in the temp directory |

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
files, datui samples evenly across the file list. A cloud dataset may also
start with a partial schema while the remaining footers load.

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

### How large remote datasets open

Cloud directories with more than 64 files open using the first and last files
by name, then read the remaining footers in the background. New columns join
the end of the table as they are found. Until then:

- The total row count is unavailable and empty cells display as `∅`.
- Notes state the partial metadata scope.
- A query, pivot or drill-down defers the new columns until you return to the original data.

Local directories read metadata before opening. Above 20,000 files, both
routes use a sample. The control bar reports footer-reading progress.

The Notes tab also flags storage layouts that may explain a slow open:

| Finding | Why it matters |
|---|---|
| Median row-group size above 64 MiB | Reading a page may require a large row group |
| More than 10,000 files, median size below 1 MiB | Many metadata reads before data can be displayed |
| Different partition keys, such as `date` and `dt` | Partition columns vary across the dataset; key order alone is fine |

`--single-spine-schema=false` skips datui's footer union and uses Polars'
single-file schema inference. That route also omits the partition-key check.

### Opening it again

Remote schema metadata is cached by URL. Datui still lists the files to check
for changes to names, sizes, timestamps or etags. A changed listing triggers
fresh metadata reads.

`--clear-cache` clears this metadata along with other cached state, including
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
