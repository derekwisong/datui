# Info panel

<kbd>i</kbd> at the table opens the Info panel: the dataset's schema, what its
format records, how it is stored and read, and what datui noticed about it.
<kbd>i</kbd> or <kbd>Esc</kbd> closes it.

![Info Panel Demo](../demos/03-info.gif)

| Tab | Shows |
|---|---|
| **Schema** | Row and column counts, column types, schema source, file coverage, and a Parquet file's per-column compression. A dataset a [collection](../reference/sources.md#documentation) documents adds what each column means (`About`), the selected column's note and codes below, and the documentation's link |
| Format's own | What the file says besides its rows, named for its format; see [the table below](#tabs-by-format) |
| **Metadata** | The metadata line a [delimited format spec](../formats/format-specs.md#delimited-text) names, as key and value; appears for files read through one |
| **Resources** | File size, how the file is read, buffered memory, and loading measurements |
| **Partitions** | Partition columns for a hive-partitioned dataset |
| **Notes** | Schema differences, skipped files and other findings; appears when there are notes |

The file size, and a Parquet, Arrow IPC, Avro or ORC file's tab, are read in the
background the first time the panel opens for a dataset: the few KB at an end of the
file the open read too, and none of its rows. Until they arrive the size and the tab
read `reading...`; a file that cannot be read shows why in their place. Remote
sources, directories, globs, datasets of several files, and compressed or streamed
copies have no file size and no such tab.

## Tabs by format

Each format has its own tab beside Schema, or none, and a file that holds several
tables lists them on the [home screen](home-screen.md) as places inside it
(`shop.db/orders`, `book.xlsx/Sales`) that recents record and Enter opens.

| Format | Tab | Lists inside the file |
|---|---|---|
| Parquet | **Parquet** | no |
| Arrow IPC | **Arrow** | no |
| Avro | **Avro** | no |
| ORC | **ORC** | no |
| Excel | **Excel** | tables |
| SQLite | **SQLite** | tables |
| CSV, TSV, PSV, JSON, NDJSON | none | no |
| SafeTensors, GGUF | **Model** | no |
| NMEA | **GPS** | tables |
| GPX | **GPS** | no |
| audio | **Audio** | no |
| MIDI | **MIDI** | no |
| VCD | **VCD** | no |
| FIX | **FIX** | no |
| SDF | **SDF** | no |
| NumPy | **NumPy** | tables |
| ELF | **ELF** | tables |
| ULog | **ULog** | tables |
| DataFlash | **DataFlash** | tables |
| candump | **CAN** | tables |
| systemd journal | **Journal** | no |
| text | none | no |

CSV, TSV, PSV, JSON, NDJSON and plain text have no tab of their own: text holds its rows and nothing else.
An `.xlsx` or `.xlsm` workbook lists its worksheets
from its directory, without reading them; an `.xls` or `.xlsb` file keeps its worksheet
names where only reading the workbook finds them, so it opens its first worksheet and
`--table` names another.

A [Hugging Face](../formats/columnar-and-json.md#arrow-ipc) cache directory lists its
splits inside it the same way (`cache/test`), above the files they are made of.

Every key of the panel is in the
[keyboard reference](../reference/keyboard-shortcuts.md#info-panel). The row
count is the dataset's, not the page's; <kbd>D</kbd> at the table shows the
column types in a second header row.

On a CSV, TSV or PSV file, <kbd>H</kbd> on the Schema tab reads the first row
as data, under `column_1`, `column_2`, …, and again as column names. It reads
the file again, so the query, filters and sort are cleared, and the panel
closes. The footer offers it only for those files.

## Model

For a [SafeTensors or GGUF file](../formats/model-files.md), <kbd>i</kbd>
opens on the Model tab:

| Line | Shows |
|---|---|
| Format | `SafeTensors` or `GGUF v3`, the tensor count, and the file count for a sharded checkpoint |
| Parameters | The sum of every tensor's parameters, in full and abbreviated (`8.0B`), and the size of the tensor data |
| Types | Each dtype or quantization type's share of the parameters, largest first |
| Metadata | Key and value: SafeTensors `__metadata__` and the index's `metadata`, or GGUF's key/value pairs |

Values are shown whole up to 64 KiB, so a chat template wraps over as many
lines as it takes; a longer value, such as a whole `tokenizer.json`, ends with
how much more there is.
Arrays of up to 16 items are listed; longer ones, such as a tokenizer's
vocabulary, show their length (`[128,256 strings]`). Across several files, the
first file to name a key gives its value.

## Audio

For a [WAV, BWF, RF64 or AIFF file](../formats/signals-and-logs.md#audio), <kbd>i</kbd>
opens on the Audio tab:

| Line | Shows |
|---|---|
| Format | `WAV`, `WAV (Broadcast WAV)`, `RF64`, `AIFF` or `AIFF-C`, the channel count and the sample rate |
| Samples | `24-bit integer`, with the valid bits when fewer; the encoding; and whether `[read] audio_float` is on |
| Frames | The frame count, the length (`1:02:03.250`) and the size of the sample data |
| Warnings | A data size the file does not hold, frames past the 4,294,967,295 a table holds, or bytes after the last whole frame |
| Metadata | `bext.*` (description, originator, origination, time reference, coding history), `ixml.*` (project, scene, take, tape, note) and the iXML document itself, `info.*` from `LIST INFO`, AIFF's name and annotation, then each marker: its time, frame, region length and label |

A data size of 0 or a placeholder, as a recorder leaves it, says so: the
frames are counted from the file's size.

## MIDI

For a [MIDI file](../formats/signals-and-logs.md#midi), <kbd>i</kbd> opens on the MIDI
tab, or on Notes first when a note never ends or a file could not be read:

| Line | Shows |
|---|---|
| Format | `MIDI format 1`, the timing (`480 ticks per quarter`, or SMPTE frames), and the track count |
| Length | The time of the last event (`2:05.250`), the event count, and the notes, with how many never end |
| Tempo | The first tempo, and the range and number of changes when it changes; the first time and key signatures |
| Copyright | The first copyright notice, when there is one |
| Tracks | Each track's number and name, its events, notes, channels and instrument name |

For a directory of songs, the lines are totals and the tempo range, and the
list is the files that could not be read, with why.

## File format tabs

For a [VCD dump](../formats/signals-and-logs.md#vcd), <kbd>i</kbd> opens on the
VCD tab; for every other format the tab sits beside Schema.

| Tab | Lines | List |
|---|---|---|
| Parquet | Rows and row groups, the rows in each; compressed and uncompressed size and the codecs; format version and writer; the footer's metadata keys | Columns: each one's least and greatest value and its nulls, from the row groups' statistics, where every group has them |
| Arrow | Record batches and dictionaries; columns and byte order | Metadata: the schema's and the footer's key and value |
| Avro | The record's name, fields and codec; its documentation | Field docs: each field's documentation, where it has one |
| ORC | Rows and stripes, the rows per stripe; format version and compression | Metadata: what the writer kept, key and value |
| Excel | Worksheets, how many are hidden or hold no cells; the worksheet opened | Worksheets: each one's range and size (`A1:D100, 100 × 4`), as the opened worksheet's cells or the other worksheets' declarations give it |
| SQLite | Page size and pages; schema version, user version and text encoding; tables and views; whether row counts are stored | Tables: each one's kind, columns and the rows `ANALYZE` stored for it. No table is counted to fill it |
| GPS | NMEA: rows of the table opened, sentences and lines. GPX: points, tracks, routes and waypoints. Both: the time span and the latitude and longitude bounds of the rows | Sentences (NMEA): each type and how many |
| VCD | Timescale, signal and scope counts; value changes and their time span; `$date`, `$version`, `$comment` | Signals: each path with its type, width and identifier |
| FIX | Messages per BeginString; the dictionaries read with the log, each with what it matches and how many messages | Tags: each column with its tag number and the names the dictionaries give it, each dictionary's when they differ |
| SDF | Records, fields, and how many records are V3000 | Fields: each one's type and how many records hold it |
| NumPy | Shape, type, order (C or Fortran) and format version; for an archive's array, the archive and how many arrays it holds | Fields: each one's type, subarray shape and byte offset |
| ELF | Class, machine, type, entry point; bytes in loaded, unwritten sections (flash) and in written ones (RAM); the symbol count | Sections: each one's address, size and flags |
| ULog | Version, topic tables, dropouts | Info and parameters: each info message, and each parameter's starting value |
| DataFlash | Message types with records and defined; records; whether the log has units | Messages: each type's records, format characters and length |
| CAN | Frames, interfaces, whether timestamps are wall-clock; each dictionary read and what it matches; frames no dictionary names | Messages: each one's id, frames, signals and comment |

A list of more than 10,000 shows the first 10,000 and how many more there are.

## Notes

Notes explain what datui found while listing files and reading metadata:

| Finding | Evidence |
|---|---|
| Missing columns, conflicting types, or types widened for reading | Parquet footers |
| Empty files, large row groups, or many small files | Listing and footers |
| Inconsistent partition keys | File paths |
| Unreadable footers or files skipped because of their format | Listing and metadata reads |
| Plain files from a Delta, Iceberg or Hudi table | Directory markers |
| Rows excluded because a filter/sort column has incompatible types | Schema metadata and the active view |

Each note states its scope, such as `in all 6,541 footers` or
`in 20,000 of 200,000 footers (sample)`. Background metadata reads can update
these findings. Notes reuse information gathered during loading; they do not
scan the data values. For null rates, duplicates and other content checks,
use [Data Quality](data-quality.md).

When all footers are available, a missing-column note may identify the first
partition containing a column, or a single partition where it appears.
Datui omits these patterns when metadata is sampled or partition names cannot
be reliably ordered, such as `part=2` and `part=10`.

**Lake tables:** datui reads their plain files without applying table metadata.
The displayed row count may include deleted rows and superseded versions.
See [lake table directories](open-files.md#directories).

<kbd>Enter</kbd> on a type-conflict note offers **read as text**; see
[files that disagree](open-files.md#files-that-disagree).

Most notes describe the dataset as opened. Filter/sort exclusion notes follow
the active view and disappear when those controls are cleared. Queries,
pivots and drill-downs hide dataset notes until you reset or return to the
original level.

Unread notes accent the <kbd>i</kbd> key and open on the Notes tab. Set
`notes_accent = false` under `[display]` in the [config](configuration.md)
to disable the accent.

## Measurements

The **Resources** tab reports work datui can measure:

| Metric | Meaning |
|---|---|
| Listing | Time spent finding files, plus the number found |
| Footers | Time spent reading Parquet metadata, plus footer reads |
| Last page | Time spent fetching the visible rows, plus files read |
| Total | Listing time plus footer time |

Listing and Footers appear for paths handled by datui's metadata reader,
including Parquet directories and remote sources. Paths delegated directly
to Polars may omit those measurements. Last page is available on either route,
unless the dataset is already known to be empty.

Footer reads can exceed the number of files: schema and row-count passes may
read the same footer more than once. Use the Listing count for dataset size.
A dataset opened from [cached metadata](large-datasets.md#opening-it-again)
reads no footers and shows no Footers row.
