# Dataset Info

Press <kbd>i</kbd> to inspect the dataset's schema and storage details.
Press <kbd>i</kbd> or <kbd>Esc</kbd> to close the panel.

![Info Panel Demo](../demos/03-info.gif)

| Tab | Shows |
|---|---|
| **Schema** | Row and column counts, column types, schema source, file coverage, and Parquet codecs/compression |
| **Model** | A SafeTensors or GGUF model's totals and header metadata; appears for model files |
| **Audio** | An audio file's format, length, metadata and markers; appears for audio files |
| **MIDI** | A MIDI file's format, timing, length, tempo and tracks; appears for MIDI files |
| **Metadata** | The metadata line a [delimited format spec](binary-formats.md#delimited-text) names, as key and value; appears for files read through one |
| **VCD**, **FIX**, **SDF**, **NumPy**, **ELF**, **ULog**, **DataFlash**, **CAN** | A value change dump's header and signals, a FIX log's versions, dictionaries and tags, an SDF file's fields, a NumPy array's shape and fields, an ELF file's flash and RAM and sections, a flight log's messages, info and parameters, or a CAN log's DBC files and messages; appears for those files |
| **Resources** | File size, buffered memory, Parquet metadata, and loading measurements |
| **Partitions** | Partition columns for a hive-partitioned dataset |
| **Notes** | Schema differences, skipped files and other findings; appears when there are notes |

The file size and Parquet metadata are read in the background the first time
the panel opens for a dataset. Until they arrive the size reads `reading...`;
a file that cannot be read shows why in its place. Remote sources,
directories, globs and datasets of several files have no file size.

## Keys

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Switch tabs |
| <kbd>Tab</kbd> | Move between the Schema tab bar and its column table |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Scroll the focused column table, the notes, the model's, audio file's or delimited file's metadata, the MIDI tracks, or the VCD signals, FIX tags or SDF fields |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> <kbd>Home</kbd> <kbd>End</kbd> | Page through the model's, audio file's or delimited file's metadata, the MIDI tracks, or the VCD, FIX or SDF list |
| <kbd>Enter</kbd> | Apply a note's offered action, when available |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> <kbd>i</kbd> | Close |

The row count covers the dataset, not just the visible page. Column types also
appear in the table's second header row; <kbd>D</kbd> toggles that row.

## Model

For a [SafeTensors or GGUF file](loading-data.md#model-files), <kbd>i</kbd>
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

For a [WAV, BWF, RF64 or AIFF file](loading-data.md#audio-files), <kbd>i</kbd>
opens on the Audio tab:

| Line | Shows |
|---|---|
| Format | `WAV`, `WAV (Broadcast WAV)`, `RF64`, `AIFF` or `AIFF-C`, the channel count and the sample rate |
| Samples | `24-bit integer`, with the valid bits when fewer; the encoding; and whether `--normalize` is on |
| Frames | The frame count, the length (`1:02:03.250`) and the size of the sample data |
| Warnings | A data size the file does not hold, frames past the 4,294,967,295 a table holds, or bytes after the last whole frame |
| Metadata | `bext.*` (description, originator, origination, time reference, coding history), `ixml.*` (project, scene, take, tape, note) and the iXML document itself, `info.*` from `LIST INFO`, AIFF's name and annotation, then each marker: its time, frame, region length and label |

A data size of 0 or a placeholder, as a recorder leaves it, says so: the
frames are counted from the file's size.

## MIDI

For a [MIDI file](loading-data.md#midi-files), <kbd>i</kbd> opens on the MIDI
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

For a [VCD dump](loading-data.md#vcd-value-change-dumps), <kbd>i</kbd> opens on the
VCD tab; for a [FIX log](loading-data.md#fix-logs), an
[SDF file](loading-data.md#sdf-compound-files), a
[NumPy array](loading-data.md#numpy-arrays), an
[ELF file](loading-data.md#elf-symbol-tables), a
[flight log](loading-data.md#flight-logs) or a [CAN log](loading-data.md#can-logs)
the tab sits beside Schema.

| Tab | Lines | List |
|---|---|---|
| VCD | Timescale, signal and scope counts; value changes and their time span; `$date`, `$version`, `$comment` | Signals: each path with its type, width and identifier |
| FIX | Messages per BeginString; the dictionaries read with the log, each with what it matches and how many messages | Tags: each column with its tag number and the names the dictionaries give it, each dictionary's when they differ |
| SDF | Records, fields, and how many records are V3000 | Fields: each one's type and how many records hold it |
| NumPy | Shape, type, order (C or Fortran) and format version; for an archive's array, the archive and how many arrays it holds | Fields: each one's type, subarray shape and byte offset |
| ELF | Class, machine, type, entry point; bytes in loaded, unwritten sections (flash) and in written ones (RAM); the symbol count | Sections: each one's address, size and flags |
| ULog | Version, topic tables, dropouts | Info and parameters: each info message, and each parameter's starting value |
| DataFlash | Message types with records and defined; records; whether the log has units | Messages: each type's records, format characters and length |
| CAN | Frames, interfaces, whether timestamps are wall-clock; each DBC file read and what it matches; frames no DBC names | Messages: each one's id, frames, signals and comment |

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
See [lake table directories](loading-data.md#directories).

### Read a conflicting column as text

Select a type-conflict note and press <kbd>Enter</kbd> when **read as text**
is offered. Values from the conflicting files become visible, but filters and
sorting now compare strings. For example, `"10"` sorts before `"2"`.

The action is unavailable if any file stores the column as a list, array,
duration, binary or unknown type. See [files that disagree](loading-data.md#files-that-disagree).

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
A dataset opened from [cached metadata](loading-data.md#opening-it-again)
reads no footers and shows no Footers row.
