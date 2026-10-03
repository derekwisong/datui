# Formats

<!-- generated: format-count -->
datui reads 27 formats: Parquet, CSV, TSV, PSV, JSON, NDJSON, Arrow IPC, Avro, ORC, Excel, SafeTensors, GGUF, NMEA, GPX, WAV/AIFF audio, MIDI, SQLite, VCD, FIX, SDF, NumPy, ELF, ULog, DataFlash, candump, plain text, systemd journal, and binary formats you describe in a format spec.
<!-- end generated: format-count -->

The format is taken from the extension, or from `--format` when there is none.

## How each format is read

<!-- generated: formats -->
| Format | `--format` | Extensions | Read | Compressed | HTTP(S) | In a bucket | Bucket prefix |
|---|---|---|---|---|---|---|---|
| [Parquet](columnar-and-json.md#parquet) | `parquet` | `.parquet` | lazy | no | downloaded | in place | in place |
| [CSV](delimited-text.md#csv-tsv-and-psv) | `csv` | `.csv` | lazy | converted once | downloaded | downloaded | in place |
| [TSV](delimited-text.md#csv-tsv-and-psv) | `tsv` | `.tsv` | lazy | converted once | downloaded | downloaded | no |
| [PSV](delimited-text.md#csv-tsv-and-psv) | `psv` | `.psv` | lazy | converted once | downloaded | downloaded | no |
| [JSON](columnar-and-json.md#json-and-ndjson) | `json` | `.json` | in memory | no | downloaded | downloaded | no |
| [NDJSON](columnar-and-json.md#json-and-ndjson) | `jsonl` | `.jsonl`, `.ndjson` | in memory | no | downloaded | downloaded | in place |
| [Arrow IPC](columnar-and-json.md#arrow-ipc) | `arrow` | `.arrow`, `.arrows`, `.ipc`, `.feather` | lazy | no | downloaded | in place | in place |
| [Avro](columnar-and-json.md#avro-and-orc) | `avro` | `.avro` | in memory | no | downloaded | downloaded | no |
| [ORC](columnar-and-json.md#avro-and-orc) | `orc` | `.orc` | in memory | no | downloaded | downloaded | no |
| [Excel](columnar-and-json.md#excel) | `excel` | `.xls`, `.xlsx`, `.xlsm`, `.xlsb` | in memory | no | downloaded | downloaded | no |
| [SafeTensors](model-files.md) | `safetensors` | `.safetensors`, `.safetensors.index.json` | in memory | no | in place | in place | in place |
| [GGUF](model-files.md) | `gguf` | `.gguf` | in memory | no | in place | in place | in place |
| [NMEA](signals-and-logs.md#gps-logs) | `nmea` | `.nmea` | converted once | converted once | downloaded | downloaded | no |
| [GPX](signals-and-logs.md#gps-logs) | `gpx` | `.gpx` | converted once | converted once | downloaded | downloaded | no |
| [WAV, BWF, RF64, AIFF](signals-and-logs.md#audio) | `audio` | `.wav`, `.wave`, `.bwf`, `.rf64`, `.aif`, `.aiff`, `.aifc` | lazy | no | downloaded | downloaded | no |
| [MIDI](signals-and-logs.md#midi) | `midi` | `.mid`, `.midi`, `.smf`, `.kar`, `.rmi` | in memory | no | downloaded | downloaded | no |
| [SQLite](databases-and-arrays.md#sqlite) | `sqlite` | `.db`, `.db3`, `.sqlite`, `.sqlite3` | lazy | no | downloaded | downloaded | no |
| [VCD](signals-and-logs.md#vcd) | `vcd` | `.vcd` | converted once | converted once | downloaded | downloaded | no |
| [FIX](signals-and-logs.md#fix-logs) | `fix` | none: by content | converted once | converted once | downloaded | downloaded | no |
| [SDF](signals-and-logs.md#sdf) | `sdf` | `.sdf`, `.sd` | converted once | converted once | downloaded | downloaded | no |
| [NumPy](databases-and-arrays.md#numpy) | `numpy` | `.npy`, `.npz` | lazy | no | downloaded | downloaded | no |
| [ELF](signals-and-logs.md#elf) | `elf` | `.elf`, `.axf` | in memory | no | downloaded | downloaded | no |
| [ULog](signals-and-logs.md#flight-logs) | `ulog` | `.ulg` | lazy | no | downloaded | downloaded | no |
| [DataFlash](signals-and-logs.md#flight-logs) | `dataflash` | none: by content | lazy | no | downloaded | downloaded | no |
| [candump](signals-and-logs.md#can-logs) | `candump` | none: by content | lazy | no | downloaded | downloaded | no |
| [Text](delimited-text.md#text-and-logs) | `text` | `.log`, `.txt` | lazy | converted once | downloaded | downloaded | no |
| [systemd journal](signals-and-logs.md#systemd-journal) | `journal` | none: by content | in memory | no | downloaded | downloaded | no |
| [Arrow IPC stream](columnar-and-json.md#arrow-ipc) | `arrow` | as Arrow IPC | converted once | no | downloaded | downloaded | downloaded |
| [Format spec](format-specs.md) | its name | its `match` | lazy | converted once | downloaded | downloaded | no |
<!-- end generated: formats -->

| Read | What it means |
|---|---|
| lazy | Scanned where it is. Browsing reads a buffer of rows; queries, sorting and analysis may read the whole input |
| converted once | Read through once into a temporary file in the temp directory (`--temp-dir`), which is then scanned lazily. The file is removed on quit ([temporary files](../user-guide/open-files.md#temporary-files)) |
| in memory | Read whole into memory before the table appears. Past `memory_warning` in `[read]` (`"1GiB"` by default; 0 never asks), datui asks first: `big.json: JSON reads 2.10 GB into memory`. A model file's table is one row per tensor, from the header, so it is small however large the model, and is never asked about; a MIDI file is at most 64 MiB |

- **Compressed** is a `.gz`, `.zst`, `.bz2` or `.xz` file; `no` means it does not
  open. `-c read.decompress_in_memory=true` reads compressed CSV, TSV, PSV
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
  [cloud source](../user-guide/home-screen.md).
- [Standard input](../user-guide/pipes-and-follow.md#standard-input) is written to a temporary file first, then
  read as **Read** says.

The home screen marks a file row that is not read lazily where it is:
`converts`, `in memory` or `downloads`. The details pane and the Info panel's
Resources tab say how it is read.

## Detected by content

The format comes from the first bytes, unless `--format` or `--compression`
names it:

| First bytes | Read as |
|---|---|
| Parquet, Arrow IPC or Avro magic number, or an Arrow IPC stream's schema message | that format |
| `SQLite format 3` | SQLite; a database of several tables needs `--table` |
| gzip, zstd, bzip2 or xz magic number | decompressed, then read by what is inside, as below: a text format, CSV or TSV, or lines |
| `{`, the first line an object with `__CURSOR` and `__REALTIME_TIMESTAMP` | [systemd journal](signals-and-logs.md#systemd-journal) |
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
| anything else | [lines](delimited-text.md#text-and-logs) |

A comma or a tab alone is not a table: a log line with a comma in it stays a
line. An unnamed CSV the first lines do not show as one opens with `--format
csv`; the Info panel's notes say so. With `--format csv`, `tsv` or `psv` and no
`--compression`, compression still comes from the first bytes.
