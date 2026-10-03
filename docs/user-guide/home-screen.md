# Browse files

Run `datui` without a path, or press <kbd>Ctrl</kbd>+<kbd>O</kbd>, to find and
open a dataset. Type to filter the list; press <kbd>Enter</kbd> to open the
selected row. The details pane shows the selected file's first rows and its
schema when available.

## Open a file or directory

1. Type part of a name to filter the list.
2. Select the file with <kbd>↑</kbd> / <kbd>↓</kbd> and press <kbd>Enter</kbd>,
   or double-click it. The wheel moves the selection; see [Mouse](../reference/keyboard-shortcuts.md#mouse).
3. To browse inside a directory instead of combining its files, press <kbd>→</kbd>.

To enter a path, clear the filter and press <kbd>~</kbd>. While you type, the
list shows the directory being typed, narrowed by the name after the last `/`.

| Key | At the `~` prompt |
|---|---|
| <kbd>Tab</kbd> | Complete the one name left, with a `/` for a directory, or what the names share |
| <kbd>↑</kbd> <kbd>↓</kbd> | Pick a name from the list |
| <kbd>Enter</kbd> | Open the file, or browse the directory as <kbd>→</kbd> does |

`s3://`, `gs://` and `az://` complete bucket and prefix names from what datui
already knows: listed sources and prefixes, recents, the dataset index and the
public catalog. Nothing is asked of the store, so `s3://noaa` + <kbd>Tab</kbd>
gives `s3://noaa-ghcn-pds/`.

<kbd>Ctrl</kbd>+<kbd>D</kbd> remembers a directory as its own section.
<kbd>Ctrl</kbd>+<kbd>O</kbd> returns home from an open table.

## The control bar

The bar leads with what a first session needs: <kbd>Enter</kbd> (named for what
it does on the selected row), `type Filter`, `~ Path`, <kbd>Esc</kbd> (named for
where it goes: `Clear`, `Up`, `Back`, or `Table` when a dataset is open), `?`
and <kbd>Ctrl</kbd>+<kbd>C</kbd>. Moves and conveniences follow and are the
first cut on a narrow terminal. The sort order (`by name`) shows at the far
right when every key fits; each section's rule counts its rows.

## Sections

| Section | Contents |
|---|---|
| `RECENT` | Opened datasets, grouped by their parent directory or cloud location |
| Current directory | Files where you launched datui |
| `CLOUD` | Detected and configured cloud sources; open one to list its contents |
| Collections | Each [`[[sources]]`](#collections) collection under its label, in configured order |
| Configured directories | Paths from `[data] directories`, in configured order |
| Remembered directories | Paths saved with <kbd>Ctrl</kbd>+<kbd>D</kbd> |
| `PUBLIC DATASETS` | The built-in [public datasets](#public-datasets) |
| `ELSEWHERE` | Directories from the desktop's recent-files list; folded initially |
| `Found` | Recursive search results while you type |

Folded sections stay folded between runs. Path sections show why they are
listed and their status: for example `configured`, `nfs4`, `listing` (with
`1,200 so far` once a slow share sends its first rows, which show as they
arrive), `unavailable`, or `first 5,000` when a listing is incomplete. An
empty current directory says `nothing to open here · ~ types a path`.

### Recent

A **place row** groups datasets by directory. Open the place to browse its
contents, including files you have not opened individually. This is why a
familiar directory can show more files than your recent individual opens.
Recent is ranked by frecency, as zoxide ranks directories: each open counts four
times within the hour, twice within the day, half within the week and a quarter
after that. Places follow their highest-ranked dataset. The cursor starts on
the dataset opened last, so <kbd>Enter</kbd> reopens it. Sorting changes the
dataset order within each place.

Initially, whole places fill up to a third of the list, with at least one
place shown. Select `… more in … places` to expand the rest for this session.
The section count and filter include entries beyond that visible limit.

### Adding a directory

Opening a dataset adds its parent to Recent. Press <kbd>Ctrl</kbd>+<kbd>D</kbd>
to keep a directory in its own section; press it again to forget the directory.
Remembered directories are stored in the cache. To keep one after clearing the
cache, add it to your [config](../reference/settings.md#data):

```toml
[data]
directories = ["/mnt/data", "~/datasets", "$WORK/warehouse"]
```

`~` and `$VAR` expand. Unreachable configured directories stay listed as
`unavailable`.

To add a cloud account or storage endpoint to the home screen, see
[Connections](../reference/cloud-sources.md#connections).

### Desktop places

`ELSEWHERE` reads directories from freedesktop's `recently-used.xbel`, used by
GTK apps and file managers. Datui does not modify that file or list a place's
contents until you enter it. Disable this with:

```toml
[data]
use_desktop_recents = false
```

## Searching below the current directory

Typing filters the visible list and starts a background search below the
working directory. `Found` results show relative paths, so identically named
files in different folders remain distinguishable. The directory walk runs
once and keeps every data file it finds; each keystroke scores those files in
the background, narrowing the last result as the filter grows.

`Found` lists the best `max_results` matches (1,000 by default). Its heading
counts the rest (`1,000 of 2,500 matches`) and how many entries the walk read
(`23,041 searched`).

An incomplete search says `partial · out of time`, `partial · too many files`,
or `partial · too deep` in its heading. A search that stopped short and matched
nothing keeps its heading to say so: `no match in 23,001 files · partial · out
of time`.

### Matching

Name matching uses fzf-style scoring: consecutive characters, word boundaries
and filename matches rank higher. Matched characters are underlined.
Known Parquet column names also match; name matches rank ahead of column
matches. A dataset you open often ranks above one that matches about as well,
in every section. Column metadata is remembered between runs.

### What is skipped

| Default rule | Examples |
|---|---|
| Hidden directories | `.git`, `.venv`, caches |
| Build and dependency directories | `node_modules`, `target`, `build`, `dist`, `vendor`, `site-packages`, `__pycache__`, `venv`, `env` |
| Filesystem boundaries | Mounted disks and network shares |
| Symlinks | Not followed |

`.gitignore` is not read by default: data directories are often gitignored.

### Tuning

Set search depth, time, result count and additional exclusions in
[`[data.search]`](../reference/settings.md#data). `cross_filesystems = false` avoids
crossing onto network shares or triggering automounts during a search.

## The details pane

| Field | Meaning |
|---|---|
| Kind / storage | File or dataset format, and the filesystem or object store |
| Read | How a file opens: `lazy`, `converted once`, `in memory`, or `downloaded, then` one of those ([formats](loading-data.md#how-each-format-is-read)) |
| Contains | Files by format, directories and partitions |
| Rows × columns | Known counts; blank when they would require scanning data |
| On disk | Stored size |
| In memory | Parquet's uncompressed size, not current process memory |
| Row groups | Parquet's read units; their size affects paging cost |
| Partitions | Keys and values found in directory names |
| Schema | Known columns and their types |

### First rows

For a local CSV, TSV, PSV, JSON Lines, Arrow IPC or Parquet file, the pane's
`ROWS` block shows the first eight rows of the leading columns, read in the
background when the row is selected. The read is the open's own first page:
<kbd>Enter</kbd> installs it, so opening the file reads nothing again.

| Setting | Effect |
|---|---|
| `[data] preview_max_mb = 64` | Largest file previewed; for Parquet, its average row group. `0` turns the preview off |

Files on network shares and in object stores are not read before they are
opened. Below about 100 columns, where there is no pane, the rows show in a
strip at the bottom of the screen when the list leaves at least four lines
free, as it does inside a small directory.

Parquet facts read metadata, not data rows. Counts cover up to 64 files;
for a larger dataset the pane shows an unknown row count and a sampled column
count, such as `? × 39+`. CSV and other scan-to-count formats omit these counts.

### What a row's label says

Every row reads the same way: the name, a `/` when it is a directory, two
spaces, then the label: `data/  3 dirs`, `events/  hive`, `Palmer penguins  csv`.

| Label | Directory contents |
|---|---|
| `hive` | `key=value` subdirectories, at least as numerous as direct data files |
| `delta`, `iceberg`, `hudi` | A lake-table marker |
| `12 parquet`, `3 csv` | Direct data files of one format |
| `3 safetensors`, `2 gguf` | A model: weight files of one format, with only JSON (config, tokenizer) beside them. Opens as one table |
| `3 tables` | A SQLite database or a NumPy `.npz` archive (a file, not a directory). One table opens; several are listed, a row each, when <kbd>Enter</kbd> or <kbd>→</kbd> goes inside |
| `mixed` | Several formats |
| `3 dirs` | Only directories: how many there are to go into |
| `dir` | No direct data files and no directories; `dir+` means the listing was cut short |
| `csv`, `parquet` | A file whose row name does not say its format, such as a public dataset |
| `bucket`, `container` | The top of an object store |
| `…`, spinner, `?` | Not inspected yet, inspecting, or inspection failed |

The details pane's `contains` lines list the directory's contents.
Job markers and names beginning with `_` or `.` are skipped, except partition
names such as `_date=2025-01-01`. Folder markers ending in `_$folder$` are
also skipped. A capped listing says so, for example `5000+ parquet`.

### How a file will be read

A file row that is not read lazily where it is says how it is read, dim,
beside its name. It gives way before the name is shortened.

| Label | Opening the file |
|---|---|
| `converts` | Reads it once into a temporary file: an Arrow stream, NMEA, GPX, VCD, FIX, SDF, compressed text |
| `in memory` | Reads it whole into memory: JSON, NDJSON, systemd journal, Avro, ORC, Excel, MIDI, ELF; SafeTensors and GGUF read only their header |
| `downloads` | Downloads it whole first: a remote file other than a Parquet or Arrow object in a bucket, or a model file |

See [how each format is read](loading-data.md#how-each-format-is-read).

Select the `… files with no reader` row, or press <kbd>Ctrl</kbd>+<kbd>A</kbd>,
to reveal files no reader takes, such as `README.md`.
They are dimmed; <kbd>Enter</kbd> on a local one shows its
bytes in the [hex view](hex-view.md). <kbd>Ctrl</kbd>+<kbd>X</kbd> shows any
local file's bytes there. Set `[data] show_unreadable_files = true`
to show them by default. Inside a SQLite database the same row and key show
its internal tables (`sqlite_master`, `sqlite_sequence`), which open like the
others. See [SQLite databases](loading-data.md#sqlite-databases).

### Opening a directory

<a id="two-doors-into-every-directory"></a>

<kbd>→</kbd> always browses inside. <kbd>Enter</kbd> follows the action shown
in the bottom bar:

| Action | Result |
|---|---|
| Open all | Combine the directory into one table |
| Inside | Browse its files |
| Open | Load the selected file |
| Look | Inspect the directory, then choose the appropriate action |

Inside a directory, the first row combines its contents into one table, even
when Enter on the parent row would browse. Its label says what Enter there
opens:

| Directory | First row | Cursor starts on |
|---|---|---|
| Hive partitions | `sales (hive table: year, month)` | this row |
| Files of one format and one schema | `same (3 Parquet files, one schema)` | this row |
| Files of one format whose columns differ | `diff (2 Parquet files, schemas differ)` | the first file |
| Model weights, with only JSON beside them | `llama (model, 3 SafeTensors files)` | this row |
| One data file | `notes (1 CSV file)` | the first file |
| Several formats, files beside subdirectories, or only subdirectories | `data (all files, mixed)` | the first row inside |
| Delta, Iceberg or Hudi | `tbl (Delta files, not the table)` | the first row inside |

The cursor starts on the first row only when it opens the directory as one
dataset, the same thing Enter on the directory's own row opens. On a mixed
directory the details pane names what is read and what is skipped
(`reads 1 parquet`, `skips 1 csv, 1 directory`). The row hides while a filter
is typed.

### Combining files

<a id="the-door-does-not-refuse"></a>

[Files and formats](loading-data.md#directories) explains format selection
and [schema differences](loading-data.md#files-that-disagree).
A hive table's row counts its partition columns in its width (`12 × 4`).
For Delta, Iceberg and Hudi, datui reads raw files without the transaction log;
the result may contain deleted rows or superseded versions.

### Storage markers

<a id="where-a-rows-data-lives"></a>

| Marker | ASCII | Location |
|---|---|---|
| `◦` | `.` | Local disk |
| `▪` | `*` | Memory, such as a tmpfs mount |
| `↕` | `~` | Network filesystem: NFS, SMB, sshfs |
| `≈` | `@` | Object store or URL |
| `◌` | `?` | Unknown |

## Collections

A collection is a list of datasets you named in the config, local and remote
alike, shown as a section under its label. See
[Dataset collections](../reference/sources.md) for `[[sources]]`.

```
▾ MY DATASETS  5   configured  ────────────────────────────────────
  ▪ Sales                                          6.8 KB   now
  ▪ Archive/ 1 csv                                          now
  ◦ Gone missing
  ≈ Weather/ dataset
  ≈ Penguins
```

| Row | <kbd>Enter</kbd> | Label |
|---|---|---|
| Local file | Opens it | Measured like any file |
| Local directory | Steps inside | What is inside (`1 csv`, `hive`) |
| Local path with nothing there | Says it does not exist | `missing` |
| Directory in an object store | Steps inside; <kbd>Backspace</kbd> at its top comes back here | `dataset` |
| File in an object store or on the web | Opens it; a web file is downloaded after asking | Its format, such as `csv`, and its `size` when given |

Nothing remote is asked for until you open or enter a dataset. Inside one, the
title bar's trail starts with the collection and the dataset's name:
`My datasets › Weather › by_year`. The details pane shows the dataset's
description, publisher, license and homepage, its path or URL, and `login`: `none`,
`auto`, or the connection that reads it.

### Public datasets

`Public datasets` is the built-in collection: data its publishers host and keep up
to date, readable with no login. It comes after everything of your own. HTTP(S)
files open as tables and object-store roots open for browsing; datui bundles no
dataset files, and hosted CSV extracts have the coverage shown. A build without
the `http` or `cloud` [feature](../getting-started/installation.md#from-source)
leaves out the rows it cannot open.

| Dataset | Data | License |
|---|---|---|
| NYC flights (2013) | Departures from JFK, LaGuardia and Newark; delays in minutes | CC0 (nycflights13) |
| Food nutrition (fast food) | 515 menu items; nutrients per item, not per 100 g | GPL-3 (OpenIntro package) |
| US baby names (1880-2017) | Published name counts by year and sex; counts below five are suppressed | CC0 / public domain |
| NOAA daily weather (GHCN-D) | Worldwide weather station observations, by year and by station | CC0 |
| Premier League (2020-21) | Match rounds, dates, teams and full-time scores | CC0 |
| NYC yellow taxis (January 2025) | One original monthly trip file; fares, distances and congestion fees | NYC Open Data terms |
| Earthquakes (past month) | Rolling month of worldwide earthquakes; magnitudes, depth and location | Public domain |
| Space launches (1957-2018) | Historical launch records and agencies; includes failed attempts | MIT (The Economist extract); credit Jonathan McDowell |
| Palmer penguins | 344 penguins: species, island, bill, flipper length and body mass | CC0; credit Horst, Hill and Gorman (2020) |
| Aqueous solubility (SDF) | 1,025 molecules: measured solubility (log mol/L), a low, medium or high class, and SMILES | BSD-3-Clause (RDKit) |
| Bitcoin and Ethereum | Blocks and transactions, partitioned by date | AWS sample-code license |
| Overture Maps | Places, buildings, addresses, roads and boundaries, by release | ODbL; places CDLA Permissive 2.0 and Apache 2.0 |

Each web file's row gives its format and its size. One under 50 MB downloads
without a question; the bottom bar says `Downloading 16.1 KB...` while it does.
A URL typed at <kbd>~</kbd> is always asked about. Opened once, a dataset comes
back under Recent by its catalog name, with the rows and columns its open counted.

The details pane gives each one's publisher, license and homepage. The license is
the publisher's: check it before you use the data.

| Dataset | Used in |
|---|---|
| Palmer penguins | [Quick start](../getting-started/quick-start.md), [charts](charting.md), [correlation](analysis-features.md#correlation-matrix), [Python](python-module.md) |
| NYC flights (2013) | [Queries](querying-data.md#run-a-query), [drill-down](querying-data.md#drill-into-a-group-by), [bar charts](charting.md#chart-one-value-per-category) |
| Food nutrition (fast food) | [Search, filter and sort](filtering-sorting.md), [copy](copying.md), [data quality](data-quality.md) |
| US baby names (1880-2017) | [Pivot and melt](reshaping.md) |
| Space launches (1957-2018) | [Count with a pivot](reshaping.md#count-with-a-pivot) |
| Premier League (2020-21) | [Dates and messy text](querying-data.md#dates-and-messy-text), [export](exporting-data.md) |
| NYC yellow taxis (January 2025) | [Describe and Distribution](analysis-features.md), [data quality on a sample](data-quality.md) |
| Earthquakes (past month) | [A map as a scatter chart](charting.md#examples-on-the-built-in-datasets) |
| NOAA daily weather (GHCN-D) | [Public data in S3](remote-data.md#examples-on-public-data), [views](views.md) |
| Bitcoin and Ethereum | [Partitions by date](remote-data.md#examples-on-public-data) |
| Aqueous solubility (SDF) | [SDF compound files](loading-data.md#sdf-compound-files) |

A collection named `public` replaces this one,
`[data] builtin_catalog = false` drops it, and
`[data] hide_sources = ["public"]` hides it; see
[Dataset collections](../reference/sources.md).

## Cloud storage

Open a source under `CLOUD` to browse buckets, projects or containers.
See [Cloud browser](cloud-browser.md)
for discovery, refresh behavior and listing errors, or
[Remote data](remote-data.md) for credentials and URLs.

<a id="which-sources-appear"></a>
<a id="what-a-cloud-row-shows"></a>

## Network locations

Listings run in the background. A location that fails to answer shows
`unavailable`; press <kbd>Ctrl</kbd>+<kbd>R</kbd> to retry. Remote metadata can
come from cache; local metadata is refreshed when file size or modification
time changes.

## Loading

<kbd>Enter</kbd> shows loading progress. Press <kbd>Ctrl</kbd>+<kbd>O</kbd> to
cancel and return home. If a file cannot open, home shows the error and
<kbd>Esc</kbd> can return to the previously open dataset.

## What datui remembers

Datui caches recent paths, how often and how lately each was opened, and
measured metadata: counts, column names, size and modification time. Clear them with:

| Command | Removes |
|---|---|
| `datui cache clear --recents` | Recent paths only |
| `datui cache clear` | Cached metadata, recents, remembered directories and query history |

These commands do not delete data files.

## Keys

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> | Select a row; <kbd>Ctrl</kbd>+<kbd>P</kbd> / <kbd>Ctrl</kbd>+<kbd>N</kbd> also work |
| <kbd>Ctrl</kbd>+<kbd>↑</kbd> <kbd>Ctrl</kbd>+<kbd>↓</kbd> | Move between sections |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Move a page |
| <kbd>Home</kbd> <kbd>End</kbd> | First or last row |
| <kbd>Enter</kbd> | Open the selected dataset, browse a directory, or expand a section |
| <kbd>→</kbd> | Browse inside a directory, including a Hive dataset; unfold a section |
| <kbd>←</kbd> | Fold a section |
| type | Filter names and known column names |
| <kbd>~</kbd> with an empty filter | Enter a path or URL; the list shows the directory being typed. <kbd>Tab</kbd> completes, <kbd>↑</kbd> <kbd>↓</kbd> pick a name. <kbd>Enter</kbd> opens a file and browses a directory |
| <kbd>Tab</kbd> | Cycle sort: natural, size, modified, rows |
| <kbd>Backspace</kbd> | Delete a character; with an empty filter, go up a directory. From the top of a collection's remote dataset, back to the list |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Space</kbd> | While the filter is empty, fold or unfold the section header under the cursor; with a filter typed, it types a space |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | Refresh the locations on screen |
| <kbd>Ctrl</kbd>+<kbd>A</kbd> | Show or hide files datui cannot read, or a SQLite database's internal tables |
| <kbd>Ctrl</kbd>+<kbd>D</kbd> | Remember or forget the selected directory; a file represents its parent |
| <kbd>Delete</kbd> | Forget a recent entry; on a place, confirm forgetting its entries; on a remembered heading, forget it; on a cloud source, hide it |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Confirm forgetting all recent entries |
| <kbd>Esc</kbd> | Clear the filter, go back to the row a directory was entered from, or return to the open table |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> | Quit |
| <kbd>?</kbd> | Help |

Letters enter the filter here: <kbd>q</kbd> types a `q`. From a table opened
through home, <kbd>q</kbd> returns home. <kbd>Esc</kbd> never quits.
The bottom bar shows the available action for the selected row.

## Limits

| Work | Default limit |
|---|---|
| Recent paths | 50 |
| Directories promoted from recents | 8 |
| Entries listed or inspected to classify a directory | 5,000 |
| Subdirectories inspected per listing | 64 |
| Files read for a preview count | 64 |
| Datasets measured concurrently | 12, visible entries only |
| Network directories probed concurrently | 4 |
| Recursive search | Depth 8; 100,000 files kept; 1,000 matches listed; 1.5 seconds |

Entries beyond an inspection limit remain browsable. The listing or search
heading marks incomplete results: a directory cut short reads `first 5,000`
beside its name, whether it is local or on a share.

## Narrow and plain terminals

The details pane hides below roughly 100 columns; size and shape columns hide
below roughly 56. On a wide terminal the list stops at 84 columns, so a row's
size and age stay near its name, and the pane takes the rest. Below 28 rows, the wordmark becomes a one-line title.
Without UTF-8 ([detection](../user-guide/configuration.md#glyphs-or-ascii)), markers
and borders use ASCII. Override detection with:

```toml
[display]
unicode = "auto"    # "always" or "never" to override
```

## Desktop launchers

Linux packages include a desktop entry for application menus and file-manager
“Open with” actions. Launching datui from a menu opens home.
