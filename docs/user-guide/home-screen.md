# Home screen

`datui` with no path opens the home screen, where you find and open a dataset:
recent files, directories, cloud storage and a catalog of public data.

```bash
datui
```

<kbd>Ctrl</kbd>+<kbd>O</kbd> returns here from anywhere. Typing narrows the
list; <kbd>Enter</kbd> does what the control bar names for the row. Every key is
in the [keyboard reference](../reference/keyboard-shortcuts.md#home-screen).

## Open a file or directory

1. Type part of a name to narrow the list.
2. Select the row and press <kbd>Enter</kbd>, or double-click it.
3. To browse a directory rather than read it as one table, press <kbd>→</kbd>.

To type a path or URL, press <kbd>~</kbd> with the filter empty. The list
shows the directory being typed, narrowed by the name after the last `/`.

| Key at the `~` prompt | Does |
|---|---|
| <kbd>Tab</kbd> | Completes the one name left, with `/` for a directory, or what the names share |
| <kbd>↑</kbd> <kbd>↓</kbd> | Picks a name from the list |
| <kbd>Enter</kbd> | Opens a file, or goes inside a directory as <kbd>→</kbd> does |
| <kbd>Esc</kbd> | Closes the prompt |

`s3://`, `gs://` and `az://` complete from names datui already knows (listed
sources and prefixes, recents, the public catalog); nothing is asked of the
store, so `s3://noaa` <kbd>Tab</kbd> gives `s3://noaa-ghcn-pds/`.

The control bar leads with <kbd>Enter</kbd>, named for what it does on the
selected row, then `type Filter`, `~ Path`, <kbd>Esc</kbd> (named for where it
goes: `Clear`, `Up`, `Back`, or `Table` when a dataset is open), help and quit.
Letters type into the filter, so <kbd>q</kbd> types `q`; <kbd>Ctrl</kbd>+<kbd>C</kbd> quits.

## Sections

| Section | Lists |
|---|---|
| `RECENT` | Datasets opened before, grouped by the directory or cloud place each lives in |
| Current directory | Where datui was launched; an empty one says `nothing to open here · ~ types a path` |
| `CLOUD` | [Cloud sources](#cloud-sources): stores found on this machine and configured ones |
| Collections | Each [collection](#collections) under its label |
| Configured directories | `[home] directories`, in order, then directories remembered with <kbd>Ctrl</kbd>+<kbd>D</kbd> |
| `PUBLIC DATASETS` | The built-in [public datasets](#public-datasets) |
| `ELSEWHERE` | Directories your desktop recorded (freedesktop `recently-used.xbel`); starts folded |
| `Found` | [Search](#search-below-the-current-directory) results, while you type |

Folds last between runs. A path section's heading says why it is listed and
how it stands: `configured`, `nfs4`, `listing` (then `1,200 so far` as a slow
share answers), `unavailable`, or `first 5,000` when a listing stops there.

### Recent

Recent ranks datasets by frecency, as zoxide ranks directories: an open counts
four times within the hour, twice within the day, half within the week and a
quarter after. A place (a directory) ranks with its best dataset, and
entering it shows all its files, opened or not. The cursor starts on the
dataset opened last, so <kbd>Enter</kbd> reopens it. Places fill up to a third
of the list at first; `… more in … places` shows the rest.

### Add a directory

<a id="adding-a-directory"></a>

Opening a dataset adds its directory to Recent. <kbd>Ctrl</kbd>+<kbd>D</kbd>
remembers the selected directory in a section of its own, and again forgets
it; these live in the cache. To keep directories whatever happens to the
cache, list them in the config (`~` and `$VAR` expand; one that cannot be
reached stays listed as `unavailable`):

```toml
[home]
directories = ["~/datasets", "/mnt/data"]
desktop_recents = false
```

`desktop_recents = false` drops `ELSEWHERE`. datui never writes the desktop's
file, and lists a place there only when you enter it.

## Search below the current directory

Typing narrows every section and searches below the working directory in the
background. `Found` lists matches by their path from there.

| What | How |
|---|---|
| Matching | fzf-style: runs of characters, word starts and file names rank higher; matched characters are underlined. Known Parquet column names match too, after names. What you open often ranks first, in every section |
| The walk | Once per directory, keeping every data file; each key narrows the last result |
| Results | The best 1,000 (`[home.search] max_results`); the heading counts the rest and the entries read: `1,000 of 2,500 matches`, `23,041 searched` |
| Cut short | The heading says `partial · out of time`, `· too many files` or `· too deep` |
| Skipped | Hidden directories (`.git`, `.venv`), build and dependency directories (`node_modules`, `target`, `build`, `dist`, `vendor`, `site-packages`, `__pycache__`, `venv`, `env`), other file systems, symlinks. `.gitignore` is not read |

[`[home.search]`](../reference/settings.md#home-search) sets the depth, time,
results and exclusions; `cross_filesystems = false` stays off network shares
and automounts.

## The details pane

| Field | Says |
|---|---|
| Kind, storage | The format, and the file system or object store |
| Read | How a file opens: `lazy`, `converted once`, `in memory`, or `downloaded, then` one of those ([formats](../formats/index.md#how-each-format-is-read)) |
| Contains | Files by format, directories and partitions |
| Rows × columns | Known counts; blank when finding them would read the data. Parquet counts come from footers, up to 64 files; past that, `? × 39+` |
| On disk, in memory | The stored size; Parquet's uncompressed size |
| Row groups | Parquet's read units |
| Partitions | Keys and values from the directory names |
| Schema | The known columns and types |
| `ROWS` | The first eight rows of a local CSV, TSV, PSV, NDJSON, Arrow IPC or Parquet file, read when the row is selected. <kbd>Enter</kbd> opens the file on those rows, so they are read once. `[home] preview_max` sets the largest file read; `0` turns it off. Network shares and object stores are not read before opening |

Below about 100 columns the pane hides, and the first rows show in a strip at
the bottom when the list leaves four lines free.

### What a row's label says

A row reads its name, `/` for a directory, two spaces, and a label:
`data/  3 dirs`, `events/  hive`, `Palmer penguins  csv`.

| Label | Means |
|---|---|
| `hive` | `key=value` subdirectories, at least as many as the data files beside them |
| `delta`, `iceberg`, `hudi` | A lake table's marker |
| `12 parquet`, `3 csv` | Data files of one format directly inside; `5000+ parquet` when the listing stopped |
| `3 safetensors`, `2 gguf` | A model: weight files with only JSON beside them; opens as one table |
| `3 tables` | A file that holds tables: SQLite, NumPy `.npz`, a flight or CAN log, a workbook, an NMEA log, an ELF file. See [Info panel](dataset-info.md#file-format-tabs) |
| `mixed` | Several formats |
| `3 dirs`, `dir`, `dir+` | Only directories; nothing; the listing was cut short |
| `bucket`, `container` | The top of an object store |
| `…`, a spinner, `?` | Not looked at yet, being looked at, failed |

Names starting `_` or `.` and `_$folder$` markers are passed over, except
partitions such as `_date=2025-01-01`.

A file not read lazily says how, dim beside its name:

| Marker | Opening it |
|---|---|
| `converts` | Reads it once into a temporary file |
| `in memory` | Reads it whole into memory (a model file: its header) |
| `downloads` | Downloads it first |

The `… files with no reader` row, or <kbd>Ctrl</kbd>+<kbd>A</kbd>, shows files
no reader takes, dimmed; `[home] show_unreadable = true` shows them always.
<kbd>Enter</kbd> on a local one, or <kbd>Ctrl</kbd>+<kbd>X</kbd> on any local
file, shows its bytes in the [hex view](hex-view.md). Inside a SQLite database
the same row shows its internal tables.

### Opening a directory

<a id="two-doors-into-every-directory"></a>

<kbd>→</kbd> always goes inside. <kbd>Enter</kbd> does what the bar says:
`Open all` (one table), `Inside`, `Open` (a file), or `Look` (find out first).
Inside, the first row reads the directory as one table and says how:

| Directory | First row | The cursor starts on |
|---|---|---|
| Hive partitions | `sales (hive table: year, month)` | this row |
| One format, one schema | `same (3 Parquet files, one schema)` | this row |
| One format, schemas differ | `diff (2 Parquet files, schemas differ)` | the first file |
| Model weights | `llama (model, 3 SafeTensors files)` | this row |
| One data file | `notes (1 CSV file)` | the first file |
| Several formats, or files beside directories | `data (all files, mixed)` | the first row inside |
| Delta, Iceberg or Hudi | `tbl (Delta files, not the table)` | the first row inside |

On a mixed directory the pane names what is read and what is skipped. Delta,
Iceberg and Hudi files are read without the transaction log, so deleted rows
and old versions may show. How files combine is in
[Open files and directories](open-files.md#files-that-disagree).

<a id="the-door-does-not-refuse"></a>
<a id="where-a-rows-data-lives"></a>

### Storage markers

| Marker | ASCII | Where the data is |
|---|---|---|
| `◦` | `.` | Local disk |
| `▪` | `*` | Memory, such as tmpfs |
| `↕` | `~` | A network file system: NFS, SMB, sshfs |
| `≈` | `@` | An object store or URL |
| `◌` | `?` | Unknown |

## Collections

A collection is a list of datasets you name in the config, local and remote,
shown as a section under its label; [Dataset collections](../reference/sources.md)
has the `[[sources]]` keys.

```text
▾ MY DATASETS  5   configured  ────────────────────────────────────
  ▪ Sales                                          6.8 KB   now
  ▪ Archive/ 1 csv                                          now
  ◦ Gone missing
  ≈ Weather/ dataset
  ≈ Penguins
```

| Row | <kbd>Enter</kbd> | Label |
|---|---|---|
| A local file | Opens it | Measured like any file |
| A local directory | Goes inside | What is inside |
| A local path with nothing there | Says so | `missing` |
| A directory in an object store | Goes inside; <kbd>Backspace</kbd> at its top comes back | `dataset` |
| A remote file | Opens it | Its format, and its `size` when given |

Nothing remote is asked for until you open or enter a dataset. Inside one, the
title reads `My datasets › Weather › by_year`, and the pane gives its
description, publisher, license, homepage, URL and login.

### Public datasets

`Public datasets` is the built-in collection: data its publishers host, read
with no login, listed after your own. datui ships none of the data.

| Dataset | Data | License |
|---|---|---|
| NYC flights (2013) | Departures from JFK, LaGuardia and Newark; delays in minutes | CC0 (nycflights13) |
| Food nutrition (fast food) | 515 menu items; nutrients per item, not per 100 g | GPL-3 (OpenIntro package) |
| US baby names (1880-2017) | Published name counts by year and sex; counts below five are suppressed | CC0 / public domain |
| NOAA daily weather (GHCN-D) | Worldwide weather station observations, by year and by station | CC0 |
| Premier League (2020-21) | Match rounds, dates, teams and full-time scores | CC0 |
| NYC yellow taxis (January 2025) | One monthly trip file; fares, distances and congestion fees | NYC Open Data terms |
| Earthquakes (past month) | A rolling month of earthquakes; magnitude, depth and location | Public domain |
| Space launches (1957-2018) | Launch records and agencies, failed attempts included | MIT (The Economist extract); credit Jonathan McDowell |
| Palmer penguins | 344 penguins: species, island, bill, flipper length and body mass | CC0; credit Horst, Hill and Gorman (2020) |
| Aqueous solubility (SDF) | 1,025 molecules: solubility (log mol/L), its class, and SMILES | BSD-3-Clause (RDKit) |
| Bitcoin and Ethereum | Blocks and transactions, partitioned by date | AWS sample-code license |
| Overture Maps | Places, buildings, addresses, roads and boundaries, by release | ODbL; places CDLA Permissive 2.0 and Apache 2.0 |

- A web file's row gives its format and size. One under 50 MB downloads
  without a question; a URL typed at <kbd>~</kbd> is always asked about.
- Once opened, a dataset comes back under Recent by its catalog name.
- The pane gives the publisher, license and homepage; check the license
  before you use the data.
- A build without the `http` or `cloud` feature leaves out the rows it cannot
  open.
- A collection named `public` replaces this one; `[home] builtin_catalog =
  false` drops it and `[home] hide = ["public"]` hides it.

The guides use them:

| Dataset | In |
|---|---|
| Palmer penguins | [Quick start](../getting-started/quick-start.md), [charts](charting.md), [Analysis](analysis-features.md), [Python](python-module.md) |
| NYC flights (2013) | [Query data](querying-data.md), [charts](charting.md) |
| Food nutrition (fast food) | [Sort and filter](filtering-sorting.md), [copy](copying.md), [data quality](data-quality.md) |
| US baby names, Space launches | [Pivot and melt](reshaping.md) |
| Premier League (2020-21) | [Query data](querying-data.md), [export](exporting-data.md) |
| NYC yellow taxis (January 2025) | [Analysis](analysis-features.md), [data quality](data-quality.md) |
| Earthquakes (past month) | [Charts](charting.md) |
| NOAA daily weather, Bitcoin and Ethereum | [Public data in the cloud](remote-data.md#examples-on-public-data), [views](views.md) |
| Aqueous solubility (SDF) | [Signals and logs](../formats/signals-and-logs.md) |

## Cloud sources

`CLOUD` lists a row per store: logins found on this machine and
[connections](../reference/cloud-sources.md) you configure. For private
storage, [sign in first](remote-data.md); for a first try with no login, use
[Public datasets](#public-datasets).

```text
▾ CLOUD  5  ──────────────────────────────────────────────────────
  ≈ Amazon S3         s3      3 buckets     datui config
  ≈ Google Cloud      gcs     4 projects    project: example-project · gcloud
  ≈ Lab MinIO         s3      1 bucket      127.0.0.1:9000 · datui config
  ≈ onprem            s3      403           minio.corp.example:9000 · datui config
```

| Column | Shows |
|---|---|
| Name | The source's `label`, or its name |
| API | `s3`, `gcs` or `azure` |
| Count | Its buckets (projects for Google Cloud, accounts for Azure), a spinner while listing, `not listed` before the first listing, or why there are none |
| Note | The endpoint, project or profile, and where the login was found |

<kbd>Enter</kbd> goes down a level: S3 source › bucket › directory › object;
Google Cloud source › project › bucket › ...; Azure source › account ›
container › .... The title shows the trail (`cloud › Lab MinIO › data › 2024`);
<kbd>Backspace</kbd> goes up one level and <kbd>Esc</kbd> back to where you
started.

### Loading

The rows show at once, with the buckets an earlier run listed. Nothing is
listed, and no credential command (`aws`, `gcloud`, `az`, a
`credential_process`) runs, until you ask:

| To list | Do |
|---|---|
| One source | <kbd>Enter</kbd> or <kbd>→</kbd> on it, once a session |
| Every source on screen | <kbd>Ctrl</kbd>+<kbd>R</kbd> |
| Every source at launch | `[cloud] list_on_start = true` |

A slow endpoint holds up only its own row. A level lists 1,000 names at a
time (`3,000 so far`) and stops at 5,000 (`first 5,000`); typing past them
asks the bucket for the names the filter starts, and the heading adds
`+ 1,907 STATION=USW*`. <kbd>Backspace</kbd> or <kbd>Esc</kbd> stops a
listing. Typing also matches bucket names listed before, from every source.

When listing fails, the row says why in a word and the pane gives the whole
message:

| Row says | Means |
|---|---|
| `403` | The login cannot list buckets; an object still opens by its URL |
| `not logged in` | No credentials reached the store |
| `no project` | A Google login that cannot search projects, and none named: set `GOOGLE_CLOUD_PROJECT` or `DATUI_GCP_PROJECT` |
| `unsupported login` | An application-default login datui cannot use itself, and no `gcloud` to ask |
| `needs gcloud` | A login through `gcloud`, which is not installed |
| `not signed in` | Azure tools installed, nobody signed in; the pane names `az login` or `Connect-AzAccount` |
| `not configured` | A variable named in `[[cloud.connections]]` is not set |
| `unavailable` | The endpoint did not answer |
| `not found` | The source went away since its row was drawn; <kbd>Ctrl</kbd>+<kbd>R</kbd> looks again |

<kbd>Delete</kbd> on a source hides it until `datui cache clear`. To hide one
for good:

```toml
[cloud]
hide = ["gcs-default"]
```

### Which sources appear

datui adds the logins it finds and the connections you configure;
[detected sources](../reference/cloud-sources.md#detected-sources) lists where
each is found, its id and the `discover` setting. Listing a bucket does not
mean its objects can be read. Collections, public or yours, have sections of
their own.

### What a cloud row shows

A listing shows name, size and modification time, and labels directories as
local ones are (`hive`, `12 parquet`, `dir`); job files such as `_SUCCESS` and
empty folder objects are left out. Row counts and schemas are read when a
dataset opens.

## Loading

<kbd>Enter</kbd> shows the load's progress; <kbd>Ctrl</kbd>+<kbd>O</kbd>
cancels it. A file that fails to open shows the error here, and
<kbd>Esc</kbd> returns to the dataset open before. A network location that
does not answer reads `unavailable`; <kbd>Ctrl</kbd>+<kbd>R</kbd> tries again.

## What datui remembers

The cache holds recent paths, how often and how lately each was opened, and
what was measured (counts, column names, size, modification time), never the
data. Local facts are measured again when a file's size or time changes.

| Command | Removes |
|---|---|
| `datui cache clear --recents` | Recent paths only |
| `datui cache clear` | Everything cached: recents, measurements, remembered directories, query history |

```bash
datui cache clear --recents
```

## Limits

| Work | Limit |
|---|---|
| Recent paths | 50 |
| Directories promoted from recents | 8 |
| Entries read to label a directory, or listed | 5,000 |
| Subdirectories looked into per listing | 64 |
| Files read for a preview count | 64 |
| Datasets measured at once | 12, those on screen |
| Network directories probed at once | 4 |
| Search | Depth 8; 100,000 files kept; 1,000 matches listed; 1.5 seconds |

## Narrow and plain terminals

| Width or height | What changes |
|---|---|
| Below about 100 columns | The details pane hides |
| Below about 56 columns | The size and shape columns hide |
| Wide | The list stops at 84 columns; the pane takes the rest |
| Below 28 rows | The wordmark becomes a one-line title |

Without UTF-8, markers and borders are ASCII;
[`display.unicode`](configuration.md#glyphs-or-ascii) overrides the guess.

Linux packages install a desktop entry for application menus and file
managers' "Open with"; launched from a menu, datui opens here.
