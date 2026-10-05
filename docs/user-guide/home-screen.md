# Home screen

`datui` with no path opens the home screen, where you find and open a dataset:
recent files, directories, cloud storage, your catalog and a catalog of public
data.

```bash
datui
```

<kbd>Ctrl</kbd>+<kbd>O</kbd> returns here from anywhere. Typing narrows the
list; <kbd>Enter</kbd> does what the footer names for the row. Every key is
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

The footer names where the list is, how many rows the filter matches and the
order, and at the right <kbd>Enter</kbd>, named for what it does on the selected
row (`Open`, `Open all`, `Inside`, `Look`), what <kbd>Ctrl</kbd>+<kbd>D</kbd> does
there (`^D Add` to `catalog.toml`, or `^D Forget` on its own rows), `^E Docs` on a
catalog row, then `? keys` (`F1 keys` once a filter is typed, since <kbd>?</kbd>
then types). Letters type into the filter, so
<kbd>q</kbd> types `q`; <kbd>Ctrl</kbd>+<kbd>C</kbd> quits.

## Sections

| Section | Lists |
|---|---|
| `RECENT` | Datasets opened before, grouped by the directory or cloud place each lives in |
| Current directory | Where datui was launched; an empty one says `nothing to open here · ~ types a path` |
| `CLOUD` | [Cloud sources](#cloud-sources): stores found on this machine and configured ones |
| `MY DATASETS` | Your [catalog](#catalogs), `catalog.toml`: what <kbd>Ctrl</kbd>+<kbd>D</kbd> added and what you wrote |
| Other catalogs | Each `*.toml` in the config directory's `catalogs/`, then each file `catalogs` lists, under its label |
| `PUBLIC DATASETS` | The bundled [public datasets](#public-datasets) |
| `ELSEWHERE` | Directories your desktop recorded (freedesktop `recently-used.xbel`); starts folded |
| `Found` | [Search](#search-below-the-current-directory) results, while you type |

Folds last between runs. A heading says why its section is listed and how it
stands: `catalog.toml`, `catalog` or `built in` for a catalog; `nfs4`, `listing`
(then `1,200 so far` as a slow share answers), `unavailable`, or `first 5,000`
when a listing stops there.

### Recent

Recent ranks datasets by frecency, as zoxide ranks directories: an open counts
four times within the hour, twice within the day, half within the week and a
quarter after. A place (a directory) ranks with its best dataset, and
entering it shows all its files, opened or not. The cursor starts on the
dataset opened last, so <kbd>Enter</kbd> reopens it. Places fill up to a third
of the list at first; `… more in … places` shows the rest.

### Add to your catalog

<a id="adding-a-directory"></a>
<a id="add-a-directory"></a>

Opening a dataset adds its directory to Recent. To keep a dataset or a
directory on the home screen, press <kbd>Ctrl</kbd>+<kbd>D</kbd> on its row: it
goes into `catalog.toml`, listed under `MY DATASETS`. <kbd>Ctrl</kbd>+<kbd>D</kbd>
again on that row forgets it. A directory there is a row to step into.

| Row | <kbd>Ctrl</kbd>+<kbd>D</kbd> adds |
|---|---|
| A file or directory | It, under the row's name |
| A heading of a directory's section, or a Recent place | That directory |
| A row of another catalog | A copy: location, login and description |
| A row of `catalog.toml` | Nothing: it forgets the row |

[Catalogs](../reference/catalogs.md) has the file's keys, team catalogs and
`datui catalog check`. `[home] desktop_recents = false` drops `ELSEWHERE`;
datui never writes the desktop's file, and lists a place there only when you
enter it.

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
| Kind, storage | The format, and the file system or object store. A file a [format spec](../formats/format-specs.md#which-spec-reads-a-file) reads, by its glob or its magic, says `acme.l2feed file` |
| Spec, match | For a spec's file: the spec's file, cut in the middle to fit, and what named the file, a chip per condition: `[magic L2FD] [version 3]`, drawn without brackets where the header tint shows ([format specs](../formats/format-specs.md#which-spec-reads-a-file)) |
| Read | How a file opens: `lazy scan`, `decompressed copy`, `converted to Arrow`, `in memory`, or `download →` one of those ([formats](../formats/index.md#how-each-format-is-read)) |
| Contains | Files by format, directories and partitions |
| Rows × columns | Known counts; blank when finding them would read the data. Parquet counts come from footers, up to 64 files; past that, `? × 39+` |
| On disk, in memory | The stored size; Parquet's uncompressed size |
| Row groups | Parquet's read units |
| Partitions | Keys and values from the directory names |
| Schema | The known columns and types; `3 columns (spec)` when a format spec says them, then each column; `on open` when only opening the file reads them |
| Records | For a file a format spec reads as several record types: `2 types (spec)`, then each type and its column count (`add 5 · cancel 3`) |
| `▲ footer unreadable` | A Parquet file whose footer could not be read; opening it will most likely fail too |
| Enter, `→` | What <kbd>Enter</kbd> and <kbd>→</kbd> do on a directory, a door, a file of tables or a file of record types: `all partitions as one table`, `step in · first row opens all`, `its tables`, `every record` and `its record types` |
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

A SQLite database with several tables lists them inside, a row each. A
workbook, an NMEA log or an ELF file opens its first worksheet, fixes or
symbols on <kbd>Enter</kbd>, and a file a [format spec](../formats/format-specs.md)
reads as several record types opens whole; <kbd>→</kbd> lists the record types,
and <kbd>Enter</kbd> on one opens it alone. A Hugging Face cache lists its splits,
as tables, above its files.

Names starting `_` or `.` and `_$folder$` markers are passed over, except
partitions such as `_date=2025-01-01`.

A file not read lazily says how, dim beside its name:

| Marker | Opening it |
|---|---|
| `decompresses` | Decompresses it whole into a temporary file, then scans that: compressed text |
| `converts` | Converts it whole into a temporary Arrow file, then scans that: an Arrow stream, NMEA, GPX, VCD, FIX, SDF |
| `in memory` | Reads it whole into memory: JSON, NDJSON, systemd journal, Avro, ORC, Excel, MIDI, ELF; SafeTensors and GGUF read only their header |
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

## Catalogs

<a id="collections"></a>

A catalog is a file of named datasets, local and remote, shown as a section
under its label: `catalog.toml` (`MY DATASETS`), each file in `catalogs/` or listed in `catalogs`, and
`PUBLIC DATASETS`. [Catalogs](../reference/catalogs.md) has the keys.

```text
▾ MY DATASETS  5   catalog.toml  ─────────────────────────────────
  ▪ Sales                                          6.8 KB   now
  ▪ Archive/ 1 csv                                          now
  ◦ Gone missing
  ≈ Weather/ dataset
  ≈ Penguins                                      ~16.1 KB
```

| Row | <kbd>Enter</kbd> | Label |
|---|---|---|
| A local file | Opens it | Measured like any file |
| A local directory | Goes inside | What is inside |
| A local path with nothing there | Says so | `missing` |
| A directory in an object store | Goes inside; <kbd>Backspace</kbd> at its top comes back | `dataset` |
| A remote file | Opens it | Its format, and its size: `~16.1 KB`, the catalog's word for it, until a `HEAD` sent when the row is selected measures it |

Nothing else remote is asked for until you open or enter a dataset. Inside one,
the title reads `My datasets › Weather › by_year`, and the pane gives its
description, publisher, license, homepage, URL and login.

### Documentation view

<kbd>Ctrl</kbd>+<kbd>E</kbd> on a catalog row, on a place inside one, or on a
file whose [format spec documents it](../reference/format-specs.md#documentation),
shows what the catalog and the spec say of it, full screen. The Info panel's
Documentation tab shows the same page for the open dataset.

| Line | Says |
|---|---|
| `catalog`, `publisher`, `license` | Where the entry is from, and the terms |
| `format`, `path` or `url`, `login`, `size` | What it is, where, how it is read, and how big (`~` until measured) |
| `format spec` | The spec that reads the file |
| `spec file` | Where that spec was read from |
| `LINKS` | `homepage` and `documentation`, a line each; a long one is cut with `…` |
| `RECORD TYPES` | A spec's variants: each one's name, the type field's value that picks it (`msg_type = 1`, `kind in ("E", "C")`), its column count and its description |
| `HEADER` | A spec's named `[header]` fields that have a description or unit, with them |
| `COLUMNS` | Each column, its meaning and unit; `▸ 30 values` when it has a legend, which a spec's `enum` gives |
| `FOOTER` | A spec's named `[footer]` fields that have a description or unit, with them |
| `BOOKMARKS` | The places to start from, and their paths |

When a catalog lists a file a spec documents, the catalog's description and
`documentation` link stand. Where both note a column, the catalog's
description, unit and legend each stand when it gives one, and the spec's fill
the rest: a catalog description of `price` keeps the spec's unit. The columns
only the spec notes, and its record types, stay.

| Key | Does |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd>, <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Move between lines |
| <kbd>Enter</kbd> | Open or close a column's value legend |
| <kbd>y</kbd> | Copy the line's link or value, whole |
| <kbd>Esc</kbd> | Back to the list |

<kbd>Ctrl</kbd>+<kbd>E</kbd> takes the place of readline's end of line: the
filter has no cursor, and is edited at its end.

### Public datasets

`Public datasets` is the bundled catalog: data its publishers host, read with
no login, listed after your own. datui ships none of the data;
`datui catalog show public` prints the
[catalog](../reference/catalogs.md#the-public-catalog).

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

- A web file's row gives its format and size, `~` until measured. One under 50 MB downloads
  without a question; if it passes 50 MB while downloading, it stops and
  asks once. A URL typed at <kbd>~</kbd> is always asked about.
- Once opened, a dataset comes back under Recent by its catalog name.
- The pane gives the publisher, license and homepage; check the license
  before you use the data.
- NYC flights, NOAA daily weather, NYC yellow taxis and Earthquakes carry
  their publisher's documentation: the pane lists what the columns mean
  under `DOCUMENTATION`, <kbd>Ctrl</kbd>+<kbd>E</kbd> shows the whole
  [Documentation view](#documentation-view), and the
  [Info panel](dataset-info.md) and the [inspector](inspecting-rows.md)
  explain them once the data is open.
- NOAA daily weather lists two bookmarks under it, `Daily highs, 2024` and
  `Central Park, NY`. <kbd>Enter</kbd> opens one whole; the dataset's own row
  still steps inside.
- A build without the `http` or `cloud` feature leaves out the rows it cannot
  open.
- A catalog file named `public.toml`, in `catalogs/` or listed, replaces this one;
  `[home] hide = ["public"]` hides it, and `["public/nyc-taxis"]` one entry.

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
mean its objects can be read. Catalogs, public or yours, have sections of
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

The cache holds recent paths, how often and how lately each was opened, what
was measured (counts, column names, size, modification time), query history,
section folds, bucket listings and hidden cloud sources, never the data. Your
catalog is in the config directory, not the cache. Local facts are measured again when a file's size or time changes.

| Command | Removes |
|---|---|
| `datui cache clear --recents` | Recent paths only |
| `datui cache clear` | Everything cached: recents, measurements, query history |

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
