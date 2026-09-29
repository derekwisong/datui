# Browse files

Run `datui` without a path, or press <kbd>Ctrl</kbd>+<kbd>O</kbd>, to find and
open a dataset. Type to filter the list; press <kbd>Enter</kbd> to open the
selected row. The details pane previews its schema when available.

## Open a file or directory

1. Type part of a name to filter the list.
2. Select the file with <kbd>↑</kbd> / <kbd>↓</kbd> and press <kbd>Enter</kbd>.
3. To browse inside a directory instead of combining its files, press <kbd>→</kbd>.

To enter a path, clear the filter and press <kbd>~</kbd>. Type the path, use
<kbd>Tab</kbd> to complete it, then press <kbd>Enter</kbd>.

<kbd>Ctrl</kbd>+<kbd>D</kbd> remembers a directory as its own section.
<kbd>Ctrl</kbd>+<kbd>O</kbd> returns home from an open table.

## Sections

| Section | Contents |
|---|---|
| `RECENT` | Opened datasets, grouped by their parent directory or cloud location |
| Current directory | Files where you launched datui |
| `CLOUD` | Detected and configured cloud sources; open one to list its contents |
| Configured directories | Paths from `[data] directories`, in configured order |
| Remembered directories | Paths saved with <kbd>Ctrl</kbd>+<kbd>D</kbd> |
| `ELSEWHERE` | Directories from the desktop's recent-files list; folded initially |
| `Found` | Recursive search results while you type |

Folded sections stay folded between runs. Path sections show why they are
listed and their status: for example `configured`, `nfs4`, `listing`,
`unavailable`, or `first 5000` when a listing is incomplete.

### Recent

A **place row** groups datasets by directory. Open the place to browse its
contents, including files you have not opened individually. This is why a
familiar directory can show more files than your recent individual opens.
Places are ordered by recency; sorting changes the dataset order within each
place.

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
[Named cloud sources](../reference/cloud-sources.md#named-sources).

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
once; later keystrokes filter its results in memory.

An incomplete search says `partial · out of time`, `partial · too many`, or
`partial · too deep` in its heading.

### Matching

Name matching uses fzf-style scoring: consecutive characters, word boundaries
and filename matches rank higher. Matched characters are underlined.
Known Parquet column names also match; name matches rank ahead of column
matches. Column metadata is remembered between runs.

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
| Contains | Files by format, directories and partitions |
| Rows × columns | Known counts; blank when they would require scanning data |
| On disk | Stored size |
| In memory | Parquet's uncompressed size, not current process memory |
| Row groups | Parquet's read units; their size affects paging cost |
| Partitions | Keys and values found in directory names |
| Schema | Known columns and their types |

Parquet previews read metadata, not data rows. Counts cover up to 64 files;
for a larger dataset the pane shows an unknown row count and a sampled column
count, such as `? × 39+`. CSV and other scan-to-count formats omit these counts.

### What a row's label says

| Label | Directory contents |
|---|---|
| `hive` | `key=value` subdirectories, at least as numerous as direct data files |
| `delta`, `iceberg`, `hudi` | A lake-table marker |
| `12 parquet`, `3 csv` | Direct data files of one format |
| `mixed` | Several formats |
| `dir` | No direct data files; `dir+` means the listing was cut short |
| `bucket`, `container` | The top of an object store |
| `…`, spinner, `?` | Not inspected yet, inspecting, or inspection failed |

The details pane's `contains` lines list the directory's contents.
Job markers and names beginning with `_` or `.` are skipped, except partition
names such as `_date=2025-01-01`. Folder markers ending in `_$folder$` are
also skipped. A capped listing says so, for example `5000+ parquet`.

Select the `… files datui can't open` row, or press <kbd>Ctrl</kbd>+<kbd>A</kbd>,
to reveal unreadable files such as `README.md`.
They are dimmed and cannot be opened. Set `[data] show_unreadable_files = true`
to show them by default.

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

Inside a directory, the first row is **(all files)** or **(all partitions)**.
Use it to combine the contents explicitly, even when Enter on the parent row
would browse. A label such as `15 parquet` describes the files, not whether
they form a single table.

### Combining files

<a id="the-door-does-not-refuse"></a>

The **(all files)** or **(all partitions)** row combines a directory into one
table. [Files and formats](loading-data.md#directories) explains format selection
and [schema differences](loading-data.md#files-that-disagree).
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

## Cloud storage

Open a source under `CLOUD` to browse buckets, projects or containers.
**Public datasets** needs no login. See [Cloud browser](cloud-browser.md)
for discovery, refresh behavior and listing errors, or
[Remote data](remote-data.md) for credentials and URLs.

<a id="which-sources-appear"></a>
<a id="public-datasets"></a>
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

Datui caches recent paths and measured metadata: counts, column names, size
and modification time. Clear them with:

| Command | Removes |
|---|---|
| `datui --clear-recents` | Recent paths only |
| `datui --clear-cache` | Cached metadata, recents, remembered directories and query history |

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
| <kbd>~</kbd> with an empty filter | Enter a path or URL; <kbd>Tab</kbd> completes paths |
| <kbd>Tab</kbd> | Cycle sort: natural, size, modified, rows |
| <kbd>Backspace</kbd> | Delete a character; with an empty filter, go up a directory |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | Refresh the locations on screen |
| <kbd>Ctrl</kbd>+<kbd>A</kbd> | Show or hide files datui cannot read |
| <kbd>Ctrl</kbd>+<kbd>D</kbd> | Remember or forget the selected directory; a file represents its parent |
| <kbd>Delete</kbd> | Forget a recent entry; on a place, confirm forgetting its entries; on a remembered heading, forget it; on a cloud source, hide it |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Confirm forgetting all recent entries |
| <kbd>Esc</kbd> | Clear the filter, leave a directory, or return to the open table |
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
| Recursive search | Depth 8; 20,000 results; 1.5 seconds |

Entries beyond an inspection limit remain browsable. The listing or search
heading marks incomplete results.

## Narrow and plain terminals

The details pane hides below roughly 100 columns; size and shape columns hide
below roughly 56. Below 28 rows, the wordmark becomes a one-line title.
Without a UTF-8 locale, markers and borders use ASCII. Override detection with:

```toml
[display]
unicode = "auto"    # "always" or "never" to override
```

## Desktop launchers

Linux packages include a desktop entry for application menus and file-manager
“Open with” actions. Launching datui from a menu opens home.
