# The Home Screen

Run `datui` with no arguments, or press <kbd>Ctrl</kbd>+<kbd>O</kbd> from
anywhere, and you get a list of datasets with what each contains before you
open it.

![Home Screen Demo](../demos/12-home-screen.gif)

```
 ╺┳┓┏━┓╺┳╸╻ ╻╻
  ┃┃┣━┫ ┃ ┃ ┃┃   ~/work/analysis
 ╺┻┛╹ ╹ ╹ ┗━┛╹
 › ▏  filter and search
 ▾ RECENT  2  ────────────────────────────────────────────────────────────
 ▎ ◦ sales  hive                            2.4M × 18    340 MB    2d
   ◦ customers.parquet                       89k × 12      4 MB    1w
 ▾ /mnt/data  2  ──────────────────────────────────── nfs4 · configured
   ⇅ events  hive                            1.1M × 9    120 MB    3h
   ⇅ lookup.parquet                           980 × 4      8 KB   2mo
 ▾ ~/work/analysis  2  ──────────────────────────────── current directory
   ◦ raw_export.csv                                     1.2 GB    3h
   ◦ notes/ dir
 Enter Open  ↑↓ Move  Esc Quit  type Filter  ~ Path  ←→ Fold  Tab Sort
```

Each row shows rows × columns, size and age. A pane on the right shows the
schema and the file details of the highlighted dataset.

## Keys

Every letter types into the filter, so `json` finds json. The keys are:

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> | Move (<kbd>Ctrl</kbd>+<kbd>P</kbd> / <kbd>Ctrl</kbd>+<kbd>N</kbd> too) |
| <kbd>Ctrl</kbd>+<kbd>↑</kbd> <kbd>Ctrl</kbd>+<kbd>↓</kbd> | Previous or next section |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Ten rows |
| <kbd>←</kbd> <kbd>→</kbd> | Fold or unfold the section. Remembered between runs |
| <kbd>Enter</kbd> | Open the dataset, enter the directory, cloud source or bucket, or fold the section |
| type | Filter by name or column name. Fuzzy: `sal` finds `sales` |
| <kbd>~</kbd> | Type a path. <kbd>Tab</kbd> completes it |
| <kbd>Tab</kbd> | Cycle the sort: default, size, modified, rows |
| <kbd>Backspace</kbd> | Delete a filter character, or go up one level |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | List again what is on screen, ignoring what is cached |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Delete</kbd> | Forget the highlighted entry under `RECENT`, or hide a cloud source |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Forget every recent entry, after confirming |
| <kbd>Esc</kbd> | Back out one layer: filter, then directory, then to the data you had open |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> | Quit |
| <kbd>?</kbd> | Help |

<kbd>Esc</kbd> never quits and <kbd>q</kbd> types a `q`. The control bar says
what <kbd>Esc</kbd> will do next.

## Sections

Datasets are grouped by where they came from, in this order:

| Section | Contents | Starts |
|---|---|---|
| `RECENT` | Datasets you have opened, newest first | open |
| current directory | Where you launched datui | open |
| `CLOUD` | One row per cloud source; <kbd>Enter</kbd> lists its buckets | open |
| configured directories | `[data] directories`, in the order listed | open |
| directories of recent datasets | The 8 most recent | folded |
| `ELSEWHERE` | Directories from your desktop's recent-files list | folded |
| `Found` | Datasets below the current directory, while you are typing | |

A folded section shows how many rows it hides. Filtering keeps the grouping, so
a match always shows which section it came from.

### Adding a directory

Opening a dataset adds its directory to the list, so a place only has to be
found by hand once (<kbd>~</kbd>, type the path, open something). Directories
gathered this way disappear when they hold nothing.

To keep a place listed even when empty or unmounted, name it in the config:

```toml
[data]
directories = ["/mnt/data", "~/datasets", "$WORK/warehouse"]
```

`~` and `$VAR` are expanded. An unreachable directory shows as `unavailable`
instead of vanishing.

### Desktop places

`ELSEWHERE` lists the directories named in freedesktop's `recently-used.xbel`,
the file GTK apps and file managers write. Only directories, never file names,
and their contents appear only after you press <kbd>Enter</kbd>. Datui reads the
file and never writes it. To ignore it:

```toml
[data]
use_desktop_recents = false
```

## Searching below the current directory

Typing filters what is on screen and also starts a recursive search of the
working directory. Matches appear in a `Found` section, named by their path
below the root so three files called `sales.parquet` stay distinct.

```
Found   ~/work/analysis · 954 searched
  europe/q3/sales.parquet          12.4 MB   1.2M rows   3 days ago
  americas/q3/sales.parquet         9.1 MB   890K rows   3 days ago
```

The walk runs once, in the background, on the first keystroke; after that every
keystroke filters in memory. If it stops early the heading says so:
`partial · out of time`, `partial · too many` or `partial · too deep`.

### Matching

Fuzzy matching uses fzf's scoring, so it ranks the way fzf, Telescope and
snacks.picker do: a match after `/` or `_` beats one mid-word, consecutive
beats scattered, a match in the file name beats one in a directory, and ties go
to the shorter name. Matched characters are underlined.

Typing also matches **column names**: `customer_id` finds every dataset with
that column, with the match shown beside the row. Name matches rank above
column matches. Columns are known for Parquet datasets that have been listed
at least once, and are remembered between runs.

### What is skipped

| Rule | Effect |
|---|---|
| Hidden directories | `.git`, `.venv`, `.tox`, caches |
| A fixed name list | `node_modules`, `target`, `build`, `dist`, `vendor`, `site-packages`, `__pycache__`, `venv`, `env` |
| Filesystem boundaries | A search never wanders onto a mount |
| Symlinks | Not followed |

`.gitignore` is **not** read: people gitignore data directories because the
data is too big to commit, which is exactly the data you want to open.

### Tuning

```toml
[data.search]
enabled           = true
max_depth         = 8
max_results       = 20000
time_budget_ms    = 1500
cross_filesystems = false   # turn on if your data is on a fast mount below the working directory
follow_gitignore  = false
skip       = ["node_modules", "target", "build", "dist", "vendor",
              "site-packages", "__pycache__", "venv", "env"]
skip_extra = []             # add to the default list instead of replacing it
extensions = []             # empty = every format datui opens; e.g. ["parquet", "csv"]
```

`cross_filesystems` is the important one: leaving it off is what stops a search
from descending onto a network share, or on autofs from mounting one by looking
at it.

## The details pane

```
 DETAILS
source      nfs4
kind        hive
rows        412M
columns     38
on disk     184 MB
in memory   1.4 GB  zstd 7.6×
row groups  12
partitions  1,460 by date, region
            date 2021-01-01 to 2024-12-31
modified    3 days ago
```

| Line | From | Why it matters |
|---|---|---|
| `source` | the mount table | `nfs4`, `cifs` and `fuse.sshfs` all behave differently from a local disk. Remote sources are colored |
| `in memory` | the Parquet footer | what the data occupies once decompressed, against what it occupies on disk |
| `row groups` | the Parquet footer | one huge group cannot be read in parallel; thousands of tiny ones cost overhead |
| `partitions` | directory names | the layout of a partitioned dataset, without opening a file |

None of this reads the data itself. Row and column counts come from Parquet
footers, summed over at most 64 files for hive and multi-file datasets; a larger
dataset shows `? × 39`. CSV and other formats that need a scan to count show
neither. Below the counts, the pane lists the full schema of a Parquet dataset,
each type in the color the table uses.

### Where a row's data lives

A marker before each name says what opening it will cost:

| Unicode | ASCII | Means |
|---|---|---|
| `◦` | `.` | local disk |
| `▪` | `*` | memory, such as `/tmp` on tmpfs |
| `⇅` | `~` | network filesystem: NFS, SMB, sshfs |
| `☁` | `@` | object store or URL |
| `◌` | `?` | unknown |

Local disk is dimmed; the rest are colored.

## Cloud storage

Every object store datui can read is one row under `CLOUD`. <kbd>Enter</kbd> on a
row lists its buckets; <kbd>Enter</kbd> on a bucket lists what is in it. Prefixes
descend like directories, objects open like files, and opened objects go into
`RECENT` like any other path.

```
▾ CLOUD  4  ──────────────────────────────────────────────────────
  ☁ Amazon S3       s3     3 buckets     datui config
  ☁ Google Cloud    gcs    2 buckets     project: example-project · gcloud
  ☁ Lab MinIO       s3     1 bucket      127.0.0.1:9000 · datui config
  ☁ onprem          s3     403           minio.corp.example:9000 · datui config
```

| Column | Shows |
|---|---|
| Name | The source's `label`, or its name |
| API | `s3` or `gcs` |
| Count | How many buckets, a spinner while listing, or why there are none |
| Note | The endpoint, project or profile, and where the login was found |

The title bar shows where you are as a trail: `cloud › Lab MinIO › data › 2024`.
<kbd>Backspace</kbd> goes up one level, from a bucket back to its source, and
<kbd>Esc</kbd> returns to where you started.

The details pane for a source lists its endpoint, region, login and when its
buckets were listed. When listing failed, the row says it in a word and the pane
gives the whole message:

| Row says | Means |
|---|---|
| `403` | The login cannot list buckets. An object can still open by its URL, `datui s3://bucket/key` |
| `not logged in` | No usable credentials reached the store |
| `no project` | A Google login with no project to list; set `GOOGLE_CLOUD_PROJECT` or `DATUI_GCP_PROJECT` |
| `not configured` | A variable named in `[[cloud.sources]]` is not set |
| `unavailable` | The endpoint did not answer |

### Loading

The rows appear at once. The buckets listed on the last run are shown straight
away, and every source is listed again in the background, a few at a time, each
row updating as its answer arrives, so a slow endpoint holds up only its own row.
<kbd>Ctrl</kbd>+<kbd>R</kbd> lists again whatever is on screen.

Typing also matches bucket names already listed, from every source, in `Found`:
`Lab MinIO › data` and `onprem › data` stay two rows.

<kbd>Delete</kbd> on a source hides it until `datui --clear-cache`. To hide one
for good:

```toml
[cloud]
hide = ["gcs-default"]
```

### Which sources appear

Exactly the ones datui can use to open the data, so a bucket that is listed is
one that can be read.

| Source | ID | Appears when |
|---|---|---|
| Amazon S3, or the endpoint in `[cloud]` | `s3-default` | Keys in `[cloud]` or `AWS_ACCESS_KEY_ID`, an ECS or Fargate task role, an EKS web identity, `AWS_PROFILE`, or a `~/.aws` directory |
| Google Cloud | `gcs-default` | `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`, `GOOGLE_SERVICE_ACCOUNT_KEY`, `GOOGLE_APPLICATION_CREDENTIALS`, or the file written by `gcloud auth application-default login` |
| Each `[[cloud.sources]]` entry | its `name` | Always |

A source in the config with the same name as one of these replaces it. See
[Loading Data](loading-data.md#several-stores-at-once) for `[[cloud.sources]]`.

Google needs a project to list buckets, taken from `DATUI_GCP_PROJECT`,
`GOOGLE_CLOUD_PROJECT`, `GCLOUD_PROJECT`, `CLOUDSDK_CORE_PROJECT` or
`GCP_PROJECT`, or from the `quota_project_id` in the gcloud credentials file.

AWS profiles are not read yet
([#168](https://github.com/derekwisong/datui/issues/168)): `AWS_PROFILE` or a
`~/.aws` directory shows the row, but it cannot list until the profile's keys are
exported; see [Loading Data](loading-data.md#amazon-s3). An EC2 instance role is
**not** discovered, because finding it means a metadata request that hangs on some
networks; opening a URL still works.

### What a cloud row shows

Inside a bucket: name, size and modification time, which is what a listing
returns. Row counts and columns would need a read per object, which someone is
billed for, so they are not fetched until you open one.

## Network locations

Nothing on the home screen touches a network location on the thread that draws
the screen. Network roots and cloud URLs are recognized from their name and the
mount table, listed in the background, and show a spinner until the listing
arrives. A location that never answers is marked `unavailable` and not retried.
Remembered counts for a remote dataset are shown without re-checking; local
datasets are re-measured when their size or modification time changes.

## Loading

<kbd>Enter</kbd> leaves the home screen at once and shows the load in
progress, with the phase it is in: scanning, caching the schema, filling the
first buffer. <kbd>Ctrl</kbd>+<kbd>O</kbd> during a load abandons it and comes
back. A dataset that will not open returns you here with the reason:

```
› corrupt▏   …parquet: 'parquet scan': the file must end with PAR1
```

## What datui remembers

Two things, both in the cache directory:

- The paths you have opened, at most 50.
- What it measured: row and column counts and column names, each stamped with
  the size and modification time it was taken from.

<kbd>Delete</kbd> forgets one recent entry, <kbd>Shift</kbd>+<kbd>Delete</kbd>
forgets them all, and so does `datui --clear-recents`. `datui --clear-cache`
clears everything including measurements and query history. Both are caches;
they cost only speed to rebuild.

## Limits

Every kind of work the home screen does is capped, so it stays fast however
long you have used it:

| Work | Limit |
|---|---|
| recent paths kept | 50 |
| directories promoted to sections by a recent | 8 |
| entries listed from one directory | 5,000 |
| subdirectories inspected per listing | 64 |
| files opened to classify a directory | 8 |
| files read to count a multi-file dataset | 64 |
| datasets measured at once | 12, only ones on screen |
| network directories probed at once | 4 |
| recursive search depth | 8 |
| recursive search results | 20,000 |
| recursive search time | 1.5 s |

A directory cut short reads `first 5000` beside its name. A subdirectory past
the 64 is still listed, as a directory rather than a dataset, until you enter it.

## Narrow and plain terminals

Below about 100 columns the details pane gives way to the list; below about 56
the size and shape columns go too. On a terminal shorter than 28 rows the
wordmark becomes a one-line title.

Without a UTF-8 locale datui falls back to ASCII everywhere: markers, arrows and
rules. No Nerd Font glyphs are used anywhere. Override the detection with:

```toml
[display]
unicode = "auto"    # "auto" (default), "always", or "never"
```

## Desktop launchers

The packages install `/usr/share/applications/datui.desktop`, so datui appears
in GNOME, KDE, rofi, wofi and Omarchy menus and file managers offer
"Open with datui" for the formats it reads. Launching it from a menu opens the
home screen.
