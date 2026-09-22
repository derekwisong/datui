# The Home Screen

Run `datui` with no arguments, or press <kbd>Ctrl</kbd>+<kbd>O</kbd> from
anywhere, and you get a list of datasets with what each contains before you
open it.

![Home and Cloud Demo](../demos/14-cloud-home.gif)

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
| <kbd>←</kbd> <kbd>→</kbd> | Fold or unfold the section. Remembered between runs. On a folder labelled `hive`, `multi`, `delta`, `iceberg` or `hudi`, <kbd>→</kbd> goes inside it, so one partition or one file can be reached |
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
footers, summed over at most 64 files for hive and multi-file datasets. A larger
one is not counted, and its columns come from a spread of the folder rather than
all of it, so it shows `? × 39+`: neither figure is a total, and both say so. CSV
and other formats that need a scan to count show neither. Below the counts,
the pane lists the full schema of a Parquet dataset, each type in the color the
table uses.

### When a folder is one dataset

A folder of `key=value` partitions is `hive`, and a folder of Parquet files that
hold the same table is `multi`. Both open with <kbd>Enter</kbd> as a single
dataset, and <kbd>→</kbd> goes inside one instead, to reach a single partition or
a single file. That is the way to look at the files when the label is wrong.
<kbd>Esc</kbd> comes back out; in a bucket the first row inside,
`<folder> (all files)`, opens the whole folder again, and locally there is no
such row.

Sharing a file extension is not enough to make a folder one table. A database
exported one Parquet file per table — `circuits.parquet`, `drivers.parquet`,
`laps.parquet` — looks identical from its names, and reading it as one table
would union things that share no columns. So the footers decide: datui compares
the columns of the folder's files, and a folder whose files do not agree is left
as a directory to look inside.

A `delta`, `iceberg` or `hudi` row is a lake table: a log beside the data files
says which of them are live. datui does not read that log yet, so it does not
offer the table as one dataset — the files a delete or an update tombstoned are
still on disk, every rewritten version is there together, and compaction leaves
both sides in place, so reading them as one table gives rows the table does not
have. <kbd>Enter</kbd> and <kbd>→</kbd> both go inside instead, where the data
files can be opened one at a time, and say so when they do.

A row on a network share that nothing has looked at yet — a recent one, say, where
reading it just to list it is how a dead mount freezes a file browser — is looked
at when you open it, on a background thread, and <kbd>Enter</kbd> then does
whatever the answer calls for. The line under the list says `Looking at …`
meanwhile, and the keys keep being read. If the share never answers,
<kbd>Ctrl</kbd>+<kbd>O</kbd> puts the wait down and gives you the home screen back.

| Format | What marks the root |
|---|---|
| Delta Lake | `_delta_log/` |
| Hudi | `.hoodie/` |
| Iceberg | `metadata/` holding a `*.metadata.json`, beside `data/` |

In a bucket the Iceberg test is `metadata/` beside `data/` with no Parquet at the
root: looking inside `metadata/` would be a second listing, and the layout is
enough. So a folder that happens to hold both names is labelled `iceberg` there.
It is still somewhere to go, which a table read as one table is not.

Files are compared by how much of the narrower one the wider one holds, not by
how much they have in common overall, because gaining a column is what a dataset
does over time. A blockchain that added `txinwitness` in 2017 is still one
dataset, and so is one that grew from five columns to fifty.

The columns come from footers that are read anyway to count the rows, so locally
this costs nothing. In a bucket, three of the folder's files are read while you
browse — one small ranged request each, never a whole object — and a folder that
cannot be read keeps the label its names suggested.

### Folders not looked into yet

| Label | Means |
|---|---|
| `dir` | An ordinary directory, looked into and found to hold no single table |
| `…` | Not looked into yet |

Telling the two apart costs a directory read each — a round trip apiece on a
network share, so a directory of thousands of partitions would be minutes before
the listing appeared. No listing pays for it, however small. Every folder is
drawn as `…` straight away, and the rows on screen are looked into a screenful
at a time as you scroll, the highlighted row first. What datui found last time
is remembered, so a folder you have already browsed is labelled before anything
is read.

Labels never re-order the list when they arrive, so a row cannot move out from
under the cursor. <kbd>Enter</kbd> on a `…` row looks into it first, so it opens
as whatever it turns out to be. Sorting by rows puts them last: there is no
count to sort them by yet.

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
row lists what is inside, one level at a time; prefixes descend like directories,
objects open like files, and opened objects go into `RECENT` like any other path.

| Source | Levels |
|---|---|
| S3 and S3-compatible | source › bucket › prefix › object |
| Google Cloud | source › project › bucket › prefix › object |
| Azure | source › account › container › folder › blob |
| Public datasets | source › dataset › prefix › object |

```
▾ CLOUD  5  ──────────────────────────────────────────────────────
  ☁ Amazon S3         s3      3 buckets     datui config
  ☁ Google Cloud      gcs     4 projects    project: example-project · gcloud
  ☁ Lab MinIO         s3      1 bucket      127.0.0.1:9000 · datui config
  ☁ onprem            s3      403           minio.corp.example:9000 · datui config
  ☁ Public datasets   public  6 datasets    built in
```

| Column | Shows |
|---|---|
| Name | The source's `label`, or its name |
| API | `s3`, `gcs`, `azure`, or `public` |
| Count | How many buckets (projects for Google Cloud, accounts for Azure, datasets for public data), a spinner while listing, or why there are none |
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
| `no project` | A Google login that cannot search for projects, and no project is named; set `GOOGLE_CLOUD_PROJECT` or `DATUI_GCP_PROJECT` |
| `unsupported login` | An application-default login datui cannot use itself (workload identity federation, impersonation), and no `gcloud` to ask |
| `needs gcloud` | A login through `gcloud`, which is not installed |
| `not signed in` | Azure tools are installed but nobody is signed in; the pane names `az login` or `Connect-AzAccount` |
| `not configured` | A variable named in `[[cloud.sources]]` is not set |
| `unavailable` | The endpoint did not answer |

### Loading

The rows appear at once. The buckets listed on the last run are shown straight
away, and every source is listed again in the background, a few at a time, each
row updating as its answer arrives, so a slow endpoint holds up only its own row.
<kbd>Ctrl</kbd>+<kbd>R</kbd> lists again whatever is on screen.

A recent from a named S3-compatible source shows the name beside it, or
`source not found: <name>` once that source has left the config.

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
| Each other AWS profile that can log in | `aws-<profile>` | Keys, `credential_process`, SSO or a role in the profile |
| Each MinIO client alias | `mc-<alias>` | An alias with keys in `mc`'s `config.json` (`~/.mc/`, `~/.mcli/`, or `MC_CONFIG_DIR`), or `MC_HOST_<alias>` in the environment, which wins |
| s3cmd's server | `s3cfg` | Keys in the `[default]` section of `~/.s3cfg` (`%APPDATA%\s3cmd.ini` on Windows, or `S3CMD_CONFIG`) |
| Azure | `az` | The Azure CLI has been used (`~/.azure`, or `AZURE_CONFIG_DIR`), or Azure PowerShell signed in (`~/.Azure/AzureRmContext.json`). Its rows are storage accounts, found across your subscriptions. With `az` on `PATH` or the Az.Accounts module installed and neither signed in, the row says `not signed in` |
| Azure from the environment | `azure-env` | `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_ACCOUNT_NAME` with a key or SAS token, or a service principal (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET` or `AZURE_FEDERATED_TOKEN_FILE`) |
| Google Cloud | `gcs-default` | `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`, `GOOGLE_SERVICE_ACCOUNT_KEY`, `GOOGLE_APPLICATION_CREDENTIALS`, the file written by `gcloud auth application-default login`, or else the active `gcloud` configuration's login. Its rows are projects |
| Each other `gcloud` configuration with a different account | `gcloud-<configuration>` | An `account` in `configurations/config_<name>` under `~/.config/gcloud` (`%APPDATA%\gcloud` on Windows, or `CLOUDSDK_CONFIG`) |
| Public datasets | `public` | The configured `public` source, or the built-in catalog unless `public_datasets = false` |
| Each `[[cloud.sources]]` entry | its `name` | Always |

A source in the config with the same name as one of these replaces it. The same
server with the same key found in several places is one row, from the first of:
the config, the environment, other tools' files; its note lists every place.
`mc`'s placeholder aliases and its public `play` server are left out. See
[Loading Data](loading-data.md#several-stores-at-once) for `[[cloud.sources]]`.

Google Cloud lists every project the login can find. The project in
`DATUI_GCP_PROJECT`, `GOOGLE_CLOUD_PROJECT`, `GCLOUD_PROJECT`, `CLOUDSDK_CORE_PROJECT`
or `GCP_PROJECT`, the `quota_project_id` in the gcloud credentials file, or the
active configuration's project comes first, and is listed alone when searching for
projects is refused.

A profile that needs the AWS CLI shows `needs the AWS CLI` when it is not
installed, and an expired SSO login shows the CLI's message; see
[Loading Data](loading-data.md#aws-profiles). A cloud VM's identity (an EC2
instance role, a GCE service account, an Azure managed identity) is **not**
discovered unless `[cloud] instance_identity = true`, because finding it means a
metadata request that hangs on some networks. Cloud Run and Cloud Functions
(`K_SERVICE`) and Azure App Service, Functions and Container Apps
(`IDENTITY_ENDPOINT`, `MSI_ENDPOINT`) say so themselves, and are used without it.
Otherwise, with no other login, a URL is read unsigned, which reaches public data
only.

### Public datasets

`Public datasets` lists data its publishers host and keep up to date, readable with
no login. Nothing is requested until you open one.

| Dataset | Data | License |
|---|---|---|
| NOAA daily weather (GHCN-D) | Weather station observations worldwide, as Parquet by year and by station | CC0 |
| Bitcoin and Ethereum | Blocks and transactions, as Parquet by date | AWS sample-code license |
| OpenAlex | Scholarly works, authors, institutions and topics, as Parquet | CC0 |
| Overture Maps | Places, buildings, addresses, roads and boundaries, as GeoParquet by release | ODbL; places CDLA Permissive 2.0 and Apache 2.0 |
| Google Open Buildings | 1.8 billion building footprints, as CSV | CC BY 4.0 or ODbL |
| BigQuery sample data | The small samples Google's documentation uses | Not stated |

The details pane gives each one's publisher, license and homepage. The license is
the publisher's: check it before you use the data. <kbd>Backspace</kbd> at a
dataset's top returns to the list. A public bucket or container you have browsed or
opened unsigned is added to the list. For a list of your own, see
[Loading Data](loading-data.md#public-data).

`datui --generate-config` writes this catalog as active
`[[cloud.sources.datasets]]` tables. Remove a table to remove that dataset, add a
table to add one, or change its metadata. A configured source named `public`
replaces the built-in list; another source name creates a separate collection. The
generated list is a snapshot and does not receive later catalog updates
automatically. See [Configuration](configuration.md#cloud) for the fields.

Listings leave out what is not data: `_SUCCESS` and other job files, and the empty
objects some tools leave to stand for folders.

### What a cloud row shows

Inside a bucket: name, size and modification time, which is what a listing
returns. Row counts and columns would need a read per object, which someone is
billed for, so they are not fetched until you open one.

Folders are looked inside, a few at a time, once each listing lands: one small
listing request per folder, for at most 48 of them. A folder of `key=value`
partitions is then labelled `hive`, and a folder of Parquet files whose schemas
agree `multi`, like a local one — see
[When a folder is one dataset](#when-a-folder-is-one-dataset). Deciding that last
one reads the footers of up to three of the folder's files, a few kilobytes each;
nothing else here reads an object, and nothing reads a whole one. <kbd>Enter</kbd>
opens it as one dataset, with the partitions as columns; <kbd>→</kbd> goes inside
instead, where the first row, `<folder> (all partitions)`, opens the whole folder
again.

A partitioned dataset whose files gained columns over time, such as a blockchain's
first day, which has no previous block, opens with every column: its schema comes
from the first and the last file, and older files read the newer columns as empty.

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
