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
 ▾ RECENT  3  ────────────────────────────────────────────────────────────
   /mnt/data/warehouse/  2 parquet                                   nfs4
 ▎   ⇅ sales  hive                          2.4M × 18    340 MB    2d
     ⇅ customers.parquet                     89k × 12      4 MB    1w
   ~/exports/
     ◦ q3.csv                                          1.2 GB    3h
 ▾ ~/work/analysis  2  current directory  ───────────────────────────────
   ◦ raw_export.csv                                     1.2 GB    3h
   ◦ notes/ dir
 ▾ /mnt/data  2  configured  ──────────────────────────────────── nfs4
   ⇅ events  hive                            1.1M × 9    120 MB    3h
   ⇅ lookup.parquet                           980 × 4      8 KB   2mo
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
| <kbd>←</kbd> <kbd>→</kbd> | Fold or unfold the section. Remembered between runs. On any directory <kbd>→</kbd> goes inside it, whatever its label, so one partition or one file can always be reached. On a place row under `RECENT` it browses the place |
| <kbd>Enter</kbd> | Open the dataset, enter the directory, cloud source, bucket or place, show the rest of `RECENT`, or fold the section |
| type | Filter by name or column name. Fuzzy: `sal` finds `sales` |
| <kbd>~</kbd> | Type a path. <kbd>Tab</kbd> completes it |
| <kbd>Tab</kbd> | Cycle the sort: default, size, modified, rows |
| <kbd>Backspace</kbd> | Delete a filter character, or go up one level |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | List again what is on screen, ignoring what is cached |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Delete</kbd> | Forget the highlighted entry under `RECENT`, or every recent under the highlighted place after confirming, or hide a cloud source |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Forget every recent entry, after confirming |
| <kbd>Esc</kbd> | Back out one layer: filter, then directory, then to the data you had open |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> | Quit |
| <kbd>?</kbd> | Help |

<kbd>Esc</kbd> never quits and <kbd>q</kbd> types a `q`. The control bar says
what <kbd>Esc</kbd> will do next.

## Sections

Datasets are grouped by where they came from, in this order:

| Section | Contents | Chip | Starts |
|---|---|---|---|
| `RECENT` | Datasets you have opened, grouped under the directory or prefix each lives in | | open |
| current directory | Where you launched datui | `current directory` | open |
| `CLOUD` | One row per cloud source; <kbd>Enter</kbd> lists its buckets | | open |
| configured directories | `[data] directories`, in the order listed | `configured` | open |
| `ELSEWHERE` | Directories from your desktop's recent-files list | | folded |
| `Found` | Datasets below the current directory, while you are typing | | |

A section titled by a path carries a chip beside its count saying why it is
there. The note at the far end of the rule says how it is doing: the filesystem
it is on (`nfs4`), `first 5000` for a listing cut short, `listing`, or
`unavailable`. A folded section shows how many rows it hides. Filtering keeps
the grouping, so a match always shows which section it came from. With thirty
list rows or more, a blank line separates the sections.

### Recent

Every dataset you have opened sits under a **place row**: the directory or
bucket prefix it lives in, newest place first. The place row names the path,
what datui last found the place to be (`hive`, `12 parquet`) when it has seen
it, and, on a network share or in an object store, what it is on. A sort orders
the rows inside each place and never flattens the section.

A Parquet dataset opened from a bucket, as one object or as a prefix, shows the
rows, columns and label the open learned. One nothing has measured yet shows
`…` for its shape.

| On a place row | Does |
|---|---|
| <kbd>Enter</kbd> or <kbd>→</kbd> | Browse the place, as it would a directory. <kbd>Esc</kbd> comes back |
| <kbd>Delete</kbd> | Forget every recent under it, after confirming |

Whole places are shown until they take a third of the list, and always at least
one. What is left is one row, `… 13 more in 5 places`; <kbd>Enter</kbd> on it
shows the whole section for the session. The count on the header is the true
count. A filter matches anywhere in `RECENT`, past the cap.

### Adding a directory

Opening a dataset puts its directory under `RECENT` as a place, so a place only
has to be found by hand once (<kbd>~</kbd>, type the path, open something).
<kbd>Enter</kbd> on the place row takes you back there.

To keep a place listed as a section of its own, even when empty or unmounted,
name it in the config:

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
one is not counted, and its columns come from a spread of the directory rather
than all of it, so it shows `? × 39+`: neither figure is a total, and both say
so. CSV and other formats that need a scan to count show neither. Below the
counts, the pane lists the full schema of a Parquet dataset, each type in the
color the table uses.

### What a row's label says

A directory's label says what is directly inside it, from one listing:

| Label | Means |
|---|---|
| `hive` | It has a `key=value` child directory, and at least as many of those as data files |
| `delta` `iceberg` `hudi` | The format's marker is present |
| `12 parquet`, `3 csv`, `40 json` | Every data file directly inside is one format, and the count is the files |
| `mixed` | Data files of more than one format |
| `dir` | No data file directly inside — `dir+` where the listing was cut short, so none was *found*. In a bucket the word for the place is used instead: `prefix`, `bucket`, `container` |
| `…` | Nothing has looked into it yet |

A directory larger than the listing cap counts what it read and says so: `5000+
parquet`. The details pane carries the whole tally on a `holds` line — `12
parquet · 2 csv · 3 directories · 5 not read · 7 skipped (.crc, _SUCCESS,
_committed_1727, _started_1727, …)` — every file in the directory is in one of
those counts. `skipped` is a name beginning with `_` or `.`, or ending
`_$folder$` — where every engine puts its own files, where a repository puts
`.git`, and what s3n and EMR write to stand in for a folder. A `key=value` name
is a partition whatever it begins with, so a dataset partitioned on `_date` is
not skipped; a marker named after one, `year=2024_$folder$`, still is. The first
four skipped are named.

A label describes; it does not promise what <kbd>Enter</kbd> will do. A
directory of fifteen unrelated tables reads `15 parquet` and is still a place to
look inside.

### Two doors into every directory

Every directory has two doors, and neither depends on the label being right.

<kbd>→</kbd> goes inside any directory, to reach a single partition or a single
file. <kbd>Esc</kbd> comes back out. The first row in there is `<directory> (all
files)`, or `(all partitions)` for a hive directory, and it opens the whole
directory whatever the directory is labelled — so a label that is wrong about
what the directory holds costs one keystroke rather than access to it.

The control bar says which key is which, for the row under the cursor:

| It says | <kbd>Enter</kbd> will |
|---|---|
| `Enter Open all` | read the whole directory as one table; <kbd>→</kbd> goes inside instead |
| `Enter Inside` | step into the directory — the same as <kbd>→</kbd>, so only one is offered |
| `Enter Open` | load the file on the row |
| `Enter Look` | find out what the row is, then do whichever of those it calls for |

On a directory <kbd>Enter</kbd> steps into, the details pane on the right says
where the whole of it can be read: the first row inside.

That row carries no label of its own: every other label counts what is directly
inside a directory, and this row reads the whole of it. Nor is it a search
result — while a filter is typed it steps out of the way, and it comes back when
the filter is cleared. Only a directory with nothing in it at all — empty, or
holding nothing but a writer's own markers — has no such row.

### The door does not refuse

Whatever the directory is, the door reads it and says what it did. What it says
is in the Notes tab, which the <kbd>i</kbd> key opens.

| Directory | Read as | What it says |
|---|---|---|
| One format, or a hive tree of Parquet | One table | — |
| Parquet, CSV or NDJSON files that differ | One table, unioned by name and widened by type | which columns differ, and whether a column was widened |
| Arrow, Avro, ORC or JSON files that differ | Refused, naming the file it stopped at | — |
| More than one format | The commonest of them; Parquet wins a tie | what it passed over, by format and count |
| `delta`, `iceberg`, `hudi` | The plain files under the table | that they are not the table, plus a chip by the row count |
| Files written with no extension | What their first bytes say: Parquet, Arrow, Avro or ORC | — |
| A prefix of CSV or NDJSON in a bucket | One table, with that reader | — |
| A prefix holding nothing datui reads | Refused, naming what is there | — |

A lake table's files are the one read worth being careful with. A log beside the
data says which files are live, and datui does not read that log yet — so the
files include rows a delete tombstoned, versions an update replaced, and both
sides of a compaction. The row count on screen is a true count of the files and
a wrong count of the table, which is why it carries `not the Delta table` beside
it as well as the note.

A directory whose data is in `key=value` subdirectories reads as one table when
that data is Parquet. Hive partitioning is a Parquet-only capability in the
reader datui uses; for anything else, open one partition.

A directory of `key=value` partitions is `hive`, and a directory of Parquet
files that hold the same table is one dataset. Both open with <kbd>Enter</kbd>
as a single dataset.

Sharing a file extension is not enough to make a directory one table. A database
exported one file per table — `circuits.csv`, `drivers.csv`, `laps.csv` — looks
identical from its names, and reading it as one table would union things that
share no columns. So the columns decide: datui compares a spread of the
directory's files, and one whose files each bring something the others lack is
left as a directory to look inside.

Where those columns are read from depends on the format, and nothing else does.
A Parquet file keeps them in its footer, a CSV on its header line, an NDJSON
file in the keys of its first object — all at one end of the file, and all read
by the same reader that would open it, from a spread of three files whatever the
directory's size.

Only those three formats are judged, and only those three are unioned. Arrow,
Avro, ORC and a `.json` document keep their columns nowhere cheap to reach, so
nothing looks at them before the open — and a union with no rule in front of it
and nothing to say behind it is the thing this rule exists to remove, not
something to spread further. Those directories still refuse when their files
differ, and the message names the file the read stopped at.

A directory of headerless files is a case of its own. datui reads a CSV as
having a header, so each file gives its first row of *data* as the column names
— and reading such a directory as one table would stack those rows as headings
and fill the rest with nulls. datui does not offer it as one table, and says so
when you open it through the door: pass `--no-header` to read those rows as
data.

The reading is a sample, three files — the ends and the middle, stepping past
files with nothing in them — so it costs the same on a directory of four files
as on one of forty thousand. As with Parquet's footers, a directory whose
disagreement lies only in the files the sample did not open is read as one
table.

A `delta`, `iceberg` or `hudi` row is a lake table: a log beside the data files
says which of them are live. datui does not read that log yet, so it does not
offer the table as one dataset — the files a delete or an update tombstoned are
still on disk, every rewritten version is there together, and compaction leaves
both sides in place, so reading them as one table gives rows the table does not
have. <kbd>Enter</kbd> and <kbd>→</kbd> both go inside instead, where the data
files can be opened one at a time, and say so when they do. The `(all files)`
row in there will read them all together, labelled; see above.

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

In a bucket the Iceberg test is `metadata/` beside `data/` with no Parquet at
the root: looking inside `metadata/` would be a second listing, and the layout
is enough. So a directory that happens to hold both names is labelled `iceberg`
there. It is still somewhere to go, which a table read as one table is not.

The test is whether every file's columns are in the widest file's. That is the
shape schema evolution makes — a file written before a column existed has all of
the widest file's columns except the ones added since — so a blockchain that
added `txinwitness` in 2017 is still one dataset, and so is one that grew from
five columns to fifty.

A file that brings a column no other file has, such as a renamed one, fails it.
Nothing in a footer separates a rename from two tables that happen to share most
of their columns, so the directory is left as a place to look inside — and the
first row in there opens the union anyway. That is the trade: a strict test
costs a keystroke, where a lenient one costs a directory read as a table it is
not.

The columns come from footers that are read anyway to count the rows, so locally
this costs nothing. In a bucket, three of the directory's files are read while
you browse — one small ranged request each, never a whole object — and a
directory that cannot be read keeps the label its names suggested.

### Directories not looked into yet

| Label | Means |
|---|---|
| `dir` | An ordinary directory, looked into and found to hold no single table |
| `…` | Not looked into yet |

Telling the two apart costs a directory read each — a round trip apiece on a
network share, so a directory of thousands of partitions would be minutes before
the listing appeared. No listing pays for it, however small. Every directory is
drawn as `…` straight away, and the rows on screen are looked into a screenful
at a time as you scroll, the highlighted row first. A directory datui has
measured before keeps what it found, so one whose files turned out to be
separate tables is not offered as one dataset again while you wait for its
footers to be read a second time.

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
| Azure | source › account › container › directory › blob |
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

Directories are looked inside, a few at a time, once each listing lands: one
small listing request per directory, for at most 48 of them. A directory of
`key=value` partitions is then labelled `hive`, and every other directory by
what it holds — `12 parquet`, `3 csv` — like a local one. A prefix with no data
file directly inside keeps the word for the place, `prefix`, rather than
becoming `dir` the moment the peek lands. A directory of Parquet files whose
schemas agree is offered as one dataset; see [Two doors into every
directory](#two-doors-into-every-directory). Deciding that last one reads the
footers of up to three of the directory's files, a few kilobytes each; nothing
else here reads an object, and nothing reads a whole one. <kbd>Enter</kbd> opens
it as one dataset, with the partitions as columns;
<kbd>→</kbd> goes inside instead, where the first row opens the whole directory
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
| entries read to classify a directory | 5,000 |
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
