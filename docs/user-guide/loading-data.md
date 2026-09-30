# Loading Data

```bash
datui data.parquet                             # a file
datui jan.csv feb.csv mar.csv                  # files of the same shape, as one table
datui /data/events/                            # a directory, read the way Enter reads its row
datui --hive "/data/events/**/*.parquet"       # a glob (quote it)
datui s3://bucket/path/file.parquet            # S3, GCS (gs://) or HTTP(S)
datui --format csv https://example.com/export  # force the format when the name gives no hint
```

## Directories

`datui <directory>` does what <kbd>Enter</kbd> on that directory's row does on
the [home screen](home-screen.md), and needs no flag:

| The directory | What happens |
|---|---|
| A hive tree, or files that are one table | Opens as one table |
| Separate tables, more than one format, or no data directly inside | The home screen, browsed into it — one keystroke from either a file or the union |
| A Delta, Iceberg or Hudi root | The home screen, browsed into it, saying datui does not read the table itself yet |

`--hive` means: read this as partitioned, which is the answer for a glob and
for a layout that does not say so itself.

Working out which of those a directory is means reading footers, or the front
of a spread of its files, and datui reads them the way it is about to read the
whole directory — `--no-header`, `--skip-rows`, `--skip-lines`,
`--infer-schema-length` and the rest all apply. So `datui --no-header exports/`
judges the directory as headerless, finds its files stack, and opens it;
without the flag the same directory is judged with a header, its files do not
agree, and the home screen opens on it instead. datui draws the screen first,
and the loading screen keeps the way out visible: <kbd>Ctrl</kbd>+<kbd>O</kbd>,
<kbd>q</kbd> and <kbd>?</kbd> stay in the bar beside the progress and act at
once (<kbd>Ctrl</kbd>+<kbd>C</kbd> and <kbd>Ctrl</kbd>+<kbd>Q</kbd> too). Keys
are not held during a load — the allowed keys act, the rest are dropped — and
abandoning a cloud load stops its reads instead of letting them run on.

Every option is listed in [Command Line Options](../reference/command-line-options.md).
Defaults for most of them can be set once in the
[configuration file](configuration.md#file-loading).

## Formats

The format is taken from the extension, or from `--format` when there is none.

| Format | Extensions | Lazy | Hive partitions |
|---|---|---|---|
| Parquet | `.parquet` | yes | yes |
| CSV and other delimited text | `.csv`, `.tsv`, `.psv` | yes | |
| NDJSON | `.jsonl` | yes | |
| Arrow IPC, Feather v2 | `.arrow`, `.ipc`, `.feather` | yes | |
| JSON | `.json` | | |
| Avro | `.avro` | | |
| Excel | `.xlsx`, `.xlsm`, `.xlsb`, `.xls` | | |
| ORC | `.orc` | | |

**Lazy** formats are scanned, so only the rows on screen are read and a file
larger than memory is fine. The others are read whole before the table appears.

**Excel** opens the first sheet unless `--sheet` names another, by index
(`--sheet 0`) or name (`--sheet Sales`).

### CSV options

They apply to `.tsv` and `.psv` files too. A directory of CSVs in a bucket takes
all of them except `--parse-strings`.

| Option | Config key | What it does |
|---|---|---|
| `--delimiter 9` | | Column separator as an ASCII code (`59` for `;`, `124` for `\|`). Default `,` for `.csv`, tab for `.tsv`, `\|` for `.psv` |
| `--no-header` | | The first row is data, not names. <kbd>H</kbd> does the same, or undoes it, on the file on screen |
| `--skip-lines N`, `--skip-rows N` | | Ignore a preamble |
| `--skip-tail-rows N` | | Ignore a footer. Counts every row first: on a directory in a bucket, that downloads every file before the table opens |
| `--null-value NA`, `--null-value amount=` | | Values to read as null, for every column or one (`COL=VAL`). Repeatable |
| `--infer-schema-length 10000` | `infer_schema_length` | Rows used to infer column types (default 1000). Raise it when a column turns from integer to text late in the file |
| `--ignore-errors` | `ignore_errors` | Skip rows that fail to parse instead of failing the load |
| `--parse-dates=false` | `parse_dates` | Stop parsing date-looking strings as Date and Datetime |
| `--parse-strings=COL`, `--no-parse-strings` | | Trim and type-infer string columns; limit it to named columns, or turn it off |

## Compression

Files ending in `.gz`, `.zst`, `.bz2` or `.xz` are decompressed before loading.
Use `--compression gzip|zstd|bzip2|xz` when the extension is missing or wrong.

Compressed CSV is decompressed to a temporary file so it can still be scanned
lazily. `--temp-dir` chooses where; `--decompress-in-memory` skips the
file and reads the whole thing into memory instead.

## Hive-partitioned data

A directory tree whose segments are `key=value` (`year=2024/month=01/...`)
opens as one table. Pass the root directory, which needs no flag, or a glob with
`--hive`; a glob usually needs quoting so your shell leaves it alone. Only
Parquet is supported — for a hive tree of anything else, open one partition.

Partition columns appear first in the table and on the **Partitions** tab of the
[Info panel](dataset-info.md). On disk, a directory is faster to open than a glob:
a local glob is handed to Polars, while a directory is walked by datui and gets the
schema union, the row count and the notes. In a bucket both are datui's — it lists
the prefix and matches the pattern itself — so a remote glob opens the same way a
remote prefix does.

### Files that disagree

The table has every column any file has. datui reads each file's Parquet footer
— a small read at the end of the file, never the data — and folds them into one
schema:

| Across the files | In the table |
|---|---|
| A column only some files have | Shown; the other files' rows read null |
| `Int32` and `Int64`, `Int` and `Float`, `ms` and `ns` | The wider type |
| Types that cannot meet, such as a number and text | The type most rows have; the column is not read from the other files |
| A file whose footer cannot be read | Left out; the rest still opens |

The **Schema** tab of the [Info panel](dataset-info.md) says which footers the
schema came from. Past 20,000 files, a sample spread evenly across them stands
in and the tab says so; so does a cloud directory that is still reading the rest
of its footers behind the data.

An empty cell says which kind of empty it is, so a gap in the data is never
confused with a gap in the files:

| Cell | Means |
|---|---|
| `∅` | A null the data holds |
| `·` | The file this row came from has no such column |
| `≠` | The file holds the column in another type, so it was not read from that file |

A mark after a column's name means it is not in every file, or the files
disagree on its type. The distinction survives a filter and a sort. Telling the
three apart needs every file's row count, so where datui does not have one —
past 20,000 files, where the schema comes from a sample, or when a footer could
not be read — the marks still appear but every empty cell reads as `∅`. A group-by,
pivot or SQL query builds new rows that stand for no one file, so their nulls are
plain nulls again, and an export writes every empty cell as null.

A `≠` cell's value can still be seen. The **Notes** tab's note about the column
offers **read this column as text**, and taking it reads the column from the files
that disagree too, at the type each of them wrote, spelled as text. Nothing is
read past any more, so the `≠` marks and the note go, and it costs no re-listing
and no footer read. A column any file stores as a list, a duration or binary has
no text form datui can show, and is not offered.

A filter or sort on a column read as text compares text: `n > 5` keeps `"sixty"`
and drops `"10"`. A note says so for as long as the column is read that way.

A file whose type merely *widens* into the column's is read at the column's type
either way, so an integer in a column read as a float still reads as `7.0`, and
the note saying the column is stored as more than one type stays.

A `≠` cell has no value the column can be compared or ordered by, so where datui
has every file's row count, filtering or sorting by a column the files disagree
on leaves those rows out rather than gathering them at one end. The **Notes** tab
says how many rows are in the files that hold the column in another type, and the
`i` key takes its quiet accent again to say there is something new there. Clearing
the filter or sort brings the rows back. Where datui does not have those counts —
the same cases that flatten the marks above — the rows stay, gathered at one end
and unremarked.

The rows go whatever else the filter says. A sidebar filter of **id** = 3 **or**
**n** = 0 leaves out a row whose file stores `n` as text even where its `id` is
3: that file's `n` was never read, so the view cannot stand behind either half.
A query typed in the [query bar](querying-data.md) is a different thing — it
builds rows of its own, and none of this applies to them.

Only `≠` rows go that way. A `·` cell's file never had the column at all, so a
sort keeps its rows. A filter is a different matter: no comparison holds against
an empty cell, so a `·` or `∅` row fails one as it would in any query, and datui
says nothing about that because nothing unusual happened.

### How large remote datasets open

A directory in the cloud of more than 64 files does not wait for every footer.
It opens from the first file and the last, by name, and reads the rest behind
the data, with the count in the control bar. Columns those files turn out to
have join the table at the end of the column order when they arrive; until
then the row count is not shown, every empty cell reads as `∅`, the Notes are
scoped to `in 2 of 6,541 footers (sample)`, and a query, pivot or drill-down
holds the new columns off until you come back to the data. Local directories
read every footer up front. While footers are read they are counted, `Reading
footers: 1,203 of 6,541`; past 20,000 files the count is the sample it reads.

The Notes tab flags layouts that cost time to read:

| Note | Means |
|---|---|
| the middle row group is past 64 MiB | rows are read a row group at a time, so over a network a page waits on the whole group |
| more than 10,000 files, the middle one under a mebibyte | many footers were opened before a row was; the remedy — fewer, larger files — is upstream in whatever writes them |
| `3 files by date, 1 file by dt` | the directories do not all partition by the same keys. The same keys in a different order are fine |

`--single-spine-schema=false` skips the footer pass and lets Polars decide the
schema from one file; the partition-keys note is not checked on that route.

### Opening it again

What a remote dataset's footers said is kept in the cache directory, under the
URL you opened, so the same dataset opens again with its columns and row count
at once. The listing still happens, and decides whether what was kept still
holds: a file added, removed, renamed, resized or rewritten — its name, size,
modification time or etag — means the footers are read again. `--clear-cache`
forgets it; deleting it costs speed and nothing else.

## Binary columns

A binary column shows a dim `‹binary›` placeholder instead of its bytes, so
scrolling past large blobs stays fast. The bytes are still read for exports and
analysis. The placeholder color is `binary_col` in the
[theme](configuration.md#colors).

## Remote data

Pass an `s3://`, `gs://` or `https://` URL where you would pass a path. In S3
and GCS, Parquet is read in place with range requests, one row group at a time:
opening fetches the footer and the first row group, <kbd>End</kbd> fetches the
last, and a query that has to look at every row transfers about the size of the
object. Row groups up to `max_buffered_rows` and the `max_buffered_mb` budget
are held whole; a larger one is read one window at a time, so small row groups
keep both the first screen and paging cheap. Each group or window is fetched
once.

Paging reads ahead: as the view nears the end of the rows on hand, the next ones
are fetched in the background while you keep paging. If you get there first, the
page you were on stays up until the new rows arrive.

| Path | Opens as |
|---|---|
| A prefix of Parquet files (`s3://bucket/events/`) | One dataset. The files are listed once; the schema is the newest file's, so older files read later columns as empty; the row count comes from the footers. Reading any part of it opens only the files holding those rows, and <kbd>End</kbd> pressed before the count is in waits for it |
| A glob (`s3://bucket/events/*/*.parquet`) | The same kind of dataset: datui lists the literal part of the key and matches the rest itself |
| A directory named without `--format` | The way its `(all files)` row opens on the home screen: CSV or JSON Lines read as one table in place; one holding only directories opens the home screen browsed into it |
| Every other format, and anything over HTTP | Downloaded to a temporary file (`--temp-dir` chooses where; you are asked first when it is large), then opened like a local file |

One remote path per run.

Once credentials are in place, the buckets they reach are also listed on the
[home screen](home-screen.md#cloud-storage), so you can browse instead of
typing URLs.

### Amazon S3

Set the keys and region in the environment:

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1          # or AWS_DEFAULT_REGION
datui s3://my-bucket/events/2024/
```

`AWS_SESSION_TOKEN` adds temporary credentials, and the keys under `[cloud]` in
the config work the same way. On ECS, Lambda and EKS the task role is used. An EC2
instance role is used only with `[cloud] instance_identity = true`, since finding it
means a request that hangs on some networks; with no other AWS login, datui reads S3
unsigned, which reaches public buckets only.

Each bucket is read in its own region, asked of S3 once per session, so one login
reaches buckets in every region.

### AWS profiles

With no keys in the environment or the config, datui uses the profile the AWS
tools would: `AWS_PROFILE`, else `default`.

```bash
AWS_PROFILE=analytics datui s3://analytics-exports/2024/
```

| Profile holds | datui |
|---|---|
| `aws_access_key_id` and `aws_secret_access_key` | Uses them |
| `credential_process` | Runs it (aws-vault, granted, 1Password and the like) |
| `sso_session`, `role_arn`, `credential_source` or `web_identity_token_file` | Runs `aws configure export-credentials --profile <name>`, so the AWS CLI must be installed |

The files are `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE`, else
`~/.aws/config` and `~/.aws/credentials`. A profile's `region` and `endpoint_url`
(or an `s3` `endpoint_url` under its `services` section) apply too, after
`AWS_ENDPOINT_URL_S3` and `AWS_ENDPOINT_URL`. Every other profile that can log in
is its own source on the [home screen](home-screen.md#cloud-storage), named
`aws-<profile>`; one with an endpoint is S3-compatible, so its URLs are
`s3://aws-<profile>@bucket/key`.

### S3-compatible storage (MinIO, R2, Ceph)

Point datui at the endpoint. Command line beats environment beats config.

```toml
# ~/.config/datui/config.toml
[cloud]
s3_endpoint_url = "http://localhost:9000"
s3_access_key_id = "minioadmin"
s3_secret_access_key = "minioadmin"
s3_region = "us-east-1"
```

```bash
# or per shell
export AWS_ENDPOINT_URL=http://localhost:9000
# or per run
datui --s3-endpoint-url http://localhost:9000 s3://bucket/file.parquet
```

The command-line flags are `--s3-endpoint-url`, `--s3-access-key-id`,
`--s3-secret-access-key` and `--s3-region`. In the environment the endpoint is
taken from the first of `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` and
`AWS_ENDPOINT` that is set, the keys from `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`, and the region from `AWS_REGION` or
`AWS_DEFAULT_REGION`. A variable or flag that is set but empty counts as unset.

### Several stores at once

Name each store in the config, with the environment variables that hold its keys.
The keys themselves never go in the file.

```toml
[[cloud.connections]]
name = "lab"                                  # lowercase letters, digits and -
kind = "s3"
endpoint_url = "http://localhost:9000"
access_key_id_env = "LAB_KEY"
secret_access_key_env = "LAB_SECRET"

[[cloud.connections]]
name = "onprem"
label = "On-prem MinIO"
kind = "s3"
endpoint_url = "https://minio.corp.example:9000"
access_key_id_env = "ONPREM_KEY"
secret_access_key_env = "ONPREM_SECRET"
```

Servers already set up in the MinIO client (`mc alias set`, or `MC_HOST_<alias>`)
or in s3cmd need no config: they are the sources `mc-<alias>` and `s3cfg`.

To keep a dataset in one of these stores on the home screen by name, list it in a
[`[[sources]]`](configuration.md#sources) collection with `connection = "onprem"`.

Open an object from a named S3-compatible store by putting its name before the
bucket. Two servers can have a bucket with the same name, and the name says which
one you mean:

```bash
datui s3://lab@data/sales.parquet
datui s3://onprem@data/sales.parquet
```

| URL | Reaches |
|---|---|
| `s3://<name>@bucket/key` | The S3-compatible store with that name |
| `s3://bucket/key` | The `[cloud] s3_*` settings, the `AWS_*` environment and the `--s3-*` flags, as above |
| `gs://bucket/key` | The Google login, as below |

A source of `kind = "s3"` without `endpoint_url` is a second AWS login. Its URLs stay
`s3://bucket/key`, and a bucket you reach by browsing it opens with its keys. See
[Configuration](configuration.md#cloud) for every field.

### Google Cloud Storage

Either login works:

```bash
gcloud auth login                         # the gcloud CLI's own login
gcloud auth application-default login     # or Application Default Credentials
datui gs://my-bucket/path/file.parquet
```

| Login | datui |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS`, a service account variable, or `gcloud auth application-default login` | Uses it directly |
| Only `gcloud auth login` | Asks `gcloud` for a token, for the active configuration |
| Workload identity federation or an impersonated service account in the application-default file | Asks `gcloud`; without it, the row says `unsupported login` |
| Another `gcloud` configuration with a different account | Its own source, `gcloud-<configuration>` |

Every project the login can see is listed on the
[home screen](home-screen.md#cloud-storage), so no project setting is needed. A
project in `GOOGLE_CLOUD_PROJECT` (or `DATUI_GCP_PROJECT`, or the active `gcloud`
configuration's) is listed first, and it is the one listed when the login cannot
search for projects.

### Azure Blob Storage

Sign in, and every storage account the login can see is listed on the
[home screen](home-screen.md#cloud-storage).

```bash
az login                  # the Azure CLI
Connect-AzAccount         # or Azure PowerShell
datui abfss://datui-test@datalake001.dfs.core.windows.net/demo/penguins.parquet
```

| URL | Also accepted |
|---|---|
| `abfss://<container>@<account>.dfs.core.windows.net/<path>` | `abfs://`, and `https://<account>.blob.core.windows.net/<container>/<path>` or its `dfs` form |
| | `az://<container>/<path>`, `adl://` and `azure://`, when the account is known: typed inside an account on the home screen, named in the environment, or the only `kind = "azure"` source |

datui writes and remembers the `abfss://` form, which Polars, Spark and DuckDB
read too.

| Login | Found by |
|---|---|
| `az login` | `~/.azure` (or `AZURE_CONFIG_DIR`); datui runs `az account get-access-token` |
| Azure PowerShell, `Connect-AzAccount` | `~/.Azure/AzureRmContext.json`; datui runs `pwsh` (or `powershell.exe`) once for its tokens. When `az` is signed in too, `az` is used |
| A service principal or AKS workload identity | `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`, with `AZURE_CLIENT_SECRET` or `AZURE_FEDERATED_TOKEN_FILE`. With `AZURE_STORAGE_ACCOUNT_NAME` it reads that account; without, it finds its accounts like a sign-in |
| `AZURE_STORAGE_CONNECTION_STRING` | A connection string with `AccountKey` or `SharedAccessSignature`; `UseDevelopmentStorage=true` for Azurite |
| `AZURE_STORAGE_ACCOUNT_NAME` with `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_STORAGE_SAS_TOKEN` | An account and its key or SAS token |

With Azure tools installed but nobody signed in, the Azure row says
`not signed in` and names the command to run.

Reading blobs with a sign-in needs the *Storage Blob Data Reader* role on the
account. Owner or Contributor on the subscription is not enough, except on an
account with hierarchical namespace where your login owns the container. When a
read is refused for that reason and your login may fetch the account's access keys
(Owner and Contributor may), datui reads that account with its key instead, as the
Azure Portal does. The details pane then says `access key`. The key stays in
memory. Accounts with shared-key access disabled are never read this way, and the
refusal says so. To read only as your sign-in:

```toml
[cloud]
azure_account_keys = false
```

In **Azure Cloud Shell**, `az` is already signed in, so the Azure row lists your
accounts with no setup. The install script puts datui in `~/.local/bin` there; see
[Installation](../getting-started/installation.md#without-root).

### Public data

Public buckets and containers open with no login at all:

```bash
datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/
datui gs://cloud-samples-data/bigquery/us-states/us-states.parquet
datui abfss://release@overturemapswestus2.dfs.core.windows.net/
```

| Machine has | datui |
|---|---|
| No login for that cloud | Reads unsigned straight away |
| A login | Signs with it. If the place refuses, datui tries once more unsigned, and remembers for the session which one worked |

The retry matters most on Azure, which refuses a public container to a login from
another tenant.

A few well-known datasets are built in; see the
[home screen](home-screen.md#public-datasets). For a list of your own, name them in
a [`[[sources]]`](configuration.md#sources) collection with `auth = "anonymous"`,
which reads them with no login even on a machine that has one:

```toml
# GBIF occurrence snapshots: CC BY-NC 4.0, see https://www.gbif.org/terms
[[sources]]
name = "gbif"
label = "GBIF"

[[sources.datasets]]
name = "Occurrences"
url = "s3://gbif-open-data-us-east-1/occurrence/"
auth = "anonymous"
license = "CC BY-NC 4.0"
```

A license is the publisher's, not datui's: check it before you use the data.

Parquet part files with no extension, like GBIF's `occurrence.parquet/000001`,
open as Parquet. So does a local file with no extension that starts and ends with
`PAR1`.

### HTTP and HTTPS

```bash
datui https://example.com/data.csv
datui --format parquet https://example.com/download?id=42
```

The file is downloaded, then opened. Use `--format` when the URL has no useful
extension.

### Building without cloud support

`cargo build --release --no-default-features` leaves out the cloud
dependencies. A binary built that way rejects remote URLs with a message
saying so.
