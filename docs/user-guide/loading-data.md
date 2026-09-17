# Loading Data

```bash
datui data.parquet                             # a file
datui jan.csv feb.csv mar.csv                  # files of the same shape, as one table
datui --hive /data/events/                     # a hive-partitioned directory
datui --hive "/data/events/**/*.parquet"       # or a glob (quote it)
datui s3://bucket/path/file.parquet            # S3, GCS (gs://) or HTTP(S)
datui --format csv https://example.com/export  # force the format when the name gives no hint
```

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

| Option | Config key | What it does |
|---|---|---|
| `--delimiter 9` | `delimiter` | Column separator as an ASCII code. Auto-detected when unset |
| `--no-header true` | `has_header` | The first row is data, not names |
| `--skip-lines N`, `--skip-rows N`, `--skip-tail-rows N` | `skip_lines`, `skip_rows` | Ignore a preamble or a footer |
| `--null-value NA`, `--null-value amount=` | | Values to read as null, for every column or one (`COL=VAL`). Repeatable |
| `--infer-schema-length 10000` | `infer_schema_length` | Rows used to infer column types (default 1000). Raise it when a column turns from integer to text late in the file |
| `--ignore-errors true` | `ignore_errors` | Skip rows that fail to parse instead of failing the load |
| `--parse-dates false` | `parse_dates` | Stop parsing date-looking strings as Date and Datetime |
| `--parse-strings COL`, `--no-parse-strings` | | Trim and type-infer string columns; limit it to named columns, or turn it off |

## Compression

Files ending in `.gz`, `.zst`, `.bz2` or `.xz` are decompressed before loading.
Use `--compression gzip|zstd|bzip2|xz` when the extension is missing or wrong.

Compressed CSV is decompressed to a temporary file so it can still be scanned
lazily. `--temp-dir` chooses where; `--decompress-in-memory true` skips the
file and reads the whole thing into memory instead.

## Hive-partitioned data

A directory tree whose segments are `key=value` (`year=2024/month=01/...`)
opens as one table with `--hive`. Pass the root directory or a glob; a glob
usually needs quoting so your shell leaves it alone. Only Parquet is supported.

Partition columns appear first in the table and on the **Partitions** tab of the
[Info panel](dataset-info.md). A directory is faster to open than a glob.

The schema is read from a single file along one partition branch, so opening a
tree of thousands of files is quick. If files disagree on their schema, let
Polars scan them all with `--single-spine-schema false`.

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
object. The buffer is planned inside the row group on screen, so paging never
pulls the next group before you reach it, and crossing into it fetches that
group once. Row groups up to `max_buffered_rows` and the `max_buffered_mb`
budget are held whole; a larger one is read one window at a time, so small row
groups keep both the first screen and paging cheap. A prefix or glob of Parquet
files opens as a partitioned dataset, buffered as a plain window. Every other
format, and anything
over HTTP, is downloaded to a temporary file (`--temp-dir` to choose where;
you are asked first when it is large) and then opened like a local file. One
remote path per run.

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
instance role is not found on its own, since finding it means a request that hangs
on some networks; with no other AWS login, datui reads S3 unsigned, which reaches
public buckets only.

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
[[cloud.sources]]
name = "lab"                                  # lowercase letters, digits and -
kind = "s3"
endpoint_url = "http://localhost:9000"
access_key_id_env = "LAB_KEY"
secret_access_key_env = "LAB_SECRET"

[[cloud.sources]]
name = "onprem"
label = "On-prem MinIO"
kind = "s3"
endpoint_url = "https://minio.corp.example:9000"
access_key_id_env = "ONPREM_KEY"
secret_access_key_env = "ONPREM_SECRET"
```

Servers already set up in the MinIO client (`mc alias set`, or `MC_HOST_<alias>`)
or in s3cmd need no config: they are the sources `mc-<alias>` and `s3cfg`.

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
another tenant. A public bucket or container read this way is listed with the
[public datasets](home-screen.md#public-datasets) from then on.

A few well-known datasets are built in; see the
[home screen](home-screen.md#public-datasets). To keep your own list, add a
source with `public = true` and its data as URLs of any cloud:

```toml
# GBIF occurrence snapshots: CC BY-NC 4.0, see https://www.gbif.org/terms
[[cloud.sources]]
name = "gbif"
label = "GBIF"
public = true
buckets = ["s3://gbif-open-data-us-east-1/occurrence/"]
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
