# Connect to cloud storage

Pass a cloud URL to datui, or use the [cloud browser](cloud-browser.md).
The setup examples below use placeholder bucket and account names; substitute
your own. [Public data](#public-data) has examples that run as written.

| Storage | Setup | Example URL |
|---|---|---|
| AWS S3 | [AWS profile or keys](#amazon-s3) | `s3://bucket/file.parquet` |
| Google Cloud | [gcloud or service account](#google-cloud-storage) | `gs://bucket/file.parquet` |
| Azure | [Azure CLI or PowerShell](#azure-blob-storage) | `abfss://container@account.dfs.core.windows.net/file.parquet` |
| MinIO, R2, Ceph | [Custom endpoint](#s3-compatible-storage-minio-r2-ceph) | `s3://bucket/file.parquet` |
| Public data | [No login](#public-data) | The **Public datasets** section on home |
| HTTP(S) | [Direct download](#http-and-https) | `https://example.com/data.csv` |

One remote path can be opened per run. A supported directory or prefix can
contain many files. See [formats and read costs](#what-gets-read).

## Amazon S3

With an existing AWS profile:

```bash
AWS_PROFILE=analytics datui s3://analytics-exports/2024/
```

For an SSO profile, sign in with `aws sso login --profile analytics` first.
Alternatively, set keys in your shell:

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1          # or AWS_DEFAULT_REGION
datui s3://my-bucket/events/2024/
```

Use `AWS_SESSION_TOKEN` with temporary credentials. Datui discovers each
bucket's region and uses supported task roles on ECS, Lambda and EKS.
EC2 instance roles require `[cloud] instance_identity = true`.
With no AWS credentials, requests are unsigned and can reach public buckets.

## AWS profiles

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
is its own source on the [home screen](cloud-browser.md), named
`aws-<profile>`; one with an endpoint is S3-compatible, so its URLs are
`s3://aws-<profile>@bucket/key`.

## S3-compatible storage (MinIO, R2, Ceph)

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

## Several stores at once

Name each store in the config, with the environment variables that hold its keys.
These connections name environment variables rather than storing their values.

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
[collection](../reference/sources.md) with `connection = "onprem"`.

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
[Configuration](../reference/cloud-sources.md) for every field.

## Google Cloud Storage

Sign in and open a file:

```bash
gcloud auth application-default login
datui gs://my-bucket/path/file.parquet
```

An existing `gcloud auth login` session also works.

| Login | datui |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS`, a service account variable, or `gcloud auth application-default login` | Uses it directly |
| Only `gcloud auth login` | Asks `gcloud` for a token, for the active configuration |
| Workload identity federation or an impersonated service account in the application-default file | Asks `gcloud`; without it, the row says `unsupported login` |
| Another `gcloud` configuration with a different account | Its own source, `gcloud-<configuration>` |

Every project the login can see is listed on the
[home screen](cloud-browser.md), so no project setting is needed. A
project in `GOOGLE_CLOUD_PROJECT` (or `DATUI_GCP_PROJECT`, or the active `gcloud`
configuration's) is listed first, and it is the one listed when the login cannot
search for projects.

## Azure Blob Storage

Sign in with the Azure CLI, then open a file:

```bash
az login
datui abfss://container@account.dfs.core.windows.net/data.parquet
```

In Azure PowerShell, use `Connect-AzAccount` instead of `az login`.
The [cloud browser](cloud-browser.md) lists the accounts the login can access.

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

## Public data

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

### Examples on public data

NOAA's daily weather for 2024 is one Hive partition in S3, `by_year/YEAR=2024/`,
with an `ELEMENT=` directory per measurement. From **Public datasets**, press
<kbd>Enter</kbd> on **NOAA daily weather (GHCN-D)**, <kbd>→</kbd> on `by_year`
and <kbd>Enter</kbd> on `YEAR=2024`. Or:

```bash
datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/
```

37,108,477 rows, with `YEAR` and `ELEMENT` as columns from the directory
names. Which measurements are most common?

```sql
SELECT ELEMENT, COUNT(*) AS observations, COUNT(DISTINCT ID) AS stations
FROM df
GROUP BY ELEMENT
ORDER BY observations DESC
```

Precipitation, `PRCP`, leads with 11,266,140 observations from 42,675
stations, then `SNOW`, `TMAX` and `TMIN`. The query reads every file, a few
seconds on a home connection.

One station, from `by_year/YEAR=2024/ELEMENT=TMAX/`: `USW00094728` is Central
Park, and `DATA_VALUE` is tenths of a degree Celsius.

```sql
SELECT CAST(STRPTIME(DATE, '%Y%m%d') AS DATE) AS day, DATA_VALUE / 10.0 AS high_c
FROM df
WHERE ID = 'USW00094728'
ORDER BY day
```

366 days, from −6.0 to 35.0 °C. Chart `day` against `high_c` as a line, or
save it as a [view](views.md) for another year.

Bitcoin blocks are partitioned by day, one directory per date. From
**Public datasets**, press <kbd>Enter</kbd> on **Bitcoin and Ethereum**,
<kbd>→</kbd> on `btc` and <kbd>Enter</kbd> on `blocks`. Or:

```bash
datui s3://aws-public-blockchain/v1.0/btc/blocks/
```

A filter on the partition column reads only the matching days:

```sql
SELECT EXTRACT(MONTH FROM mediantime) AS month, COUNT(*) AS blocks,
       SUM(transaction_count) AS txs
FROM df
WHERE date >= '2024-01-01' AND date < '2025-01-01'
GROUP BY month
ORDER BY month
```

12 rows, 4,179 to 4,761 blocks a month; October has the most transactions,
20,482,111. The table opens before every file's footer is read; the bottom bar counts
them, `Reading footers: …`, while you work. The feed adds a partition a day.

For public data without a URL, run `datui` and open a dataset under
[**Public datasets**](home-screen.md#public-datasets), which lists publishers and
licenses. For a list of your own, name the datasets in a
[collection](../reference/sources.md) with `auth = "anonymous"`, which reads them
with no login even on a machine that has one:

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

Parquet part files with no extension, like GBIF's `occurrence.parquet/000001`,
open as Parquet. So does a local file with no extension that starts and ends with
`PAR1`.

## HTTP and HTTPS

```bash
datui https://example.com/data.csv
datui --format parquet https://example.com/download?id=42
```

The file is downloaded to the temp directory (`--temp-dir`), then opened. Use
`--format` when the URL has no useful extension. The copy is removed when datui
exits, including a quit mid-download; see
[temporary files](loading-data.md#temporary-files).

## What gets read

| Source | Read behavior |
|---|---|
| Cloud Parquet file, prefix or glob | Reads metadata and required row groups in place |
| Cloud CSV or JSONL directory | Scans the files in place |
| HTTP(S), or other download routes | Downloads the file before opening |

Paging reuses buffered rows and reads ahead. Leaving the buffer can require
another read; queries, sorting and analysis may scan the full input.
See [large datasets](../advanced/performance-tips.md) for buffering and sampling.

## Building without cloud support

`cargo build --release --locked --no-default-features --features sql,streaming`
leaves out the cloud and HTTP dependencies
([features](../getting-started/installation.md#from-source)). A
binary built that way rejects remote URLs with a message saying so, and a bucket
in a configured collection reads `cloud support not in this build` when browsed.
