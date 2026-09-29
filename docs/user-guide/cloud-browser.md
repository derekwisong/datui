# Cloud browser

Run `datui` and select a source under `CLOUD`. <kbd>Enter</kbd> on a
row lists what is inside, one level at a time; directories in a bucket descend like local ones,
objects open like files, and opened objects go into `RECENT` like any other path.

| Source | Levels |
|---|---|
| S3 and S3-compatible | source › bucket › directory › object |
| Google Cloud | source › project › bucket › directory › object |
| Azure | source › account › container › directory › blob |
| Public datasets | source › dataset › directory › object |

```
▾ CLOUD  5  ──────────────────────────────────────────────────────
  ≈ Amazon S3         s3      3 buckets     datui config
  ≈ Google Cloud      gcs     4 projects    project: example-project · gcloud
  ≈ Lab MinIO         s3      1 bucket      127.0.0.1:9000 · datui config
  ≈ onprem            s3      403           minio.corp.example:9000 · datui config
  ≈ Public datasets   public  6 datasets    built in
```

| Column | Shows |
|---|---|
| Name | The source's `label`, or its name |
| API | `s3`, `gcs`, `azure`, or `public` |
| Count | How many buckets (projects for Google Cloud, accounts for Azure, datasets for public data), a spinner while listing, `not listed` before the first listing, or why there are none |
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
| `not found` | The source was removed or hidden since its row was drawn; <kbd>Ctrl</kbd>+<kbd>R</kbd> at the top looks again |

## Loading

The rows appear at once, with the buckets an earlier run listed. No source is
listed, and no credential command (`aws`, `gcloud`, `az`, a profile's
`credential_process`) run for one, until you ask:

| To list | Do |
|---|---|
| One source | <kbd>Enter</kbd> or <kbd>→</kbd> on it. Once a session |
| Every source on screen | <kbd>Ctrl</kbd>+<kbd>R</kbd> |
| Every source, at launch | `[cloud] list_on_start = true` |

Sources are listed a few at a time, each row updating as its answer arrives, so a
slow endpoint holds up only its own row.

A recent from a named S3-compatible source shows the name beside it, or
`source not found: <name>` once that source has left the config.

Typing also matches bucket names already listed, this session or an earlier one,
from every source, in `Found`:
`Lab MinIO › data` and `onprem › data` stay two rows.

<kbd>Delete</kbd> on a source hides it until `datui --clear-cache`. To hide one
for good:

```toml
[cloud]
hide = ["gcs-default"]
```

## Which sources appear

Sources appear when datui finds a supported login or an explicit configuration.
Listing a bucket does not guarantee permission to read every object inside it.

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

To show only some kinds of login found on the machine, or none:

| `[cloud] discover` | `--cloud-discover` | Shows |
|---|---|---|
| unset, `true` or `"all"` | `all` | Every login found |
| `false` or `"none"` | `none` | None |
| `["gcs"]`, `"s3,azure"` | `gcs`, `s3,azure` | Those kinds. `s3` covers AWS profiles, `mc`, s3cmd, and `s3-default` whether its keys come from `[cloud] s3_*`, `--s3-*` or `AWS_*` |

`[[cloud.sources]]` entries and public datasets appear whatever it says. The flag
overrides the config for one run.

A source in the config with the same name as one of these replaces it. The same
server with the same key found in several places is one row; its note lists
every place. `mc`'s placeholder aliases and its public `play` server are left
out. See [Loading Data](remote-data.md#several-stores-at-once) for
`[[cloud.sources]]`.

Google Cloud lists every project the login can find, the one named in the
environment or the active `gcloud` configuration first — and alone when
searching for projects is refused. A profile that needs the AWS CLI shows
`needs the AWS CLI` when it is not installed, and an expired SSO login shows
the CLI's message; see [Loading Data](remote-data.md#aws-profiles). A cloud
VM's identity is not discovered unless `[cloud] instance_identity = true`; see
[Configuration](configuration.md#cloud).

## Public datasets

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

The details pane gives each one's publisher, license and homepage. Each entry links to the publisher’s terms. <kbd>Backspace</kbd> at a
dataset's top returns to the list. A public bucket or container you have browsed or
opened unsigned is added to the list. For a list of your own, see
[Loading Data](remote-data.md#public-data).

`datui --generate-config` writes this catalog as active
`[[cloud.sources.datasets]]` tables to edit; see
[Configuration](configuration.md#cloud) for the fields and how the snapshot
behaves.

Listings leave out what is not data: `_SUCCESS` and other job files, and the empty
objects some tools leave to stand for folders.

## What a cloud row shows

Inside a bucket: name, size and modification time, which is what a listing
returns. Row counts and columns would need a read per object, which someone is
billed for, so they are not fetched until you open one. A directory of
`key=value` partitions is labelled `hive`, and every other directory by what
it holds — `12 parquet`, `3 csv`, or `dir` — like a local one. How a remote
dataset then opens is in [Loading Data](loading-data.md#remote-data).

