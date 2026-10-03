# Cloud sources

For login commands, use [Connect to cloud storage](../user-guide/remote-data.md).
This page lists settings for credentials, discovery and named connections. Named
lists of datasets, the built-in public datasets among them, are
[dataset collections](sources.md).

## Defaults

```toml
[cloud]
use_azure_account_keys = true   # read Azure with the account key after a sign-in is refused for want of a data role
env_files = [".env"]            # read cloud variables from these files; off unless listed
instance_identity = false       # use the EC2, GCE or Azure VM's own identity
discover = true                 # logins found on this machine: true, false, or ["s3", "gcs", "azure"]
list_on_start = false           # list every source's buckets at launch, not when entered
```

An S3-compatible endpoint, its keys and region come from `AWS_*` variables or a
named connection below, never from keys in this file. See
[Loading Data](../user-guide/loading-data.md#remote-data).

## Connections

To keep a local folder on the home screen, see
[Adding a directory](../user-guide/home-screen.md#adding-a-directory).

Add one `[[cloud.connections]]` table per account or endpoint. Each is a row under
`CLOUD`, and a dataset in a [collection](sources.md) can name one with `connection`:

```toml
[[cloud.connections]]
name = "onprem"
label = "On-prem MinIO"
kind = "s3"
endpoint_url = "https://minio.corp.example:9000"
region = "us-east-1"
addressing = "path"
access_key_id_env = "ONPREM_KEY"
secret_access_key_env = "ONPREM_SECRET"
buckets = ["sales", "logs"]
```

| Field | Kinds | Meaning |
|---|---|---|
| `name` | all | Required. Lowercase letters, digits and `-`, at most 40 characters. Used in `s3://<name>@bucket/key` |
| `label` | all | Shown instead of the name |
| `kind` | all | Required. `s3`, `gcs` or `azure` |
| `buckets` | s3, gcs | Bucket names to show when the keys can read but not list |
| `endpoint_url` | s3 | An S3-compatible server. Without it, the source is AWS |
| `region` | s3 | Region to sign for |
| `addressing` | s3 | `path` or `virtual`. Default: `path` with an endpoint, `virtual` without |
| `access_key_id_env`, `secret_access_key_env`, `session_token_env` | s3 | Names of the environment variables holding the keys |
| `profile` | s3 | An AWS profile to take the keys, endpoint and region from, instead of the `*_env` keys |
| `account` | azure | Required, except with `connection_string_env`. The storage account |
| `account_key_env`, `sas_env`, `connection_string_env` | azure | The environment variable holding the account key, a SAS token, or a connection string. At most one; with none, `az` or Azure PowerShell signs in |
| `secret_command` | s3, azure | A program that prints the secret: the S3 secret access key (with `access_key_id_env`), or the Azure account key (with `account`). Instead of `secret_access_key_env` or `account_key_env` |
| `credentials_file` | gcs | A service account key or application-default login file, absolute or under `~`. Its project is listed first |
| `configuration` | gcs | A `gcloud` configuration whose login to use. Without it, the application-default login |
| `project` | gcs | The project listed first, and the one listed when projects cannot be searched |

## Secrets from files and commands

A password manager or vault can supply a secret without it touching the config or
the environment:

```toml
[[cloud.connections]]
name = "onprem"
kind = "s3"
endpoint_url = "https://minio.corp.example:9000"
access_key_id_env = "ONPREM_KEY"
secret_command = "pass show minio/onprem"        # or: op read op://vault/minio/secret

[[cloud.connections]]
name = "analytics"
kind = "gcs"
credentials_file = "~/keys/analytics-sa.json"   # a path, never the key itself
```

`secret_command` runs the program directly, split into arguments like a shell would
but with no shell, so `|`, `$VAR` and globs mean nothing. It runs once, the first
time the source is used, with a 30-second limit. What it prints is kept in memory
for the session and never written, logged or shown; when it fails, only its error
output is reported. On Windows, a `.cmd` or `.bat` wrapper works.

`env_files` reads variables from files such as a project's `.env`, relative to the
directory datui starts in (or under `~`). Only cloud variable names are taken: the
`AWS_*`, `GOOGLE_*` and `AZURE_*` ones datui reads, `MC_HOST_<alias>`, and the
names `[[cloud.connections]]` point at with `*_env`. Anything else in the file, a
database password for one, is ignored. A variable already set in the environment
wins, and nothing is exported, so no program datui starts sees them. No `.env` file is read unless it is listed in `env_files`.

See [The Home Screen](../user-guide/home-screen.md) for
[`discover`](../user-guide/cloud-browser.md#which-sources-appear) and
[`list_on_start`](../user-guide/home-screen.md#loading). `-c cloud.discover=none` overrides
`discover` for one run.

`instance_identity = true` lets datui ask the cloud VM it runs on for credentials:
an EC2 instance role, a GCE service account, an Azure VM's managed identity. This is off by default because metadata requests can time out outside those VMs. Cloud Run and Cloud Functions, and
Azure App Service, Functions and Container Apps, set variables that say an identity
is there (`K_SERVICE`, `IDENTITY_ENDPOINT`, `MSI_ENDPOINT`), and are used without
the setting.

A secret written directly into a source (`secret_access_key = "..."`) is refused,
and so is any key datui does not recognize, with the key named. A variable that is
named but not set is reported when the source is used; the source never falls back
to other keys in the environment.

## Detected sources

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
| Each `[[cloud.connections]]` entry | its `name` | Always |

To show only some kinds of login found on the machine, or none:

| `[cloud] discover` | `-c cloud.discover=` | Shows |
|---|---|---|
| unset, `true` or `"all"` | `all` | Every login found |
| `false` or `"none"` | `none` | None |
| `["gcs"]`, `"s3,azure"` | `gcs`, `s3,azure` | Those kinds. `s3` covers AWS profiles, `mc`, s3cmd, and `s3-default` its keys from `AWS_*` |

`[[cloud.connections]]` entries appear whatever it says.

A source in the config with the same name as one of these replaces it. The same
server with the same key found in several places is one row; its note lists
every place. `mc`'s placeholder aliases and its public `play` server are left
out. See [Loading Data](../user-guide/remote-data.md#several-stores-at-once) for
`[[cloud.connections]]`.

Google Cloud lists every project the login can find, the one named in the
environment or the active `gcloud` configuration first — and alone when
searching for projects is refused. A profile that needs the AWS CLI shows
`needs the AWS CLI` when it is not installed, and an expired SSO login shows
the CLI's message; see [Loading Data](../user-guide/remote-data.md#aws-profiles). A cloud
VM's identity is not discovered unless `[cloud] instance_identity = true`; see
[Configuration](cloud-sources.md).
