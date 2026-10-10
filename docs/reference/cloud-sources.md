# Cloud connections

The `[cloud]` settings and `[[cloud.connections]]` tables decide which stores the
home screen lists and how each one logs in. For a guide to logging in, see
[Connect to cloud storage](../user-guide/remote-data.md). Named lists of datasets
are [catalogs](catalogs.md). [Settings](settings.md#cloud) lists every `[cloud]`
key and its default.

```toml
[cloud]
env_files = [".env"]
discover = ["s3", "gcs"]
list_on_start = false
```

An S3-compatible endpoint, its keys and its region come from `AWS_*` variables or
a connection (below), never from keys in this file.

## Connections

Add one `[[cloud.connections]]` table per account or endpoint. Each appears as a row
under `CLOUD`, and a dataset in a [catalog](catalogs.md) can name one with
`connection`. Replace `<ENDPOINT>` with your server's URL and `<BUCKET>` with a
bucket the keys can read. The keys come from the environment variables the entry
names:

```toml,template
[[cloud.connections]]
name = "onprem"
label = "On-prem MinIO"
kind = "s3"
endpoint_url = "<ENDPOINT>"
region = "us-east-1"
addressing = "path"
access_key_id_env = "ONPREM_KEY"
secret_access_key_env = "ONPREM_SECRET"
buckets = ["<BUCKET>"]
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
| `account_key_env`, `sas_env`, `connection_string_env` | azure | The environment variable holding the account key, a SAS token, or a connection string. Set at most one. With none, `az` or Azure PowerShell signs in |
| `secret_command` | s3, azure | A program that prints the secret: the S3 secret access key (with `access_key_id_env`), or the Azure account key (with `account`). Use it instead of `secret_access_key_env` or `account_key_env` |
| `credentials_file` | gcs | A service account key or application-default login file, absolute or under `~`. Its project is listed first |
| `configuration` | gcs | A `gcloud` configuration whose login to use. Without it, the application-default login |
| `project` | gcs | The project listed first, and the one listed when projects cannot be searched |

## Secrets from files and commands

A password manager or vault can supply a secret without it touching the config or
the environment. Replace `<ENDPOINT>` with your server, `<SECRET_COMMAND>` with
the command that prints the secret (`pass show minio/onprem`,
`op read op://vault/minio/secret`) and `<KEY_FILE>` with the path of a service
account key:

```toml,template
[[cloud.connections]]
name = "onprem"
kind = "s3"
endpoint_url = "<ENDPOINT>"
access_key_id_env = "ONPREM_KEY"
secret_command = "<SECRET_COMMAND>"

[[cloud.connections]]
name = "analytics"
kind = "gcs"
credentials_file = "<KEY_FILE>"
```

`secret_command` splits the command into arguments as a shell would, but runs the
program directly with no shell, so `|`, `$VAR` and globs have no special meaning. It runs once, the first
time the source is used, with a 30-second limit. What it prints is kept in memory
for the session and never written, logged or shown. When it fails, only its error
output is reported. On Windows, a `.cmd` or `.bat` wrapper works.

`env_files` reads variables from files such as a project's `.env`. A path is
relative to the directory datui starts in, or can start with `~`. Only cloud
variables are read: the `AWS_*`, `GOOGLE_*` and `AZURE_*` ones datui uses,
`MC_HOST_<alias>`, and the names `[[cloud.connections]]` entries point at with
`*_env`. Anything else in the file, such as a database password, is ignored. A
variable already set in the environment wins. Nothing is exported, so programs
datui starts do not see these variables. No `.env` file is read unless it is listed in `env_files`.

The [home screen](../user-guide/home-screen.md#which-sources-appear) says what
`discover` and `list_on_start` change there. `-c cloud.discover=none` overrides
`discover` for one run.

`instance_identity = true` lets datui ask the cloud VM it runs on for credentials:
an EC2 instance role, a GCE service account, an Azure VM's managed identity. It is off by default because outside those VMs the metadata request waits for a
timeout. Cloud Run and Cloud Functions, and Azure App Service, Functions and
Container Apps, set variables that show an identity is available (`K_SERVICE`,
`IDENTITY_ENDPOINT`, `MSI_ENDPOINT`), so datui uses those identities without the
setting.

A secret written directly into a source (`secret_access_key = "..."`) is refused,
and so is any key datui does not recognize; the error names the key. A variable
that is named but not set is reported when the source is used. The source never
falls back to other keys in the environment.

## Detected sources

Sources appear when datui finds a supported login or an explicit configuration.
Listing a bucket does not guarantee permission to read every object inside it.

| Source | ID | Appears when |
|---|---|---|
| Amazon S3, or the `AWS_ENDPOINT_URL` endpoint | `s3-default` | `AWS_ACCESS_KEY_ID`, an ECS or Fargate task role, an EKS web identity, `AWS_PROFILE`, or a `~/.aws` directory |
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
| `["gcs"]`, `"s3,azure"` | `gcs`, `s3,azure` | Those kinds. `s3` covers AWS profiles, `mc`, s3cmd, and `s3-default` with its keys from `AWS_*` |

`[[cloud.connections]]` entries appear whatever `discover` says.

A connection in the config with the same name as a detected source replaces it.
When the same server and key are found in several places, they share one row,
and its note lists every place. `mc`'s placeholder aliases and its public `play` server are left
out. [Connect to cloud storage](../user-guide/remote-data.md#several-stores-at-once)
has worked examples of `[[cloud.connections]]`.

Google Cloud lists every project the login can find. The project named in the
environment or the active `gcloud` configuration comes first, and is the only
one listed when searching for projects is refused. A profile that needs the AWS CLI shows
`needs the AWS CLI` when it is not installed, and an expired SSO login shows
the CLI's message; see [AWS profiles](../user-guide/remote-data.md#aws-profiles). A cloud
VM's identity is discovered only with `[cloud] instance_identity = true` (above).
