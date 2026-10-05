# Environment variables

<!-- Generated from crates/datui-cli/src/settings.rs by `gen_docs`. Do not edit. -->

The variables datui reads.

## datui

| Variable | What it does |
|---|---|
| `DATUI_CONFIG_DIR` | The config directory, in place of the platform's (`~/.config/datui` on Linux). Saved views and format specs live there too |
| `DATUI_CACHE_DIR` | The cache directory, in place of the platform's (`~/.cache/datui` on Linux) |
| `DATUI_FORMATS_PATH` | Directories of format specs and dictionaries, separated as `PATH` is, searched before `[formats] path` |
| `DATUI_LOG` | The log level: `error`, `warn`, `info`, `debug`, `trace` or `off`. Beats `log.level` in a file; `-c` and `--log-level` beat it |
| `DATUI_DEBUG` | `1` shows the debug overlay |
| `DATUI_GCP_PROJECT` | The Google Cloud project to list when projects cannot be searched, as `GOOGLE_CLOUD_PROJECT` |
| `DATUI_TRACE_FIRST_ROWS` | A file to write the time to, in Unix nanoseconds, once the first rows are drawn. For benchmarks |

## Terminal

| Variable | What it does |
|---|---|
| `NO_COLOR` | Set to anything: no colors, the terminal's own for everything |
| `COLORTERM`, `TERM`, `FORCE_COLOR` | How many colors the terminal draws: 24-bit, 256 or 16. Theme colors are brought down to fit |
| `COLORFGBG` | With `theme.mode = "auto"`, says whether the background is light or dark, for a terminal that does not answer when asked |
| `LC_ALL`, `LC_CTYPE`, `LANG` | With `display.unicode = "auto"`, the first one set says whether the terminal takes UTF-8; when it does not, glyphs are ASCII |
| `WT_SESSION`, `TERM_PROGRAM` | Windows only: Windows Terminal, or VS Code's terminal (`TERM_PROGRAM=vscode`), draws Unicode glyphs whatever the code page |

## Programs datui starts

| Variable | What it does |
|---|---|
| `VISUAL`, `EDITOR`, `PAGER` | The inspector's `o` opens text in the first one set, else `less` (on Windows, the system's opener) |

## Cloud logins

Read as each provider's own tools read them; a variable set but empty counts as unset. [Connect to cloud storage](../user-guide/remote-data.md) says which login wins, and `[cloud] env_files` can read them from `.env` files.

| Variable | What it does |
|---|---|
| `AWS_PROFILE` | The AWS profile for `s3://`, else `default` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | AWS keys, and the token of temporary ones |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | The AWS region |
| `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, `AWS_ENDPOINT` | An S3-compatible endpoint (MinIO, R2, Ceph); the first one set |
| `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE` | The AWS config and credentials files, in place of `~/.aws/config` and `~/.aws/credentials` |
| `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`, `GOOGLE_SERVICE_ACCOUNT_KEY` | A Google Cloud service account or credentials file for `gs://` |
| `GOOGLE_CLOUD_PROJECT`, `GCLOUD_PROJECT`, `CLOUDSDK_CORE_PROJECT`, `GCP_PROJECT` | The Google Cloud project to list buckets in, after `DATUI_GCP_PROJECT`; the first one set |
| `CLOUDSDK_CONFIG` | The `gcloud` configuration directory, in place of `~/.config/gcloud` |
| `AZURE_STORAGE_CONNECTION_STRING` | An Azure storage connection string, with `AccountKey` or `SharedAccessSignature` |
| `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_SAS_TOKEN` | An Azure storage account and its key or SAS token |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_FEDERATED_TOKEN_FILE` | An Azure service principal, or AKS workload identity |
| `AZURE_CONFIG_DIR` | The Azure CLI's directory, in place of `~/.azure` |
