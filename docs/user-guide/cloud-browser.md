# Browse cloud data

Run `datui` and select a source under **CLOUD**.

1. Press <kbd>Enter</kbd> to list its buckets, projects or accounts.
2. Select a bucket or container and press <kbd>Enter</kbd>.
3. Open a directory to browse, or a file to load its table.

For a first try with no credentials, open a dataset under
[**Public datasets**](home-screen.md#public-datasets), further down the home screen.
<kbd>Backspace</kbd> goes up a level and <kbd>Esc</kbd> returns to the previous screen.
For private storage, [sign in first](remote-data.md).

| Source | Levels |
|---|---|
| S3 and S3-compatible | source › bucket › directory › object |
| Google Cloud | source › project › bucket › directory › object |
| Azure | source › account › container › directory › blob |

```
▾ CLOUD  5  ──────────────────────────────────────────────────────
  ≈ Amazon S3         s3      3 buckets     datui config
  ≈ Google Cloud      gcs     4 projects    project: example-project · gcloud
  ≈ Lab MinIO         s3      1 bucket      127.0.0.1:9000 · datui config
  ≈ onprem            s3      403           minio.corp.example:9000 · datui config
```

| Column | Shows |
|---|---|
| Name | The source's `label`, or its name |
| API | `s3`, `gcs` or `azure` |
| Count | How many buckets (projects for Google Cloud, accounts for Azure), a spinner while listing, `not listed` before the first listing, or why there are none |
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
| `not configured` | A variable named in `[[cloud.connections]]` is not set |
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

<kbd>Delete</kbd> on a source hides it until `datui cache clear`. To hide one
for good:

```toml
[cloud]
hide = ["gcs-default"]
```

## Which sources appear

Datui finds supported logins on your machine and adds configured sources.
Listing a bucket does not guarantee permission to read its objects.
See [detected sources](../reference/cloud-sources.md#detected-sources) for
credential locations, source IDs and the `discover` setting.

Public datasets and your own named datasets are
[collections](home-screen.md#collections), listed in sections of their own rather
than under `CLOUD`.

Listings leave out what is not data: `_SUCCESS` and other job files, and the empty
objects some tools leave to stand for folders.

A bucket or prefix lists a page of 1,000 names at a time, like a local directory:

| What | Where it shows |
|---|---|
| Rows as each page arrives | The heading reads `3,000 so far` |
| More than 5,000 names | The listing stops; the heading reads `first 5,000` |
| <kbd>Backspace</kbd> or <kbd>Esc</kbd> while it lists | The listing stops; entering again lists again |
| Typing past the first 5,000 | The bucket is asked for names starting with the filter, after the part the listed names share: `usw` in a level of `STATION=…` asks for `STATION=USW`. The heading adds `+ 1,907 STATION=USW*` |

## What a cloud row shows

Bucket listings show name, size and modification time. Row counts and schemas
are fetched when a dataset opens; listing does not read every object's metadata. A directory of
`key=value` partitions is labeled `hive`, and every other directory by what
it holds — `12 parquet`, `3 csv`, or `dir` — like a local one. How a remote
dataset then opens is in [Loading Data](loading-data.md#remote-data).
