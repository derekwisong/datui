# Browse cloud data

Run `datui` and select a source under **CLOUD**.

1. Press <kbd>Enter</kbd> to list its buckets, projects or accounts.
2. Select a bucket or container and press <kbd>Enter</kbd>.
3. Open a directory to browse, or a file to load its table.

For a first try, choose **Public datasets**; it needs no credentials.
<kbd>Backspace</kbd> goes up a level and <kbd>Esc</kbd> returns to the previous screen.
For private storage, [sign in first](remote-data.md).

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
  ≈ Public datasets   public  11 datasets    built in
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

Datui finds supported logins on your machine and adds configured sources.
Listing a bucket does not guarantee permission to read its objects.
See [detected sources](../reference/cloud-sources.md#detected-sources) for
credential locations, source IDs and the `discover` setting.

## Public datasets

`Public datasets` lists data its publishers host and keep up to date, readable with
no login. Nothing is requested until you open one. HTTP(S) files open as tables; object-store
roots open for browsing. Datui bundles no dataset files. Hosted CSV extracts have
the coverage shown below; they are not the separately prepared demo tables.

| Dataset | Data | License |
|---|---|---|
| NYC flights (2013) | Departures from JFK, LaGuardia and Newark; delays in minutes | CC0 (nycflights13) |
| Food nutrition (fast food) | 515 menu items; nutrients per item, not per 100 g | GPL-3 (OpenIntro package) |
| US baby names (1880-2017) | Published name counts by year and sex; counts below five are suppressed | CC0 / public domain |
| NOAA daily weather (GHCN-D) | Worldwide weather station observations, by year and by station | CC0 |
| Premier League (2020-21) | Match rounds, dates, teams and full-time scores | CC0 |
| NYC yellow taxis (January 2025) | One original monthly trip file; fares, distances and congestion fees | NYC Open Data terms |
| Earthquakes (past month) | Rolling month of worldwide earthquakes; magnitudes, depth and location | Public domain |
| Space launches (1957-2018) | Historical launch records and agencies; includes failed attempts | MIT (The Economist extract); credit Jonathan McDowell |
| Palmer penguins | 344 penguins: species, island, bill, flipper length and body mass | CC0; credit Horst, Hill and Gorman (2020) |
| Bitcoin and Ethereum | Blocks and transactions, partitioned by date | AWS sample-code license |
| Overture Maps | Places, buildings, addresses, roads and boundaries, by release | ODbL; places CDLA Permissive 2.0 and Apache 2.0 |

The details pane gives each one's publisher, license and homepage. Each entry links to the publisher’s terms. <kbd>Backspace</kbd> at a
dataset's top returns to the list. A public bucket or container you have browsed or
opened unsigned is added to the list. For a list of your own, see
[Loading Data](remote-data.md#public-data).

`datui --generate-config` writes this catalog as active
`[[cloud.sources.datasets]]` tables to edit; see
[Configuration](../reference/cloud-sources.md) for the fields and how the snapshot
behaves.

Listings leave out what is not data: `_SUCCESS` and other job files, and the empty
objects some tools leave to stand for folders.

## What a cloud row shows

Bucket listings show name, size and modification time. Row counts and schemas
are fetched when a dataset opens; listing does not read every object's metadata. A directory of
`key=value` partitions is labeled `hive`, and every other directory by what
it holds — `12 parquet`, `3 csv`, or `dir` — like a local one. How a remote
dataset then opens is in [Loading Data](loading-data.md#remote-data).
