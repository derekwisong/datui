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

Pass an `s3://`, `gs://` or `https://` URL where you would pass a path. Parquet
is read in place, and a prefix or glob of Parquet files opens as a partitioned
dataset. Every other format is downloaded to a temporary file
(`--temp-dir` to choose where) and then opened like a local file. One remote
path per run.

Once credentials are in place, the buckets they reach are also listed on the
[home screen](home-screen.md#cloud-storage), so you can browse instead of
typing URLs.

### Amazon S3

Datui uses the standard AWS credential chain, in this order:

1. Environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
   (or `AWS_DEFAULT_REGION`), and `AWS_SESSION_TOKEN` for temporary credentials.
2. `~/.aws/credentials` and `~/.aws/config`, with `AWS_PROFILE` to pick a profile.
3. The instance or task role on EC2, ECS, Lambda and EKS.

```bash
export AWS_PROFILE=analytics
datui s3://my-bucket/events/2024/
```

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
`--s3-secret-access-key` and `--s3-region`.

### Google Cloud Storage

Credentials come from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials):
`GOOGLE_APPLICATION_CREDENTIALS` pointing at a key file, the login written by
`gcloud auth application-default login`, or workload identity on GCE and Cloud Run.

```bash
gcloud auth application-default login
datui gs://my-bucket/path/file.parquet
```

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
