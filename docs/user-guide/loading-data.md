# Loading Data

Load data with Datui by passing it [command line options](../reference/command-line-options.md)
and a path to open. The path can be a **local file or directory**, an **S3 URI** (Amazon S3 or MinIO), a **GCS URI** (`gs://`), or an **HTTP/HTTPS URL**. Cloud and HTTP support are included by default.

## Remote data (S3, GCS, and HTTP)

You can open data from **Amazon S3**, **Google Cloud Storage (GCS)**, **S3-compatible storage** (e.g. MinIO), or **HTTP/HTTPS** by passing the appropriate URI. Support is included by default when you build or install datui.

**Same behavior for all cloud and HTTP:** Parquet (and Parquet globs/prefixes) are read directly where supported; **all other formats** (CSV, JSON, NDJSON, etc.) are downloaded to a temporary file first, then loaded. That applies to AWS S3, custom S3 (MinIO, etc.), and GCS.

**One remote path at a time:** If you pass more than one path and the first is a remote URL (S3, GCS, or HTTP), datui reports an error. Open a single remote URL per run; local files can still be opened as multiple paths (concatenated).

### Amazon S3 (`s3://`)

**Credentials:** Datui does not add its own credential system. It uses the same credentials as the rest of the AWS ecosystem:

1. **Environment variables** (good for scripts and one-off use):
   - `AWS_ACCESS_KEY_ID` — access key
   - `AWS_SECRET_ACCESS_KEY` — secret key
   - `AWS_REGION` (or `AWS_DEFAULT_REGION`) — e.g. `us-east-1`
   - Optionally `AWS_SESSION_TOKEN` for temporary credentials

2. **Shared config** (good for daily use):
   - `~/.aws/credentials` — profiles and keys
   - `~/.aws/config` — region and other settings

3. **IAM roles** — If you run on EC2, ECS, Lambda, or similar, the instance/task role is used automatically; no env or config needed.

Set at least one of these before running Datui. Example:

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
datui s3://my-bucket/data/myfile.parquet
```

### Usage

- **Single Parquet file**:  
  `datui s3://bucket-name/path/to/file.parquet`

- **Hive-style partitioned data on S3**:  
  Use a prefix or glob so Polars can discover partitions:
  - `datui s3://bucket-name/prefix/`  
  - `datui "s3://bucket-name/prefix/**/*.parquet"`

- **Other formats (CSV, JSON, NDJSON, etc.) on S3**:  
  Datui downloads the object to a temporary file and then loads it like a local file. Use the same URI style:  
  `datui s3://bucket-name/path/to/file.csv`  
  The file is downloaded to your system temp directory (or `--temp-dir` if set), then opened normally. This is the same approach used for HTTP/HTTPS URLs.

### Configurable S3 (MinIO and other S3-compatible backends)

You can point S3 at a custom endpoint (e.g. MinIO) via **config**, **environment variables**, or **CLI options**. Priority is: **CLI > env > config**. That lets you keep a default in config and override per run when you use multiple backends.

**Config** — In `~/.config/datui/config.toml` add a `[cloud]` section:

```toml
[cloud]
# MinIO example
s3_endpoint_url = "http://localhost:9000"
s3_access_key_id = "minioadmin"
s3_secret_access_key = "minioadmin"
s3_region = "us-east-1"
```

**Environment variables** (override config; standard for S3 tools):

- `AWS_ENDPOINT_URL` or `AWS_ENDPOINT_URL_S3` — custom endpoint (e.g. `http://localhost:9000`)
- `AWS_ACCESS_KEY_ID` — access key
- `AWS_SECRET_ACCESS_KEY` — secret key
- `AWS_REGION` or `AWS_DEFAULT_REGION` — region (e.g. `us-east-1`)

**CLI options** (override env and config):

- `--s3-endpoint-url URL`
- `--s3-access-key-id KEY`
- `--s3-secret-access-key SECRET`
- `--s3-region REGION`

Examples for multiple backends without editing config each time:

```bash
# MinIO in another terminal/shell
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
datui s3://my-bucket/file.parquet

# Different MinIO or S3-compatible endpoint
datui --s3-endpoint-url https://s3.other.example s3://other-bucket/file.parquet
```

### Google Cloud Storage (`gs://`)

You can open Parquet files from **Google Cloud Storage** using `gs://` URIs. Credentials use [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials):

1. **User or service account key file**: set `GOOGLE_APPLICATION_CREDENTIALS` to the path of your JSON key file.
2. **gcloud CLI**: run `gcloud auth application-default login`.
3. **GCE/Cloud Run**: workload identity is used automatically.

Example:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
datui gs://my-bucket/path/to/file.parquet
```

- **Parquet** and **Hive-style prefixes/globs** work as for S3:  
  `datui gs://bucket/prefix/` or `datui "gs://bucket/prefix/**/*.parquet"`.
- **Other formats (CSV, JSON, NDJSON, etc.)** work the same as S3 and HTTP: the object is downloaded to a temporary file, then loaded. Example:  
  `datui gs://my-bucket/path/to/file.csv`

### Minimal build (no cloud)

To build without S3 support and avoid the extra cloud dependencies:

```bash
cargo build --release --no-default-features
```

If you pass an S3 or `gs://` URI to a binary built that way, you will see an error suggesting a build with default features.

## Supported Formats

| Format | Extensions | Eager load only | Hive partitioning |
|--------|------------|-----------------|-------------------|
| Parquet | `.parquet` | No | Yes |
| CSV (or other-delimited) | `.csv`, `.tsv`, `.psv`, etc. | No | No |
| NDJSON | `.jsonl` | No | No |
| JSON | `.json` | Yes | No |
| Arrow IPC / Feather v2 | `.arrow`, `.ipc`, `.feather` | No | No |
| Avro | `.avro` | Yes | No |
| Excel | `.xls`, `.xlsx`, `.xlsm`, `.xlsb` | Yes | No |
| ORC | `.orc` | Yes | No |

**Eager load only** — The file is read fully into memory before use; no lazy streaming. **Hive partitioning** — Use the `--hive` flag with a directory or glob; see [Hive-partitioned data](#hive-partitioned-data) below.
**Excel** — Use the `--sheet` flag to specify which sheet to open.

**CSV date inference** — By default, CSV string columns that look like dates (e.g. `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`) are parsed as Polars Date/Datetime. Use `--parse-dates false` or set `parse_dates = false` in [configuration](configuration.md) to disable.

## Compression

Compressed files are identified by extension and decompressed before loading. Use the `--compression` option to specify the format when the file has no extension or the extension is wrong.

### Supported Compression Formats

- gz
- zstd
- bzip2
- xz

## Hive-partitioned data

You can load a Hive-style partitioned dataset (e.g. a directory tree with `key=value` segment names such as `year=2024/month=01/`) by using the `--hive` flag and passing a **directory** or a **glob pattern** instead of a single file.

- **Directory**: point at the partition root, e.g. `datui --hive /path/to/data`
- **Glob**: use a pattern that matches the partition layout, e.g. `datui --hive /path/to/data/**/*.parquet`  
  You may need to quote the glob so your shell does not expand it (e.g. `datui --hive "/path/to/data/**/*.parquet"`).

Only Parquet is supported for hive-partitioned loading. If you pass a single file with `--hive`, it is loaded as usual and the flag is ignored.

**Schema from one file (default):** For faster loading, datui infers the Parquet schema from a single file along one partition branch (single-spine) instead of scanning all files. This applies to both local Hive directories and S3/GCS prefixes (e.g. `s3://bucket/prefix/` or `gs://bucket/prefix/`). If your dataset has inconsistent schemas or other complications and you prefer Polars to discover the schema over all files, disable this with `--single-spine-schema=false` or set `single_spine_schema = false` under `[file_loading]` in [configuration](configuration.md).

Partition columns (the keys from the path, e.g. `year`, `month`) are shown first in the table and listed in the Info panel under the **Partitioned data** tab.
