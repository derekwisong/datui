# Configuration

Datui reads one [TOML](https://toml.io) file. Generate it with every option
present and commented out:

```bash
datui --generate-config
```

| OS | Path |
|---|---|
| Linux | `~/.config/datui/config.toml` |
| macOS | `~/Library/Application Support/datui/config.toml` |
| Windows | `%APPDATA%\datui\config.toml` |

Settings apply in this order, later winning: built-in defaults, imported files
in the order listed, this file, command-line flags.

A minimal file:

```toml
[display]
row_numbers = true
number_format = "thousands"

[data]
directories = ["/mnt/data", "~/datasets"]

[theme.colors]
accent = "#ff9e64"
```

## Sections

### File loading

Defaults for the CSV options; the [command-line flags](../reference/command-line-options.md)
of the same name override them per run.

A file's layout is not here: `--delimiter`, `--no-header` and the skips describe
the one file being opened, so they are flags only. A config that still sets
`delimiter`, `has_header`, `skip_lines`, `skip_rows` or `skip_tail_rows` gets a
warning on stderr and is otherwise ignored.

```toml
[file_loading]
null_values = ["NA", "amount="]   # Read as null: everywhere, or in one column with COL=VAL
parse_dates = true            # Parse date-looking strings as Date / Datetime
parse_strings = true          # Trim and type-infer string columns
parse_strings_sample_rows = 1000  # Rows sampled for that inference
infer_schema_length = 1000    # Rows used to infer column types
ignore_errors = false         # Skip unparseable rows instead of failing
decompress_in_memory = false  # Compressed CSV: decompress to a temp file (false) or into memory (true)
temp_dir = "/tmp"             # Where that temp file goes. Omit for the system default
single_spine_schema = true    # Partitioned Parquet: every column any file has, from the footers
```

### Display

```toml
[display]
row_numbers = false         # Row numbers on the left (N toggles)
row_start_index = 1         # First row number, 0 or 1
number_format = "none"      # Digit grouping, see below (F toggles)
align_numeric_right = true  # Right-align numbers and their headers
column_colors = true        # Color cells and headers by type
dtype_row = true            # Second header row naming each type (D toggles)
notes_accent = true         # Accent the i key when there is something to note
table_cell_padding = 1      # Spaces between columns
# sidebar_width = 70        # Fixed width for every sidebar. Omit for each sidebar's own default
pages_lookahead = 3         # Pages buffered ahead of the screen
pages_lookback = 3          # Pages buffered behind
max_buffered_rows = 100000  # Cap on buffered rows, 0 for none. Remote Parquet row groups
                            # under it are held whole, larger ones read a window at a time
max_buffered_mb = 512       # Cap on buffer memory, 0 for none. Planned before the read
                            # from the schema, so a wide table buffers fewer rows
unicode = "auto"            # "auto", "always" or "never": glyphs or ASCII
```

#### Number formatting

`number_format` groups digits so `248956422` reads as `248,956,422`.
<kbd>F</kbd> toggles it for the session.

| Preset | `1234567.89` becomes |
|---|---|
| `none` (default) | `1234567.89` |
| `thousands` | `1,234,567.89` |
| `european` | `1.234.567,89` |
| `si` | `1 234 567.89` (narrow no-break space) |
| `swiss` | `1'234'567.89` |
| `indian` | `12,34,567.89` |
| `underscore` | `1_234_567.89` |
| `system` | whatever `LC_ALL` / `LC_NUMERIC` / `LANG` says, else `thousands` |

For finer control, use a table:

```toml
[display.number_format]
grouping = "thousands"
group_separator = ","
decimal_separator = "."
floats = true                        # group float columns too
float_precision = 2                  # omit to keep the file's own decimals
exclude_columns = ["year", "*_id", "zip"]   # never group these (globs)
```

Every value in a grouped column is grouped, with no size threshold. Use
`exclude_columns` for numbers that are labels: years, IDs, postcodes.

Formatting is display-only. Exports, queries, filters and templates use the raw
values. Datui does not read your locale unless you ask with `"system"`: a data
file has no locale, and `LC_NUMERIC` is unset on most servers.

### Performance

```toml
[performance]
# sampling_threshold = 1000000   # Analyze a sample when a table has this many rows or more
event_poll_interval_ms = 25      # Lower is more responsive and uses more CPU
```

Sampling is off unless `sampling_threshold` is set. `--sampling-threshold N`
overrides it for a run and `0` forces the full dataset. See
[Analysis](analysis-features.md#sampling).

### Charts

```toml
[chart]
row_limit = 10000   # Rows used to build a chart, 1 to 10_000_000. Adjustable in the chart view
```

### Data

```toml
[data]
directories = ["/mnt/data", "~/datasets"]   # Places the home screen always lists
use_desktop_recents = true                  # Offer directories from the desktop's recent-files list

[data.search]                               # The recursive search typing starts
enabled           = true
max_depth         = 8
max_results       = 20000
time_budget_ms    = 1500
cross_filesystems = false
follow_gitignore  = false
skip_extra        = []                      # Directory names to skip, beyond the defaults
extensions        = []                      # Empty means every format datui opens
```

See [The Home Screen](home-screen.md#searching-below-the-current-directory)
for what each does.

### Cloud

```toml
[cloud]
s3_endpoint_url = "http://localhost:9000"   # MinIO, R2, Ceph and other S3-compatible stores
s3_access_key_id = "..."
s3_secret_access_key = "..."
s3_region = "us-east-1"
public_datasets = true                      # compatibility switch for the built-in catalog; false hides it
azure_account_keys = true                   # read Azure with the account key after a sign-in is refused for want of a data role
env_files = [".env"]                        # read cloud variables from these files; off unless listed
instance_identity = false                   # use the EC2, GCE or Azure VM's own identity
discover = true                             # logins found on this machine: true, false, or ["s3", "gcs", "azure"]
list_on_start = false                       # list every source's buckets at launch, not when entered
```

Environment variables override these, and command-line flags override both.
See [Loading Data](loading-data.md#remote-data).

More stores go in `[[cloud.sources]]`, one table each:

```toml
[[cloud.sources]]
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
| `kind` | all | Required, except with `public`. `s3`, `gcs` or `azure` |
| `public` | | `true` for data anyone can read. `buckets` and `datasets` are then URLs from any supported cloud, read without credentials |
| `buckets` | all | Buckets to show when the keys can read but not list |
| `datasets` | public | Structured dataset tables with `name`, `url`, and optional metadata |
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

A public source lists data from any cloud and reads it unsigned:

```toml
[[cloud.sources]]
name = "open-data"
public = true
buckets = [
  "s3://noaa-ghcn-pds/parquet/",
  "gs://cloud-samples-data/bigquery/",
  "abfss://release@overturemapswestus2.dfs.core.windows.net/",
]
```

Use `[[cloud.sources.datasets]]` when the home screen should show a stable name and
details. Each table belongs to the preceding source:

```toml
[[cloud.sources]]
name = "public"
label = "Public datasets"
public = true

[[cloud.sources.datasets]]
name = "NOAA daily weather (GHCN-D)"
url = "s3://noaa-ghcn-pds/parquet/"
description = "Worldwide weather station observations, by year and by station"
publisher = "NOAA"
license = "CC0"
homepage = "https://registry.opendata.aws/noaa-ghcn/"
```

| Dataset field | Meaning |
|---|---|
| `name` | Required, nonempty name shown on the home screen; unique in this source |
| `url` | Required `s3://`, `gs://` or Azure URL; unique in this source |
| `description` | Optional summary shown in the details pane |
| `publisher` | Optional publisher |
| `license` | Optional license name |
| `homepage` | Optional publisher page |

A configured source named `public` replaces the built-in catalog. Other public
source names create separate collections. `hide = ["public"]` hides the catalog,
and the older `public_datasets = false` switch remains supported.

`datui --generate-config` writes the current built-in `public` source and dataset
tables as active TOML. Delete a dataset table to exclude it, edit one to change its
metadata, or add another table. The generated catalog is a snapshot: a retained
config does not automatically receive datasets or metadata added by later datui
releases. Run `datui --generate-config --force` to take a new snapshot, after saving
any local changes you want to keep.

#### Secrets that live elsewhere

A password manager or vault can supply a secret without it touching the config or
the environment:

```toml
[[cloud.sources]]
name = "onprem"
kind = "s3"
endpoint_url = "https://minio.corp.example:9000"
access_key_id_env = "ONPREM_KEY"
secret_command = "pass show minio/onprem"        # or: op read op://vault/minio/secret

[[cloud.sources]]
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
names `[[cloud.sources]]` point at with `*_env`. Anything else in the file, a
database password for one, is ignored. A variable already set in the environment
wins, and nothing is exported, so no program datui starts sees them. It is off
unless you list files: reading whatever `.env` sits in the current directory,
unasked, would be reading secrets you did not mean to hand over.

See [The Home Screen](home-screen.md) for
[`discover`](home-screen.md#which-sources-appear) and
[`list_on_start`](home-screen.md#loading). `--cloud-discover` overrides `discover`
for one run.

`instance_identity = true` lets datui ask the cloud VM it runs on for credentials:
an EC2 instance role, a GCE service account, an Azure VM's managed identity. That
means a request to a link-local metadata address, which on other networks can hang
until it times out, so it is off by default. Cloud Run and Cloud Functions, and
Azure App Service, Functions and Container Apps, set variables that say an identity
is there (`K_SERVICE`, `IDENTITY_ENDPOINT`, `MSI_ENDPOINT`), and are used without
the setting.

A secret written directly into a source (`secret_access_key = "..."`) is refused,
and so is any key datui does not recognize, with the key named. A variable that is
named but not set is reported when the source is used; the source never falls back
to other keys in the environment.

### Query, templates, debug

```toml
[query]
history_limit = 1000     # Queries remembered
enable_history = true

[templates]
auto_apply = false       # Apply the best-matching template when a file opens

[debug]
enabled = false          # Debug overlay (--debug)
show_performance = true
show_query = true
show_transformations = true
```

## Theme

### Light and dark

```toml
[theme]
mode = "auto"   # "auto" (default), "dark" or "light"
```

The header fill, row stripe, borders and dim text sit a few shades off the
terminal background, and the shades for a dark terminal are unreadable on a
light one. `auto` reads `COLORFGBG` and falls back to `dark`. **Alacritty,
Kitty and Ghostty do not set `COLORFGBG`**, so with a light scheme in those
terminals set `mode = "light"`.

### Colors

Every color is a slot under `[theme.colors]`. The defaults are datui's own
palette, after Tokyo Night, in a dark set and a light set of the same hues
darkened. Set any slot to change it.

| Slot | Used for | Dark default |
|---|---|---|
| `accent` | Key chips, focused titles, the selection rail | `#7dcfff` |
| `accent_bright` | The section the cursor is in | `#a4daff` |
| `gradient_start`, `gradient_end` | The wordmark on the home screen | `#7aa2f7`, `#bb9af7` |
| `keybind_hints`, `keybind_labels` | Keys and their labels in the control bar | `#7dcfff`, `#a9b1d6` |
| `throbber` | The busy spinner | `#7dcfff` |
| `background`, `surface` | Main and modal backgrounds | `default` (the terminal's) |
| `controls_bg` | Control bar and count chips | `#262a3f` |
| `text_primary`, `text_secondary`, `text_inverse` | Text; secondary text; text on a key chip | `default`, `#737aa2`, `#1a1b26` |
| `table_header`, `table_header_bg` | Header text and fill | `#c0caf5`, `#2b3047` |
| `row_numbers` | The row-number column | `#565f89` |
| `alternate_row_color` | Every other row (`"default"` turns the stripe off) | `#1e2030` |
| `table_selected` | Tint under the current row (`"reversed"` swaps fg and bg instead) | `#283457` |
| `column_separator` | Rule after frozen columns, rules beside section titles | `#3b4261` |
| `sidebar_border`, `modal_border_active`, `modal_border_error` | Borders | `#565f89`, `#7dcfff`, `#f7768e` |
| `str_col`, `int_col`, `float_col`, `bool_col`, `temporal_col`, `binary_col` | Cells and headers by type | `#9ece6a`, `#7aa2f7`, `#2ac3de`, `#e0af68`, `#bb9af7`, `#565f89` |
| `success`, `warning`, `error`, `dimmed` | Status colors; nulls and axes use `dimmed` | `#9ece6a`, `#e0af68`, `#f7768e`, `#565f89` |
| `primary_chart_series_color`, `secondary_chart_series_color` | Histogram bars and Q-Q points; theoretical overlays | `#7dcfff`, `#565f89` |
| `chart_series_color_1` to `chart_series_color_7` | Series in the chart view | `#7dcfff`, `#bb9af7`, `#9ece6a`, `#e0af68`, `#7aa2f7`, `#f7768e`, `#ff9e64` |
| `distribution_normal`, `distribution_skewed`, `distribution_other`, `outlier_marker` | Analysis view | `#9ece6a`, `#e0af68`, `#c0caf5`, `#f7768e` |
| `cursor_focused`, `cursor_dimmed` | The text cursor, focused and not | `default` |

Three formats are accepted:

```toml
[theme.colors]
accent = "#ff9e64"          # hex, adapted to what the terminal can show
error = "bright_red"        # a name: black red green yellow blue magenta cyan white,
                            # bright_*, gray dark_gray light_gray, default (the terminal's)
controls_bg = "indexed(236)"   # an entry of the xterm 256-color palette
```

Hex colors display exactly on a true-color terminal, snap to the nearest of
256 on `xterm-256color`, and to basic ANSI on anything less. `NO_COLOR=1`
turns color off.

A ready-made Dracula theme:

```toml
[theme.colors]
accent = "#bd93f9"
accent_bright = "#ff79c6"
gradient_start = "#8be9fd"
gradient_end = "#ff79c6"
keybind_hints = "#bd93f9"
keybind_labels = "#ff79c6"
background = "#282a36"
surface = "#44475a"
controls_bg = "#44475a"
text_primary = "#f8f8f2"
text_secondary = "#6272a4"
text_inverse = "#282a36"
table_header = "#f8f8f2"
table_header_bg = "#44475a"
table_selected = "#44475a"
alternate_row_color = "default"
column_separator = "#bd93f9"
str_col = "#50fa7b"
int_col = "#8be9fd"
float_col = "#bd93f9"
bool_col = "#f1fa8c"
temporal_col = "#ff79c6"
success = "#50fa7b"
warning = "#ffb86c"
error = "#ff5555"
dimmed = "#6272a4"
chart_series_color_1 = "#8be9fd"
chart_series_color_2 = "#ff79c6"
chart_series_color_3 = "#50fa7b"
chart_series_color_4 = "#f1fa8c"
chart_series_color_5 = "#bd93f9"
chart_series_color_6 = "#ff5555"
chart_series_color_7 = "#ffb86c"
```

## Importing other config files

`import` names TOML files to merge in before this file's own settings. It is
how datui follows a theme generated by something else; see
[Theming from Your System](system-theming.md).

```toml
import = ["~/.local/state/omarchy/current/theme/datui.toml"]

[display]
row_numbers = true
```

- Imports apply in the order listed, each overriding the last; this file's own
  values apply after all of them.
- An imported file may itself `import`. Chains stop at 8 files; a cycle is an
  error.
- Paths may be absolute, relative to the importing file, or use `~` and
  `$VAR`.
- A missing file is skipped with a warning on stderr. A file that exists but
  cannot be parsed is fatal.

### A caveat when overriding an imported color

Datui decides whether you set a color by comparing it with the built-in
default, so a color set to **exactly the default value** looks unset and will
not override an import. If an import sets `error` to `#ff5345` and you want
red back, write `#ff0000` or `indexed(9)` rather than the default `#f7768e`.

## Command-line overrides

Any flag beats the file for that run:

```bash
datui data.csv --row-numbers
datui data.csv --number-format thousands
datui data.csv --infer-schema-length 10000
datui data.csv --sampling-threshold 0 # no sampling, whatever the file says
```

## Troubleshooting

**The file is ignored.** Check the path for your OS above, and that the TOML
parses. Warnings go to stderr, which the UI hides; run
`datui data.csv 2> /tmp/datui.log` and read the log after quitting. A `version`
key, if present, must start with `0.2`.

**An import does not apply.** Confirm the file exists at the resolved path, that
your value differs from datui's default (see the caveat above), and that nothing
later in the chain, including a command-line flag, overrides it.

**A color is rejected.** `Invalid color value for 'accent': Unknown color name`
means a typo in a name. Names are case-insensitive; hex needs six digits;
indexed is `indexed(0)` to `indexed(255)`.

**Colors look wrong.** Your terminal may not support true color, so hex
values are being approximated; try names or `indexed(...)`. If everything is
monochrome, `NO_COLOR` is set.

**Header or toolbar text is cut off or garbled** in VS Code's terminal or on
`xterm-256color`. Some terminals mishandle a background color on those rows.
Set them to the terminal's own background:

```toml
[theme.colors]
controls_bg = "default"
table_header_bg = "default"
```

**Start over.** `datui --generate-config --force` rewrites the file with the
defaults, or delete it and datui runs with none.
