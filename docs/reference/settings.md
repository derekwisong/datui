# Settings reference

<!-- Generated from crates/datui-cli/src/settings.rs by `gen_docs settings`. Do not edit. -->

Set these in `config.toml` (`datui config init` writes one with every key
commented out), or for one run with `-c KEY=VALUE`:

```bash
datui -c display.row_numbers=true data.csv
```

A flag beats `-c`, which beats the config files, which beat the defaults.
`datui config keys` lists every key with its value in effect and where it was
set. See [Configure datui](../user-guide/configuration.md) for where the file
lives, imports, the theme and troubleshooting.

| Type | Written as |
|---|---|
| size | A number and a unit: `512MiB`, `2GiB`, `100KiB` (`MB`, `GB` are powers of 1000). `0` needs none |
| duration | A number and a unit: `250ms`, `1.5s`, `2m` |
| list | In a file, a TOML array; with `-c`, `a,b` or the array |
| color | A name (`red`, `bright_blue`, `default`), `#rrggbb` or `indexed(0-255)` |

## Top level

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `import` | list | `[]` |  | Config files merged in before this one, in order; this file's own values win. Paths may be relative to this file, or use ~ and $VAR. |
| `version` | string | `"0.2"` |  | Configuration format version. |
| `formats_path` | list | `[]` |  | Directories of format specs, searched after ~/.config/datui/formats and $DATUI_FORMATS_PATH. Adds up across imports. |
| `sources` | tables | unset |  | Named collections of datasets on the home screen; see Dataset collections. |

## File loading

`[file_loading]` Defaults for reading files. A file's layout (delimiter, header, rows to skip) is a flag for the one file, not a setting.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `file_loading.parse_dates` | bool | unset |  | Read CSV and JSON strings that look like dates or ISO 8601 timestamps as Date or Datetime (default true). |
| `file_loading.decompress_in_memory` | bool | unset |  | Decompress a compressed CSV, TSV or PSV into memory instead of to a temp file (default false). |
| `file_loading.temp_dir` | path | unset | `--temp-dir` | Directory for decompression temp files. Unset: the system's. |
| `file_loading.single_spine_schema` | bool | unset |  | A partitioned Parquet dataset's schema is every column any of its files has, from their footers; false lets Polars take one file's (default true). |
| `file_loading.null_values` | list | unset | `--null` | Values read as null: VAL in every column, COL=VAL in column COL only. --null is repeatable and replaces this list. |
| `file_loading.parse_strings` | bool | unset | `--infer-types` | Trim string columns and read them as dates, times, durations or numbers where they all parse (default true). --infer-types=off turns it off, --infer-types=a,b limits it to those columns. |
| `file_loading.parse_strings_sample_rows` | integer | unset |  | Rows sampled to infer string column types (default 1000). |
| `file_loading.infer_schema_length` | integer | unset | `--infer-rows` | Rows read to infer a CSV's column types (default 1000). |
| `file_loading.ignore_errors` | bool | unset | `--ignore-errors` | Skip CSV rows that do not parse instead of failing (default false). |
| `file_loading.comment_char` | string | unset | `--comment` | CSV lines starting with this are comments, before the header and among the data. |
| `file_loading.header_join` | string | unset |  | Joins a column's names when --header-rows names several lines (default " "). |
| `file_loading.skip_initial_space` | bool | unset | `--skip-initial-space` | Ignore the spaces after a CSV delimiter, so padded numbers are numbers (default false). |
| `file_loading.audio_float` | bool | unset |  | Show integer audio samples as float in [-1, 1] (default false: the integers as stored). |
| `file_loading.follow_interval_ms` | integer | unset |  | --follow: milliseconds between checks for new rows, or on Linux the least time between two reads, 10 to 60000 (default 250). |
| `file_loading.memory_warning_mb` | integer | unset |  | Ask before reading more than this many MB of a file whole into memory (JSON, Avro, ORC, Excel and the other formats read in memory); 0 never asks (default 1024). |

## Display

`[display]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `display.unicode` | auto \| always \| never | `"auto"` |  | Box-drawing and arrow glyphs, or plain ASCII. auto uses them when the locale is UTF-8. |
| `display.pages_lookahead` | integer | `3` |  | Pages of rows buffered ahead of the screen. |
| `display.pages_lookback` | integer | `3` |  | Pages of rows buffered behind the screen. |
| `display.max_buffered_rows` | integer | `100000` |  | Most rows the table buffers; 0 for no limit. |
| `display.max_buffered_mb` | integer | `512` |  | Most MiB of rows the table buffers between reads; 0 for no limit. |
| `display.row_numbers` | bool | `false` | `--row-numbers` | Show row numbers on the left (# toggles). |
| `display.row_start_index` | integer | `1` |  | The first row's number. |
| `display.table_cell_padding` | "comfortable" \| "compact" \| integer | `"comfortable"` |  | Space between columns: comfortable (2 cells), compact (1) or a number of cells. |
| `display.column_colors` | bool | `true` |  | Color cells by column type. |
| `display.dtype_row` | bool | `true` |  | A second header row naming each column's type (D toggles). |
| `display.notes_accent` | bool | `true` |  | Accent the i key when datui has noticed something about the data. |
| `display.mouse` | bool | `true` | `--mouse` | Take the mouse: the wheel scrolls, a click selects. false leaves it to the terminal. |
| `display.sidebar_width` | integer | unset |  | Width of every sidebar, in cells. Unset: each sidebar's own. |
| `display.align_numeric_right` | bool | `true` |  | Right-align numeric columns and their headers. |
| `display.number_format` | preset \| table | `"none"` | `--number-format` | Digit grouping: none, thousands, european, si, swiss, indian, underscore or system, or a [display.number_format] table (, toggles). |

## Performance

`[performance]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `performance.analysis_sample_rows` | integer | `100000` | `--sample-rows` | Rows an analysis samples from a larger table, spread across all of it; 0 reads every row. |
| `performance.polars_streaming` | bool | `true` |  | Use the Polars streaming engine where it applies. |
| `performance.quality_local_copy_mb` | integer | `2048` |  | Most MiB a Data Quality full scan of a remote dataset copies into the cache to read once; 0 never copies. |

## Charts

`[chart]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `chart.row_limit` | integer | `10000` |  | Rows a chart reads; a larger table is sampled across all of it. |
| `chart.grid` | bool | `false` |  | Start charts with a grid at the major ticks (g toggles). |

## Data

`[data]` The home screen.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `data.directories` | list | `[]` |  | Directories the home screen always lists. ~ and $VAR expand. |
| `data.use_desktop_recents` | bool | `true` |  | Also list directories from the desktop's recently-used files; never the file names. |
| `data.show_unreadable_files` | bool | `false` |  | List files datui cannot read, dimmed (Ctrl+A toggles). |
| `data.builtin_catalog` | bool | `true` |  | Offer the built-in public collection of datasets. |
| `data.hide_sources` | list | `[]` |  | Collections not shown, by name. Adds up across imports. |
| `data.preview_max_mb` | integer | `64` |  | Largest local file, in MB, whose first rows the home screen previews; 0 turns the preview off. |

## Data search

`[data.search]` Searching below the working directory as you type on the home screen.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `data.search.enabled` | bool | `true` |  | Search below the working directory as you type. |
| `data.search.max_depth` | integer | `8` |  | How many directories deep the search goes. |
| `data.search.max_results` | integer | `1000` |  | Matches listed; the rest are counted. |
| `data.search.time_budget_ms` | integer | `1500` |  | Milliseconds the search walks before keeping what it found. |
| `data.search.cross_filesystems` | bool | `false` |  | Descend into other filesystems, network mounts included. |
| `data.search.follow_gitignore` | bool | `false` |  | Skip what .gitignore ignores. |
| `data.search.skip` | list | `["node_modules", "target", "build", "dist", "vendor", "site-packages", "__pycache__", "venv", "env"]` |  | Directory names never searched. Replaces the defaults; skip_extra adds to them. |
| `data.search.skip_extra` | list | `[]` |  | Directory names never searched, besides skip. |
| `data.search.extensions` | list | `[]` |  | Extensions searched for; empty means every format datui opens. |

## Cloud

`[cloud]` See [Cloud sources](cloud-sources.md) for `[[cloud.connections]]`.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `cloud.s3_endpoint_url` | string | unset |  | Endpoint for S3-compatible storage such as MinIO. |
| `cloud.s3_access_key_id` | string | unset |  | S3 access key. |
| `cloud.s3_secret_access_key` | string | unset |  | S3 secret key. Prefer AWS_SECRET_ACCESS_KEY. |
| `cloud.s3_region` | string | unset |  | S3 region. |
| `cloud.connections` | tables | unset |  | Cloud stores to list on the home screen; see Cloud sources. |
| `cloud.hide` | list | unset |  | Cloud source IDs not shown on the home screen. Adds up across imports. |
| `cloud.azure_account_keys` | bool | unset |  | Read an Azure account with its access keys when a sign-in has no data role (default true). |
| `cloud.env_files` | list | unset |  | Files to read cloud variables from, relative to the working directory. Adds up across imports. |
| `cloud.instance_identity` | bool | unset |  | Use the identity of the cloud VM datui runs on (default false). |
| `cloud.discover` | bool \| "all" \| "none" \| list | unset |  | Logins found on this machine that become home-screen sources: all, none, or kinds from s3, gcs, azure. |
| `cloud.list_on_start` | bool | unset |  | List every source's buckets when the home screen opens, not when one is entered (default false). |

## Query

`[query]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `query.history_limit` | integer | `1000` |  | Queries remembered. |
| `query.enable_history` | bool | `true` |  | Remember queries. |
| `query.default_mode` | sql \| search \| q-style | `"sql"` |  | The mode / opens on when no query is active. |

## Views

`[templates]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `templates.auto_apply` | bool | `false` |  | Apply the best-matching view when a file opens. |

## Clipboard

`[clipboard]` How the copy dialog (`y`) reaches the system clipboard.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `clipboard.backend` | auto \| native \| osc52 | `"auto"` |  | auto: the display server where one answers, osc52 elsewhere (SSH). osc52 is an escape sequence the terminal applies. |
| `clipboard.osc52_limit_kb` | integer | `100` |  | Longest osc52 copy to attempt, in KB of base64. |

## Debug

`[debug]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `debug.enabled` | bool | `false` |  | Show the debug overlay. |
| `debug.show_performance` | bool | `true` |  | Unused. |
| `debug.show_query` | bool | `true` |  | Unused. |
| `debug.show_transformations` | bool | `true` |  | Unused. |
| `debug.log_file` | path | unset | `--log-file` | Where the log goes. Unset: datui.log in the cache directory. |

## Theme

`[theme]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `theme.mode` | auto \| dark \| light | unset |  | Which built-in palette to start from. auto reads COLORFGBG and falls back to dark. |

## Colors

`[theme.colors]` Each slot takes a name (`red`, `bright_blue`, `default`), `#rrggbb` or `indexed(0-255)`. Unset slots take the palette for `theme.mode`.

| Key | Dark | Light | Description |
|---|---|---|---|
| `theme.colors.keybind_hints` | `#7dcfff` | `#2e7de9` | Keys in the control bar and dialogs. |
| `theme.colors.keybind_labels` | `#a9b1d6` | `#3760bf` | Labels beside keys in the control bar. |
| `theme.colors.throbber` | `#7dcfff` | `#2e7de9` | The busy spinner. |
| `theme.colors.primary_chart_series_color` | `#7dcfff` | `#2e7de9` | Histogram bars, bar charts and Q-Q points. |
| `theme.colors.secondary_chart_series_color` | `#565f89` | `#848cb5` | Theoretical overlays and the Q-Q reference line. |
| `theme.colors.success` | `#9ece6a` | `#587539` | Success. |
| `theme.colors.error` | `#f7768e` | `#f52a65` | Errors. |
| `theme.colors.warning` | `#e0af68` | `#8c6c3e` | Warnings. |
| `theme.colors.dimmed` | `#565f89` | `#848cb5` | Dimmed text, nulls and axes. |
| `theme.colors.background` | `default` | `default` | Main background. |
| `theme.colors.surface` | `default` | `default` | Dialog background. |
| `theme.colors.controls_bg` | `#262a3f` | `#d0d5e3` | Control bar and count chips. |
| `theme.colors.text_primary` | `default` | `default` | Text. |
| `theme.colors.text_secondary` | `#737aa2` | `#6172b0` | Secondary text. |
| `theme.colors.text_inverse` | `#1a1b26` | `#e1e2e7` | Text on a key chip. |
| `theme.colors.table_header` | `#c0caf5` | `#3760bf` | Header text. |
| `theme.colors.table_header_bg` | `#2b3047` | `#c4c8da` | Header fill. |
| `theme.colors.row_numbers` | `#565f89` | `#848cb5` | The row-number column. |
| `theme.colors.column_separator` | `#3b4261` | `#a8aecb` | The rule after frozen columns and beside section titles. |
| `theme.colors.table_selected` | `#283457` | `#b6bfe2` | Tint under the current row; reversed swaps text and background instead. |
| `theme.colors.column_cursor` | `#292e42` | `#cbd3f2` | Tint under the column cursor's cells. |
| `theme.colors.cell_cursor` | `#3b4261` | `#a0aef0` | The column cursor's header and the current cell. |
| `theme.colors.sidebar_border` | `#565f89` | `#6172b0` | Sidebar and dialog borders. |
| `theme.colors.modal_border_active` | `#7dcfff` | `#2e7de9` | The focused dialog's border. |
| `theme.colors.modal_border_error` | `#f7768e` | `#f52a65` | An error dialog's border. |
| `theme.colors.distribution_normal` | `#9ece6a` | `#587539` | Analysis: a normal distribution. |
| `theme.colors.distribution_skewed` | `#e0af68` | `#8c6c3e` | Analysis: a skewed distribution. |
| `theme.colors.distribution_other` | `#c0caf5` | `#3760bf` | Analysis: other distributions. |
| `theme.colors.outlier_marker` | `#f7768e` | `#f52a65` | Analysis: outliers. |
| `theme.colors.cursor_focused` | `default` | `default` | The text cursor; default reverses the text under it. |
| `theme.colors.cursor_dimmed` | `default` | `default` | Unused. |
| `theme.colors.cursor_text` | `default` | `default` | Text under the cursor block; default picks black or white by contrast. |
| `theme.colors.alternate_row_color` | `#1e2030` | `#dcdfea` | Every other row; default turns the stripe off. |
| `theme.colors.str_col` | `#9ece6a` | `#587539` | String columns. |
| `theme.colors.int_col` | `#7aa2f7` | `#2e7de9` | Integer columns. |
| `theme.colors.float_col` | `#2ac3de` | `#007197` | Float columns. |
| `theme.colors.bool_col` | `#e0af68` | `#8c6c3e` | Boolean columns. |
| `theme.colors.temporal_col` | `#bb9af7` | `#9854f1` | Date, time and datetime columns. |
| `theme.colors.binary_col` | `#565f89` | `#848cb5` | Binary columns' placeholder. |
| `theme.colors.chart_series_color_1` | `#7dcfff` | `#2e7de9` | Chart series 1. |
| `theme.colors.chart_series_color_2` | `#bb9af7` | `#9854f1` | Chart series 2. |
| `theme.colors.chart_series_color_3` | `#9ece6a` | `#587539` | Chart series 3. |
| `theme.colors.chart_series_color_4` | `#e0af68` | `#8c6c3e` | Chart series 4. |
| `theme.colors.chart_series_color_5` | `#7aa2f7` | `#007197` | Chart series 5. |
| `theme.colors.chart_series_color_6` | `#f7768e` | `#f52a65` | Chart series 6. |
| `theme.colors.chart_series_color_7` | `#ff9e64` | `#b15c00` | Chart series 7. |
| `theme.colors.chart_grid` | `#3d4785` | `#70aabf` | The chart grid, a shade dimmer than dimmed. |
| `theme.colors.accent` | `#7dcfff` | `#2e7de9` | Key chips, focused titles and the selection rail. |
| `theme.colors.accent_bright` | `#a4daff` | `#1a6cd0` | The section the cursor is in. |
| `theme.colors.gradient_start` | `#7aa2f7` | `#2e7de9` | The wordmark's first stop. |
| `theme.colors.gradient_end` | `#bb9af7` | `#9854f1` | The wordmark's last stop. |
| `theme.colors.find_match` | `#e0af68` | `#f0c35a` | Behind the cell a find landed on. |
| `theme.colors.hex_null` | `#565f89` | `#848cb5` | Hex view: the byte 0x00. |
| `theme.colors.hex_printable` | `#7dcfff` | `#007197` | Hex view: printable ASCII. |
| `theme.colors.hex_whitespace` | `#9ece6a` | `#587539` | Hex view: whitespace bytes. |
| `theme.colors.hex_control` | `#bb9af7` | `#9854f1` | Hex view: other control bytes. |
| `theme.colors.hex_high` | `#e0af68` | `#8c6c3e` | Hex view: 0x80 to 0xFE. |
| `theme.colors.hex_ff` | `#f7768e` | `#f52a65` | Hex view: the byte 0xFF. |

## Glyphs

`[glyphs]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `glyphs.*` | string \| list | unset |  | A glyph slot from glyphs.rs, replaced when the Unicode set is active. Keeps the width of the glyph it replaces. |
