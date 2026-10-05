# Settings

<!-- Generated from crates/datui-cli/src/settings.rs by `gen_docs settings`. Do not edit. -->

Set these in `config.toml` (`datui config init` writes one with every key
commented out), or for one run with `-c KEY=VALUE`:

```bash
printf 'a,b\n1,2\n' | datui -c display.row_numbers=true
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
| `catalogs` | list of path \| { path, id, label } | `[]` |  | Catalog files elsewhere, listed on the home screen after catalog.toml and the config directory's catalogs/*.toml, each a section; see Catalogs. Each is a path, or { path, id, label } to give it another id or label. Paths may be relative to this file. Adds up across imports. |

## Read

`[read]` How files are read. A file's own layout (delimiter, header, rows to skip) is a flag for that file, not a setting.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `read.infer_types` | bool \| list of columns | `true` | `--infer-types` | Read string columns as dates, times, durations or numbers where every value parses, after trimming: true for all, false for none, or a list of columns. CSV, and dates in JSON. A column with a leading zero (02134) stays text; a later value that does not parse is null, and the Notes tab counts them. |
| `read.parquet_schema` | union \| first | `"union"` |  | A partitioned Parquet dataset's schema: union is every column any file has, from their footers; first lets Polars take one file's. |
| `read.decompress_in_memory` | bool | `false` |  | Decompress a compressed CSV, TSV or PSV into memory instead of to a temp file. |
| `read.temp_dir` | path | unset | `--temp-dir` | Directory for decompression temp files. Unset: the system's. |
| `read.follow_interval` | duration | `"250ms"` |  | With --follow, how often the file is checked for new rows, or on Linux the least time between two reads, 10ms to 1m. Appends within one interval are one refresh. |
| `read.exact_count_files` | integer | `50000` |  | A dataset of more files than this shows a row count estimated from a sample of its footers until c in the Info panel counts it; 0 always counts. |
| `read.memory_warning` | size | `"1GiB"` |  | Ask before reading more than this of a file whole into memory (JSON, Avro, ORC, Excel and the other formats read in memory); 0 never asks. |
| `read.audio_float` | bool | `false` |  | Show integer audio samples as float in [-1, 1]. |

## CSV

`[csv]` CSV, TSV and PSV. A [delimited format spec](../formats/format-specs.md#delimited-text) takes these keys too.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `csv.comment` | string | unset | `--comment` | Lines starting with this are comments, before the header and among the data. |
| `csv.header_join` | string | `" "` |  | Joins a column's names when --header-rows names several lines. |
| `csv.skip_initial_space` | bool | `false` | `--skip-initial-space` | Ignore the spaces after a delimiter, so padded numbers are numbers and a cell of spaces is null. |
| `csv.null_values` | list | `[]` | `--null` | Values read as null: VAL in every column, COL=VAL in column COL only. --null is repeatable and replaces this list. |
| `csv.infer_rows` | integer | `1000` | `--infer-rows` | Rows read to infer column types. |
| `csv.ignore_errors` | bool | `false` | `--ignore-errors` | Skip rows that do not parse instead of failing. |

## Display

`[display]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `display.unicode` | auto \| always \| never | `"auto"` |  | Box-drawing and arrow glyphs, or plain ASCII. auto uses them when the locale is UTF-8. |
| `display.row_numbers` | "auto" \| bool | `"auto"` | `--row-numbers` | Number rows on the left by their place in the source, kept through a sort or filter (# toggles). auto: for text and logs; true or false: for all of them. |
| `display.row_numbers_start` | integer | `1` |  | The number of the source's first row. |
| `display.cell_padding` | "comfortable" \| "compact" \| integer | `"comfortable"` |  | Space between columns: comfortable (2 cells), compact (1) or a number of cells. |
| `display.column_colors` | bool | `true` |  | Color cells by column type. |
| `display.type_row` | bool | `true` |  | A second header row naming each column's type (D toggles). |
| `display.notes_accent` | bool | `true` |  | Accent the i key when datui has noticed something about the data. |
| `display.mouse` | bool | `true` | `--mouse` | Take the mouse: the wheel scrolls, a click selects. false leaves it to the terminal. |
| `display.sidebar_width` | integer | unset |  | Width of every sidebar, in cells. Unset: each sidebar's own. |
| `display.right_align_numbers` | bool | `true` |  | Right-align numeric columns and their headers. |
| `display.number_format` | preset \| table | `"none"` | `--number-format` | Digit grouping: none, thousands, european, si, swiss, indian, underscore or system, or a [display.number_format] table (, toggles). |

## Performance

`[performance]` The rows the table buffers between reads, and the engine.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `performance.pages_ahead` | integer | `3` |  | Pages of rows buffered ahead of the screen. |
| `performance.pages_behind` | integer | `3` |  | Pages of rows buffered behind the screen. |
| `performance.max_buffered_rows` | integer | `100000` |  | Most rows the table buffers between reads; 0 for no limit. |
| `performance.max_buffered` | size | `"512MiB"` |  | Most memory the buffered rows may take, estimated from the schema; 0 for no limit. Rounded up to whole MiB. |
| `performance.streaming` | bool | `true` |  | Use the Polars streaming engine where it applies. |

## Analysis

`[analysis]` Analysis, Data Quality and charts.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `analysis.sample_rows` | integer | `100000` | `--sample-rows` | Rows an analysis samples from a larger table, spread across all of it; 0 reads every row. |
| `analysis.chart_rows` | integer | `10000` |  | Rows a chart reads; a larger table is sampled across all of it. |
| `analysis.chart_grid` | bool | `false` |  | Start charts with a grid at the major ticks (g toggles). |
| `analysis.quality_local_copy` | size | `"2GiB"` |  | Most a Data Quality full scan of a remote dataset copies into the cache to read once; 0 never copies. |

## Home

`[home]` The home screen.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `home.desktop_recents` | bool | `true` |  | Also list directories from the desktop's recently-used files; never the file names. |
| `home.show_unreadable` | bool | `false` |  | List files datui cannot read, dimmed (Ctrl+A toggles). |
| `home.hide` | list | `[]` |  | Catalogs not shown, by id: mine (catalog.toml), examples, or a listed file's name; one entry as catalog/id, such as examples/nyc-taxis. Adds up across imports. |
| `home.preview_max` | size | `"64MiB"` |  | Largest local file whose first rows the home screen previews; 0 turns the preview off. |

## Home search

`[home.search]` Searching below the working directory as you type on the home screen.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `home.search.enabled` | bool | `true` |  | Search below the working directory as you type. |
| `home.search.max_depth` | integer | `8` |  | How many directories deep the search goes. |
| `home.search.max_results` | integer | `1000` |  | Matches listed; the rest are counted. |
| `home.search.time_budget` | duration | `"1500ms"` |  | How long the search walks before keeping what it found. |
| `home.search.cross_filesystems` | bool | `false` |  | Descend into other filesystems, network mounts included. |
| `home.search.follow_gitignore` | bool | `false` |  | Skip what .gitignore ignores. |
| `home.search.skip` | list | `["node_modules", "target", "build", "dist", "vendor", "site-packages", "__pycache__", "venv", "env"]` |  | Directory names never searched. Replaces the defaults; skip_extra adds to them. |
| `home.search.skip_extra` | list | `[]` |  | Directory names never searched, besides skip. |
| `home.search.extensions` | list | `[]` |  | Extensions searched for; empty means those of the formats datui reads. |

## Cloud

`[cloud]` See [Cloud sources](cloud-sources.md) for `[[cloud.connections]]`.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `cloud.connections` | tables | unset |  | Cloud stores to list on the home screen; see Cloud sources. |
| `cloud.hide` | list | `[]` |  | Cloud source IDs not shown on the home screen. Adds up across imports. |
| `cloud.use_azure_account_keys` | bool | `true` |  | Read an Azure account with its access keys when a sign-in has no data role, as the Portal does. |
| `cloud.env_files` | list | `[]` |  | Files to read cloud variables from, relative to the working directory, such as .env. Adds up across imports. |
| `cloud.instance_identity` | bool | `false` |  | Use the identity of the cloud VM datui runs on (EC2, GCE, Azure). |
| `cloud.discover` | bool \| "all" \| "none" \| list | unset |  | Logins found on this machine that become home-screen sources: all (unset), none, or kinds from s3, gcs, azure. |
| `cloud.list_on_start` | bool | `false` |  | List every source's buckets when the home screen opens, not when one is entered. |

## HTTP

`[http]` Every request datui makes: HTTP(S) files, cloud stores and their sign-ins.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `http.user_agent` | string | `""` |  | The User-Agent header on every request. Empty sends datui/VERSION (+https://github.com/derekwisong/datui), which names datui and its version and nothing about you. |

## Query

`[query]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `query.history_limit` | integer | `1000` |  | Queries remembered. |
| `query.history` | bool | `true` |  | Remember queries. |
| `query.default_mode` | sql \| q | `"sql"` |  | The language : starts in, until Ctrl+T picks another. |

## Views

`[views]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `views.auto_apply` | bool | `false` |  | Apply the best-matching view when a file opens. |

## Clipboard

`[clipboard]` How the copy dialog (`y`) reaches the system clipboard.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `clipboard.backend` | auto \| native \| osc52 | `"auto"` |  | auto: the display server where one answers, osc52 elsewhere (SSH). osc52 is an escape sequence the terminal applies. |
| `clipboard.osc52_limit` | size | `"100KiB"` |  | Longest osc52 copy to attempt, as base64. Terminals cap what they accept. |

## Formats

`[formats]` Where [format specs](../formats/format-specs.md) and dictionaries are found.

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `formats.path` | list | `[]` |  | Directories of format specs and dictionaries, searched after ~/.config/datui/formats and $DATUI_FORMATS_PATH. Adds up across imports. |

## Log

`[log]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `log.file` | path | unset | `--log-file` | Where the log goes. Unset: datui.log in the cache directory. |
| `log.level` | error \| warn \| info \| debug \| trace \| off | unset | `--log-level` | How much the log says (default warn). DATUI_LOG beats a config file's; -c and --log-level beat DATUI_LOG. |

## Theme

`[theme]`

| Key | Type | Default | Flag | Description |
|---|---|---|---|---|
| `theme.mode` | auto \| dark \| light | unset |  | Which mode's theme to use: theme.dark or theme.light. auto follows the terminal's answer about its background, else its last answer, then COLORFGBG, then dark; it asks again when the terminal regains focus. |
| `theme.dark` | string | `"night-market"` |  | The theme used when the terminal is dark: night-market, day-market, or a file's name in the config directory's themes/. A name that cannot be used falls back to night-market, with a warning when dark is in use. |
| `theme.light` | string | `"day-market"` |  | The theme used when the terminal is light: night-market, day-market, or a file's name in the config directory's themes/. A name that cannot be used falls back to day-market, with a warning when light is in use. |

## Colors

`[theme.colors]` Each slot takes a name (`red`, `bright_blue`, `default`), `#rrggbb` or `indexed(0-255)`. They lie over the theme in use, `theme.dark` or `theme.light`, in either mode; a whole theme of your own goes in a file in `themes/`.

| Key | Dark | Light | Description |
|---|---|---|---|
| `theme.colors.chip_key` | `#7dcfff` | `#2e7de9` | Keys named in the footer, dialogs, the breadcrumb and the correlation matrix. |
| `theme.colors.chip_label` | `#a9b1d6` | `#3760bf` | Labels beside keys in the footer, and the footer's status. |
| `theme.colors.throbber` | `#7dcfff` | `#2e7de9` | The busy spinner. |
| `theme.colors.success` | `#9ece6a` | `#587539` | Success. |
| `theme.colors.error` | `#f7768e` | `#f52a65` | Errors. |
| `theme.colors.warning` | `#e0af68` | `#8c6c3e` | Warnings. |
| `theme.colors.dimmed` | `#565f89` | `#848cb5` | Dimmed text, nulls and axes. |
| `theme.colors.background` | `default` | `default` | Main background. |
| `theme.colors.surface` | `default` | `default` | Dialog background. |
| `theme.colors.controls_bg` | `#262a3f` | `#d0d5e3` | Count chips and dialogs' key chips. |
| `theme.colors.text_primary` | `default` | `default` | Text. |
| `theme.colors.text_secondary` | `#737aa2` | `#6172b0` | Secondary text. |
| `theme.colors.text_inverse` | `#1a1b26` | `#e1e2e7` | Text on a key chip. |
| `theme.colors.table_header` | `#c0caf5` | `#3760bf` | Header text. |
| `theme.colors.table_header_bg` | `#2b3047` | `#c4c8da` | Header fill. |
| `theme.colors.table_row_numbers` | `#565f89` | `#848cb5` | The row-number column. |
| `theme.colors.table_column_separator` | `#3b4261` | `#a8aecb` | The rule after frozen columns and beside section titles. |
| `theme.colors.table_selected` | `#283457` | `#b6bfe2` | Tint under the current row; reversed swaps text and background instead. |
| `theme.colors.table_column_cursor` | `#292e42` | `#cbd3f2` | Tint under the column cursor's cells. |
| `theme.colors.table_cell_cursor` | `#3b4261` | `#a0aef0` | The column cursor's header and the current cell. |
| `theme.colors.sidebar_border` | `#565f89` | `#6172b0` | Sidebar and dialog borders. |
| `theme.colors.modal_border_active` | `#7dcfff` | `#2e7de9` | The focused dialog's border. |
| `theme.colors.modal_border_error` | `#f7768e` | `#f52a65` | An error dialog's border. |
| `theme.colors.distribution_normal` | `#9ece6a` | `#587539` | Analysis: a normal distribution. |
| `theme.colors.distribution_skewed` | `#e0af68` | `#8c6c3e` | Analysis: a skewed distribution. |
| `theme.colors.distribution_other` | `#c0caf5` | `#3760bf` | Analysis: other distributions. |
| `theme.colors.outlier_marker` | `#f7768e` | `#f52a65` | Analysis: outliers. |
| `theme.colors.input_cursor` | `default` | `default` | The text caret; default reverses the text under it. |
| `theme.colors.input_cursor_text` | `default` | `default` | Text under the caret block; default picks black or white by contrast. |
| `theme.colors.table_alternate_row` | `#1e2030` | `#dcdfea` | Every other row; default turns the stripe off. |
| `theme.colors.type_str` | `#9ece6a` | `#587539` | String columns. |
| `theme.colors.type_int` | `#7aa2f7` | `#2e7de9` | Integer columns. |
| `theme.colors.type_float` | `#2ac3de` | `#007197` | Float columns. |
| `theme.colors.type_bool` | `#e0af68` | `#8c6c3e` | Boolean columns. |
| `theme.colors.type_temporal` | `#bb9af7` | `#9854f1` | Date, time and datetime columns. |
| `theme.colors.type_binary` | `#565f89` | `#848cb5` | Binary columns' placeholder. |
| `theme.colors.chart_1` | `#7dcfff` | `#2e7de9` | Chart series 1; also histogram bars, bar charts and Q-Q points. |
| `theme.colors.chart_2` | `#bb9af7` | `#9854f1` | Chart series 2. |
| `theme.colors.chart_3` | `#9ece6a` | `#587539` | Chart series 3. |
| `theme.colors.chart_4` | `#e0af68` | `#8c6c3e` | Chart series 4. |
| `theme.colors.chart_5` | `#7aa2f7` | `#007197` | Chart series 5. |
| `theme.colors.chart_6` | `#f7768e` | `#f52a65` | Chart series 6. |
| `theme.colors.chart_7` | `#ff9e64` | `#b15c00` | Chart series 7. |
| `theme.colors.chart_8` | `#1abc9c` | `#118c74` | Chart series 8. |
| `theme.colors.chart_9` | `#ff5fd2` | `#d1188c` | Chart series 9. |
| `theme.colors.chart_10` | `#f4ef8a` | `#24357a` | Chart series 10. |
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

The environment variables datui reads are in
[Environment variables](environment.md).
