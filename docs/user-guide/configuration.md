# Configuration

Datui supports extensive customization through TOML configuration files. You can customize colors, defaults, performance settings, and more.

## Quick Start

Generate a default configuration file:

```bash
datui --generate-config
```

This creates `~/.config/datui/config.toml` with all available options and helpful comments.

## Configuration File Location

- **Linux**: `~/.config/datui/config.toml`
- **macOS**: `~/.config/datui/config.toml`
- **Windows**: `%APPDATA%\datui\config.toml`

## Configuration Priority

Settings are applied in this order (later values override earlier ones):

1. **Default values** (hardcoded)
2. **Imported files** (each entry of `import`, in the order listed — see below)
3. **Config file** (`~/.config/datui/config.toml`)
4. **Command-line arguments** (highest priority)

## Importing Other Config Files

The top-level `import` key names other TOML files to merge in *before* your own
settings. It is the seam that lets an external theme system — Omarchy, chezmoi,
home-manager, a dotfiles repo — restyle datui without datui knowing anything
about that system.

```toml
# ~/.config/datui/config.toml
import = ["~/.local/state/omarchy/current/theme/datui.toml"]

[display]
row_numbers = true
```

Rules:

- **Order is precedence.** Imports apply in the order listed, each overriding the
  last. Your own file's values are applied last, so they always win over an
  imported theme.
- **Imports may nest.** An imported file can declare its own `import`; those are
  merged before the file that pulled them in. Chains are capped at 8 files, and a
  cycle is reported as an error rather than followed.
- **Paths** may be absolute, relative to the importing file's directory, or use
  `~` and `$VAR` / `${VAR}`.
- **A missing file is skipped** with a warning on stderr, and datui starts
  normally on the remaining layers. That is deliberate: a generated theme file
  may not exist yet (or at all, on another machine), and that must not stop datui
  from running.
- **A file that exists but is broken is an error.** If an import cannot be read
  or parsed, datui reports the file and exits, rather than quietly looking like
  the theme failed to apply.

### Following your system theme

> For a full walkthrough — including the Omarchy template and per-theme overrides —
> see [Theming from Your System](system-theming.md).

datui does not detect any particular desktop or theme manager. Point `import` at
whatever file your system generates, exactly as Alacritty's `general.import` and
btop's `color_theme` do. On [Omarchy](https://omarchy.org/), a template rendered
to `~/.local/state/omarchy/current/theme/datui.toml` is picked up by the import
line shown above, and `omarchy theme set <name>` restyles datui along with
everything else.

To stop following a system theme, delete the `import` line. datui has no opinion
about it either way — with no `import`, you get datui's stock theme.

### A caveat when overriding an imported color

datui decides whether you set a color by comparing it against the built-in
default. A color you set *to the same value as datui's default* is
indistinguishable from one you never set, so it will not override an imported
theme.

For example, datui's default `error` is `red`. Given an import that sets `error`
to `#FF5345`, writing `error = "red"` in your own config has no effect — the
imported value wins. Use an explicit form the default does not already use (for
instance `#ff0000`, or `indexed(9)`) when you need to override an imported color
back to something datui also uses by default.

This only affects colors you want to pin to one of datui's own default values;
every other override behaves as expected.

## Configuration Sections

### File Loading Defaults

Customize how data files are loaded:

```toml
[file_loading]
delimiter = 44        # CSV delimiter (44 = comma). null = auto-detect
has_header = true     # Whether files have headers. null = auto-detect
skip_lines = 0        # Lines to skip at file start
skip_rows = 0         # Rows to skip when reading
parse_dates = true    # When true (default), CSV reader tries to parse string columns as dates (e.g. YYYY-MM-DD, ISO datetime)
decompress_in_memory = false  # When true, decompress compressed CSV into memory; when false (default), decompress to a temp file so scan can be used
temp_dir = null       # Directory for temp files when decompressing compressed CSV. null = system default (e.g. /tmp)
infer_schema_length = 1000   # Rows to use when inferring CSV column types. Default 1000; higher reduces risk of wrong type (e.g. int then N/A)
ignore_errors = false # When true, CSV reader skips rows that fail to parse instead of failing the load
```

- **delimiter** — ASCII value of the CSV column separator (e.g. 44 for comma). Omit or set to `null` to use auto-detection.
- **has_header** — Whether the first row is a header. Omit or `null` for auto-detect; `true` or `false` to force.
- **skip_lines** / **skip_rows** — Number of lines (or rows) to skip before reading the header and data.
- **parse_dates** — When `true` (default), the CSV reader attempts to parse string columns that look like dates (e.g. `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`) into Polars Date or Datetime. Set to `false` to keep such columns as strings. Overridden by the `--parse-dates` CLI flag (e.g. `--parse-dates false` to disable; see [command-line options](../reference/command-line-options.md)).
- **decompress_in_memory** — When `false` (default), compressed CSV is decompressed to a temporary file so the lazy CSV reader can scan it. When `true`, compressed data is decompressed into memory (eager load). Use `true` if you have no temp filesystem. Overridden by `--decompress-in-memory` / `--decompress-in-memory false`.
- **temp_dir** — Directory used for temporary files when decompressing compressed CSV. Omit or set to `null` to use the system default (e.g. `/tmp` on Unix). Overridden by the `--temp-dir` CLI option.
- **infer_schema_length** — Number of rows used to infer CSV column types (default 1000). If a column looks like integers in the first N rows but later has a non-numeric value (e.g. `N/A`), increasing this or adding that value to **null_values** avoids parse errors. Overridden by `--infer-schema-length`.
- **ignore_errors** — When `false` (default), a CSV parse error (e.g. wrong type) fails the load. When `true`, the reader skips rows that fail to parse. Overridden by `--ignore-errors`.

### Display Settings

Control how data is displayed:

```toml
[display]
pages_lookahead = 3   # Pages to buffer ahead (smoother scrolling)
pages_lookback = 3    # Pages to buffer behind
row_numbers = false   # Show row numbers on left side
row_start_index = 1   # Starting index for row numbers (0 or 1)
table_cell_padding = 1   # Spaces between columns in the main table (>= 0)
align_numeric_right = true   # Right-align numeric columns and their headers
number_format = "none"       # Digit grouping — see below
```

**Example: Enable row numbers starting at 0**
```toml
[display]
row_numbers = true
row_start_index = 0
```

#### Number formatting

Large integers — genomic coordinates, row counts, IDs — are hard to read as an
unbroken run of digits. `number_format` adds digit grouping:

```toml
[display]
number_format = "thousands"   # 1,234,567
```

Press <kbd>F</kbd> in the data table to toggle formatting on and off at any
time. The toggle is session-only; the config file decides the state at launch.
With the default `"none"`, <kbd>F</kbd> turns on comma grouping.

| Preset | Renders `1234567.89` as |
|--------|-------------------------|
| `none` (default) | `1234567.89` |
| `thousands` | `1,234,567.89` |
| `european` | `1.234.567,89` |
| `si` | `1 234 567.89` (narrow no-break space, ISO 31-0) |
| `swiss` | `1'234'567.89` |
| `indian` | `12,34,567.89` (lakh / crore) |
| `underscore` | `1_234_567.89` |

For finer control, replace the shorthand with a table:

```toml
[display.number_format]
grouping = "thousands"     # none | thousands | indian | system | any preset above
group_separator = ","
decimal_separator = "."
floats = true              # group float columns too
float_precision = 2        # omit to keep the file's own decimal rendering
exclude_columns = ["*_id", "year"]   # never format these (globs: * and ?)
```

**Every value in a formatted column is grouped**, with no size threshold — a
column never mixes `1000` and `248,956,422`. Uniform treatment of a column reads
better in a table than the prose convention of leaving four-digit numbers alone.

**`exclude_columns`** is how you keep a column plain. Use it for columns that
are numeric but are not quantities — years, sample IDs, ZIP codes, accession
numbers:

```toml
[display.number_format]
grouping = "thousands"
exclude_columns = ["year", "*_id", "zip"]
```

##### Why formatting is not taken from your locale

By default datui never reads `LC_NUMERIC` or `LANG` to decide how to render
numbers. A data file has no locale, so the same file should look the same on
your laptop and over SSH on a cluster — and `LC_NUMERIC` is unset or `C` on much
of the infrastructure this feature is aimed at, so detection would silently do
nothing exactly where it was wanted. Proper locale formatting also needs
ICU/CLDR data, which is megabytes for a tool that ships as a single binary.

The presets above cover the same conventions explicitly. If you do want the
environment consulted, opt in:

```toml
[display.number_format]
grouping = "system"   # reads LC_ALL / LC_NUMERIC / LANG, falls back to "thousands"
```

##### What formatting does and does not affect

Formatting is **display-only**. Exported files, query and filter expressions,
templates, and group-by keys always use raw values — what you see grouped on
screen is written out ungrouped.

The control bar's row count and the info panel's totals are datui's own labels
rather than your data, so they always group and ignore both this setting and
the <kbd>F</kbd> toggle.

#### Numeric alignment

`align_numeric_right` (default `true`) renders integer and float columns, and
their headers, flush right so magnitudes line up. Strings, booleans and
temporal columns stay left-aligned. Set it to `false` for the pre-0.2.56
appearance:

```toml
[display]
align_numeric_right = false
```

Unlike grouping, alignment is on by default: it changes neither the characters
of a value nor a column's width, so nothing reflows and copied text is
identical.

### Performance Settings

Tune performance and responsiveness:

```toml
[performance]
# sampling_threshold = 10000   # Optional: when set, sample datasets >= this size for analysis
event_poll_interval_ms = 25  # UI polling interval (lower = more responsive)
```

- **event_poll_interval_ms** — UI event polling interval in milliseconds. Lower values feel more responsive but use more CPU.

#### sampling_threshold (optional)

Controls whether [Analysis Mode](../user-guide/analysis-features.md) uses a sample of the data for large datasets. **Default: sampling is off** (full dataset is used).

| Config / CLI | Behavior |
|--------------|----------|
| Omit `sampling_threshold` in config (default) | Full dataset is used. No "Resample" keybind or "(sampled)" label. |
| `sampling_threshold = N` in config | For datasets with ≥ N rows, analysis runs on a sample (faster, less memory). **r** resamples; tool shows "(sampled)". |
| `--sampling-threshold N` on the command line | Overrides config for that run. Use a positive N to enable sampling, or `0` to force full-dataset analysis. |

Example: to sample only when a table has at least 50,000 rows, set `sampling_threshold = 50000` under `[performance]`, or run `datui --sampling-threshold 50000 …`. See [command-line options](../reference/command-line-options.md) for the CLI flag.

### Chart View

Default limit for how many rows are used when building chart data (display and export). You can also change this in chart view with the **Limit Rows** option.

```toml
[chart]
row_limit = 10000  # Max rows for chart data (1 to 10_000_000). Default 10000
```

### Theme Mode (light and dark terminals)

Some of datui's colors — header fills, alternating row stripes, borders, dim text —
need to sit *near* the terminal background without matching it. No ANSI color means
"slightly off from the background", so those slots resolve to fixed shades, and a
set tuned for a dark terminal is unreadable on a light one.

```toml
[theme]
mode = "auto"   # "auto" (default), "dark", or "light"
```

- **auto** — reads the `COLORFGBG` environment variable, falling back to `dark`.
- **dark** / **light** — pick a set explicitly.

Alacritty, Kitty and Ghostty do not set `COLORFGBG`. **If you use a light terminal
color scheme in one of those, set `mode = "light"`** — otherwise the header bar and
row striping will render as near-black blocks on your light background.

`mode` only chooses the starting point; any color you set under `[theme.colors]`
overrides it. An imported theme can also declare `mode`, which is how a generated
light theme gets light chrome automatically — see
[Theming from Your System](system-theming.md).

### Color Themes

Customize the entire UI appearance:

```toml
[theme.colors]
keybind_hints = "cyan"              # Keybind hints
keybind_labels = "yellow"           # Action labels
primary_chart_series_color = "cyan" # Chart data
secondary_chart_series_color = "dark_gray" # Chart theory
chart_series_color_1 = "cyan"       # Chart view: first series
chart_series_color_2 = "magenta"    # Chart view: second series
chart_series_color_3 = "green"      # Chart view: third series
chart_series_color_4 = "yellow"    # Chart view: fourth series
chart_series_color_5 = "blue"      # Chart view: fifth series
chart_series_color_6 = "red"       # Chart view: sixth series
chart_series_color_7 = "bright_cyan" # Chart view: seventh series
error = "red"                       # Error messages
success = "green"                   # Success indicators
warning = "yellow"                  # Warnings
dimmed = "dark_gray"                # Dimmed elements
alternate_row_color = "default"    # Background for every other row ("default" = off, or a color name)
```

#### Color Formats

Three color formats are supported:

**1. Named Colors**
```toml
keybind_hints = "cyan"
error = "bright_red"
dimmed = "dark_gray"
background = "default"  # Use terminal default background
text_primary = "default"  # Use terminal default text color
```

Available names:
- Basic: `black`, `red`, `green`, `yellow`, `blue`, `magenta`, `cyan`, `white`
- Bright: `bright_red`, `bright_green`, `bright_blue`, etc.
- Grays: `gray`, `dark_gray`, `light_gray`
- Special: `reset` or `default` (uses terminal default colors - works in both light and dark themes)

**2. Hex Colors**
```toml
background = "#1e1e1e"
sidebar_border = "#007acc"
controls_bg = "#2d2d2d"
```

- Format: `#rrggbb` (6 hex digits)
- Case-insensitive: `#FF0000` or `#ff0000`
- Automatically adapted to your terminal's capabilities

**3. Indexed Colors**
```toml
controls_bg = "indexed(236)"  # Example: specific palette entry
surface = "indexed(239)"
```

- Direct reference to xterm 256-color palette (0-255)
- Respects your terminal's color scheme
- Useful for matching specific terminal themes

#### Terminal Compatibility

Colors automatically adapt to your terminal:

- **True color terminals** (Alacritty, kitty, iTerm2): Hex colors display exactly
- **256-color terminals** (xterm-256color): Hex converted to nearest palette match
- **Basic terminals** (8/16 colors): Colors map to nearest ANSI color
- **Monochrome**: Set `NO_COLOR=1` to disable colors
- **Light Theme Support:**
  - The default values for `background` and `text_primary` are set to `"default"`
  - Datui will inherit your terminal's default colors
  - The application renders correctly in both light and dark terminal themes
  - Be aware that setting explicit colors like `"black"` or `"white"` may result in poor visibility in certain terminal themes

### Available Colors

All UI colors can be customized:

| Color | Purpose | Default |
|-------|---------|---------|
| `keybind_hints` | Keybind hints (modals, breadcrumb, correlation matrix) | cyan |
| `keybind_labels` | Action labels in controls bar | yellow |
| `throbber` | Busy indicator (spinner) in control bar | cyan |
| `primary_chart_series_color` | Chart data (histogram bars, Q-Q plot data points) | cyan |
| `secondary_chart_series_color` | Chart theory (histogram overlays, Q-Q plot reference line) | dark_gray |
| `chart_series_color_1` … `chart_series_color_7` | Chart view: series colors (line/scatter/bar) | cyan, magenta, green, yellow, blue, red, bright_cyan |
| `success` | Success indicators, normal distributions | green |
| `error` | Error messages, outliers | red |
| `warning` | Warnings, skewed distributions | yellow |
| `dimmed` | Dimmed elements, axis lines | dark_gray |
| `background` | Main background | default (uses terminal default) |
| `surface` | Modal/surface backgrounds | default (uses terminal default) |
| `controls_bg` | Controls bar and table header backgrounds | indexed(235) |
| `text_primary` | Primary text | default (uses terminal default) |
| `text_secondary` | Secondary text | dark_gray |
| `text_inverse` | Text on light backgrounds | black |
| `table_header` | Table column header text | white |
| `table_header_bg` | Table column header background | indexed(235) |
| `column_separator` | Vertical line between table columns | cyan |
| `table_selected` | Selected row style | reversed |
| `sidebar_border` | Sidebar borders | dark_gray |
| `modal_border_active` | Active modal elements | yellow |
| `modal_border_error` | Error modal borders | red |
| `distribution_normal` | Normal distribution indicator | green |
| `distribution_skewed` | Skewed distribution indicator | yellow |
| `distribution_other` | Other distribution types | white |
| `outlier_marker` | Outlier indicators | red |

### Query System

Configure query behavior:

```toml
[query]
history_limit = 1000      # Max queries to remember
enable_history = true     # Enable query history
```

### Template Settings

Configure template behavior:

```toml
[templates]
auto_apply = false  # Auto-apply most relevant template on file open
```

### Debug Settings

Configure debug overlay:

```toml
[debug]
enabled = false             # Show debug overlay by default
show_performance = true     # Show performance metrics
show_query = true           # Show LazyFrame query
show_transformations = true # Show transformation state
```

## Example Configurations

### Minimal Configuration

Simple customization for common preferences:

```toml
version = "0.2"

[display]
row_numbers = true
row_start_index = 0

[theme.colors]
keybind_hints = "blue"
```

### Dracula Theme

Complete Dracula color scheme using the [official palette](https://spec.draculatheme.com/) (hex colors):

```toml
version = "0.2"

[theme.colors]
# Keybinds and UI chrome
keybind_hints = "#bd93f9"              # Purple
keybind_labels = "#ff79c6"             # Pink
throbber = "#bd93f9"                   # Purple

# Chart colors
primary_chart_series_color = "#bd93f9" # Purple
secondary_chart_series_color = "#6272a4" # Comment
chart_series_color_1 = "#8be9fd"       # Cyan
chart_series_color_2 = "#ff79c6"       # Pink
chart_series_color_3 = "#50fa7b"       # Green
chart_series_color_4 = "#f1fa8c"       # Yellow
chart_series_color_5 = "#bd93f9"       # Purple
chart_series_color_6 = "#ff5555"       # Red
chart_series_color_7 = "#ffb86c"       # Orange

# Status
success = "#50fa7b"                    # Green
error = "#ff5555"                      # Red
warning = "#ffb86c"                    # Orange
dimmed = "#6272a4"                     # Comment

# Backgrounds
background = "#282a36"                 # Background
surface = "#44475a"                    # Selection / current line
controls_bg = "#44475a"                # Controls bar

# Text
text_primary = "#f8f8f2"               # Foreground
text_secondary = "#6272a4"             # Comment
text_inverse = "#282a36"               # Background (for inverse)

# Table
table_header = "#f8f8f2"               # Foreground
table_header_bg = "#44475a"            # Selection
row_numbers = "#6272a4"                # Comment
column_separator = "#bd93f9"           # Purple
table_selected = "reversed"
alternate_row_color = "default"        # No stripe (or use "#3d3f4a" for subtle stripe)

# Column type colors (when column_colors enabled)
str_col = "#50fa7b"                    # Green
int_col = "#8be9fd"                    # Cyan
float_col = "#bd93f9"                  # Purple
bool_col = "#f1fa8c"                  # Yellow
temporal_col = "#ff79c6"               # Pink
binary_col = "dark_gray"               # Binary column ‹binary› placeholder (always applied, shown italic)

# Borders and modals
sidebar_border = "#6272a4"             # Comment
modal_border_active = "#ff79c6"        # Pink
modal_border_error = "#ff5555"         # Red

# Cursor (query input, etc.)
cursor_focused = "#f8f8f2"             # Foreground
cursor_dimmed = "#6272a4"              # Comment

# Analysis / distributions
distribution_normal = "#50fa7b"        # Green
distribution_skewed = "#ffb86c"        # Orange
distribution_other = "#f8f8f2"         # Foreground
outlier_marker = "#ff5555"             # Red
```

### Performance Tuned

Optimize for large datasets:

```toml
version = "0.2"

[display]
pages_lookahead = 5   # More buffering for smoother scrolling
pages_lookback = 5

[performance]
sampling_threshold = 50000  # Optional: sample only datasets >= 50k rows (omit to use full data)
event_poll_interval_ms = 16 # ~60 FPS polling (more responsive)
```

### High Contrast Theme

Using named colors for maximum compatibility:

```toml
version = "0.2"

[theme.colors]
keybind_hints = "bright_cyan"
keybind_labels = "bright_yellow"
primary_chart_series_color = "bright_cyan"
secondary_chart_series_color = "dark_gray"
error = "bright_red"
success = "bright_green"
warning = "bright_yellow"
dimmed = "dark_gray"

background = "black"
controls_bg = "dark_gray"
text_primary = "bright_white"
```

## Command-Line Overrides

CLI arguments always override config file settings:

```bash
# Config has row_numbers = true, but disable for this run:
datui data.csv --row-numbers=false

# Override page buffering:
datui data.csv --pages-lookahead 10

# Override delimiter:
datui data.csv --delimiter=9  # Tab character (ASCII 9)
```

## Managing Configuration

### View Current Config

Your config file is at `~/.config/datui/config.toml`. Edit it with any text editor:

```bash
# Linux/macOS
nano ~/.config/datui/config.toml
vim ~/.config/datui/config.toml
code ~/.config/datui/config.toml

# Windows
notepad %APPDATA%\datui\config.toml
```

### Reset to Defaults

Regenerate the default config file:

```bash
datui --generate-config --force
```

This overwrites your existing config with a fresh template.

### Remove Configuration

Simply delete the config file:

```bash
# Linux/macOS
rm ~/.config/datui/config.toml

# Windows
del %APPDATA%\datui\config.toml
```

Datui will use default values when no config file exists.

## Troubleshooting

### Config Not Loading

If your config isn't being used:

1. **Check file location**: Ensure config is at `~/.config/datui/config.toml`
2. **Check syntax**: TOML must be valid. Run `datui <file>` and check for warnings
3. **Check version**: Config must start with `version = "0.2"`
4. **Check validation**: Ensure values are in valid ranges (e.g., if set, `sampling_threshold > 0`)
5. **Check imports**: If you use `import`, a missing file is reported on stderr —
   run `datui <file> 2>/tmp/datui.log` and read the log after quitting

### Imported Theme Not Applying

If an `import` does not seem to take effect:

1. **Confirm the file exists** at the resolved path. A missing import is skipped
   with a warning on stderr, which the TUI hides while it is running — redirect
   stderr to a file to see it.
2. **Confirm the value differs from datui's default.** A color set to the same
   string as datui's default cannot override an import — see
   [the caveat above](#a-caveat-when-overriding-an-imported-color).
3. **Confirm nothing later overrides it.** Your own config file and any
   command-line flags both win over an imported file.

### Invalid Color

If you see an error about invalid colors:

```
Error: Invalid color value for 'keybind_hints': Unknown color name: 'notacolor'
```

**Solutions:**
- Use valid color names (see list above)
- Use hex format: `#ff0000`
- Use indexed format: `indexed(236)`
- Check spelling and case (names are case-insensitive)

### Config Parse Error

If TOML parsing fails:

```
Error: Failed to parse config file: expected newline, found ...
```

**Solutions:**
- Check TOML syntax at https://toml.io/
- Ensure proper quotes around strings
- Verify no typos in section names
- Regenerate config: `datui --generate-config --force`

### Colors Look Wrong

If colors don't look right:

1. **Check terminal capabilities**: Some terminals don't support true color
2. **Try named colors**: More portable than hex colors
3. **Try indexed colors**: Match your terminal's palette exactly
4. **Check NO_COLOR**: Unset with `unset NO_COLOR` if colors are disabled

### Table Headers or Toolbar Text Cut Off or Deformed (VS Code, xterm-256)

On some terminals (e.g. VS Code integrated terminal, xterm-256color), **custom background colors** on headers/toolbar can cause text to render cut off or deformed. By default, `controls_bg` and `table_header_bg` use **`indexed(235)`**, which works well on most setups.

If you see deformed text, set them to **`"default"`** or **`"none"`** for no custom background:

```toml
[theme.colors]
controls_bg = "default"
table_header_bg = "default"
```


## See Also

- [Command-Line Options](../reference/command-line-options.md) - CLI flags that override config
- [Quick Start Guide](../getting-started/quick-start.md) - Getting started with datui
- [Keyboard Shortcuts](../reference/keyboard-shortcuts.md) - Available keybindings
