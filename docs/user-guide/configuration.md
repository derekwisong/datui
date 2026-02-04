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
2. **Config file** (`~/.config/datui/config.toml`)
3. **Command-line arguments** (highest priority)

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
```

- **delimiter** — ASCII value of the CSV column separator (e.g. 44 for comma). Omit or set to `null` to use auto-detection.
- **has_header** — Whether the first row is a header. Omit or `null` for auto-detect; `true` or `false` to force.
- **skip_lines** / **skip_rows** — Number of lines (or rows) to skip before reading the header and data.
- **parse_dates** — When `true` (default), the CSV reader attempts to parse string columns that look like dates (e.g. `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`) into Polars Date or Datetime. Set to `false` to keep such columns as strings. Overridden by the `--parse-dates` CLI flag (e.g. `--parse-dates false` to disable; see [command-line options](../reference/command-line-options.md)).
- **decompress_in_memory** — When `false` (default), compressed CSV is decompressed to a temporary file so the lazy CSV reader can scan it. When `true`, compressed data is decompressed into memory (eager load). Use `true` if you have no temp filesystem. Overridden by `--decompress-in-memory` / `--decompress-in-memory false`.
- **temp_dir** — Directory used for temporary files when decompressing compressed CSV. Omit or set to `null` to use the system default (e.g. `/tmp` on Unix). Overridden by the `--temp-dir` CLI option.

### Display Settings

Control how data is displayed:

```toml
[display]
pages_lookahead = 3   # Pages to buffer ahead (smoother scrolling)
pages_lookback = 3    # Pages to buffer behind
row_numbers = false   # Show row numbers on left side
row_start_index = 1   # Starting index for row numbers (0 or 1)
table_cell_padding = 1   # Spaces between columns in the main table (>= 0)
```

**Example: Enable row numbers starting at 0**
```toml
[display]
row_numbers = true
row_start_index = 0
```

### Performance Settings

Tune performance and responsiveness:

```toml
[performance]
sampling_threshold = 10000   # Sample datasets >= this size
event_poll_interval_ms = 25  # UI polling interval (lower = more responsive)
```

**Memory vs Speed:**
- **Increase `sampling_threshold`** to avoid sampling (uses more memory, full accuracy)
- **Decrease `sampling_threshold`** for faster analysis on large datasets (uses less memory)

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

Complete Dracula color scheme using hex colors:

```toml
version = "0.2"

[theme.colors]
keybind_hints = "#bd93f9"              # Purple
keybind_labels = "#ff79c6"             # Pink
primary_chart_series_color = "#bd93f9" # Purple
secondary_chart_series_color = "#6272a4" # Comment gray
success = "#50fa7b"                    # Green
error = "#ff5555"                      # Red
warning = "#ffb86c"                    # Orange
dimmed = "#6272a4"                     # Comment gray

background = "#282a36"                 # Background (Dracula dark)
surface = "#44475a"                    # Current line
controls_bg = "#44475a"                # Controls bar

text_primary = "#f8f8f2"               # Foreground
text_secondary = "#6272a4"             # Comment
text_inverse = "#282a36"               # Background (for inverse)

table_header = "#f8f8f2"               # Foreground
table_header_bg = "#44475a"            # Current line
column_separator = "#bd93f9"            # Purple
table_selected = "reversed"

sidebar_border = "#6272a4"             # Comment gray
modal_border_active = "#ff79c6"        # Pink
modal_border_error = "#ff5555"         # Red

distribution_normal = "#50fa7b"         # Green
distribution_skewed = "#ffb86c"         # Orange
distribution_other = "#f8f8f2"          # Foreground
outlier_marker = "#ff5555"              # Red
```

### Performance Tuned

Optimize for large datasets:

```toml
version = "0.2"

[display]
pages_lookahead = 5   # More buffering for smoother scrolling
pages_lookback = 5

[performance]
sampling_threshold = 50000  # Sample only very large datasets
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
4. **Check validation**: Ensure values are in valid ranges (e.g., `sampling_threshold > 0`)

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
