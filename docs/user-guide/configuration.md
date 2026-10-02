# Configure datui

```bash
datui --generate-config
```

Edit the generated TOML file, then restart datui:

| OS | Config file |
|---|---|
| Linux | `~/.config/datui/config.toml` |
| macOS | `~/Library/Application Support/datui/config.toml` |
| Windows | `%APPDATA%\datui\config.toml` |

## Set common defaults

```toml
[display]
row_numbers = true
number_format = "thousands"

[data]
directories = ["~/datasets"]

[theme]
mode = "light"
```

This shows row numbers, groups digits, adds a directory to the home screen,
and selects the light palette. Remove any setting you do not want.
Most generated settings are commented examples; remove the leading `#` to
activate one. The generated public-dataset collection is already active.

## Find a setting

| Change | Reference |
|---|---|
| CSV types, nulls and decompression | [File loading](../reference/settings.md#file-loading) |
| Number formatting, columns and row buffers | [Display](../reference/settings.md#display) |
| Analysis sample size or chart row limit | [Performance](../reference/settings.md#performance) · [Charts](../reference/settings.md#charts) |
| Home directories and search | [Data](../reference/settings.md#data) |
| Named datasets, local or remote, and the public catalog | [Dataset collections](../reference/sources.md) |
| Cloud accounts and connections | [Cloud sources](../reference/cloud-sources.md) |
| Clipboard over SSH | [Clipboard](../reference/settings.md#clipboard) |
| Colors and symbols | [Theme](../reference/settings.md#theme) · [Glyphs](../reference/settings.md#glyphs) |
| Desktop theme integration | [System theming](system-theming.md) |

## Override a setting

Settings apply in this order: built-in defaults, imported files, your config,
then command-line flags. Cloud environment variables override the config but
not explicit command-line flags.

```bash
datui data.csv --row-numbers
datui data.csv --sample-rows 0
```

The second command makes every analysis tool, Data Quality included, read
every row.

A setting you write wins over an import, even when it equals the built-in
default; one you leave out keeps the imported value. See
[import precedence](../reference/settings.md#importing-other-config-files).

## Mouse and text selection

datui takes the mouse by default: the wheel scrolls and a click selects (see
[Mouse](../reference/keyboard-shortcuts.md#mouse)). To select text with the
terminal while it does, hold the terminal's bypass modifier as you drag:

| Terminal | Select text |
|---|---|
| Most (GNOME Terminal, Konsole, kitty, Alacritty, WezTerm, Windows Terminal, xterm) | <kbd>Shift</kbd>+drag |
| iTerm2 | <kbd>Option</kbd>+drag |
| tmux with `set -g mouse on` | <kbd>Shift</kbd>+drag, or tmux's own copy mode |

To leave the mouse to the terminal:

```toml
[display]
mouse = false
```

or run `datui --mouse=false`.

## Fix a config problem

Check the [troubleshooting entries](../reference/settings.md#troubleshooting)
for ignored settings, rejected colors or unreadable terminal colors.
To replace the file with generated defaults, back up your edits first, then run:

```bash
datui --generate-config --force
```
