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

The second command makes Describe, Distribution and Correlation read every row.
It does not change Data Quality's separate sample budget.

Imports have a limitation: values equal to built-in defaults may be treated
as unset. See [import precedence](../reference/settings.md#importing-other-config-files)
before using one file to override another.

## Fix a config problem

Check the [troubleshooting entries](../reference/settings.md#troubleshooting)
for ignored settings, rejected colors or unreadable terminal colors.
To replace the file with generated defaults, back up your edits first, then run:

```bash
datui --generate-config --force
```
