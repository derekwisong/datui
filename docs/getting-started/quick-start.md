# Quick Start

Datui is used to visualize tabular data in the terminal.

> See the [Installation Manual](installation.md) for help installing Datui.

## Opening a File

To open a file, simply provide it as an argument.

```bash
datui /path/to/file.csv
```

Datui will load your data into a full screen terminal display.

> See the [Loading Data](../user-guide/loading-data.md) section for details about supported
> file formats and options.

## Navigating

You may scroll through your data using the arrow keys, or familiar `vim` keybindings (`j`/`k` for
up/down, `h`/`l` for left/right).

You may also jump pages with the `Page Up` and `Page Down` keys.

## Getting Help

See command line arguments:
```bash
datui --help
```

Activate the built-in help display at any time by pressing `?` or `F1` (`F1` works
in text fields too, e.g. query input). Press `Esc` or `?` to close it.

## Understanding the UI

- The main Datui view window shows your data in full screen, with a header row at the top and a
  toolbar across the bottom of the screen.
- The toolbar at the bottom contains a quick reference for the keystrokes to activate the various
  features of Datui.
- Certain features will open pop-over menus, or change the full screen view. Press `Esc` to go
  back to the prior page.

## More Examples

Open a hive-partitioned directory:
```bash
datui --hive /path/to/directory
```

> **Note:** The directory should contain files all of the same type

Or, a glob pattern to hive-partitioned parquet:
```bash
datui --hive "/path/to/directory/**/*.parquet"
```

> **Note:** It is usually necessary to quote the glob pattern to prevent shell expansion.

## Learning More

Now that you understand the basics, learn about the rest of Datui's features by reading the
[User Guide](../user-guide.md).
