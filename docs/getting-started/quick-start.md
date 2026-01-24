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

## Understanding the UI

- The main Datui view window shows your data in full screen, with a header row at the top and a
  toolbar across the bottom of the screen.
- The toolbar at the bottom contains a quick reference for the keystrokes to activate the various
  features of Datui.
- Certain features will open pop-over menus, or change the full screen view. Press `Esc` to go
  back to the prior page.

## Getting Help

Datui has a built-in help display. Activate it at any time by pressing `Ctrl-h`. To close it,
press `Esc`.

## Learning More

Now that you understand the basics, learn about the rest of Datui's features by reading the
[User Guide](../user-guide.md).