# The Home Screen

Run `datui` with no arguments to open the home screen: a list of datasets, with
what each contains shown before you open it. <kbd>Ctrl</kbd>+<kbd>O</kbd> returns
here from anywhere.

```
 datui                                                         ~/work/analysis
› ▏  type to filter
▾ RECENT
▸ sales                          hive      2.4M × 18    340 MB    2d
  customers.parquet                          89k × 12     4 MB    1w
▾ /mnt/data                                        network · configured
  events                         hive       1.1M × 9    120 MB    3h
  lookup.parquet                              980 × 4     8 KB    2mo
▾ ~/work/analysis                                     current directory
  raw_export.csv                                        1.2 GB    3h
  notes/                         dir
↑↓ Move  ⏎ Open  type Filter  ←→ Fold  ~ Path  Esc Quit
```

## Keys

| key | action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> / <kbd>k</kbd> <kbd>j</kbd> | move |
| <kbd>←</kbd> <kbd>→</kbd> / <kbd>h</kbd> <kbd>l</kbd> | collapse / expand the section |
| <kbd>Enter</kbd> | open the dataset, enter the directory, or fold the section |
| type anything | filter by name (fuzzy: `sal` matches `sales`) |
| <kbd>~</kbd> | type a path directly |
| <kbd>Backspace</kbd> | delete a filter character, or leave a directory |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | clear the filter |
| <kbd>Esc</kbd> | back out one layer: clear filter, leave directory, return to your data, quit |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> | quit |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | return here from anywhere, including during a load |

<kbd>q</kbd> does not quit here — plain characters go into the filter. The control
bar shows what <kbd>Esc</kbd> will do next.

## Where the list comes from

The list is grouped by *root* — a directory datui looks in. Sections appear in this
order:

| # | section | source | shown when empty |
|---|---|---|---|
| 1 | `RECENT` | datasets you have opened, most recent first | no |
| 2 | a configured directory | `[data] directories`, in the order you list them | yes |
| 3 | directories of recent datasets | added when you open something | no |
| 4 | the current directory | where you launched datui | yes |
| 5 | `ELSEWHERE` | [desktop places](#desktop-places) | no |

A directory reached more than one way appears once, under the earliest of these
that names it. A root on a network filesystem is marked `network`; one that cannot
be read is marked `unavailable` rather than hidden.

Sections fold with <kbd>←</kbd> and <kbd>→</kbd>. A folded section shows how many
rows it is hiding, and stays folded until you expand it or restart datui.

Filtering keeps the grouping, so a match always shows which root it came from.

### Adding a root

Name it in your config. This is the only explicit way, and the only one that
keeps a place listed when it is empty or its mount is down (it shows as
`unavailable` rather than disappearing):

```toml
[data]
directories = ["/mnt/data", "~/datasets", "$WORK/warehouse"]
```

`~` and `$VAR` are expanded.

Otherwise roots accumulate on their own: **opening a dataset adds the directory
holding it**, so somewhere on a mount only has to be found by hand once. Press
<kbd>~</kbd> to type a path, open something, and the place is listed from then on.
Roots gathered this way disappear again when they hold nothing.

### Desktop places

datui reads freedesktop's `recently-used.xbel` — written by file managers and GTK
applications — and offers the **directories** it mentions. Never the file names:
those places are listed unexpanded under `ELSEWHERE`, and their contents appear
only after you press <kbd>Enter</kbd>.

```
  ELSEWHERE                        opened elsewhere · press Enter to look
    ~/Downloads/                 dir
```

datui reads that file, never writes to it, and sends nothing anywhere. To ignore
it:

```toml
[data]
use_desktop_recents = false
```

## What counts as a dataset

| on disk | shown as |
|---|---|
| `customers.parquet` | one dataset |
| `sales/year=2024/…`, `sales/year=2025/…` | one row, `sales · hive` |
| `exports/` holding several matching Parquet files | one row, `exports · multi` |
| a directory of source code with a stray CSV in it | a directory to enter |

## Reading the columns

```
sales          hive     2.4M × 18     340 MB     2d
                        rows × cols   size       last modified
```

Row and column counts come from Parquet footers, summed across at most 64 files for
hive and multi-file datasets. A dataset larger than that shows `? × 39` — its width
is known, its length is not. Formats that need a scan to count rows (CSV among them)
show neither.

Rows are measured a few per frame, and only those on screen, so a directory of large
datasets appears immediately and the counts fill in. Measurements are kept for the
session.

The preview pane shows the full schema for Parquet datasets, each type in the colour
the table will use.

## Loading

<kbd>Ctrl</kbd>+<kbd>O</kbd> works while a dataset is loading; scanning runs off
the interface thread.

Leaving a load abandons it rather than cancelling it. The scan runs to completion
in the background and its result is discarded — it cannot overwrite whatever you
open instead, but it does use CPU until it finishes.

## What datui remembers

A list of recently opened paths, in the cache directory. Everything else on this
screen is read when the screen is drawn. `datui --clear-cache` removes the list.

## Plain terminals

On a terminal without a UTF-8 locale, datui falls back to ASCII:

```
  > _  type to filter

  RECENT
  > sales                        hive       2.4M x 18    340 MB    2d
```

Override the detection with:

```toml
[display]
unicode = "auto"    # "auto" (default), "always", or "never"
```

No Nerd Font characters are used anywhere, so no patched font is needed.

## Desktop launchers

datui installs `/usr/share/applications/datui.desktop`, so it appears in GNOME,
KDE, rofi, wofi and Omarchy's menu under **Apps**. Its keywords include `data`,
`parquet`, `dataframe` and `csv`.

Launching with no file opens the home screen. The entry declares the formats datui
reads, so a file manager offers "Open with datui" for them; which application is
the *default* for a format stays your choice in `mimeapps.list`.

## See Also

- [Configuration](configuration.md) — `[data] directories` and the rest
- [Theming from Your System](system-theming.md)
- [Loading Data](loading-data.md) — opening datasets from the command line
