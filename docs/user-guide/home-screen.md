# The Home Screen

Run `datui` with no arguments and you land on its home screen: a list of datasets
you can open, with what each one contains shown before you open it.

Press <kbd>Ctrl</kbd>+<kbd>O</kbd> from anywhere to come back to it. That is the
point — datui is somewhere you stay while you work through several datasets, not a
command you re-run for each one.

```
  datui                                                        ~/work/analysis

  › ▏  type to filter

  RECENT
  ▸ sales                        hive       2.4M × 18    340 MB    2d
    customers.parquet                         89k × 12     4 MB    1w

  /mnt/data                                                    configured
    events                       hive        1.1M × 9    120 MB    3h
    lookup.parquet                             980 × 4     8 KB    2mo

  ~/work/analysis                                       current directory
    raw_export.csv                                       1.2 GB    3h
    notes/                       dir
```

## Where the list comes from

Datasets are gathered from three places, and datui needs no setup for two of them.

**Recent** — datasets you have opened before, most recent first. For data that lives
on a mount this is usually what you want, so it leads.

**Configured directories** — set in your config, for places you visit often:

```toml
[data]
directories = ["/mnt/data", "~/datasets"]
```

Think of this like `PATH`: a short list of *places*. datui records nothing about what
it finds in them.

**The current directory** — where you launched datui. Useful for local exports and
fixtures, though the big datasets usually live elsewhere, which is why it comes last.

### Opening something teaches datui where it lives

Open a dataset on a mount and the directory holding it becomes somewhere datui
looks from then on. You never configure the association between "my code is here"
and "my data is over there" — you establish it by using it once.

So a brand-new install starts empty, and the way out is <kbd>~</kbd>: type a path
directly. After that the place is remembered.

## What counts as a dataset

The home screen lists datasets, not files. That means:

| on disk | shown as |
|---|---|
| `customers.parquet` | one dataset |
| `sales/year=2024/…`, `sales/year=2025/…` | one row, `sales · hive` |
| `exports/` holding several matching Parquet files | one row, `exports · multi` |
| a folder of source code with a stray CSV in it | a folder to look inside |

Collapsing a hive dataset into a single row is the part that saves the most typing:
its path is a directory tree, and tab-completion walks you down into the partitions
rather than stopping at the dataset.

## Reading the columns

Each row shows what can be known **without reading any data**:

```
sales          hive     2.4M × 18     340 MB     2d
                        rows × cols   size       last modified
```

Row and column counts come from Parquet footers, so they are free. A CSV's row count
cannot be known without scanning the whole file, so datui leaves it blank rather than
guessing. The preview pane on the right shows the full schema for Parquet datasets,
with each type in the same colour the table will use once opened.

For a hive or multi-file dataset, datui sums the footers of up to 64 files. Past that
the row count is left blank rather than reported as a partial total — a home screen
that stalls on your largest dataset would be worse than one that admits it does not
know.

## Keys

| key | does |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> / <kbd>k</kbd> <kbd>j</kbd> | move |
| <kbd>Enter</kbd> | open the dataset, or descend into a directory |
| type anything | filter by name; matching is fuzzy, so `sal` finds `sales` |
| <kbd>~</kbd> | type a path directly |
| <kbd>Backspace</kbd> | delete a filter character, or leave a directory |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | clear the filter |
| <kbd>Esc</kbd> | back to the data you already had open |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | return here from anywhere, even mid-load |

<kbd>Ctrl</kbd>+<kbd>O</kbd> works while a dataset is still loading, so opening a
large file by mistake costs one keystroke rather than a wait.

## What datui remembers

**One thing: a list of recently opened paths**, in your cache directory. Everything
else on this screen is read from the filesystem when the screen is drawn, and
forgotten when datui exits.

This is deliberate. datui is not a data catalogue: there is nothing to register,
nothing to curate, and no metadata store to go stale. Clearing the cache
(`datui --clear-cache`) loses the ordering of that list and nothing else.

## Plain terminals

The screen above uses box-drawing and arrow characters. On a terminal that is not
running a UTF-8 locale those would render as replacement boxes, so datui checks the
locale and falls back to plain ASCII:

```
  > _  type to filter

  RECENT
  > sales                        hive       2.4M x 18    340 MB    2d
```

Override the detection if it guesses wrong:

```toml
[display]
unicode = "auto"    # "auto" (default), "always", or "never"
```

datui's interface uses no Nerd Font characters anywhere, so no patched font is
needed. (The Omarchy menu entry below does use one, because Omarchy provides it.)

## Omarchy

Merge `contrib/omarchy/omarchy-menu.jsonc` into
`~/.config/omarchy/extensions/omarchy-menu.jsonc` for a **Data** entry in the
launcher. It has two rows, and the distinction matters:

- **Explore** focuses a datui that is already running. Asking for "a datui" when one
  is open almost always means you want the one you have, and <kbd>Ctrl</kbd>+<kbd>O</kbd>
  inside it reaches anything else.
- **New window** always starts a fresh instance — for when you are working in one
  dataset and want a second beside it to compare.

Any row you add that names a specific dataset should open a new window too: naming a
dataset means you want *that* one, not whatever is currently loaded.

See [Theming from Your System](system-theming.md) for making datui match your
Omarchy theme while you are there.

## See Also

- [Configuration](configuration.md) — including `[data] directories`
- [Loading Data](loading-data.md) — opening datasets from the command line
