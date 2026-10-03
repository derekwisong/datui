# Save and apply views

Press <kbd>v</kbd> to save or apply a view. A view stores the active query,
filters, sort, column layout, frozen columns and reshape settings.

## Save and reuse a query

Central Park's daily highs, in NOAA's public weather data, for 2024 and then
2023:

1. Open `s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/`, as the
   `datui` argument or from the home screen: <kbd>Enter</kbd> on **NOAA daily
   weather (GHCN-D)**, <kbd>→</kbd> on `by_year` and on `YEAR=2024`, then
   <kbd>Enter</kbd> on `ELEMENT=TMAX`. Typing narrows each list.
2. Run the [station query](remote-data.md#examples-on-public-data): 366 rows.
3. Press <kbd>v</kbd>, then <kbd>s</kbd>. The name starts as the directory's,
   `ELEMENT=TMAX`: type `Central Park highs` over it and press
   <kbd>Enter</kbd> to save.
4. Open `YEAR=2023/ELEMENT=TMAX/` the same way. Press <kbd>v</kbd>: the view is
   listed with Match `same columns`. Press <kbd>Enter</kbd>.

The view runs the query on 2023: 365 rows, 2023-01-01 to 2023-12-31.

A view stores transformations, not a copy of the data. The next file produces
its own results. Saving is unavailable until the current table has a change
to store. In the description field, <kbd>Enter</kbd> inserts a newline;
<kbd>Ctrl</kbd>+<kbd>J</kbd> saves from there, or <kbd>Tab</kbd> out and press <kbd>Enter</kbd>.

## Open the views list

| Key | Action |
|---|---|
| <kbd>v</kbd> | Open the views list |
| <kbd>V</kbd> | Apply the best-matching view without opening the list; when none matches, the list opens instead |

When <kbd>V</kbd> or [automatic application](#applying-on-open) applies a
view, the bottom bar names it and says why it matched:
`View "Central Park highs" applied: same columns`.

Or from the command line:

```bash
datui --view "Central Park highs" s3://noaa-ghcn-pds/parquet/by_year/YEAR=2022/ELEMENT=TMAX/
```

A view's pivot and first rows are read in the background, with a spinner in
the bottom bar. <kbd>Esc</kbd> stops it and keeps the table as it was. A view
that fails on the data is not applied, and a dialog says why.

## List controls

Views are listed by how well they fit the open file. A check mark marks the
one currently applied, and the Match column says why a view fits:

| Match | The view's rule that fits |
|---|---|
| `same file` | Exact path or relative path |
| `same columns` | Schema |
| `glob` | Path pattern or filename pattern |

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Apply the selected view |
| <kbd>s</kbd> | Save the current state as a new view |
| <kbd>e</kbd> | Edit the selected view |
| <kbd>d</kbd> | Delete it, after confirming (<kbd>Enter</kbd>, <kbd>d</kbd> or <kbd>D</kbd> confirms) |
| <kbd>i</kbd> | Show how the selected view's score was computed |
| <kbd>Esc</kbd> | Close |

## Saving

The save form starts with the filename as its name, selected: typing
replaces it, and an arrow key keeps it for editing. Add a description if
needed, then expand **Matching** with <kbd>Space</kbd> to choose which files
should match the view:

| Match | Fits a file when |
|---|---|
| Exact path | its absolute path, or its URL for remote data, is the same |
| Relative path | its path relative to the current directory is the same; local files only |
| Path pattern | its path matches a glob |
| Filename pattern | its name matches a glob |
| Schema | it has all the view's columns; extra columns are fine |

A view saved on one table of a SQLite database or NumPy archive records the
table, shown as **Table** under Matching. Its path rules then fit only that
table: `shop.db/orders` and `shop.db --table orders` are the same file, and
`shop.db/customers` is not. Schema matching still carries the view to any
table with its columns.

Data piped to standard input and frames passed from Python
(`datui.view(frame)`) have no path, so their views match by schema alone.

Schema matching is enabled by default. It records the columns as loaded,
before the query, so a view whose query renames columns still matches the next
file. Matching views rank above unrelated ones; exact schemas rank above
schemas with extra columns. Other match rules and usage history also affect
the score. Press <kbd>i</kbd> to inspect it.
<kbd>V</kbd> and automatic application use only views with a matching rule.

Only the active query is saved, in its own mode: **SQL**, **Search** or **q-style**. Filters,
sort, column order and reshape are saved regardless. After a pivot or melt, the
view also keeps the query, filters and sort the reshape ran over, and applies
them before it. A reshape of a reshape, such as a melt of a pivot, cannot be
replayed: the view keeps only the last one.

Editing (<kbd>e</kbd>) changes a view's name, description and matching. Its
saved settings — and the columns its schema rule matches on — follow the
table only while the view is the one applied, so renaming a view never
overwrites what it carries.

## Applying on open

```toml
[views]
auto_apply = true   # apply the best match when a file opens
```

Views are JSON files in the `views/` directory beside your
[config file](configuration.md).

```bash
datui views list         # name and the files each matches
datui views rm NAME      # remove one
datui views clear        # remove them all
```
