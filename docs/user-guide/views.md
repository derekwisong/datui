# Views

A view saves what you did to a table so you can do it again on the next
one: the query, filters, sort, column order, frozen columns, and any pivot or
melt. Views are scored against each file you open, so the right one is at
the top of the list. (Views were called templates before 0.4; the config
section, flags and files on disk keep that name, so nothing breaks.)

| Key | Action |
|---|---|
| <kbd>v</kbd> | Open the views list |
| <kbd>V</kbd> | Apply the best-matching view without opening the list; when none matches, the list opens instead |

Or from the command line: `datui --template quarterly data.csv`.

## The views list

Views are listed by how well they fit the open file. A check mark marks the
one currently applied, and the Match column says why a view fits: the same
file, the same columns, or a pattern.

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Apply the selected view |
| <kbd>s</kbd> | Save the current state as a new view |
| <kbd>e</kbd> | Edit the selected view |
| <kbd>d</kbd> | Delete it, after confirming (<kbd>Enter</kbd>, <kbd>d</kbd> or <kbd>D</kbd> confirms) |
| <kbd>i</kbd> | Show how the selected view's score was computed |
| <kbd>Esc</kbd> | Close |

## Saving

Press <kbd>s</kbd> in the list. There must be something to save: on an
untouched table <kbd>s</kbd> refuses, rather than minting a view that carries
nothing. The name starts as the file's name; add an optional description, and
expand the Matching section (<kbd>Space</kbd>) to change how the view offers
itself to future files:

| Match | Fits a file when |
|---|---|
| Exact path | its absolute path is the same |
| Relative path | its path relative to the current directory is the same |
| Path pattern | its path matches a glob |
| Filename pattern | its name matches a glob |
| Schema | it has all the view's columns; extra columns are fine |

Schema match starts enabled: it is what carries the view to the next table
shaped like this one. <kbd>V</kbd> and `auto_apply` only ever apply a view
one of whose rules fits the open file; the list shows every view regardless,
ranked. Matches combine, so a view fitting by relative path and exact schema
outranks one fitting by exact path alone; a dataset whose columns merely
include a view's scores low, below a pattern match; and how often and how
recently a view was used adds points. <kbd>i</kbd> in the list shows the
arithmetic.

In the form <kbd>Enter</kbd> saves; in the description it types, so
<kbd>Tab</kbd> out of the description, then <kbd>Enter</kbd> — or
<kbd>Ctrl</kbd>+<kbd>Enter</kbd>, on a terminal that tells it from
<kbd>Enter</kbd>.

Only the active query tab is saved: **Query**, **SQL** or **Fuzzy**. Filters,
sort, column order and reshape are saved regardless.

Editing (<kbd>e</kbd>) changes a view's name, description and matching. Its
saved settings — and the columns its schema rule matches on — follow the
table only while the view is the one applied, so renaming a view never
overwrites what it carries.

## Applying on open

```toml
[templates]
auto_apply = true   # apply the best match when a file opens
```

Views are JSON files in `~/.config/datui/templates/`.
`datui --remove-templates` deletes them all.
