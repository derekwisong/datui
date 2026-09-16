# Templates

A template saves what you did to a table so you can do it again on the next
one: the query, filters, sort, column order, frozen columns, and any pivot or
melt. Templates are scored against each file you open, so the right one is at
the top of the list.

| Key | Action |
|---|---|
| <kbd>t</kbd> | Open the template list |
| <kbd>T</kbd> | Apply the best-matching template without opening the list |

Or from the command line: `datui --template quarterly data.csv`.

## The template list

Templates are listed by how well they fit the open file. A checkmark marks the
one currently applied.

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Apply the selected template |
| <kbd>s</kbd> | Save the current state as a new template |
| <kbd>e</kbd> | Edit the selected template |
| <kbd>d</kbd> | Delete it, after confirming |
| <kbd>Esc</kbd> | Close |

## Saving

Press <kbd>s</kbd> in the list, give the template a name and an optional
description, and choose how it should match future files:

| Match | Fits a file when |
|---|---|
| Exact path | its absolute path is the same |
| Relative path | its path relative to the current directory is the same |
| Path pattern | its path matches a glob |
| Filename pattern | its name matches a glob |
| Schema | it has the same column names |

A template with no match rules is generic and fits everything, at the lowest
score. Exact path scores highest, then relative path, then an exact schema
match, then patterns. Matching both a path and the schema scores higher still.

Only the active query tab is saved: **Query**, **SQL** or **Fuzzy**. Filters,
sort, column order and reshape are saved regardless.

## Applying on open

```toml
[templates]
auto_apply = true   # apply the best match when a file opens
```

Templates are JSON files in `~/.config/datui/templates/`.
`datui --remove-templates` deletes them all.
