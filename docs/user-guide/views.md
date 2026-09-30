# Save and apply views

Press <kbd>v</kbd> to save or apply a view. A view stores the active query,
filters, sort, column layout, frozen columns and reshape settings.

## Save and reuse a query

1. Run a query, such as the species summary in the [quick start](../getting-started/quick-start.md#2-group-by-species).
2. Press <kbd>v</kbd>, then <kbd>s</kbd>. Enter a name and press <kbd>Enter</kbd> to save.
3. Open another file with the same columns, press <kbd>v</kbd>, select the view and press <kbd>Enter</kbd>.

A view stores transformations, not a copy of the data. The next file produces
its own results. Saving is unavailable until the current table has a change
to store. In the description field, <kbd>Enter</kbd> inserts a newline;
<kbd>Ctrl</kbd>+<kbd>J</kbd> saves from there, or <kbd>Tab</kbd> out and press <kbd>Enter</kbd>.

## Open the views list

| Key | Action |
|---|---|
| <kbd>v</kbd> | Open the views list |
| <kbd>V</kbd> | Apply the best-matching view without opening the list; when none matches, the list opens instead |

Or from the command line: `datui --template quarterly data.csv`.

## List controls

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

The save form starts with the filename as its name. Add a description if
needed, then expand **Matching** with <kbd>Space</kbd> to choose which files
should match the view:

| Match | Fits a file when |
|---|---|
| Exact path | its absolute path, or its URL for remote data, is the same |
| Relative path | its path relative to the current directory is the same; local files only |
| Path pattern | its path matches a glob |
| Filename pattern | its name matches a glob |
| Schema | it has all the view's columns; extra columns are fine |

Schema matching is enabled by default. It records the columns as loaded,
before the query, so a view whose query renames columns still matches the next
file. Matching views rank above unrelated ones; exact schemas rank above
schemas with extra columns. Other match rules and usage history also affect
the score. Press <kbd>i</kbd> to inspect it.
<kbd>V</kbd> and automatic application use only views with a matching rule.

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

The config and CLI retain the older name **templates**. Views are JSON files in the `templates/` directory beside your
[config file](configuration.md).
`datui --remove-templates` deletes them all.
