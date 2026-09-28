# Keyboard Shortcuts

<kbd>?</kbd> or <kbd>F1</kbd> shows the keys for whatever is on screen;
<kbd>F1</kbd> works inside text fields too, and on the home screen, where
letters type into the filter, <kbd>?</kbd> opens help until you start
typing. The bar at the bottom of the
screen shows the ones that matter most. A spinner in that bar means datui is
busy; keys you type at it are queued and replayed once the work is done, so
typing ahead is never lost, while <kbd>Ctrl</kbd>+<kbd>Q</kbd>,
<kbd>Ctrl</kbd>+<kbd>C</kbd> and <kbd>Ctrl</kbd>+<kbd>O</kbd> still act at once.

## Table

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move one row |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Scroll columns |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> or <kbd>Ctrl</kbd>+<kbd>B</kbd> <kbd>Ctrl</kbd>+<kbd>F</kbd> | One page |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> <kbd>Ctrl</kbd>+<kbd>D</kbd> | Half a page |
| <kbd>Home</kbd> <kbd>End</kbd> or <kbd>G</kbd> | First and last row |
| <kbd>:</kbd> | Go to a row number: type it, <kbd>Enter</kbd> |
| <kbd>Enter</kbd> | On a grouped row, drill into the group. <kbd>Esc</kbd> comes back |
| <kbd>/</kbd> | [Query](../user-guide/querying-data.md) |
| <kbd>s</kbd> | [Sort and filter](../user-guide/filtering-sorting.md) |
| <kbd>r</kbd> | Reverse the sort |
| <kbd>R</kbd> | Reset: clear query, filters, sort, column order and frozen columns |
| <kbd>c</kbd> | [Chart](../user-guide/charting.md) |
| <kbd>a</kbd> | [Analysis](../user-guide/analysis-features.md) |
| <kbd>p</kbd> | [Pivot and melt](../user-guide/reshaping.md) |
| <kbd>e</kbd> | [Export](../user-guide/exporting-data.md) |
| <kbd>i</kbd> | [Dataset info](../user-guide/dataset-info.md) |
| <kbd>v</kbd> | [Views](../user-guide/views.md) |
| <kbd>V</kbd> | Apply the best-matching view; with no match, open the list |
| <kbd>N</kbd> | Toggle row numbers |
| <kbd>F</kbd> | Toggle [digit grouping](../user-guide/configuration.md#number-formatting) |
| <kbd>D</kbd> | Toggle the type row under the headers |
| <kbd>H</kbd> | CSV, TSV, PSV: read the first row as data, or as column names again. Reads the file again, clearing query, filters and sort |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | [Home screen](../user-guide/home-screen.md) |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>q</kbd> | Back to the home screen when the dataset was opened from it; otherwise quit — the control bar says which |
| <kbd>Q</kbd> or <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit (<kbd>Ctrl</kbd>+<kbd>Q</kbd> works from anywhere, including a long load) |

The three toggles last for the session; the config file sets the state at
launch.

## Home screen

Letters type into the filter here, so none of them is a key.

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | Move |
| <kbd>Ctrl</kbd>+<kbd>↑</kbd> <kbd>Ctrl</kbd>+<kbd>↓</kbd> | Previous or next section |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A screenful, stopping at the first and last |
| <kbd>Home</kbd> <kbd>End</kbd> | The first or last row |
| <kbd>←</kbd> <kbd>→</kbd> | Fold or unfold the section; <kbd>→</kbd> on any directory, or on a place row under `RECENT`, goes inside it |
| <kbd>Enter</kbd> | Open the dataset, enter the directory, cloud source, bucket or place, show the rest of `RECENT`, or fold the section. The control bar names which, for the row you are on |
| <kbd>Enter</kbd> on `(all files)` | Read the whole directory as one table, whatever its label. The first row inside every directory |
| type | Filter by name or column name, and search below the working directory |
| <kbd>~</kbd> | Type a path or URL; <kbd>Tab</kbd> completes a path |
| <kbd>Tab</kbd> | Cycle the sort: default, size, modified, rows |
| <kbd>Backspace</kbd> | Delete a filter character, or go up one level |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | List what is on screen, ignoring what is cached |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Ctrl</kbd>+<kbd>A</kbd> | Show or hide files datui cannot read |
| <kbd>Delete</kbd> | Forget the highlighted recent entry, or every recent under the highlighted place after confirming, or hide a cloud source |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Forget every recent entry, after confirming |
| <kbd>Esc</kbd> | Back out one layer: filter, directory, then to the open data |
| <kbd>?</kbd> or <kbd>F1</kbd> | Help (<kbd>?</kbd> until you start typing; <kbd>F1</kbd> always) |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | Return here from anywhere, including during a load |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> or <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit |

## Query prompt

| Key | Action |
|---|---|
| <kbd>Tab</kbd> | Move between the input and the tab bar |
| <kbd>←</kbd> <kbd>→</kbd> | On the tab bar, switch Query, SQL, Fuzzy |
| <kbd>↑</kbd> <kbd>↓</kbd> | History for the current tab |
| <kbd>Enter</kbd> | Run. An empty query restores the full table |
| <kbd>Esc</kbd> | Cancel |

## Text fields

Every text field, from the query prompt to a file path, edits the same way:

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> <kbd>Home</kbd> <kbd>End</kbd> | Move the cursor |
| <kbd>Ctrl</kbd>+<kbd>←</kbd> <kbd>Ctrl</kbd>+<kbd>→</kbd> or <kbd>Alt</kbd>+<kbd>B</kbd> <kbd>Alt</kbd>+<kbd>F</kbd> | Move a word |
| <kbd>Ctrl</kbd>+<kbd>A</kbd> <kbd>Ctrl</kbd>+<kbd>E</kbd> | Start and end of line |
| <kbd>Ctrl</kbd>+<kbd>W</kbd> <kbd>Alt</kbd>+<kbd>D</kbd> | Delete the word before or after the cursor |
| <kbd>Ctrl</kbd>+<kbd>K</kbd> | Delete to the end of the line |
| <kbd>Ctrl</kbd>+<kbd>Y</kbd> | Paste the last deletion |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Undo |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> | Copy the selection (does not quit while a text field is focused) |
| <kbd>F1</kbd> | Help |

## Sort and filter

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch Columns and Filters |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus between the tab bar and the body |
| <kbd>Enter</kbd> | Apply and close (on the Filters tab, add or edit a filter instead) |
| <kbd>a</kbd> | On the Filters tab, apply and close |
| <kbd>Ctrl</kbd>+<kbd>Enter</kbd> | Apply from anywhere, including mid-edit (on terminals that distinguish it from Enter) |
| <kbd>Esc</kbd> | Close without applying |

On the Columns tab, with a column highlighted:

| Key | Action |
|---|---|
| <kbd>Space</kbd> | Cycle its sort: none, ascending, descending |
| <kbd>1</kbd> to <kbd>9</kbd> | Put it at that position in the sort order; <kbd>0</kbd> removes it |
| <kbd>Del</kbd> | Remove it from the sort |
| <kbd>[</kbd> <kbd>]</kbd> | Move it earlier or later in the sort order |
| <kbd>+</kbd> <kbd>-</kbd> | Move it left or right in the table |
| <kbd>L</kbd> | Freeze it and the columns above it |
| <kbd>v</kbd> | Hide or show it |
| <kbd>C</kbd> | Clear the staged sort, order, locks and hidden columns |

On the Filters tab:

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Edit the row under the cursor, or add one on the last row |
| type, <kbd>↑</kbd> <kbd>↓</kbd>, <kbd>Enter</kbd> | In the editor: narrow the column or operator, move, choose; then type the value and <kbd>Enter</kbd> saves |
| <kbd>Space</kbd> | Toggle and/or on the row |
| <kbd>d</kbd> <kbd>Del</kbd> | Delete the row |
| <kbd>C</kbd> | Clear every staged filter |
| <kbd>Esc</kbd> | Back out of the edit, then close |

## Chart

| Key | Action |
|---|---|
| <kbd>1</kbd>–<kbd>5</kbd> | Switch chart type directly (<kbd>[</kbd> <kbd>]</kbd> cycle) |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the option rows |
| <kbd>Enter</kbd> <kbd>Space</kbd> | Open a column row's picker, toggle an option, or cycle the plot style |
| <kbd>←</kbd> <kbd>→</kbd> | Cycle the plot style, or adjust bins, bandwidth or the row limit (<kbd>+</kbd> <kbd>-</kbd> too, <kbd>PgUp</kbd> <kbd>PgDn</kbd> for bigger steps on the row limit) |
| type, <kbd>↑</kbd> <kbd>↓</kbd>, <kbd>Enter</kbd> or <kbd>Space</kbd> | In the picker: narrow, move, choose (on the Y series row <kbd>Space</kbd> toggles a series in or out) |
| <kbd>e</kbd> | Export to PNG or EPS |
| <kbd>Esc</kbd> | Back to the table, or out of the open picker |

## Analysis

| Key | Action |
|---|---|
| <kbd>Tab</kbd> | Switch focus between the tool list and the result |
| <kbd>↑</kbd> <kbd>↓</kbd> <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd> | Move |
| <kbd>Home</kbd> <kbd>End</kbd> <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Jump |
| <kbd>Enter</kbd> | Run the highlighted tool, or open the detail of a column or a correlation pair |
| <kbd>s</kbd> | In the distribution detail, toggle the histogram between linear and log |
| <kbd>r</kbd> | Redraw a sample when sampling is on |
| <kbd>Esc</kbd> | Back one level |

In Data Quality:

| Key | Action |
|---|---|
| <kbd>1</kbd>–<kbd>4</kbd> | Open Overview, Columns, Segments, or Trends |
| <kbd>e</kbd> | Edit scope, grain, compute, comparison, and time roles in a copy of the plan |
| <kbd>Enter</kbd> on Scope | In the plan editor, open the precise scope entry |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> on Scope | Scroll the numbered source-file inventory |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>[</kbd> <kbd>]</kbd> | In Segments/Trends, choose a column |
| <kbd>m</kbd> | In Segments/Trends, choose a measurement |
| <kbd>b</kbd> | In Segments, use the selected segment as baseline |
| <kbd>r</kbd> | Rerun with a new seed |

## Pivot and melt

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch Pivot and Melt; in a text field, move the cursor |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the rows |
| <kbd>Space</kbd> or typing | Open the focused row's picker, narrowed by what you type |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move; <kbd>Space</kbd> chooses, or toggles where several can be chosen |
| <kbd>Enter</kbd> | In the picker, choose; otherwise apply, from anywhere in the form |
| <kbd>Esc</kbd> | Close the picker; pressed again, close without applying |

## Export

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between format, path and options |
| <kbd>↑</kbd> <kbd>↓</kbd> | Change the format, or the compression |
| <kbd>Space</kbd> | Toggle a checkbox |
| <kbd>Enter</kbd> | Export, from anywhere in the form |
| <kbd>Esc</kbd> | Close |

## Dataset info

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch Schema, Resources, Partitions, Notes |
| <kbd>Tab</kbd> | On Schema, move between the tab bar and the column table |
| <kbd>↑</kbd> <kbd>↓</kbd> | Scroll the column table, or move through the notes |
| <kbd>Enter</kbd> | On a note, take its offer, where it has one |
| <kbd>Esc</kbd> <kbd>i</kbd> | Close |

## Views

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move through the list |
| <kbd>Enter</kbd> | Apply the selected view |
| <kbd>s</kbd> | Save the current state as a new view |
| <kbd>e</kbd> | Edit the selected view |
| <kbd>d</kbd> | Delete it, after confirming (<kbd>Enter</kbd> or <kbd>d</kbd> confirms) |
| <kbd>i</kbd> | Show how the selected view's score was computed |
| <kbd>Tab</kbd> | In the save and edit form, move between rows; <kbd>Space</kbd> expands Matching; <kbd>Enter</kbd> saves, <kbd>Esc</kbd> returns to the list |
| <kbd>Esc</kbd> | Close |

## Terminal notes

If <kbd>F1</kbd> does nothing in Alacritty, it is bound in
`~/.config/alacritty/alacritty.toml`; <kbd>?</kbd> still works outside text
fields.
