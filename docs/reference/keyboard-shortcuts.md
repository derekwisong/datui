# Keyboard Shortcuts

<kbd>?</kbd> or <kbd>F1</kbd> shows the keys for whatever is on screen;
<kbd>F1</kbd> works inside text fields too. The bar at the bottom of the
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
| <kbd>t</kbd> | [Templates](../user-guide/templates.md) |
| <kbd>T</kbd> | Apply the best-matching template |
| <kbd>N</kbd> | Toggle row numbers |
| <kbd>F</kbd> | Toggle [digit grouping](../user-guide/configuration.md#number-formatting) |
| <kbd>D</kbd> | Toggle the type row under the headers |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | [Home screen](../user-guide/home-screen.md) |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>q</kbd> or <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit (<kbd>Ctrl</kbd>+<kbd>Q</kbd> works from anywhere, including a long load) |

The three toggles last for the session; the config file sets the state at
launch.

## Home screen

Letters type into the filter here, so none of them is a key.

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | Move |
| <kbd>Ctrl</kbd>+<kbd>↑</kbd> <kbd>Ctrl</kbd>+<kbd>↓</kbd> | Previous or next section |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Ten rows |
| <kbd>←</kbd> <kbd>→</kbd> | Fold or unfold the section; <kbd>→</kbd> on a `hive` or `multi` folder goes inside it |
| <kbd>Enter</kbd> | Open the dataset, enter the directory, cloud source or bucket, or fold the section |
| type | Filter by name or column name, and search below the working directory |
| <kbd>~</kbd> | Type a path; <kbd>Tab</kbd> completes |
| <kbd>Tab</kbd> | Cycle the sort: default, size, modified, rows |
| <kbd>Backspace</kbd> | Delete a filter character, or go up one level |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | List again what is on screen, ignoring what is cached |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Delete</kbd> | Forget the highlighted recent entry, or hide a cloud source |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Forget every recent entry, after confirming |
| <kbd>Esc</kbd> | Back out one layer: filter, directory, then to the open data |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | Return here from anywhere, including during a load |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> or <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit |

## Query prompt

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Switch tab (Query, SQL, Fuzzy) and move into the input |
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
| <kbd>←</kbd> <kbd>→</kbd> | On the tab bar, switch Sort and Filter |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus through the form to Apply, Cancel, Clear |
| <kbd>Enter</kbd> | Press the focused button; on the Filter tab, add the filter |
| <kbd>Esc</kbd> | Close without applying |

On the Sort tab, with a column highlighted:

| Key | Action |
|---|---|
| <kbd>Space</kbd> | Sort by it, or stop |
| <kbd>1</kbd> to <kbd>9</kbd> | Put it at that position in the sort order |
| <kbd>[</kbd> <kbd>]</kbd> | Move it earlier or later in the sort order |
| <kbd>+</kbd> <kbd>-</kbd> | Move it left or right in the table |
| <kbd>L</kbd> | Freeze it and the columns above it |
| <kbd>v</kbd> | Hide or show it |
| <kbd>Ctrl</kbd>+<kbd>Enter</kbd> | Apply |

## Chart

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus: tab bar, then each sidebar field |
| <kbd>←</kbd> <kbd>→</kbd> | Change chart type, plot style, or a numeric option |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move through a column list |
| <kbd>Enter</kbd> <kbd>Space</kbd> | Select a column or toggle an option |
| <kbd>+</kbd> <kbd>-</kbd> | Adjust bins, bandwidth or the row limit |
| <kbd>e</kbd> | Export to PNG or EPS |
| <kbd>Esc</kbd> | Back to the table |

## Analysis

| Key | Action |
|---|---|
| <kbd>Tab</kbd> | Switch focus between the tool list and the result |
| <kbd>↑</kbd> <kbd>↓</kbd> <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd> | Move |
| <kbd>Home</kbd> <kbd>End</kbd> <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Jump |
| <kbd>Enter</kbd> | Run the highlighted tool, or open the detail of a column or a correlation pair |
| <kbd>s</kbd> | In the distribution detail, toggle the histogram between linear and log |
| <kbd>r</kbd> | Draw a new sample, when sampling is on |
| <kbd>Esc</kbd> | Back one level |

## Pivot and melt

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | On the tab bar, switch Pivot and Melt; in a text field, move the cursor |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus through the form to Apply, Cancel, Clear |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move through a list |
| <kbd>Space</kbd> | Select or deselect in a multi-select list |
| <kbd>Enter</kbd> | Press the focused button |
| <kbd>Esc</kbd> | Close without applying |

## Export

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between format, path, options and buttons |
| <kbd>↑</kbd> <kbd>↓</kbd> | Change the format |
| <kbd>Enter</kbd> | Toggle a checkbox, or press Export or Cancel |
| <kbd>Esc</kbd> | Close |

## Dataset info

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch Schema, Resources, Partitions |
| <kbd>Tab</kbd> | On Schema, move between the tab bar and the column table |
| <kbd>↑</kbd> <kbd>↓</kbd> | Scroll the column table |
| <kbd>Esc</kbd> <kbd>i</kbd> | Close |

## Templates

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> | Move through the list |
| <kbd>Enter</kbd> | Apply the selected template |
| <kbd>s</kbd> | Save the current state as a new template |
| <kbd>e</kbd> | Edit the selected template |
| <kbd>d</kbd> | Delete it, after confirming (<kbd>D</kbd> confirms) |
| <kbd>Esc</kbd> | Close |

## Terminal notes

If <kbd>F1</kbd> does nothing in Alacritty, it is bound in
`~/.config/alacritty/alacritty.toml`; <kbd>?</kbd> still works outside text
fields.
