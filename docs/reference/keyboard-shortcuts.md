# Keyboard Shortcuts

<kbd>?</kbd> or <kbd>F1</kbd> shows the keys for whatever is on screen;
<kbd>F1</kbd> works inside text fields too, and on the home screen, where
letters type into the filter, <kbd>?</kbd> opens help until you start
typing. The bar at the bottom of the screen shows the ones that matter
most. A spinner in that bar means datui is busy. While it is, at the plain
table <kbd>q</kbd>, <kbd>Q</kbd>, <kbd>←</kbd> <kbd>→</kbd> (<kbd>h</kbd>
<kbd>l</kbd>), <kbd>?</kbd> and <kbd>F1</kbd> act at once, and
<kbd>Ctrl</kbd>+<kbd>Q</kbd>, <kbd>Ctrl</kbd>+<kbd>C</kbd> and
<kbd>Ctrl</kbd>+<kbd>O</kbd> act from anywhere. Other keys are queued and
replayed in order once the work is done — except a bare <kbd>Enter</kbd> or
<kbd>Esc</kbd>, which is dropped. At most 32 keys are held, and held keys
die with the screen they were typed at. At the loading screen nothing is
held: the allowed keys act, the rest are dropped.

## Table

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move one row |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Scroll columns |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> or <kbd>Ctrl</kbd>+<kbd>B</kbd> <kbd>Ctrl</kbd>+<kbd>F</kbd> | One page |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> <kbd>Ctrl</kbd>+<kbd>D</kbd> | Half a page |
| <kbd>Home</kbd> <kbd>End</kbd> or <kbd>G</kbd> | First and last row |
| <kbd>:</kbd> | Go to a row number: type it, <kbd>Enter</kbd>. <kbd>Esc</kbd> cancels; <kbd>F1</kbd> opens help |
| <kbd>Enter</kbd> | On a grouped row, drill into the group. <kbd>Esc</kbd> comes back |
| <kbd>/</kbd> | [Query](../user-guide/querying-data.md) |
| <kbd>s</kbd> | [Sort and filter](../user-guide/filtering-sorting.md) |
| <kbd>r</kbd> | Reverse the sort; with no sort, reverse the row order |
| <kbd>R</kbd> | Reset: clear query, filters, sort, column order and hidden columns, frozen columns, pivot/melt, drill-down and the applied view |
| <kbd>c</kbd> | [Chart](../user-guide/charting.md) |
| <kbd>a</kbd> | [Analysis](../user-guide/analysis-features.md) |
| <kbd>p</kbd> | [Pivot and melt](../user-guide/reshaping.md) |
| <kbd>e</kbd> | [Export](../user-guide/exporting-data.md) |
| <kbd>y</kbd> | [Copy to the clipboard](../user-guide/copying.md) |
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
| <kbd>Q</kbd>, <kbd>Ctrl</kbd>+<kbd>Q</kbd> or <kbd>Ctrl</kbd>+<kbd>C</kbd> | Quit (<kbd>Ctrl</kbd>+<kbd>Q</kbd> works from anywhere, including a long load; <kbd>Ctrl</kbd>+<kbd>C</kbd> anywhere outside a text field) |

The three toggles last for the session; the config file sets the state at
launch. A letter with <kbd>Ctrl</kbd> or <kbd>Alt</kbd> held is not a table
key, beyond the paging chords above.

## Home screen

Letters type into the filter here, so none of them is a key.

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | Move |
| <kbd>Ctrl</kbd>+<kbd>↑</kbd> <kbd>Ctrl</kbd>+<kbd>↓</kbd> | Previous or next section |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A screenful, stopping at the first and last |
| <kbd>Home</kbd> <kbd>End</kbd> | The first or last row |
| <kbd>←</kbd> <kbd>→</kbd> | Fold or unfold the section; <kbd>→</kbd> on any directory, or on a place row under `RECENT`, goes inside it |
| <kbd>Enter</kbd> | Open the dataset, enter the directory, cloud source, bucket or place, show the rest of `RECENT` or the hidden files, or fold the section. The control bar names which, for the row you are on |
| <kbd>Enter</kbd> on `(all files)` | Read the whole directory as one table, whatever its label. The first row inside a directory with something openable — `(all partitions)` in a hive directory; hidden while a filter is typed |
| type | Filter by name or column name, and search below the directory you are inside |
| <kbd>~</kbd> | While the filter is empty, type a path or URL. The prompt is a plain editor: characters, <kbd>Backspace</kbd>, <kbd>Ctrl</kbd>+<kbd>U</kbd> clears, <kbd>Tab</kbd> completes, <kbd>Enter</kbd> opens, <kbd>Esc</kbd> closes |
| <kbd>Tab</kbd> | Cycle the sort: natural (name, or recency under `RECENT`), size, modified, rows — the control bar names the order in effect |
| <kbd>Backspace</kbd> | Delete a filter character; on an empty filter, go up one level |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | List again what is on screen |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Ctrl</kbd>+<kbd>A</kbd> | Show or hide files datui cannot read |
| <kbd>Ctrl</kbd>+<kbd>D</kbd> | Remember the directory under the cursor so it stays listed, or forget it if remembered |
| <kbd>Delete</kbd> | Forget the highlighted recent entry, or every recent under the highlighted place after confirming, or a remembered directory on its heading, or hide a cloud source |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Forget every recent entry, after confirming |
| <kbd>Esc</kbd> | Back out one layer: path prompt, filter, directory, then to the open data |
| <kbd>?</kbd> or <kbd>F1</kbd> | Help (<kbd>?</kbd> until you start typing; <kbd>F1</kbd> always) |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | Return here from anywhere, including during a load |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> or <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit |

## Query prompt

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between the input and the tab bar |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | On the tab bar, switch Query, Fuzzy, SQL |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | History; each tab keeps its own |
| <kbd>Enter</kbd> | Run. An empty query restores the full table. On the tab bar, return to the input |
| <kbd>Esc</kbd> | Cancel |

Running a query — or clearing one — starts a fresh view: sidebar filters,
sort, frozen columns and pivot/melt are dropped. This is deliberate; apply
them after the query.

## Text fields

Every text field, from the query prompt to a file path, edits the same way,
with two simpler exceptions: a picker's type-to-narrow filter takes
characters and <kbd>Backspace</kbd>, plus <kbd>Ctrl</kbd>+<kbd>W</kbd> to
drop a word and <kbd>Ctrl</kbd>+<kbd>U</kbd> to clear; and the home
screen's <kbd>~</kbd> path prompt takes characters, <kbd>Backspace</kbd>,
<kbd>Ctrl</kbd>+<kbd>U</kbd>, <kbd>Tab</kbd> to complete, <kbd>Enter</kbd>
and <kbd>Esc</kbd>.

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
| <kbd>←</kbd> <kbd>→</kbd> | Switch Columns and Filters (<kbd>h</kbd> <kbd>l</kbd> on the tab bar) |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus between the tab bar and the body |
| <kbd>Enter</kbd> | Apply and close (on the Filters tab, add or edit a filter instead) |
| <kbd>a</kbd> | On the Filters tab, outside the row editor, apply and close |
| <kbd>Ctrl</kbd>+<kbd>Enter</kbd> | Apply from anywhere, including mid-edit — the row in progress is saved. Needs a terminal that tells <kbd>Ctrl</kbd>+<kbd>Enter</kbd> from <kbd>Enter</kbd>; elsewhere finish the row with <kbd>Enter</kbd>, then press <kbd>a</kbd> |
| <kbd>Esc</kbd> | Close without applying |

On the Columns tab, with a column highlighted:

| Key | Action |
|---|---|
| type | In the find field, narrow the column list |
| <kbd>Space</kbd> | Cycle its sort: none, ascending, descending |
| <kbd>1</kbd> to <kbd>9</kbd> | Put it at that position in the sort order; <kbd>0</kbd> removes it (digits past the end of the order do nothing) |
| <kbd>Del</kbd> | Remove it from the sort |
| <kbd>[</kbd> <kbd>]</kbd> | Move it earlier or later in the sort order |
| <kbd>+</kbd> <kbd>-</kbd> | Move it left or right in the table |
| <kbd>L</kbd> | Freeze it and the columns above it; on a column already frozen, pull the boundary back |
| <kbd>v</kbd> | Hide or show it |
| <kbd>C</kbd> | Clear the staged sort, order, locks and hidden columns |

On the Filters tab:

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Edit the row under the cursor, or add one on the last row |
| type, <kbd>↑</kbd> <kbd>↓</kbd> (<kbd>j</kbd> <kbd>k</kbd>), <kbd>Enter</kbd> | In the editor: narrow the column or operator, move, choose (<kbd>Tab</kbd>, <kbd>→</kbd> and <kbd>Space</kbd> also choose; <kbd>Shift</kbd>+<kbd>Tab</kbd> steps back; <kbd>↓</kbd> jumps from the find field into the list); then type the value and <kbd>Enter</kbd> saves |
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
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Cycle the plot style, or adjust bins, bandwidth or the row limit (<kbd>+</kbd> <kbd>-</kbd> too, <kbd>=</kbd> works as <kbd>+</kbd>, <kbd>PgUp</kbd> <kbd>PgDn</kbd> for bigger steps on the row limit) |
| type, <kbd>↑</kbd> <kbd>↓</kbd>, <kbd>Enter</kbd> or <kbd>Space</kbd> | In the picker: narrow, move, choose (on the Y series row <kbd>Space</kbd> toggles a series in or out; <kbd>Tab</kbd> and <kbd>Shift</kbd>+<kbd>Tab</kbd> choose and move to the next or previous row) |
| <kbd>e</kbd> | Export to PNG or EPS; needs the chart's required columns picked |
| <kbd>Esc</kbd> | Back to the table, or out of the open picker |

In the chart export dialog:

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Cycle Format, Path, Title, Width, Height |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Change the format |
| <kbd>Enter</kbd> | Export, from anywhere. An existing file asks Overwrite or No, starting on No; declining returns to the filled dialog |
| <kbd>Esc</kbd> | Back to the chart |

## Analysis

| Key | Action |
|---|---|
| <kbd>Tab</kbd> | Switch focus between the tool list and the result |
| <kbd>↑</kbd> <kbd>↓</kbd> <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd> | Move |
| <kbd>Home</kbd> <kbd>End</kbd> <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Jump |
| <kbd>Enter</kbd> | Pick the highlighted tool: its result, or a run on the shared sample; the first run on a dataset starts in the Sample form, where Enter runs it. In a result, open the detail of a column or a correlation pair |
| <kbd>s</kbd> | On a tool's main view, open the Sample form: the rows every tool reads. In the distribution detail, toggle the histogram between linear and log |
| <kbd>v</kbd> | On a tool's main view, show the sample's rows in the table viewer; <kbd>Esc</kbd> returns to the tool |
| <kbd>r</kbd> | On a sampled result, draw another sample for every tool (from the main analysis view, not inside a detail) |
| <kbd>a</kbd> | On a sampled result, read every row instead, after confirming; the sample's method becomes Every row |
| <kbd>Esc</kbd> | Cancel a run in progress; otherwise back one level |

In the Sample form:

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Apply the sample and run the tool on screen again |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Tab</kbd> | Move between the settings |
| <kbd>←</kbd> <kbd>→</kbd> | Change the focused choice |
| typing | On Values, Files, From row, To row, From, Before and Random seed: the value |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | On Files, scroll the numbered source files |
| <kbd>Esc</kbd> | Discard the edit |

In Data Quality:

| Key | Action |
|---|---|
| <kbd>1</kbd>–<kbd>4</kbd> | Open Overview (the report), Columns, Segments, or Trends |
| <kbd>Enter</kbd> | Run the plan, apply the edit in progress, open a finding, show its exact rows, show all checks on the clean entry, or close an open popup |
| <kbd>e</kbd> | Edit the plan's seven fields — Sample, Grain, Values, Compare, Time roles, Latency threshold, Rows per segment — in a copy of the plan |
| <kbd>s</kbd> | Open the shared Sample form |
| <kbd>v</kbd> | Show the rows Data Quality reads (up to 50,000 of the sample) in the table viewer |
| <kbd>←</kbd> <kbd>→</kbd> | In the plan editor, change the focused field's value; on Time roles, cycle the column |
| <kbd>Enter</kbd> on Sample | Open the Sample form |
| <kbd>Enter</kbd> on Time roles | Open the role editor: <kbd>↑</kbd> <kbd>↓</kbd> pick the role, <kbd>←</kbd> <kbd>→</kbd> the column, <kbd>Enter</kbd> done |
| <kbd>←</kbd> <kbd>→</kbd> on Rows per segment | Cycle 1,000 to 50,000 rows kept per segment (default: `[performance] quality_sample_rows`) |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>[</kbd> <kbd>]</kbd> | In Segments/Trends, choose a column |
| <kbd>m</kbd> | In Segments/Trends, choose a measurement |
| <kbd>b</kbd> | In Segments, use the selected segment as baseline |
| <kbd>r</kbd> | Rerun with a new seed, for every tool |
| <kbd>Tab</kbd> | Move between the result and the Analysis tools |
| <kbd>Esc</kbd> | Back out one layer: a popup, the edit (on Time roles, restoring the whole pre-edit plan), a column's Detail back to the Plan, an evidence drill back to its finding, then Analysis itself |

## Pivot and melt

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch Pivot and Melt (<kbd>h</kbd> <kbd>l</kbd> too, outside text fields); in a text field, move the cursor |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the rows |
| <kbd>Space</kbd> or typing | Open the focused row's picker, narrowed by what you type |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move; <kbd>Enter</kbd> or <kbd>Space</kbd> chooses; <kbd>Tab</kbd> and <kbd>Shift</kbd>+<kbd>Tab</kbd> choose and move rows |
| <kbd>Space</kbd> where several can be chosen | Toggle a column in or out; <kbd>Enter</kbd> then closes the picker (Done), and <kbd>Esc</kbd> keeps the toggles made so far |
| <kbd>Enter</kbd> | In a single-choice picker, choose; otherwise apply, from anywhere in the form |
| <kbd>Esc</kbd> | Close the picker; pressed again, close without applying |

## Export

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between format, path and options |
| <kbd>↑</kbd> <kbd>↓</kbd> | Change the format, or the compression; in Path and Delimiter they do nothing |
| <kbd>Space</kbd> | Toggle a checkbox |
| <kbd>Enter</kbd> | Export, from anywhere in the form. On a blank path the form says "Enter a file path." instead |
| <kbd>Esc</kbd> | Close |

If the file exists, <kbd>Enter</kbd> asks first, starting on No:
<kbd>←</kbd> <kbd>→</kbd> (<kbd>h</kbd> <kbd>l</kbd>) or <kbd>Tab</kbd>
pick Overwrite or No, <kbd>Enter</kbd> confirms the one picked, and
<kbd>Esc</kbd> declines. Declining returns to the filled form.

## Copy

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between rows; in a picker, choose |
| <kbd>Space</kbd> | Open the focused row's picker; on Header, toggle; in a picker, choose |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move focus, or the picker cursor; typing narrows a picker (<kbd>j</kbd> <kbd>k</kbd> narrow there, only <kbd>↑</kbd> <kbd>↓</kbd> move) |
| <kbd>Enter</kbd> | Copy, from anywhere in the form; in a picker, choose |
| <kbd>Esc</kbd> | Close a picker, then the dialog |

## Dataset info

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Switch Schema, Resources, Partitions, Notes. Afterward focus rests on the tab bar |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | On Schema, move between the tab bar and the column table |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Scroll the column table (when it is focused), or move through the notes |
| <kbd>Enter</kbd> | On a note, take its offer, where it has one |
| <kbd>Esc</kbd> <kbd>i</kbd> | Close |

## Views

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move through the list |
| <kbd>Enter</kbd> | Apply the selected view |
| <kbd>s</kbd> | Save the current state as a new view |
| <kbd>e</kbd> | Edit the selected view |
| <kbd>d</kbd> | Delete it, after confirming (<kbd>Enter</kbd>, <kbd>d</kbd> or <kbd>D</kbd> confirms) |
| <kbd>i</kbd> | Show how the selected view's score was computed; <kbd>Esc</kbd> closes it |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | In the save and edit form, move between rows (<kbd>↑</kbd> <kbd>↓</kbd> too, outside the description) |
| <kbd>Enter</kbd> in the form | Save. In the description <kbd>Enter</kbd> types: <kbd>Tab</kbd> out of it, then <kbd>Enter</kbd> — or <kbd>Ctrl</kbd>+<kbd>Enter</kbd>, on a terminal that tells it from <kbd>Enter</kbd> |
| <kbd>Space</kbd> in the form | Expand or collapse Matching; toggle schema match |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> in the form | Move five lines in the description |
| <kbd>Esc</kbd> | In the form, back to the list discarding edits; in the list, close |

## Help overlay

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Scroll |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page |
| <kbd>Home</kbd> <kbd>End</kbd> | Top and bottom |
| <kbd>Esc</kbd> or <kbd>?</kbd> | Close |

## Terminal notes

If <kbd>F1</kbd> does nothing in Alacritty, it is bound in
`~/.config/alacritty/alacritty.toml`; <kbd>?</kbd> still works outside text
fields.
