# Keyboard Shortcuts

<kbd>?</kbd> or <kbd>F1</kbd> shows the keys for whatever is on screen;
<kbd>F1</kbd> works inside text fields too, and on the home screen, where
letters type into the filter, <kbd>?</kbd> opens help until you start
typing. The bottom bar shows the main actions for the current screen.

## Table

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move one row |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Move the [column cursor](../user-guide/filtering-sorting.md#move-across-a-wide-table) one column; the columns scroll only when it would leave the screen |
| <kbd>[</kbd> <kbd>]</kbd> or <kbd>Shift</kbd>+<kbd>←</kbd> <kbd>→</kbd> | A page of columns left or right, the cursor on the page's first column |
| <kbd>{</kbd> <kbd>}</kbd> | First column; last column |
| <kbd>g</kbd> | Go to a column by name, cursor and all |
| <kbd>b</kbd> | A binary file read through a [format spec](../user-guide/binary-formats.md): read it again with another spec |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> or <kbd>Ctrl</kbd>+<kbd>B</kbd> <kbd>Ctrl</kbd>+<kbd>F</kbd> | One page |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> <kbd>Ctrl</kbd>+<kbd>D</kbd> | Half a page |
| <kbd>Home</kbd> <kbd>End</kbd> or <kbd>G</kbd> | First and last row |
| <kbd>:</kbd> | Go to a row number: type it, <kbd>Enter</kbd>. <kbd>Esc</kbd> cancels; <kbd>F1</kbd> opens help |
| <kbd>f</kbd> | [Find](../user-guide/finding.md) text or a regex; the cursor, column cursor and all, goes to the first match at or after its row |
| <kbd>n</kbd> <kbd>N</kbd> | Next and previous match from the cursor's cell, wrapping round the view; each one typed while a find reads runs in turn. <kbd>Esc</kbd> stops a find still reading, and those typed behind it |
| <kbd>Enter</kbd> | On a row of a `by` query or a SQL `GROUP BY`, drill into its rows. <kbd>Esc</kbd> comes back. Anywhere else, inspect the row, as <kbd>Space</kbd> does |
| <kbd>Space</kbd> | [Inspect the row](../user-guide/inspecting-rows.md): every field, each value whole and exact |
| <kbd>/</kbd> | [Query](../user-guide/querying-data.md) |
| <kbd>s</kbd> | [Sort and filter](../user-guide/filtering-sorting.md), open on the cursor's column |
| <kbd>F</kbd> | [Value counts](../user-guide/value-counts.md) of the cursor's column |
| <kbd>r</kbd> | Reverse the sort; with no sort, reverse the row order |
| <kbd>R</kbd> | Reset: clear query, filters, sort, column order, hidden columns and widths, frozen columns, pivot/melt, drill-down and the applied view |
| <kbd>c</kbd> | [Chart](../user-guide/charting.md) |
| <kbd>a</kbd> | [Analysis](../user-guide/analysis-features.md) |
| <kbd>p</kbd> | [Pivot and melt](../user-guide/reshaping.md) |
| <kbd>e</kbd> | [Export](../user-guide/exporting-data.md) |
| <kbd>y</kbd> | [Copy to the clipboard](../user-guide/copying.md); a cell is the cursor's; the Python (Polars) scope copies the view as code |
| <kbd>i</kbd> | [Dataset info](../user-guide/dataset-info.md) |
| <kbd>v</kbd> | [Views](../user-guide/views.md) |
| <kbd>V</kbd> | Apply the best-matching view; with no match, open the list |
| <kbd>#</kbd> | Toggle row numbers |
| <kbd>,</kbd> | Toggle [digit grouping](settings.md#number-formatting) (was <kbd>F</kbd>) |
| <kbd>D</kbd> | Toggle the type row under the headers |
| <kbd>H</kbd> | CSV, TSV, PSV: read the first row as data, or as column names again. Reads the file again, clearing query, filters and sort |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | [Home screen](../user-guide/home-screen.md) |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>q</kbd> | Back to the home screen when the dataset was opened from it; otherwise quit — the control bar says which |
| <kbd>Q</kbd>, <kbd>Ctrl</kbd>+<kbd>Q</kbd> or <kbd>Ctrl</kbd>+<kbd>C</kbd> | Quit (<kbd>Ctrl</kbd>+<kbd>Q</kbd> works from anywhere, including a long load; <kbd>Ctrl</kbd>+<kbd>C</kbd> anywhere outside a text field) |

The three toggles last for the session; the config file sets the state at
launch. A letter with <kbd>Ctrl</kbd> or <kbd>Alt</kbd> held is not a table
key, beyond the paging chords above.

## Find

<kbd>f</kbd> at the table opens the find prompt.

| Key | Action |
|---|---|
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | Regex on or off |
| <kbd>Ctrl</kbd>+<kbd>L</kbd> | Only the column cursor's column, or every column shown |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | History |
| <kbd>Enter</kbd> | Find |
| <kbd>Esc</kbd> | Cancel |

## Go to column

<kbd>g</kbd> at the table lists the columns it shows, in its order.

| Key | Action |
|---|---|
| type | Narrow the list to the names that contain it |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move |
| <kbd>Enter</kbd> | Go to the column; the column cursor moves to it |
| <kbd>Backspace</kbd> | Delete a character; <kbd>Ctrl</kbd>+<kbd>W</kbd> a word, <kbd>Ctrl</kbd>+<kbd>U</kbd> all |
| <kbd>Esc</kbd> | Close without moving |

## Value counts

<kbd>F</kbd> at the table counts the column cursor's column.

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page |
| <kbd>Home</kbd> <kbd>End</kbd> or <kbd>G</kbd> | First and last line |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Previous or next column; the table's column cursor moves with it |
| <kbd>Enter</kbd> | The rows holding the value, as a drill-down; <kbd>Esc</kbd> there comes back |
| <kbd>s</kbd> | Sort by count or by value |
| <kbd>a</kbd> | Count every row, when the counts are of a sample |
| <kbd>y</kbd> | Copy the counts as TSV |
| <kbd>e</kbd> | Export the counts |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |
| <kbd>Esc</kbd> | Back to the table; while every row is being counted, stop and keep the sample |

## Format picker

| Key | Action |
|---|---|
| Type | Narrow the list of specs |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move |
| <kbd>Enter</kbd> | Read the file again with that spec, clearing the query, filters and sort |
| <kbd>Esc</kbd> | Close and keep the format |

## Hex view

A local file no reader and no spec takes opens here; so does any local file
from <kbd>Ctrl</kbd>+<kbd>X</kbd> on the home screen, <kbd>x</kbd> in the Info
panel, or `datui --hex FILE`. See [Hex view](../user-guide/hex-view.md).

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | A byte |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | A row |
| <kbd>w</kbd> <kbd>b</kbd> | The next group of four bytes, or back one |
| <kbd>0</kbd> <kbd>$</kbd> | The start or end of the row |
| <kbd>g</kbd> <kbd>G</kbd> or <kbd>Home</kbd> <kbd>End</kbd> | The start or end of the file |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> or <kbd>Ctrl</kbd>+<kbd>B</kbd> <kbd>Ctrl</kbd>+<kbd>F</kbd> | A page; <kbd>Ctrl</kbd>+<kbd>U</kbd> <kbd>Ctrl</kbd>+<kbd>D</kbd> half a page |
| <kbd>:</kbd> | Go to an offset: `4096`, `0x1000`, `+16`, `-16`, or `e-8` from the end |
| <kbd>f</kbd> | Find text, `0x…`, or hex pairs with `??` for any byte; <kbd>Ctrl</kbd>+<kbd>U</kbd> in the prompt finds text as UTF-16 |
| <kbd>n</kbd> <kbd>N</kbd> | Next and previous match, round the end of the file. <kbd>Esc</kbd> stops a find that is reading |
| <kbd>R</kbd> | When the matches repeat at one distance, make it the bytes per row |
| <kbd>r</kbd> | Bytes per row; empty for as many as fit |
| <kbd>v</kbd> | Mark a range from the cursor; again, or <kbd>Esc</kbd>, unmarks |
| <kbd>i</kbd> <kbd>Enter</kbd> | Show or hide the byte inspector |
| <kbd>#</kbd> | Offsets in decimal or hex |
| <kbd>B</kbd> | Read the file with a [format spec](../user-guide/binary-formats.md) |
| <kbd>Esc</kbd> | Back to the table or the home screen it was opened from |
| <kbd>q</kbd> | Home, when opened from there; otherwise quit |
| <kbd>?</kbd> <kbd>F1</kbd> | Help |

## Home screen

Letters type into the filter here, so none of them is a key.

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | Move |
| <kbd>Ctrl</kbd>+<kbd>↑</kbd> <kbd>Ctrl</kbd>+<kbd>↓</kbd> | Previous or next section |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A screenful, stopping at the first and last |
| <kbd>Home</kbd> <kbd>End</kbd> | The first or last row |
| <kbd>←</kbd> <kbd>→</kbd> | Fold or unfold the section; <kbd>→</kbd> on any directory, a SQLite database, or a place row under `RECENT`, goes inside it |
| <kbd>Enter</kbd> | Open the dataset, enter the directory, SQLite database of several tables, cloud source, bucket or place, show the rest of `RECENT` or the hidden files, or fold the section. The control bar names which, for the row you are on |
| <kbd>Enter</kbd> on the first row inside a directory | Read the directory as one table, as its label says: `(hive table: year, month)`, `(3 Parquet files, one schema)`, `(2 Parquet files, schemas differ)`, `(all files, mixed)`. The cursor starts there only for a hive table or one schema; hidden while a filter is typed |
| <kbd>Space</kbd> | While the filter is empty, fold or unfold the section header under the cursor; with a filter typed, it types a space |
| type | Filter by name or column name, and search below the directory you are inside |
| <kbd>~</kbd> | While the filter is empty, type a path or URL. The prompt is a plain editor: characters, <kbd>Backspace</kbd>, <kbd>Ctrl</kbd>+<kbd>U</kbd> clears, <kbd>Tab</kbd> completes, <kbd>Enter</kbd> opens a file or browses a directory, <kbd>Esc</kbd> closes |
| <kbd>Tab</kbd> | Cycle the sort: natural (name, or recency under `RECENT`), size, modified, rows — the control bar names the order in effect |
| <kbd>Backspace</kbd> | Delete a filter character; on an empty filter, go up one level. From the top of a collection's remote dataset, back to the list |
| <kbd>Ctrl</kbd>+<kbd>R</kbd> | List again what is on screen |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> | Clear the filter |
| <kbd>Ctrl</kbd>+<kbd>A</kbd> | Show or hide files no reader takes (labeled `binary`; <kbd>Enter</kbd> on a local one shows its bytes), or a SQLite database's internal tables |
| <kbd>Ctrl</kbd>+<kbd>X</kbd> | Show the local file under the cursor as bytes, in the [hex view](../user-guide/hex-view.md) |
| <kbd>Ctrl</kbd>+<kbd>D</kbd> | Remember the directory under the cursor so it stays listed, or forget it if remembered |
| <kbd>Delete</kbd> | Forget the highlighted recent entry, or every recent under the highlighted place after confirming, or a remembered directory on its heading, or hide a cloud source |
| <kbd>Shift</kbd>+<kbd>Delete</kbd> | Forget every recent entry, after confirming |
| <kbd>Esc</kbd> | Back out one layer: path prompt, filter, directory (back to the row it was entered from), then to the open data |
| <kbd>?</kbd> or <kbd>F1</kbd> | Help (<kbd>?</kbd> until you start typing; <kbd>F1</kbd> always) |
| <kbd>Ctrl</kbd>+<kbd>O</kbd> | Return here from anywhere, including during a load |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> or <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit |

In a collection, including Public datasets, Enter opens a file (a web file after
asking to download it) and steps into a directory or object-store prefix.
<kbd>→</kbd> steps into directories only; it does not open a web file.

## Query prompt

<kbd>/</kbd> opens on SQL, or on the active query's mode when you edit one.
`[query] default_mode` picks another starting mode.

| Key | Action |
|---|---|
| <kbd>Ctrl</kbd>+<kbd>T</kbd> | Next mode: SQL, Search, q-style, from the input or the tab bar |
| <kbd>Shift</kbd>+<kbd>Tab</kbd> | From the input, go to the tab bar; from the tab bar, back to the input |
| <kbd>Tab</kbd> | In SQL, complete a column name or `df` (again for the next match). In Search and q-style, and on the tab bar, move between the input and the tab bar |
| <kbd>Alt</kbd>+<kbd>Enter</kbd> | In SQL, start a new line |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | On the tab bar, switch mode |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | History; each mode keeps its own. In a SQL statement of several lines, <kbd>↑</kbd> <kbd>↓</kbd> move between them first |
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

A default a form fills in, such as a new view's name, melt's `variable`
and `value`, or a sample's seed, is selected while the field has focus:
typing or pasting replaces it, <kbd>Backspace</kbd>, <kbd>Delete</kbd> or a
deletion key in the table below clears it, and a cursor key, <kbd>Enter</kbd>
or <kbd>Tab</kbd> keeps it. Editing a saved view opens its values unselected.

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> <kbd>Home</kbd> <kbd>End</kbd> | Move the cursor |
| <kbd>Ctrl</kbd>+<kbd>←</kbd> <kbd>Ctrl</kbd>+<kbd>→</kbd> or <kbd>Alt</kbd>+<kbd>B</kbd> <kbd>Alt</kbd>+<kbd>F</kbd> | Move a word |
| <kbd>Ctrl</kbd>+<kbd>A</kbd> <kbd>Ctrl</kbd>+<kbd>E</kbd> | Start and end of line |
| <kbd>Ctrl</kbd>+<kbd>W</kbd> <kbd>Alt</kbd>+<kbd>D</kbd> | Delete the word before or after the cursor |
| <kbd>Ctrl</kbd>+<kbd>U</kbd> <kbd>Ctrl</kbd>+<kbd>K</kbd> | Delete to the start or the end of the line |
| <kbd>Ctrl</kbd>+<kbd>Y</kbd> | Paste the last deletion |
| <kbd>Ctrl</kbd>+<kbd>Z</kbd> <kbd>Ctrl</kbd>+<kbd>R</kbd> | Undo, redo |
| <kbd>Ctrl</kbd>+<kbd>J</kbd> | The same as <kbd>Ctrl</kbd>+<kbd>Enter</kbd>, on every terminal: saves a view from its description and applies Sort & Filter. In the query and go-to-line prompts it submits, like <kbd>Enter</kbd> |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> | Copy the selection (does not quit while a text field is focused) |
| <kbd>F1</kbd> | Help |

## Sort and filter

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch Columns and Filters (<kbd>h</kbd> <kbd>l</kbd> on the tab bar) |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move focus between the tab bar and the body |
| <kbd>Enter</kbd> | Apply and close (on the Filters tab, add or edit a filter instead) |
| <kbd>a</kbd> | On the Filters tab, outside the row editor, apply and close |
| <kbd>Ctrl</kbd>+<kbd>Enter</kbd> or <kbd>Ctrl</kbd>+<kbd>J</kbd> | Apply from anywhere, including mid-edit — the row in progress is saved. <kbd>Ctrl</kbd>+<kbd>Enter</kbd> needs a terminal that tells it from <kbd>Enter</kbd>; <kbd>Ctrl</kbd>+<kbd>J</kbd> works on every terminal |
| <kbd>Esc</kbd> | Close without applying |

On the Columns tab, with a column highlighted:

| Key | Action |
|---|---|
| type | In the find field, narrow the column list |
| <kbd>Space</kbd> | Cycle its sort: none, ascending, descending |
| <kbd>1</kbd> to <kbd>9</kbd> | Put it at that position in the sort order; <kbd>0</kbd> removes it. A digit past the end of the order says so on the sidebar's status line |
| <kbd>Del</kbd> | Remove it from the sort |
| <kbd>[</kbd> <kbd>]</kbd> | Move it earlier or later in the sort order |
| <kbd>+</kbd> <kbd>-</kbd> | Move it left or right in the table |
| <kbd>L</kbd> | Freeze it and the columns above it; on a column already frozen, pull the boundary back |
| <kbd>v</kbd> | Hide or show it |
| <kbd>&lt;</kbd> <kbd>&gt;</kbd> (<kbd>,</kbd> <kbd>.</kbd>) | Make it 4 cells narrower or wider |
| <kbd>f</kbd> | Fit it to the rows on screen |
| <kbd>w</kbd> | Back to the automatic width |
| <kbd>C</kbd> | Clear the staged sort, order, locks, hidden columns and widths |

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
| <kbd>1</kbd>–<kbd>6</kbd> | Switch chart type directly (<kbd>[</kbd> <kbd>]</kbd> cycle) |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the option rows |
| <kbd>Enter</kbd> <kbd>Space</kbd> | Open a column row's picker, toggle an option, or cycle the plot style, range or bar order |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Cycle the plot style, range or bar order, or adjust bins, bandwidth or the sample size (<kbd>+</kbd> <kbd>-</kbd> too, <kbd>=</kbd> works as <kbd>+</kbd>, <kbd>PgUp</kbd> <kbd>PgDn</kbd> for bigger steps on the sample size) |
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
| <kbd>Enter</kbd> | Pick the highlighted tool: its result, or a run on the shared sample; the first run on a dataset starts in the Sample form, where Enter runs it. Data Quality opens its Setup instead, where Enter runs it. In a result, open the detail of a column or a correlation pair |
| <kbd>s</kbd> | On a tool's main view, open the Sample form: the rows every tool reads (in Data Quality, over its Setup). In the distribution detail, toggle the histogram between linear and log |
| <kbd>v</kbd> | On a tool's main view, show the sample's rows in the table viewer; <kbd>Esc</kbd> returns to the tool |
| <kbd>r</kbd> | On a sampled result, draw another sample for every tool (from the main analysis view, not inside a detail) |
| <kbd>a</kbd> | On a sampled result, read every row instead, after confirming; the sample's method becomes Every row |
| <kbd>Esc</kbd> | Cancel a run in progress; otherwise back one level |

In the Sample form:

| Key | Action |
|---|---|
| <kbd>Enter</kbd> | Apply the sample and run the tool on screen again; in Data Quality, apply it to Setup, which runs on its own Enter |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Tab</kbd> | Move between the settings |
| <kbd>←</kbd> <kbd>→</kbd> | Change the focused choice |
| typing | On Values, Files, From row, To row, From, Before and Random seed: the value |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | On Files, scroll the numbered source files |
| <kbd>Esc</kbd> | Discard the edit |

In Data Quality Setup:

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between the rows |
| <kbd>←</kbd> <kbd>→</kbd> | Change Grain, Compare, Values, Latency over or Window by in place; on another row, <kbd>→</kbd> opens it |
| <kbd>Space</kbd> | Open the row under the cursor: the Sample form, Text as time, the Time roles, Intervals, Column intent or Expected editor, or a list of choices (type to narrow, <kbd>Enter</kbd> chooses, <kbd>Esc</kbd> cancels) |
| <kbd>s</kbd> | Open the Sample form; its <kbd>Enter</kbd> applies the sample to Setup |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>d</kbd> | Release the rows runs kept for reuse and a full scan's local copy of a remote dataset, named on the Read rule; the next run that would have used them reads again |
| <kbd>Enter</kbd> | Run, from any row: the one key in Setup that reads. A full scan asks first; while a cancelled read finishes, Run waits and Setup says why |
| <kbd>Esc</kbd> | Discard every staged change and go back to the report, or to the tool list before the first run |
| In the Time roles editor | <kbd>↑</kbd> <kbd>↓</kbd> pick the role, <kbd>←</kbd> <kbd>→</kbd> its column, <kbd>Enter</kbd> done, <kbd>Esc</kbd> cancel. With no date, time or text column there is nothing to assign, and the row says so |
| In the Intervals editor | <kbd>↑</kbd> <kbd>↓</kbd> pick a start and end, <kbd>Space</kbd> (or <kbd>←</kbd> <kbd>→</kbd>) measure it or not, <kbd>Enter</kbd> done, <kbd>Esc</kbd> cancel. Needs two assigned roles |
| In the Column intent editor | <kbd>↑</kbd> <kbd>↓</kbd> pick a column, <kbd>Space</kbd> (or <kbd>→</kbd>) open its form, <kbd>Enter</kbd> done, <kbd>Esc</kbd> put back the intent as it was |
| In a column's intent form | <kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> move between rows, <kbd>Space</kbd> ticks Key or Required, <kbd>←</kbd> <kbd>→</kbd> change Read as, typing fills Allowed, Minimum and Maximum, <kbd>Enter</kbd> applies, <kbd>Esc</kbd> drops the edit |
| In the Expected editor | <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>Tab</kbd> move between Windows, From and Before; <kbd>←</kbd> <kbd>→</kbd> (or <kbd>Space</kbd>) choose the windows; typing fills From and Before; <kbd>Enter</kbd> done, <kbd>Esc</kbd> cancel. Needs a time-window grain |

In the Data Quality report:

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> (<kbd>h</kbd> <kbd>l</kbd>) | Previous or next page: Overview, Columns, Segments, Trends, Intervals |
| <kbd>1</kbd>–<kbd>5</kbd> | Open Overview (the report), Columns, Segments, Trends, or Intervals directly |
| <kbd>Enter</kbd> | Open a finding, show its rows from the rows the run kept (the sample's, on a sampled run), show all checks on the clean entry, or close an open popup. Rows the run did not keep show a Read Rows dialog first; <kbd>Enter</kbd> there reads them, <kbd>Esc</kbd> reads nothing. On an empty Segments, Trends or Intervals page, open the Setup row that fills it |
| <kbd>c</kbd> | On Overview, show only the findings that name one column, chosen from a list |
| <kbd>t</kbd> | On Overview, show only one type's findings, chosen from a list |
| <kbd>e</kbd> | Open Setup |
| <kbd>s</kbd> | Open Setup with the Sample form over it |
| <kbd>v</kbd> | Show the sample's rows, the ones Data Quality reads, in the table viewer |
| <kbd>↑</kbd> <kbd>↓</kbd> <kbd>PgUp</kbd> <kbd>PgDn</kbd> <kbd>Home</kbd> <kbd>End</kbd> in a finding | Scroll a finding taller than the screen; its last row counts the lines below |
| <kbd>p</kbd> | Show the detailed access plan |
| <kbd>Enter</kbd> on a column | Open its detail: its findings, then what was measured; <kbd>Enter</kbd> again returns to the list |
| <kbd>Enter</kbd> on a segment | List its columns' measures beside the compared segment, largest change first |
| <kbd>Enter</kbd> on a Trends line | Open its bars: <kbd>↑</kbd> <kbd>↓</kbd> walk them, each with its span, segments, rows, rate, 95% interval and the bar it is compared with; <kbd>Enter</kbd> or <kbd>Esc</kbd> returns |
| <kbd>Enter</kbd> on an interval | Open its detail: ends, rows with both, missing and unread ends, negative and zero durations, percentiles, breaches; <kbd>↑</kbd> <kbd>↓</kbd> move between the counts and <kbd>Enter</kbd> shows the rows behind one (the kept rows', or a Read Rows dialog first when the run kept none) |
| <kbd>o</kbd> | On Overview, order findings ranked, by rows affected or by rate. In Segments, list the largest change first, or back in order |
| <kbd>m</kbd> | In Trends and a bar's detail, choose the measure the table draws |
| <kbd>w</kbd> | In Trends, stage the next coarser window (or row chunk) in Setup; <kbd>Enter</kbd> there runs it, <kbd>Esc</kbd> puts the grain back |
| <kbd>g</kbd> | In Trends, list the expected windows with no rows; offered once Setup states Expected |
| <kbd>b</kbd> | In Segments, use the selected segment as baseline |
| <kbd>r</kbd> | On a sampled report, run again with a new seed, for every tool |
| <kbd>x</kbd> | Export the report on screen to JSON or Markdown: <kbd>Tab</kbd> moves between the path and the format, <kbd>←</kbd> <kbd>→</kbd> change the format, <kbd>Enter</kbd> writes (asking first over a file that exists), <kbd>Esc</kbd> cancels. Nothing is read |
| <kbd>Tab</kbd> | Move between the result and the Analysis tools |
| <kbd>Esc</kbd> | Back out one layer: a popup, a column's, a segment's, a bar's or an interval's detail and the gaps back to their list, an evidence drill back to the finding or count it came from, a narrowed Overview back to every finding, then Analysis itself. While a run reads, cancel it |

## Pivot and melt

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> | Switch Pivot and Melt (<kbd>h</kbd> <kbd>l</kbd> too, outside text fields); in a text field, move the cursor |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> or <kbd>↑</kbd> <kbd>↓</kbd> | Move between the rows |
| <kbd>Space</kbd> or typing | Open the focused row's picker, narrowed by what you type |
| <kbd>↑</kbd> <kbd>↓</kbd> in the picker | Move; <kbd>Enter</kbd> or <kbd>Space</kbd> chooses; <kbd>Tab</kbd> and <kbd>Shift</kbd>+<kbd>Tab</kbd> choose and move rows |
| <kbd>Space</kbd> where several can be chosen | Toggle a column in or out; <kbd>Enter</kbd> then closes the picker (Done), and <kbd>Esc</kbd> keeps the toggles made so far |
| <kbd>Enter</kbd> | In a single-choice picker, choose; otherwise apply, from anywhere in the form |
| <kbd>Esc</kbd> | Stop a pivot being computed; close the picker; otherwise close without applying |

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

## Row inspector

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Move between fields |
| <kbd>Home</kbd> <kbd>End</kbd> | First and last field |
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Previous and next row; the table's cursor moves with it |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Scroll a long value; on the footer when the value runs past the pane |
| <kbd>Enter</kbd> | Open a struct, a list or JSON text one level down; show more of a long value; or read the row's hidden and binary fields |
| <kbd>→</kbd> <kbd>l</kbd> / <kbd>←</kbd> <kbd>h</kbd> | Inside a level: open the focused item / go up a level (at the row they move between rows) |
| <kbd>y</kbd> | Copy the focused field's exact value |
| <kbd>e</kbd> | Show text or bytes escaped, or as itself; offered only on text and bytes |
| <kbd>/</kbd> | Find a field: type to narrow, <kbd>Enter</kbd> or <kbd>↓</kbd> keeps the list narrowed, <kbd>Esc</kbd> clears it |
| <kbd>Esc</kbd> <kbd>Space</kbd> | Close; <kbd>Esc</kbd> clears a find first, and inside a level goes up one |

## Dataset info

| Key | Action |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> or <kbd>h</kbd> <kbd>l</kbd> | Switch Schema, Model, Audio, MIDI, Resources, Partitions, Notes. Afterward focus rests on the tab bar |
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | On Schema, move between the tab bar and the column table |
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Scroll the column table (when it is focused), move through the notes, or scroll the model's or audio file's metadata or the MIDI tracks |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> <kbd>Home</kbd> <kbd>End</kbd> | On Model, Audio or MIDI, page through the list |
| <kbd>Enter</kbd> | On a note, take its offer, where it has one |
| <kbd>x</kbd> | Show the dataset's file as bytes in the [hex view](../user-guide/hex-view.md), when it is one local file; <kbd>Esc</kbd> there comes back |
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
| <kbd>Enter</kbd> in the form | Save. In the description <kbd>Enter</kbd> types: <kbd>Tab</kbd> out of it, then <kbd>Enter</kbd> — or press <kbd>Ctrl</kbd>+<kbd>J</kbd>, or <kbd>Ctrl</kbd>+<kbd>Enter</kbd> on a terminal that tells it from <kbd>Enter</kbd> |
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

## Keys while busy

The bar at the bottom of the screen shows the ones that matter
most. A spinner in that bar means datui is busy. While it is, at the plain
table <kbd>q</kbd>, <kbd>Q</kbd>, <kbd>←</kbd> <kbd>→</kbd> (<kbd>h</kbd>
<kbd>l</kbd>), <kbd>[</kbd> <kbd>]</kbd>, <kbd>{</kbd> <kbd>}</kbd>,
<kbd>?</kbd> and <kbd>F1</kbd> act at once, and
<kbd>Ctrl</kbd>+<kbd>Q</kbd>, <kbd>Ctrl</kbd>+<kbd>C</kbd> and
<kbd>Ctrl</kbd>+<kbd>O</kbd> act from anywhere. Other keys are queued and
replayed in order once the work is done — except a bare <kbd>Esc</kbd>, or
an <kbd>Enter</kbd> that would drill, which is dropped; an <kbd>Enter</kbd>
that would inspect the row waits as <kbd>Space</kbd>. At most 32 keys are
held, and held keys die with the screen they were typed at. At the loading
screen nothing is held: the allowed keys act, the rest are dropped. While a
view is being applied, <kbd>Esc</kbd> stops it and keeps the table as it was.

## Mouse

The mouse is a shortcut to the keys: it never does what no key does.

| Action | Effect |
|---|---|
| Wheel | <kbd>↑</kbd> <kbd>↓</kbd> three rows a notch, in whatever has the keys: the table, help, the inspector, a sidebar. On the home screen it moves the selection and stops at the first and last row |
| <kbd>Shift</kbd>+wheel, or a sideways wheel | <kbd>←</kbd> <kbd>→</kbd>: the column cursor, at the table only |
| Click a cell | Puts the cursor on its row and column; on a header, its column |
| Click a home row | Selects it |
| Double-click | <kbd>Enter</kbd> on the row: inspect or drill at the table, open on the home screen |
| Click a chip on the bottom bar | Presses its key |

In a text field the wheel does nothing, so it cannot recall history.

Mouse input is never queued. While datui is busy, the sideways wheel and the
busy bar's chips act as their keys would; the wheel down and a click on the
table are dropped, as is a click behind keys already queued.

While datui has the mouse, the terminal's own text selection needs its bypass
modifier: <kbd>Shift</kbd>+drag in most terminals, <kbd>Option</kbd>+drag in
iTerm2. `mouse = false` under `[display]`, or
`--mouse=false`, leaves the mouse to the terminal.

## Terminal notes

If <kbd>F1</kbd> does nothing in Alacritty, it is bound in
`~/.config/alacritty/alacritty.toml`; <kbd>?</kbd> still works outside text
fields.
