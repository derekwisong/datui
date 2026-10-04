# Keyboard shortcuts

<kbd>?</kbd> or <kbd>F1</kbd> shows the keys of the screen you are on;
<kbd>F1</kbd> works in text fields too. The control bar at the bottom shows
the main ones. The tables below are the help each screen shows, generated from
it.

<!-- generated: keys -->
## Table

Where a dataset opens.

### Table · Navigation

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move the row cursor |
| `← / → (h/l)` | Move the column cursor, frozen columns included; the columns scroll only when it would leave the screen |
| `[ / ]` | A page of columns left or right, the cursor on the page's first column (Shift+←/→ too) |
| `{ / }` | First column, last column |
| `g` | Go to a column by name; the cursor goes with it |
| `PgUp/PgDn` | Scroll pages (Ctrl+F down, Ctrl+B up) |
| `Home/End` | Go to first/last row (G = End) |
| `Ctrl+D/Ctrl+U` | Half page down/up |
| `:` | Go to row number (e.g. :0 Enter for top) |
| `f` | Find text or a regex in the view; the cursor, column cursor and all, goes to the first match at or after its row |
| `n / N` | Next / previous match from the cursor's cell, wrapping round the view |
| `Enter` | On a row of a by query or a SQL GROUP BY, drill down to its rows (Esc comes back); elsewhere, inspect the row |
| `Space` | Inspect the row: every field, each value whole and exact (Esc or Space closes). The bar's first chip says what Enter does: Inspect, or Drill |

### Table · Data operations

| Key | Action |
|---|---|
| `/` | Query: SQL, Text or q |
| `c` | Open charts |
| `s` | Open the Sort & Filter sidebar (tabs: Columns, Filters), on the cursor's column |
| `F` | Value counts of the cursor's column: each value's rows, percent and a bar, with a summary |
| `+ / -` | Filter on the cursor's cell: + keeps the rows with its value, - drops them (a null cell: the nulls). Each adds a row to the Filters tab, joined with "and"; the value is the cell's exactly as stored |
| `a` | Open Analysis. In a Data Quality evidence drill a is disabled; Esc returns to the observation |
| `p` | Open Pivot & Melt |
| `e` | Export data to file |
| `y` | Copy to the clipboard (cell, row, view or table); a cell is the cursor's |
| `r` | Reverse sort order (sorted columns carry a direction mark in the header); with no sort, reverse the row order |
| `R` | Reset table: clear the query, filters, sort, column order, hidden columns and widths, frozen columns, pivot/melt, drill-down and the applied view |
| `V` | Apply the best-matching view (or open the list) |
| `v` | Open the views list |

### Table · Display

| Key | Action |
|---|---|
| `i` | Open Info panel (tabs: Schema, Resources, Partitions, Notes). H on its Schema tab reads a CSV's first row as data, or as column names |
| `#` | Toggle row numbers |
| `< / >` | The column cursor's column 4 cells narrower or wider |
| `= / w` | Fit it to the rows on screen; back to automatic width |
| `,` | Toggle number formatting (digit grouping) |
| `D` | Toggle the type row under the column headers |
| `b` | A binary file read through a format spec: read it again with another spec. Clears the query, filters and sort |
| `H / L` | Move the cursor's column one place left or right, the cursor with it; a frozen column moves among the frozen ones. R puts the order back |
| `t` | Follow the file as it grows (CSV, TSV, PSV, NDJSON). Reads it again, so the query, filters and sort are cleared; while following, t pauses and resumes, and Esc stops |
| `? / F1` | Open this help (F1 works in text fields). Esc or ? to close. |

### Table · Help navigation

| Key | Action |
|---|---|
| `↑↓ (j/k)` | Scroll help content |
| `PageUp/PageDown` | Scroll help pages |
| `Home/End` | Jump to top/bottom |

### Table · Exit

| Key | Action |
|---|---|
| `Ctrl+O` | Home screen (works during a load; abandons it) |
| `Ctrl+Q` | Quit from anywhere (Ctrl+C too) |
| `q` | Back to the home screen when the dataset was opened from it; otherwise quit (the control bar says which) |
| `Q` | Quit |

### Table · Mouse

| Key | Action |
|---|---|
| `Click` | Put the cursor on the cell; on a header, its column |
| Double-click | Enter on the row |
| `Wheel` | ↑ / ↓, three rows a notch; the same in help, the inspector and the sidebars |
| Shift+wheel | ← / →, the column cursor (a sideways wheel too) |
| Click a chip | Press its key |

## Home screen

`datui` with no path, or <kbd>Ctrl</kbd>+<kbd>O</kbd> from anywhere.

### Home screen · Navigation

| Key | Action |
|---|---|
| `↑ / ↓` | Move the selection (Ctrl+P / Ctrl+N too) |
| `Ctrl+↑ / Ctrl+↓` | Previous or next section |
| `PgUp/PgDn` | Move a screenful, stopping at the first and last |
| `Home / End` | The first or last row |
| `← / →` | Fold or unfold a section; → on a directory or a file of tables goes inside it |
| `Enter` | What the control bar says on this row: "Open all" reads a whole directory as one table, "Inside" steps into it, "Open" loads a file, "Look" finds out first. A place a collection suggests, indented under its dataset, opens whole. On a section header, fold or unfold it; on the More row, show the rest; on the hidden-files row, show them |
| `Space` | While the filter is empty: fold or unfold the section header under the cursor. With a filter typed, it types |
| `Backspace` | Delete a filter character; on an empty filter, up a level (from a bucket, back to its cloud source; from the top of a collection's remote dataset, back here) |
| `Tab` | Cycle the sort; the control bar names the order in effect when it has room |
| `Esc` | Back out one layer: the path prompt, the filter, the directory (back to the row it was entered from), then to the open table, which the control bar's chip names |
| `Click` | Select the row; double-click is Enter |
| `Wheel` | Move the selection three rows, stopping at the ends |

### Home screen · Finding

| Key | Action |
|---|---|
| `(type)` | Narrow by name or column; fuzzy, so "sal" finds "sales". What you open often ranks first. Typing also searches below the directory you are inside and the listed bucket names; matches appear under "Found" |
| `~` | While the filter is empty: type a path or URL by hand. The list shows the directory being typed, narrowed by the name after the last /. The prompt is a plain editor: characters, Backspace, Ctrl+U clears, Tab completes the one name left or what the names share, ↑ / ↓ pick a name, Enter opens a file or goes inside a directory, Esc closes. s3://, gs:// and az:// complete from buckets and prefixes already known. With a filter typed, ~ types into it |
| `Ctrl+U` | Clear the filter or path input |
| `Ctrl+R` | List again what is on screen |
| `Ctrl+A` | Show or hide files datui cannot read; inside a SQLite database, its internal tables |
| `Ctrl+X` | Show the local file under the cursor as bytes, in the hex view, whatever datui would read it as |
| `Ctrl+D` | Remember the directory under the cursor, so it stays listed; again to forget it. A file stands for the directory it is in, a heading for the one it lists |
| `Delete` | Forget the highlighted recent entry, or a whole place after confirming, or a remembered place on its heading, or hide a cloud source |
| `Shift+Delete` | Forget every recent entry, after confirming |

### Home screen · Help and leaving

| Key | Action |
|---|---|
| `? / F1` | This help (? before typing starts; F1 always) |
| `Ctrl+Q / Ctrl+C` | Quit |

## Query prompt

<kbd>/</kbd> at the table.

### Query prompt · Keys

| Key | Action |
|---|---|
| `Enter` | Run the query (reopening / restores the last query, selected: typing replaces it, arrows edit it). On the tab bar, Enter returns to the input |
| `Ctrl+T` | Next mode: SQL, Text, q (from the input too) |
| `Shift+Tab` | Input to the tab bar and back |
| `Tab` | SQL: complete a column name or df; again for the next match. Text, q: to the tab bar |
| `Alt+Enter` | SQL: start a new line |
| `← / → (h/l)` | On the tab bar: switch mode |
| `↑ / ↓` | Earlier and later queries from the history (Ctrl+P / Ctrl+N too; each mode keeps its own). In SQL over several lines, they move between lines first |
| `Ctrl+U / Ctrl+K` | Delete to the start / end of the line |
| `Ctrl+Z / Ctrl+R` | Undo / redo |
| `Ctrl+J` | Run, the same as Enter |
| `F1` | Show this help (works from the input) |
| `Esc` | Close |

## Find

<kbd>f</kbd> at the table.

| Key | Action |
|---|---|
| (text) | What to find. Plain text, or a regex with Ctrl+R |
| `Enter` | Find: the cursor, column cursor and all, goes to the first match at or after its row. On an empty field, clear the find |
| `Ctrl+R` | Regex on or off |
| `Ctrl+L` | Only the column cursor's column, or every column shown |
| `↑↓` | Earlier patterns (Ctrl+P / Ctrl+N too) |
| `Esc` | Cancel |
| `F1` | Show this help |

### Find · At the table

| Key | Action |
|---|---|
| `n / N` | Next / previous match from the cursor's cell. Past the last match the find comes round to the first, and the bar says so. Each one typed while a find reads runs in turn; Esc stops them all |
| `f` | Find again, the last pattern ready to edit |
| `Esc` | Clear the find (or stop one still reading) |

## Go to row

<kbd>:</kbd> at the table.

| Key | Action |
|---|---|
| (digits) | The row to go to (:0 Enter is the top) |
| `Enter` | Go |
| `Backspace` | Delete a digit |
| `Esc` | Cancel |
| `F1` | Show this help |

## Go to column

<kbd>g</kbd> at the table.

| Key | Action |
|---|---|
| `(type)` | Narrow the list to the names that contain it |
| `↑ / ↓` | Move |
| `Enter` | Go. The column cursor moves to it. A column already whole on screen stays where it is; another becomes the first after the frozen ones, or lands on the last page when it is there |
| `Backspace` | Delete a character (Ctrl+W a word, Ctrl+U all) |
| `Esc` | Close without moving |
| `F1` | Show this help |

## Inspector

<kbd>Space</kbd> at the table.

### Inspector · The fields

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move between fields |
| `Home / End` | First and last field |
| `PgUp / PgDn` | A page of fields |
| `← / → (h/l)` | Previous and next row; the table's cursor moves with it |
| `Tab` | Move into the value, to scroll and find in it |
| `Enter` | On a group's row, its rows, as at the table. Else open a struct, a list, or text holding a JSON object or array; or read a field the table's rows do not hold (hidden and binary columns) |
| `r` | On a group's row, read a field the rows do not hold |
| `y` | Copy the focused value as its view shows it |
| `Y` | Copy the whole row as one JSON object |
| `e` | The value's next view, where it has more than one |
| `w` | Word wrap or hard wrap for long text |
| `o` | Open the value in another program |
| `f` | Only the fields with a value; comparing, only those that differ |
| `s` | Order: the table's, A-Z, or filled first |
| `c` | Compare: a column for the next row, or the pinned one |
| `m` | Pin this row to compare others with; again to unpin |
| `/` | Find a field by name, then by value: type to narrow, Enter or ↓ keeps the list narrowed, Esc clears it |
| `? / F1` | Show this help (in a find line ? types; use F1) |
| `Esc / Space` | Close; Esc clears a find first |

### Inspector · The value, after Tab

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Scroll a line |
| `PgUp / PgDn` | Scroll a page |
| `Home / End` | Top and end, at once however long the value |
| `/` | Find in the value; n and N go to the next and last place |
| `e, w, y, o` | As in the fields |
| `← / → (h/l)` | Previous and next row |
| `Esc / Tab` | Back to the fields; Esc clears a find first |

### Inspector · Inside a level

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move between items |
| `Enter / → (l)` | Open the focused item |
| `Esc / ← (h)` | Up a level; at the row, Esc closes |
| `y` | Copy the focused item: text as itself, a JSON object or array indented |
| `Space` | Close |

## Info panel

<kbd>i</kbd> at the table.

| Key | Action |
|---|---|
| `Tab / Shift+Tab` | On Schema tab: move focus (tab bar ↔ schema table). On other tabs: focus stays on tab bar. |
| `← / → (h/l)` | Switch tabs. Afterward focus rests on the tab bar, so Tab returns to the body before ↑/↓ scroll |
| `↑ / ↓ (j/k)` | Schema tab (when focused) or Notes tab: move the cursor. Model, Audio, MIDI, Metadata and format tabs: scroll the list |
| `PgUp / PgDn` | Model, Audio, MIDI, Metadata and format tabs: scroll the list a page |
| `Home / End` | Model, Audio, MIDI, Metadata and format tabs: the top or the end of the list |
| `Enter` | Notes tab: take the offer on the note, where it has one |
| `H` | Schema tab, CSV, TSV, PSV: read the first row as data, under column_1, column_2, …; again to read it as column names. Reads the file again, so the query, filters and sort are cleared, and the panel closes |
| `? / F1` | Show this help |
| `Esc / i` | Close info panel |

## Value counts

<kbd>F</kbd> at the table.

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move |
| `PgUp/PgDn` | A page |
| `Home/End` | First and last row |
| `← / → (h/l)` | The previous or next column; the table's column cursor moves with it |
| `Enter` | The rows holding the value, as a drill-down; Esc there comes back here |
| `s` | Sort by count or by value |
| `a` | Count every row, when the counts are of a sample |
| `y` | Copy the counts as TSV: every value, its count, percent and cumulative percent |
| `e` | Export the counts to a file |
| `t` | While following a file, count again with the rows that arrived since; the bar says how many |
| `Esc` | Back to the table; while every row is being counted, stop that and keep the sample |
| `? / F1` | Show this help |

## Sort and filter

<kbd>s</kbd> at the table.

| Key | Action |
|---|---|
| `Tab / Shift+Tab` | Move focus between the tab bar and the body |
| `← / →` | Switch Columns / Filters (h/l on the tab bar) |
| `Enter` | Apply everything staged and close (on the Filters tab, Enter adds or edits instead; see below) |
| `a` | On the Filters tab, outside the row editor: apply and close |
| `Ctrl+J` | Apply from anywhere, including mid-edit (the row in progress is saved). Ctrl+Enter does the same, on a terminal that tells it from Enter. |
| `? / F1` | Show this help (F1 works in text fields) |
| `Esc` | Cancel and close; staged changes are discarded, and reopening shows what is applied |

### Sort and filter · Columns tab

| Key | Action |
|---|---|
| `(type)` | Narrow the column list, when the find field is focused |
| `Space` | Cycle the column's sort: none → ascending → descending (each column carries its own direction) |
| `1-9` | Put the column at that place in the sort order; 0 removes it (a digit past the end of the order says so) |
| `Del` | Remove the column from the sort |
| `[ / ]` | Move the column earlier or later in the sort order |
| `+ / - (= / _)` | Move the column's display position |
| `L` | Freeze this column and every column up to it at the left edge; on a column already frozen, pull the boundary back |
| `v` | Show or hide the column (it keeps its place, dimmed) |
| `< / > (, / .)` | Make the column 4 cells narrower or wider. A number column is never narrower than its numbers |
| `f` | Fit the column to the rows on screen, its name up to the automatic limit |
| `w` | Back to the automatic width |
| `C` | Clear the staged sort, order, locks, hidden columns and widths |

### Sort and filter · Filters tab

| Key | Action |
|---|---|
| `Enter` | Edit the row under the cursor, or add one on the last row. Editing walks three steps on the row: pick the column (type to narrow, Enter chooses), pick the operator the same way, then type the value and Enter saves the row. Tab, → and Space also choose at the column and operator steps; Shift+Tab steps back. ↑↓ (j/k) move in both lists, and ↓ jumps from the find field into the list. Esc backs out of the edit and only the edit. |
| `Space` | Toggle and/or on the row |
| `d / Del` | Delete the row |
| `C` | Clear every staged filter |
| Date | 2024-01-01 |
| Date and time | 2024-01-01, 2024-01-01 05:30, 2024-01-01T05:30:00.25, a clock in the column's zone; +01:00 or Z names an instant |
| Time | 05:30, 05:30:00.25 |
| Duration | 1d 2h 30m, 90s, 1500ms (d h m s ms us ns) |
| Decimal | 1.5, at the column's scale |

## Pivot and melt

<kbd>p</kbd> at the table.

| Key | Action |
|---|---|
| `Tab / Shift+Tab` | Move between the rows (↑/↓ too); in a picker: choose and move to the next or previous row |
| `← / →` | Switch Pivot and Melt (h/l too, outside text fields); in a text field ←/→ move the cursor |
| `Space or typing` | Open the row's picker, narrowed by what you type |
| `Enter` | Apply the spec echoed above the footer |
| `Esc` | Close without applying; while a pivot is computed, stop it and keep the form |
| `? / F1` | Show this help (F1 works in text fields) |

## Chart

<kbd>c</kbd> at the table.

| Key | Action |
|---|---|
| `1-6` | Switch chart type directly ([ / ] cycle) |
| `Tab / Shift+Tab` | Move between the option rows (↑/↓ too) |
| `Enter / Space` | Open a column row's picker, toggle an option, or cycle the plot style, range or order |
| `← / → (h/l)` | Cycle the plot style, range or order; adjust bins, bandwidth, or Sample size (+ / - too) |
| `PgUp/PgDn` | Adjust Sample size in bigger steps |
| Sample size | Rows a chart reads. A larger table is sampled across all of it, and the chart says so below the plot |
| Range | Histogram, Box Plot and KDE: all values, or p1-p99 (the 1st to 99th percentile); the chart counts the values left out |
| Bar | One bar per category: a text, categorical, boolean or integer column. Count counts the rows per category, exactly, over the whole view; a numeric value takes one row per category (group first with a query). Order by value or label; bars that do not fit are counted |
| `g` | Grid on or off at the labeled ticks: XY, Histogram, Box Plot and KDE. [analysis] chart_grid sets where it starts |
| `x` | XY: the plot takes the keys, and a crosshair reads out x and every series' value under the plot. ← / → (h/l) step to the next point or column, Home/End go to the ends; x, Tab or Esc hand the keys back to the option rows. A click on the plot puts the crosshair there |
| `e` | Export the chart to PNG or EPS. Needs the chart's required columns picked first |
| `t` | While following a file, draw again with the rows that arrived since; the bar says how many |
| `? / F1` | Show this help (with a picker open ? types into the filter; F1 still opens help) |
| `Esc` | Back to the table |

### Chart · Export dialog

| Key | Action |
|---|---|
| `Tab / Shift+Tab` | Cycle Format → Path → Title → Width → Height |
| `↑ / ↓ (j/k)` | Change the format |
| `Enter` | Export, from anywhere in the dialog |
| `Esc` | Back to the chart |

## Analysis: Describe

<kbd>a</kbd> at the table.

### Analysis: Describe · Navigation

| Key | Action |
|---|---|
| `Tab` | Switch focus between main area and sidebar |
| `↑↓ / j/k` | Navigate rows (or sidebar tools if sidebar focused) |
| `←→ / h/l` | Scroll the statistics; the header counts those out of view |
| `Home/End` | Jump to first/last row |
| `PgUp/PgDn` | Navigate by page |
| `Enter` | Select tool from sidebar (when sidebar focused). The first run on a dataset starts in the Sample form, where Enter runs it; later tools reuse that sample |

### Analysis: Describe · Actions

| Key | Action |
|---|---|
| `s` | Choose the sample every tool reads: which rows, how they are picked, how many, the seed |
| `v` | View the sample's rows as a table; Esc comes back |
| `r` | Draw another sample (when the result is a sample) |
| `a` | Read every row instead, after confirming |
| `t` | While following a file, read again with the rows that arrived since the results were read |
| `Esc` | Cancel a run in progress; otherwise close the analysis view or this help |

## Analysis: Distribution

Distribution in the Analysis sidebar.

### Analysis: Distribution · Navigation

| Key | Action |
|---|---|
| `↑↓ / j/k` | Navigate rows |
| `←→ / h/l` | Scroll the statistics; the header counts those out of view |
| `Home/End` | Jump to first/last row |
| `PgUp/PgDn` | Navigate by page |
| `Tab` | Switch focus between main area and sidebar |
| `Enter` | Open detail view for selected column (shows Q-Q plot and histogram); with the sidebar focused, select a tool |
| `Esc` | Cancel a run in progress; otherwise close the analysis view or this help |
| `s` | Choose the sample every tool reads: which rows, how they are picked, how many, the seed |
| `v` | View the sample's rows as a table; Esc comes back |
| `r` | Draw another sample (when the result is a sample) |
| `a` | Read every row instead, after confirming |
| `t` | While following a file, read again with the rows that arrived since the results were read |

## Analysis: Distribution detail

<kbd>Enter</kbd> on a column in Distribution.

## Analysis: Correlation

Correlation Matrix in the Analysis sidebar.

### Analysis: Correlation · Navigation

| Key | Action |
|---|---|
| `Tab` | Switch focus between main area and sidebar |
| `↑↓ / j/k` | Navigate matrix rows (or sidebar tools if sidebar focused) |
| `←→ / h/l` | Navigate matrix columns |
| `Home/End` | Jump to the first/last corner cell (the column resets too) |
| `PgUp/PgDn` | Navigate by page |
| `Enter` | Open pair detail view (on a cell) or select tool (sidebar); does nothing on a diagonal cell |

### Analysis: Correlation · Actions

| Key | Action |
|---|---|
| `s` | Choose the sample every tool reads: which rows, how they are picked, how many, the seed |
| `v` | View the sample's rows as a table; Esc comes back |
| `r` | Draw another sample (when the result is a sample) |
| `a` | Read every row instead, after confirming |
| `t` | While following a file, read again with the rows that arrived since the results were read |
| `Esc` | Cancel a run in progress; otherwise close the analysis view or this help |

## Analysis: Correlation detail

<kbd>Enter</kbd> on a pair in the correlation matrix.

### Analysis: Correlation detail · Keys

| Key | Action |
|---|---|
| `?` | Toggle this help |
| `Esc` | Return to the correlation matrix |

## Analysis: Data Quality

Data Quality in the Analysis sidebar.

### Analysis: Data Quality · Setup

| Key | Action |
|---|---|
| Rows & sample | The sample every tool reads |
| Columns | Text read as time, time roles, the intervals between them, and what each column must hold |
| Study | Grain, expected windows, comparison, values, latency threshold, and the window intervals go in |
| Read | What Run will read, before it reads |
| `↑ / ↓, Tab` | Move between rows |
| `← / →` | Change a short list's choice |
| `Space` | Open the row: the Sample form, a list (type to narrow), Time roles, Intervals, Column intent or Expected |
| `s` | The Sample form; its Enter applies to Setup and returns there |
| `p` | The access plan: what a run reads |
| `d` | Release the rows runs kept and a full scan's local copy, named on the Read rule; the next run that would reuse them reads again |
| `Enter` | Run, from any row. A full read asks first; the report on screen or in the session cache is shown, not read again |
| `Esc` | Discard every staged edit |

### Analysis: Data Quality · Time roles

| Key | Action |
|---|---|
| `↑ / ↓` | Choose the role |
| `← / →` | Choose its column |
| `Enter` | Done |
| `Esc` | Put back the roles as they were |

### Analysis: Data Quality · Intervals in Setup

| Key | Action |
|---|---|
| `↑ / ↓` | Choose the pair |
| `Space` | Measure it or not; ← / → too |
| `Enter` | Done |
| `Esc` | Put back the pairs as they were |

### Analysis: Data Quality · Column intent

| Key | Action |
|---|---|
| `↑ / ↓` | Choose the column |
| `Space` | Declare its intent in a form |
| `Enter` | Done |
| `Esc` | Put back the intent as it was |

### Analysis: Data Quality · Expected in Setup

| Key | Action |
|---|---|
| `↑ / ↓, Tab` | Windows, From, Before |
| `← / →` | Choose the windows |
| `Enter` | Done |
| `Esc` | Put back Expected as it was |

### Analysis: Data Quality · Keys on the report

| Key | Action |
|---|---|
| `← / → (h/l)` | Previous or next page |
| `1 - 5` | Overview, Columns, Segments, Trends, Intervals |
| `e` | Setup |
| `↑ / ↓ (j/k)` | Move, or scroll a tall finding |
| `PgUp/PgDn` | Page; Home/End jump to either end |
| `Enter` | Open a finding, then its rows. On an empty page, open the setting it needs |
| `c / t` | Overview: only one column's or one type's findings |
| `o` | Overview: ranked, by rows, by rate |
| `s` | Setup, with the Sample form open |
| `v` | View the sample's rows; Esc returns |
| `p` | Show the access plan: what a run reads |
| `r` | On a sample, run again with a new seed |
| `x` | Export the report to JSON or Markdown; nothing is read |
| `Tab` | Move between the result and the tools |
| `Esc` | Back one level: all findings again, then close |
| `? / F1` | Show this help |

### Analysis: Data Quality · Segments

| Key | Action |
|---|---|
| `Enter` | A segment's columns beside the one it is compared with, largest change first |
| `o` | Largest change first, or back in order |
| `b` | Compare with the highlighted segment |

### Analysis: Data Quality · Trends

| Key | Action |
|---|---|
| `Enter` | A line's bars: span, segments, rows sampled of counted, rate, 95% interval, and the bar before |
| `↑ / ↓` | In the bars, the next bar |
| `m` | Next measure |
| `w` | Stage a coarser window in Setup, for segments the sample reached thinly or not at all |
| `g` | The expected windows with no rows: empty by exact count, not sampled, or out of scope |
| `Esc` | Back to Trends |

### Analysis: Data Quality · Intervals page

| Key | Action |
|---|---|
| `Enter` | An interval's detail: its ends, the rows with both, missing and unread ends, negative and zero durations, percentiles and breaches. Enter there shows the rows behind the count under the cursor: a sample's from the rows the run kept |
| `Esc` | Back to the list |

## Export

<kbd>e</kbd> at the table.

| Key | Action |
|---|---|
| `Tab / Shift+Tab` | Move focus between fields |
| `↑ / ↓ (j/k)` | In the format list: change format On Compression: change compression In Path and Delimiter: nothing (no history there, and j/k are characters) |
| `← / → (h/l)` | Move the cursor in text fields On Compression: change compression On Include header and Source file: move focus |
| `Space` | Toggle a checkbox (Include header, Source file) |
| `Enter` | Export, from anywhere in the form. On a blank path the form says "Enter a file path." instead of exporting |
| `? / F1` | Show this help (F1 works in text fields) |
| `Esc` | Close without exporting |

## Copy

<kbd>y</kbd> at the table.

| Key | Action |
|---|---|
| `Tab / Shift+Tab` | Move focus between rows; in an open picker: choose |
| `↑ / ↓` | Move focus; in an open picker: move the cursor (j/k narrow the picker; only ↑/↓ move there) |
| `Space` | Open the focused row's picker; on Header: toggle In an open picker: choose |
| `Enter` | Copy, from anywhere in the form In an open picker: choose On the Cell scope with no column picked, Enter re-accents the spec line instead of copying |
| `type` | In an open picker: narrow the list |
| `? / F1` | Show this help (F1 works in text fields; with a picker open ? narrows, so use F1) |
| `Esc` | Close a picker, then the dialog, without copying |

## Views

<kbd>v</kbd> at the table.

### Views · Views list

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move in the list |
| `Enter` | Apply the selected view |
| `s` | Save the current state as a view (an untouched table has nothing to save, and s says so) |
| `e` | Edit the selected view |
| `d` | Delete the selected view, after confirming |
| `i` | Show how the selected view's score was computed (Esc closes the score view) |
| `? / F1` | Show this help (F1 works in text fields) |
| `Esc` | Close |

### Views · Save and edit form

| Key | Action |
|---|---|
| `Tab / Shift+Tab` | Move between rows (↑/↓ outside the description) |
| `Enter` | Save. In the description Enter types: Tab out of it, then Enter |
| `Ctrl+J` | Save from anywhere, the description included; so does Ctrl+Enter, on a terminal that tells it from Enter |
| `PgUp / PgDn` | Move five lines in the description |
| `Space` | Expand or collapse Matching; toggle schema match |
| `Esc` | Back to the list, discarding edits |

### Views · Delete confirmation

| Key | Action |
|---|---|
| `Enter (d / D)` | Delete |
| `Esc` | Cancel |

## Format picker

<kbd>b</kbd> at a table read through a format spec.

| Key | Action |
|---|---|
| `(type)` | Narrow the list to the names that contain it |
| `↑ / ↓` | Move |
| `Enter` | Read the file again with the spec chosen. The query, filters and sort are cleared |
| `Backspace` | Delete a character (Ctrl+W a word, Ctrl+U all) |
| `Esc` | Close and keep the format |
| `F1` | Show this help |

## Hex view

`datui --hex FILE`, <kbd>Ctrl</kbd>+<kbd>X</kbd> on the home screen, or <kbd>x</kbd> in the Info panel.

### Hex view · Moving

| Key | Action |
|---|---|
| `← / → (h/l)` | A byte |
| `↑ / ↓ (j/k)` | A row |
| `w / b` | The next group of four, or back one |
| `0 / $` | The start or end of the row |
| `g / G` | The start or end of the file (Home and End too) |
| `PgUp/PgDn` | A page (Ctrl+B/F too; Ctrl+U/D half a page) |
| `:` | Go to an offset: 4096 or 0x1000; +16 or -16 from the cursor; e-8 for the eighth byte from the end |

### Hex view · Finding

| Key | Action |
|---|---|
| `f` | Find bytes. Text is found as its UTF-8 bytes (Ctrl+U in the prompt: as UTF-16 little-endian). 0x1acffc1d, or two or more hex pairs (de ad be ef), is a byte pattern, where ?? matches any byte. Text in double quotes is text, even when it looks like hex. A match may span rows |
| `n / N` | The next or previous match, round the end of the file |
| `Esc` | Stop a find that is reading |

### Hex view · Rows

| Key | Action |
|---|---|
| `r` | Bytes per row, so that records line up; empty goes back to as many as fit. --hex-width N sets it on the command line |
| `#` | Offsets in decimal or hex |

### Hex view · The byte inspector

| Key | Action |
|---|---|
| `i / Enter` | Show or hide it. Beside the bytes when there is room for it and 16 bytes a row, over them when there is not |
| `v` | Mark from the cursor; move to mark a range, and the status line counts it. v again, or Esc, unmarks |

### Hex view · Leaving

| Key | Action |
|---|---|
| `B` | Read the file with a format spec instead |
| `Esc` | Back to the table, when opened from the Info panel; back home, when opened from there |
| `q` | Home, when opened from there; otherwise quit |
| `? / F1` | Show this help |
<!-- end generated: keys -->

## Text fields

Every text field, from the query prompt to a file path, edits the same way,
with two simpler exceptions: a picker's type-to-narrow filter takes
characters and <kbd>Backspace</kbd>, plus <kbd>Ctrl</kbd>+<kbd>W</kbd> to
drop a word and <kbd>Ctrl</kbd>+<kbd>U</kbd> to clear; and the home
screen's <kbd>~</kbd> path prompt takes characters, <kbd>Backspace</kbd>,
<kbd>Ctrl</kbd>+<kbd>U</kbd>, <kbd>Tab</kbd> to complete, <kbd>↑</kbd>
<kbd>↓</kbd> to pick from the list, <kbd>Enter</kbd> and <kbd>Esc</kbd>.

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
| <kbd>Ctrl</kbd>+<kbd>J</kbd> | The same as <kbd>Ctrl</kbd>+<kbd>Enter</kbd>, on every terminal: saves a view from its description and applies Sort & Filter. In the query and go-to-row prompts it submits, like <kbd>Enter</kbd> |
| <kbd>Ctrl</kbd>+<kbd>C</kbd> | Copy the selection (does not quit while a text field is focused) |
| <kbd>F1</kbd> | Help |

## Help overlay

| Key | Action |
|---|---|
| <kbd>↑</kbd> <kbd>↓</kbd> or <kbd>j</kbd> <kbd>k</kbd> | Scroll |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | A page |
| <kbd>Home</kbd> <kbd>End</kbd> | Top and bottom |
| <kbd>Esc</kbd> or <kbd>?</kbd> | Close |

## Keys while busy

A spinner in the control bar means datui is busy. While it is, at the plain
table <kbd>q</kbd>, <kbd>Q</kbd>, <kbd>←</kbd> <kbd>→</kbd> (<kbd>h</kbd>
<kbd>l</kbd>), <kbd>[</kbd> <kbd>]</kbd>, <kbd>{</kbd> <kbd>}</kbd>,
<kbd>#</kbd>, <kbd>,</kbd>, <kbd>D</kbd>, the width keys, <kbd>?</kbd> and <kbd>F1</kbd> act
at once, as do <kbd>↑</kbd> <kbd>↓</kbd> (<kbd>j</kbd> <kbd>k</kbd>) inside
the rows already read while all that is awaited is more rows; and
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
| Click a chart's plot | XY: the crosshair on the point nearest, as <kbd>x</kbd> and <kbd>←</kbd> <kbd>→</kbd> would |
| Double-click | <kbd>Enter</kbd> on the row: inspect or drill at the table, open on the home screen |
| Click a chip on the bottom bar | Presses its key |

In a text field the wheel does nothing, so it cannot recall history.

Mouse input is never queued. While datui is busy, the sideways wheel and the
busy bar's chips act as their keys would; the wheel down and a click on the
table are dropped, as is a click behind keys already queued.

To select text with the mouse while datui has it, see
[Mouse and text selection](../user-guide/configuration.md#mouse-and-text-selection).
