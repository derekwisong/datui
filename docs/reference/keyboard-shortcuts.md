# Keyboard shortcuts

<kbd>?</kbd> or <kbd>F1</kbd> shows the keys of the screen you are on, grouped
by task; <kbd>F1</kbd> works in text fields too. In the help, <kbd>/</kbd>
narrows the keys to those whose text matches, and <kbd>Enter</kbd> closes the
help and presses the key on the selected line. `datui man keys` prints this
page. The tables below hold every screen's keys, with the longer description
where the help shows one line.

<!-- generated: keys -->
## Every screen

These keys work on every screen.

| Key | Action |
|---|---|
| `? / F1` | This screen's keys. In a text field ? types, and F1 opens the help |
| `Ctrl+O` | The home screen; abandons a load |
| `Ctrl+Q / Ctrl+C` | Quit from anywhere; Ctrl+C quits from a text field too, where Alt+W copies |

## Help

<kbd>?</kbd> or <kbd>F1</kbd>.

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move between the keys |
| `Enter` | Close the help and press the key on the line, where help was opened |
| `/` | Narrow to the keys whose text matches |
| `Esc` | Clear the filter, then close |

## Table

Where a dataset opens.

### Table · Explore

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move the row cursor |
| `← / → (h/l)` | Move the column cursor, frozen columns included; the columns scroll only when it would leave the screen |
| `Shift+← / Shift+→` | A page of columns left or right, the cursor on the page's first column |
| `{ / }` | First column, last column |
| `PgUp / PgDn` | A page up or down (Ctrl+B / Ctrl+F too) |
| `Ctrl+D / Ctrl+U` | Half a page down or up |
| `Home / End` | First or last row (G = End) |
| `:` | The command line: digits go to that row (the prefix says row:), anything else runs as SQL or q, as the prefix says; Ctrl+T switches. With a query in effect it opens on the query's text |
| `g` | Go to a column by name |
| `/ (f)` | Find text, a regex, or letters in order in the view; matches on screen light up as you type, and Enter takes the cursor, column cursor and all, to the first at or after its row. f opens it too |
| `n / N` | Next / previous match from the cursor's cell, wrapping round the view |
| `Enter` | On a row of a by query or a SQL GROUP BY, drill down to its rows (Esc comes back); elsewhere, inspect the row |
| `Space` | Inspect the row: every field, each value whole and exact (Esc or Space closes) |

### Table · Shape

| Key | Action |
|---|---|
| `[ / ]` | Sort by the cursor's column, [ ascending and ] descending, replacing the sort in effect; the same key again on that column removes it. The s sidebar adds secondary sorts |
| `s` | Open the Sort & Filter sidebar (tabs: Columns, Filters), on the cursor's column |
| `+ / -` | Filter on the cursor's cell: + keeps the rows with its value, - drops them (a null cell: the nulls). Each adds a filter to the Sort & Filter sidebar, joined with "and"; the value is the cell's exactly as stored |
| `r` | Reverse sort order (sorted columns carry a direction mark in the header); with no sort, reverse the row order |
| `H / L` | Move the cursor's column one place left or right, the cursor with it; a frozen column moves among the frozen ones. R puts the order back |
| `p` | Pivot or melt |
| `R` | Reset table: clear the query, filters, sort, column order, hidden columns and widths, frozen columns, pivot/melt, drill-down and the applied view |

### Table · Analyze

| Key | Action |
|---|---|
| `F` | Value counts of the cursor's column: each value's rows, percent and a bar, with a summary |
| `a` | Open Analysis. In a Data Quality evidence drill a is disabled; Esc returns to the observation |
| `c` | Chart the view |
| `i` | Open the Info panel (tabs: Schema, Resources, Partitions, Notes). H on its Schema tab reads a CSV's first row as data, or as column names |

### Table · Output

| Key | Action |
|---|---|
| `e` | Export the view to a file |
| `y` | Copy to the clipboard (cell, row, view or table); a cell is the cursor's |
| `v` | The saved views list |
| `V` | Apply the best-matching view (or open the list) |

### Table · Display

| Key | Action |
|---|---|
| `#` | Row numbers on or off |
| `,` | Number formatting (digit grouping) on or off |
| `D` | The type row under the headers on or off |
| `< / >` | The cursor's column 4 cells narrower or wider |
| `= / w` | Fit the cursor's column to the rows on screen; w puts it back to automatic width |
| `b` | A binary file read through a format spec: read it again with another spec. Clears the query, filters and sort |
| `t` | Follow the file as it grows (CSV, TSV, PSV, NDJSON). Reads it again, so the query, filters and sort are cleared; while following, t pauses and resumes, and Esc stops |

### Table · Go

| Key | Action |
|---|---|
| `q` | Back to the home screen when the dataset was opened from it; otherwise quit |
| `Q` | Quit |
| `Esc` | Leave a drill-down, stop a find or a follow |

### Table · Mouse

| Key | Action |
|---|---|
| `Click` | The cursor to the cell; on a header, its column |
| `Double-click` | Enter on the row |
| `Wheel` | ↑ / ↓, three rows a notch; the same in help, the inspector and the sidebars |
| `Shift+wheel` | ← / →, the column cursor (a sideways wheel too) |
| `Click a key` | Press a key the footer shows |

## Home screen

`datui` with no path, or <kbd>Ctrl</kbd>+<kbd>O</kbd> from anywhere.

### Home screen · Explore

| Key | Action |
|---|---|
| `↑ / ↓` | Move the selection (Ctrl+P / Ctrl+N too) |
| `Ctrl+↑ / Ctrl+↓` | Previous or next section |
| `PgUp / PgDn` | A screenful, stopping at the first and last |
| `Home / End` | The first or last row. The filter has no cursor to move: it is edited at its end |
| `← / →` | Fold or unfold a section; → on a directory or a file of tables goes inside it |
| `Space` | While the filter is empty: fold or unfold the section header under the cursor. With a filter typed, it types |
| `Tab` | Cycle the sort; the footer names the order in effect when it has room |

### Home screen · Go

| Key | Action |
|---|---|
| `Enter` | What the footer says on this row: "Open all" reads a whole directory as one table, "Inside" steps into it, "Open" loads a file, "Look" finds out first. A catalog bookmark, indented under its dataset, opens whole. On a section header, fold or unfold it; on the More row, show the rest; on the hidden-files row, show them |
| `Backspace` | Delete a filter character; on an empty filter, up a level (from a bucket, back to its cloud source; from the top of a catalog's remote dataset, back here) |
| `Esc` | Back out one layer: the path prompt, the filter, the directory (back to the row it was entered from), then to the open table |

### Home screen · Find

| Key | Action |
|---|---|
| `(type)` | Narrow by name or column; fuzzy, so "sal" finds "sales". What you open often ranks first. Typing also searches below the directory you are inside and the listed bucket names; matches appear under "Found" |
| `~` | While the filter is empty: type a path or URL by hand. The list shows the directory being typed, narrowed by the name after the last /. The prompt is a plain editor: characters, Backspace, Ctrl+U clears, Tab completes the one name left or what the names share, ↑ / ↓ pick a name, Enter opens a file or goes inside a directory, Esc closes. s3://, gs:// and az:// complete from buckets and prefixes already known. With a filter typed, ~ types into it |
| `Ctrl+U` | Clear the filter or path input |
| `Ctrl+R` | List again what is on screen |

### Home screen · Manage

| Key | Action |
|---|---|
| `Ctrl+A` | Show or hide files datui cannot read; inside a SQLite database, its internal tables |
| `Ctrl+X` | Show the local file under the cursor as bytes, in the hex view, whatever datui would read it as |
| `Ctrl+D` | Add the dataset or directory under the cursor to catalog.toml, listed under My datasets; on a row from catalog.toml, forget it. A heading stands for the directory it lists. Only catalog.toml is written; another catalog is hidden with home.hide |
| `Ctrl+E` | Open the Documentation view of a catalog row, or of a place inside one: publisher, license, links, columns with units and value legends, bookmarks. ↑ / ↓ move, Enter opens a column's legend, y copies the line's link or value, Esc goes back. Ctrl+E here is not readline's end of line: the filter is edited at its end |
| `Delete` | Forget the highlighted recent entry, or a whole place after confirming, or a row from catalog.toml, or hide a cloud source |
| `Shift+Delete` | Forget every recent entry, after confirming |

### Home screen · Mouse

| Key | Action |
|---|---|
| `Click` | Select the row; double-click is Enter |
| `Wheel` | Move the selection three rows, stopping at the ends |

## Command line

<kbd>:</kbd> at the table.

### Command line · Run

| Key | Action |
|---|---|
| `(digits)` | Digits alone go to that row, and the prefix says row: (:0 Enter is the top) |
| `Enter` | Go to the row, or run the query as the prefix says, sql: or q:. Reopened with a query in effect, the line holds its text, selected: typing replaces it, arrows edit it |
| `Ctrl+J` | Run, the same as Enter |
| `Ctrl+T` | Switch between SQL and q, keeping what is typed; the choice is remembered. [query] default_mode sets the first |
| `Esc` | Close |

### Command line · Edit

| Key | Action |
|---|---|
| `Tab` | Complete a column name, or df in SQL; again for the next match. The line under the input lists the names that fit |
| `Alt+Enter` | SQL: start a new line |
| `↑ / ↓` | Earlier and later queries from the history (Ctrl+P / Ctrl+N too; SQL and q keep their own). In SQL over several lines, they move between lines first |
| `Ctrl+U / Ctrl+K` | Delete to the start or end of the line |
| `Ctrl+Z / Ctrl+R` | Undo, redo |

## Find

<kbd>/</kbd> (or <kbd>f</kbd>) at the table.

### Find · Find

| Key | Action |
|---|---|
| `(text)` | What to find: text (any case until a capital is typed), a regex with Ctrl+R, or letters in order with Ctrl+T. Matches in the rows on screen light up as you type, and the line says how many are on screen |
| `Enter` | Find: the cursor, column cursor and all, goes to the first match at or after its row, reading past the rows on hand when it must (the footer counts the rows read; Esc stops). On an empty field, clear the find |
| `Ctrl+G` | Keep only the rows with a match, as a filter: the footer and the Sort & Filter sidebar show it, and removing it there (or R) brings the rows back |
| `Ctrl+R` | Regex on or off |
| `Ctrl+T` | Letters in order on or off: smth finds Smith |
| `Ctrl+L` | Only the column cursor's column, or every column shown |
| `↑ / ↓` | Earlier patterns (Ctrl+P / Ctrl+N too) |
| `Esc` | Cancel |

### Find · At the table

| Key | Action |
|---|---|
| `n / N` | Next / previous match from the cursor's cell. Past the last match the find comes round to the first, and the footer says so. Each one typed while a find reads runs in turn; Esc stops them all |
| `/` | Find again, the last pattern ready to edit |
| `Esc` | Clear the find (or stop one still reading) |

## Go to column

<kbd>g</kbd> at the table.

### Go to column · Go

| Key | Action |
|---|---|
| `(type)` | Narrow to the names that contain it |
| `↑ / ↓` | Move |
| `Enter` | Go. The column cursor moves to it. A column already whole on screen stays where it is; another becomes the first after the frozen ones, or lands on the last page when it is there |
| `Backspace` | Delete a character (Ctrl+W a word, Ctrl+U all) |
| `Esc` | Close without moving |

## Inspector

<kbd>Space</kbd> at the table.

### Inspector · Fields

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move between fields |
| `Home / End` | First and last field |
| `PgUp / PgDn` | A page of fields |
| `← / → (h/l)` | Previous and next row; the table's cursor moves with it |
| `Tab` | Into the value, to scroll and find in it |
| `Enter` | On a group's row, its rows, as at the table. Else open a struct, a list, or text holding a JSON object or array; or read a field the table's rows do not hold (hidden and binary columns) |
| `r` | On a group's row, read a field the rows do not hold |
| `/` | Find a field by name, then by value: type to narrow, Enter or ↓ keeps the list narrowed, Esc clears it |
| `f` | Nulls: shown or hidden (null and empty fields); comparing, only the fields that differ |
| `s` | Order: the table's, A-Z, or nulls last |
| `c` | Compare: a column for the next row, or the pinned one |
| `m` | Pin this row to compare others with; again to unpin |
| `Esc / Space` | Close; Esc clears a find first |

### Inspector · Output

| Key | Action |
|---|---|
| `y` | Copy the value as its view shows it |
| `Y` | Copy the whole row as one JSON object |
| `e` | The value's next view, where it has more than one |
| `w` | Word wrap or hard wrap for long text |
| `o` | Open the value in another program |

### Inspector · Value

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Scroll a line |
| `PgUp / PgDn` | Scroll a page |
| `Home / End` | Top and end, at once however long the value |
| `/` | Find in the value; n and N go to the next and last place |
| `e, w, y, o` | As in the fields |
| `Esc / Tab` | Back to the fields; Esc clears a find first |

### Inspector · Nested

| Key | Action |
|---|---|
| `Enter / → (l)` | Open the focused item |
| `Esc / ← (h)` | Up a level; at the row, Esc closes |
| `y` | Copy the focused item: text as itself, a JSON object or array indented |

## Info panel

<kbd>i</kbd> at the table.

### Info panel · Panel

| Key | Action |
|---|---|
| `← / → (h/l)` | Previous or next tab, from anywhere in the panel; Shift+Tab and Tab do the same. The panel is a viewer, not a form: its body always has the keys |
| `↑ / ↓ (j/k)` | Schema, Notes or Documentation tab: move the cursor. Model, Audio, MIDI, Metadata and format tabs: scroll the list |
| `PgUp / PgDn` | Model, Audio, MIDI, Metadata and format tabs: scroll the list a page |
| `Home / End` | Model, Audio, MIDI, Metadata and format tabs: the top or the end of the list |
| `Enter` | Notes tab: take the offer on the note, where it has one. Documentation tab: open or close the value legend of the column under the cursor |
| `y` | Documentation tab: copy the link or value on the cursor's line, whole, however it is cut on screen |
| `H` | Schema tab, CSV, TSV, PSV: read the first row as data, under column_1, column_2, …; again to read it as column names. Reads the file again, so the query, filters and sort are cleared, and the panel closes |
| `x` | Show the file's bytes in the hex view |
| `Esc / i` | Close the panel |

## Value counts

<kbd>F</kbd> at the table.

### Value counts · Explore

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move |
| `PgUp / PgDn` | A page |
| `Home / End` | First and last row |
| `← / → (h/l)` | The previous or next column; the table's column cursor moves with it |
| `Enter` | The rows holding the value, as a drill-down; Esc there comes back here |
| `s` | Sort by count or by value |
| `a` | Count every row, when the counts are of a sample |
| `t` | While following a file, count again with the rows that arrived since; the bar says how many |

### Value counts · Output

| Key | Action |
|---|---|
| `y` | Copy the counts as TSV: every value, its count, percent and cumulative percent |
| `e` | Export the counts to a file |
| `Esc` | Back to the table; while every row is being counted, stop that and keep the sample |

## Sort and filter

<kbd>s</kbd> at the table.

### Sort and filter · Sidebar

| Key | Action |
|---|---|
| `Tab / Shift+Tab (↑ / ↓)` | Next or previous field, wrapping; a field the form does not offer right now is skipped. The arrows move from the moment the dialog opens |
| `← / →` | On the tab bar: switch Sort & Filter / Columns. On a sort: flip its direction. On a filter: and / or. On a column: step its sort (none, ascending, descending). h/l too |
| `Space` | On a sort: flip its direction. On a filter: edit it (column, operator, value). On "add sort": pick a column to sort by, last and ascending. On "add filter": a new filter, starting on the table's column cursor. On a column: step its sort |
| `Enter` | Apply everything staged and close, from any row (in the filter editor Enter takes the step; Ctrl+J applies) |
| `Ctrl+J` | Apply from anywhere, including mid-edit (the row in progress is saved). Ctrl+Enter does the same, on a terminal that tells it from Enter |
| `Esc` | Close an open picker or filter editor; otherwise cancel and close, discarding what is staged. Reopening shows what is applied |

### Sort and filter · In effect

| Key | Action |
|---|---|
| `[ / ]` | Move the focused sort earlier or later in the sort order, or the focused filter in the list |
| `d / Del` | Remove the sort or filter |
| `C` | Remove every sort and filter |

### Sort and filter · Columns

| Key | Action |
|---|---|
| `(type)` | Narrow the column list, when the find field is focused. ↓ from find goes to the list, on the table's column cursor |
| `Space` | Cycle the column's sort: none → ascending → descending (← steps back). Each column carries its own direction |
| `1-9` | Put the column at that place in the sort order; 0 removes it (a digit past the end of the order says so) |
| `Del` | Remove the column from the sort |
| `[ / ]` | Earlier or later in the sort order |
| `+ / - (= / _)` | Move the column's display position |
| `L` | Freeze this column and every column up to it at the left edge; on a column already frozen, pull the boundary back |
| `v` | Show or hide the column (it keeps its place, dimmed) |
| `< / > (, / .)` | Make the column 4 cells narrower or wider. A number column is never narrower than its numbers |
| `f` | Fit the column to the rows on screen, its name up to the automatic limit |
| `w` | Back to the automatic width |
| `C` | Clear the staged sort, order, locks, hidden columns and widths |

### Sort and filter · Filter editor

| Key | Action |
|---|---|
| `(type)` | Narrow the column or operator list |
| `Enter` | Pick the column (type to narrow, Enter chooses), the operator the same way, then type the value and Enter saves the row. Tab, → and Space also choose at the column and operator steps; Shift+Tab steps back; ↑ / ↓ move in the lists |
| `Esc` | End the edit, and only the edit |

## Pivot and melt

<kbd>p</kbd> at the table.

### Pivot and melt · Form

| Key | Action |
|---|---|
| `Tab / Shift+Tab (↑ / ↓)` | Next or previous field, wrapping; a field the form does not offer right now is skipped. The arrows move from the moment the dialog opens |
| `← / →` | On the tab bar: switch Pivot and Melt. On the aggregation, strategy or type: the previous or next value. On a single column row: the previous or next column. In a text field: move the cursor. h/l too, outside text fields |
| `Space` | On a choice: its next value, wrapping. On a column row: open its picker, scoped to that row; typing narrows it |
| `Enter` | Apply the spec echoed above the footer |
| `Esc` | Close without applying; while a pivot is computed, stop it and keep the form |

### Pivot and melt · Picker

| Key | Action |
|---|---|
| `↑ / ↓` | Move; typing narrows |
| `Enter` | Choose; on a several-choice row, done |
| `Space` | Choose; toggle a column on a several-choice row |
| `Esc` | Back out of the picker, keeping toggles |

## Chart

<kbd>c</kbd> at the table.

### Chart · Options

| Key | Action |
|---|---|
| `1-6` | Switch chart type directly: XY, Histogram, Box Plot, KDE, Heatmap, Bar ([ / ] cycle) |
| `[ / ]` | Previous or next chart type |
| `Tab / Shift+Tab (↑ / ↓)` | Next or previous option row, wrapping (j/k too) |
| `Space / Enter` | Open a column row's picker, toggle an option, or take the next plot style, range, order or number. The options apply as they change, so Enter acts as Space does |
| `← / → (h/l)` | Step the plot style, range or order; adjust bins, bandwidth, or Sample size (+ / - too); on a single column row, the previous or next column; flip a toggle |
| `PgUp / PgDn` | Adjust Sample size in bigger steps |
| `g` | Grid on or off at the labeled ticks: XY, Histogram, Box Plot and KDE. [analysis] chart_grid sets where it starts |
| `Esc` | Back to the table |

### Chart · Plot

| Key | Action |
|---|---|
| `x` | XY: the plot takes the keys, and a crosshair reads out x and every series' value under the plot. ← / → (h/l) step to the next point or column, Home/End go to the ends; x, Tab or Esc hand the keys back to the option rows. A click on the plot puts the crosshair there |
| `e` | Export the chart to PNG or EPS. Needs the chart's required columns picked first |
| `t` | While following a file, draw again with the rows that arrived since; the bar says how many |

### Chart · Picker

| Key | Action |
|---|---|
| `↑ / ↓` | Move; typing narrows |
| `Enter / Space` | Choose; on the Y series row Space toggles a series in or out |
| `Tab / Shift+Tab` | Choose and move to the next or previous row |
| `Esc` | Back out of the picker alone |

### Chart · Export dialog

| Key | Action |
|---|---|
| `Tab / Shift+Tab (↑ / ↓)` | Format, Path, Title, Width, Height |
| `← / →` | Change the format, on its row |
| `Ctrl+P / Ctrl+N` | Earlier or later paths in the path field |
| `Enter` | Export, from anywhere in the dialog. An existing file asks Overwrite / No, starting on No; ←/→ (h/l) or Tab pick, Enter confirms, and declining returns to the filled dialog |
| `Esc` | Back to the chart |

## Analysis: Describe

<kbd>a</kbd> at the table.

### Analysis: Describe · Explore

| Key | Action |
|---|---|
| `Tab` | Between the results and the sidebar |
| `↑ / ↓ (j/k)` | Rows, or the sidebar's tools |
| `← / → (h/l)` | Scroll the statistics; the header counts those out of view |
| `Home / End` | First or last row |
| `PgUp / PgDn` | A page |
| `Enter` | Select tool from sidebar (when sidebar focused). The first run on a dataset starts in the Sample form, where Enter runs it; later tools reuse that sample |
| `Esc` | Cancel a run in progress; otherwise close the analysis view |

### Analysis: Describe · Sample

| Key | Action |
|---|---|
| `s` | Choose the sample every tool reads: which rows, how they are picked, how many, the seed |
| `v` | View the sample's rows as a table; Esc comes back |
| `r` | Draw another sample (when the result is a sample) |
| `a` | Read every row instead, after confirming |
| `t` | While following a file, read again with the rows that arrived since the results were read |

## Analysis: Distribution

Distribution in the Analysis sidebar.

### Analysis: Distribution · Explore

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Rows, or the sidebar's tools |
| `← / → (h/l)` | Scroll the statistics; the header counts those out of view |
| `Home / End` | First or last row |
| `PgUp / PgDn` | A page |
| `Tab` | Between the results and the sidebar |
| `Enter` | Open detail view for selected column (shows Q-Q plot and histogram); with the sidebar focused, select a tool |
| `Esc` | Cancel a run in progress; otherwise close the analysis view |

### Analysis: Distribution · Sample

| Key | Action |
|---|---|
| `s` | Choose the sample every tool reads: which rows, how they are picked, how many, the seed |
| `v` | View the sample's rows as a table; Esc comes back |
| `r` | Draw another sample (when the result is a sample) |
| `a` | Read every row instead, after confirming |
| `t` | While following a file, read again with the rows that arrived since the results were read |

## Analysis: Distribution detail

<kbd>Enter</kbd> on a column in Distribution.

### Analysis: Distribution detail · Detail

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Compare the values with another family |
| `s` | Histogram scale: linear or log |
| `Esc` | Back to the distribution table |

## Analysis: Correlation

Correlation Matrix in the Analysis sidebar.

### Analysis: Correlation · Explore

| Key | Action |
|---|---|
| `Tab` | Between the matrix and the sidebar |
| `↑ / ↓ (j/k)` | Matrix rows, or the sidebar's tools |
| `← / → (h/l)` | Matrix columns |
| `Home / End` | Jump to the first/last corner cell (the column resets too) |
| `PgUp / PgDn` | A page |
| `Enter` | Open pair detail view (on a cell) or select tool (sidebar); does nothing on a diagonal cell |
| `m` | Method: Pearson r or Spearman ρ, named in the title (both come from the one run, so it reads nothing) |
| `Esc` | Cancel a run in progress; otherwise close the analysis view |

### Analysis: Correlation · Sample

| Key | Action |
|---|---|
| `s` | Choose the sample every tool reads: which rows, how they are picked, how many, the seed |
| `v` | View the sample's rows as a table; Esc comes back |
| `r` | Draw another sample (when the result is a sample) |
| `a` | Read every row instead, after confirming |
| `t` | While following a file, read again with the rows that arrived since the results were read |

## Analysis: Correlation detail

<kbd>Enter</kbd> on a pair in the correlation matrix.

### Analysis: Correlation detail · Detail

| Key | Action |
|---|---|
| `m` | Pearson or Spearman |
| `Esc` | Return to the correlation matrix. Resampling (r) works from the matrix, not from inside this detail |

## Analysis: Data Quality

Data Quality in the Analysis sidebar.

### Analysis: Data Quality · Setup

| Key | Action |
|---|---|
| `↑ / ↓, Tab` | Move between rows |
| `← / →` | Change a short list's choice |
| `Space` | Open the row: the Sample form, a list (type to narrow), Time roles, Intervals, Column intent or Expected |
| `s` | The Sample form; its Enter applies to Setup and returns there |
| `p` | The access plan: what a run reads |
| `d` | Release the rows runs kept and a full scan's local copy, named on the Read rule; the next run that would reuse them reads again |
| `Enter` | Run, from any row. A full read asks first; the report on screen or in the session cache is shown, not read again |
| `Esc` | Discard every staged edit |

### Analysis: Data Quality · Setup lists

| Key | Action |
|---|---|
| `↑ / ↓` | Time roles: choose the role. Intervals: choose the pair. Column intent: choose the column. Expected: Windows, From, Before (Tab too) |
| `← / →` | Time roles: choose the role's column. Expected: choose the windows. Intervals: measure the pair or not |
| `Space` | Intervals: measure the pair or not. Column intent: declare the column's intent in a form |
| `Enter` | Done |
| `Esc` | Put the list back as it was |

### Analysis: Data Quality · Report

| Key | Action |
|---|---|
| `← / → (h/l)` | Previous or next page |
| `1 - 5` | Overview, Columns, Segments, Trends, Intervals |
| `↑ / ↓ (j/k)` | Move, or scroll a tall finding |
| `PgUp / PgDn` | Page; Home/End jump to either end |
| `Enter` | Open a finding, then its rows. On an empty page, open the setting it needs |
| `c / t` | Overview: only one column's or one type's findings |
| `o` | Overview: ranked, by rows, by rate |
| `e` | Setup |
| `s` | Setup, with the Sample form open |
| `v` | View the sample's rows; Esc returns |
| `p` | The access plan: what a run reads |
| `r` | On a sample, run again with a new seed |
| `x` | Export the report to JSON or Markdown; nothing is read |
| `Tab` | Between the result and the tools |
| `Esc` | Back one level: all findings again, then close |

### Analysis: Data Quality · Segments

| Key | Action |
|---|---|
| `Enter` | A segment's columns beside the one it is compared with, largest change first |
| `o` | Largest change first, or back in order |
| `b` | Compare with the highlighted segment |

### Analysis: Data Quality · Trends

| Key | Action |
|---|---|
| `Enter` | A line's bars: span, segments, rows sampled of counted, rate, 95% interval, and the bar before. ↑ / ↓ there: the next bar |
| `m` | Next measure |
| `w` | Stage a coarser window in Setup, for segments the sample reached thinly or not at all |
| `g` | The expected windows with no rows: empty by exact count, not sampled, or out of scope |
| `Esc` | Back to Trends |

### Analysis: Data Quality · Intervals

| Key | Action |
|---|---|
| `Enter` | An interval's detail: its ends, the rows with both, missing and unread ends, negative and zero durations, percentiles and breaches. Enter there shows the rows behind the count under the cursor: a sample's from the rows the run kept |
| `Esc` | Back to the list |

## Export

<kbd>e</kbd> at the table.

### Export · Form

| Key | Action |
|---|---|
| `Tab / Shift+Tab (↑ / ↓)` | Next or previous field, wrapping; the fields follow the format. The dialog opens on Path, and the arrows move from there |
| `← / →` | On Format or Compression: the previous or next value (h/l too). On Include header or Source file: toggle. In Path and Delimiter: move the cursor |
| `Space` | Toggle a checkbox (Include header, Source file); on Format or Compression, the next value, wrapping |
| `Ctrl+P / Ctrl+N` | In the path field: the paths exported to before, earlier or later (↑ and ↓ move between fields) |
| `Enter` | Export, from anywhere in the form. On a blank path the form says "Enter a file path." instead of exporting |
| `Esc` | Close without exporting |

### Export · File exists

| Key | Action |
|---|---|
| `← / → (h/l)` | Pick Overwrite or No (Tab too) |
| `Enter` | Confirm the one picked |
| `Esc` | Decline: back to the filled form |

## Copy

<kbd>y</kbd> at the table.

### Copy · Form

| Key | Action |
|---|---|
| `Tab / Shift+Tab (↑ / ↓)` | Next or previous row |
| `← / →` | The previous or next scope, column or format (h/l too); on Header: toggle |
| `Space` | On Scope or Format: the next value, wrapping. On Column: open its picker. On Header: toggle |
| `Enter` | Copy, from anywhere in the form. On the Cell scope with no column picked, Enter re-accents the spec line instead of copying |
| `Esc` | Close a picker, then the dialog, without copying |

### Copy · Picker

| Key | Action |
|---|---|
| `(type)` | Narrow the list |
| `↑ / ↓` | Move the cursor (j/k narrow the picker; only ↑/↓ move there) |
| `Enter / Space` | Choose |
| `Tab / Shift+Tab` | Choose and move on |

## Views

<kbd>v</kbd> at the table.

### Views · List

| Key | Action |
|---|---|
| `↑ / ↓ (j/k)` | Move in the list |
| `Enter` | Apply the selected view |
| `s` | Save the current state as a view (an untouched table has nothing to save, and s says so) |
| `e` | Edit the selected view |
| `d` | Delete the selected view, after confirming |
| `i` | Show how the selected view's score was computed (Esc closes the score view) |
| `Esc` | Close |

### Views · Save and edit

| Key | Action |
|---|---|
| `Tab / Shift+Tab (↑ / ↓)` | Next or previous row. In the description ↑ / ↓ move between its lines, and leave it from the first or last |
| `Enter` | Save, from any row. In the description Enter types: Ctrl+J saves from there |
| `Ctrl+J` | Save from anywhere, the description included; so does Ctrl+Enter, on a terminal that tells it from Enter |
| `PgUp / PgDn` | Five lines in the description |
| `Space` | Expand or collapse Matching; toggle schema match |
| `Esc` | Back to the list, discarding edits |

### Views · Delete

| Key | Action |
|---|---|
| `Enter (d / D)` | Delete |
| `Esc` | Cancel |

## Format picker

<kbd>b</kbd> at a table read through a format spec.

### Format picker · Pick

| Key | Action |
|---|---|
| `(type)` | Narrow to the names that contain it |
| `↑ / ↓` | Move |
| `Enter` | Read the file again with the spec chosen. The query, filters and sort are cleared |
| `Backspace` | Delete a character (Ctrl+W a word, Ctrl+U all) |
| `Esc` | Close and keep the format |

## Hex view

`datui --hex FILE`, <kbd>Ctrl</kbd>+<kbd>X</kbd> on the home screen, or <kbd>x</kbd> in the Info panel.

### Hex view · Explore

| Key | Action |
|---|---|
| `← / → (h/l)` | A byte |
| `↑ / ↓ (j/k)` | A row |
| `w / b` | The next group of four, or back one |
| `0 / $` | The start or end of the row |
| `g / G` | The start or end of the file (Home and End too) |
| `PgUp / PgDn` | A page (Ctrl+B/F too; Ctrl+U/D half a page) |
| `:` | Go to an offset: 4096 or 0x1000; +16 or -16 from the cursor; e-8 for the eighth byte from the end |

### Hex view · Find

| Key | Action |
|---|---|
| `f` | Find bytes. Text is found as its UTF-8 bytes (Ctrl+U in the prompt: as UTF-16 little-endian). 0x1acffc1d, or two or more hex pairs (de ad be ef), is a byte pattern, where ?? matches any byte. Text in double quotes is text, even when it looks like hex. A match may span rows |
| `n / N` | The next or previous match, round the end of the file |
| `R` | When the matches after the one found are all the same distance apart, make that the bytes per row |
| `Esc` | Stop a find that is reading |

### Hex view · Display

| Key | Action |
|---|---|
| `r` | Bytes per row, so that records line up; empty goes back to as many as fit. --hex-width N sets it on the command line |
| `#` | Offsets in decimal or hex |
| `i / Enter` | Show or hide the byte inspector. Beside the bytes when there is room for it and 16 bytes a row, over them when there is not |
| `v` | Mark from the cursor; move to mark a range, and the status line counts it. v again, or Esc, unmarks |

### Hex view · Go

| Key | Action |
|---|---|
| `B` | Read the file with a format spec instead |
| `Esc` | Back to the table, when opened from the Info panel; back home, when opened from there |
| `q` | Home, when opened from there; else quit |
<!-- end generated: keys -->

## Text fields

Every text field, from the command line to a file path, edits the same way,
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
| <kbd>Ctrl</kbd>+<kbd>J</kbd> | The same as <kbd>Ctrl</kbd>+<kbd>Enter</kbd>, on every terminal: saves a view from its description and applies Sort & Filter. In the command line and the find prompt it submits, like <kbd>Enter</kbd> |
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

A spinner in the footer means datui is busy. While it is, at the plain
table <kbd>q</kbd>, <kbd>Q</kbd>, <kbd>←</kbd> <kbd>→</kbd> (<kbd>h</kbd>
<kbd>l</kbd>, <kbd>Shift</kbd> for a page), <kbd>{</kbd> <kbd>}</kbd>,
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
| Click a key in the footer | Presses it |

In a text field the wheel does nothing, so it cannot recall history.

Mouse input is never queued. While datui is busy, the sideways wheel and the
footer's keys act as their keys would; the wheel down and a click on the
table are dropped, as is a click behind keys already queued.

To select text with the mouse while datui has it, see
[Mouse and text selection](../user-guide/configuration.md#mouse-and-text-selection).
