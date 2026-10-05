# Glossary

One word per concept, in the interface, the help (`?`), these docs, the
command line, the config file and Python.

| Use | Not | Means |
|---|---|---|
| **view** (saved view) | template | A saved query, filters, sort, column layout and reshape, matched to files (`v`, `V`, `--view`, `[views]`) |
| **query** | search | SQL or **q** run on the command line |
| **command line** | query prompt, query bar | `:` at the table: a row number (`row:`), or a query (`sql:`, `q:`) |
| **footer** | control bar, bottom bar, status bar | The line under the thin rule at the bottom: what is in effect, the position, the mode's keys and `? keys` |
| **find** | search, locate | Move to a match without changing the rows: `/` (or `f`), `n`, `N`, find in the hex view and the inspector |
| **filter** | narrow, search | Keep only the matching rows: Sort & Filter (`s`), `+` `-` on a cell, a find kept with `Ctrl+G`, a query |
| **narrow** | filter | Shrink a picker or a list by typing: pickers, the home screen's filter |
| **search** | find | On the home screen only: look below the current directory for files |
| **catalog** | collection, sources, remembered directory | A file of named datasets the home screen lists as a section: `catalog.toml` (yours; <kbd>Ctrl</kbd>+<kbd>D</kbd> adds to it), the files in `catalogs/` and those `catalogs` lists, and the bundled `public` |
| **theme** | color scheme | A named set of colors, one per slot: `night-market`, `day-market`, or a file in `themes/`; `theme.dark` and `theme.light` pick one per mode |
| **bookmark** | suggested place | A place inside a catalog dataset to start from: `bookmarks."Name" = "path/"` |
| **documentation** | codebook, data dictionary, column notes | What a catalog says of its dataset and its columns: `documentation`, `columns`; the `DOCUMENTATION` heading on the home screen, the Documentation view (<kbd>Ctrl</kbd>+<kbd>E</kbd>) and Info's Documentation tab |
| **Info** (the Info panel) | Dataset Info | `i`: facts about the dataset |
| **inspector** | row inspector, detail | One row's values (`Space`) |
| **byte inspector** | inspector | The hex view's decoder of the bytes at the cursor |
| **table** | sheet, variant, split, topic, member, array | One table inside a file: `--table`, "3 tables" on the home screen |
| **format spec** | binary spec, spec file | A TOML file that describes a format (`--format`) |
| **dictionary** | dict, DBC file, FIX dictionary | Field or signal definitions a log is decoded with (`--dict`), in QuickFIX XML, DBC or TOML |
| **home screen** | browse files, start screen | Where `datui` with no path, `q` and `Ctrl+O` go |
| **cloud source** | cloud browser, remote | A store listed on the home screen. *Remote* only as an adjective for files not on this machine |
| **recent** | history | A dataset opened before. *History* is only the prompts' history |
| **sample** | limit, row limit | The rows an analysis reads from a larger table |
| **export** / **copy** / **save** | | To a file (`e`) / to the clipboard (`y`) / a view (`s` in the views list). Never "save" for an export |
| **Analysis** | statistics, Statistical Analysis | `a`: Describe, Distribution, Correlation, Data Quality |
| **value counts** | count values, Value Count | `F`: how often each value of a column occurs |
| **drill down** (verb), **drill-down** (noun) | drill into | Open the rows behind a group's row (`Enter`) |
| **row** | line | A row of the table; `:` and digits go to a row. *Line* only for raw text, as in `--skip-lines` |

## How a file is read

The details pane, the Info panel's Resources tab and the
[formats table](../formats/index.md#how-each-format-is-read) name a read with
these terms and no others:

| Term | Not | Means |
|---|---|---|
| **lazy scan** | lazy, streamed | Scanned where it is; only the rows shown, and what a query needs, are read |
| **decompressed copy** | converted once, unpacked | Decompressed whole to a temporary file of the same format, then scanned; removed on quit |
| **converted to Arrow** | converted once, cached | Read whole into a temporary Arrow IPC file, then scanned; removed on quit, not kept between sessions |
| **in memory** | loaded, eager | Read whole into memory before the table appears |
| **download →** | downloaded, then | A remote file copied to the temp directory first, then read as the term after the arrow says |
| **schema** | columns (for what a file declares) | The columns and their types. `on open` when only opening the file reads them, `not read` when nothing has looked yet |

## Pane wording

A pane or status line is a `label  value` pair: the value is a term or a
number, not a clause. The only free-standing line is a one-line callout behind
`▲` (ASCII `!`) for something that will surprise, such as `▲ footer unreadable`.
Why something is so belongs in these docs.
