# Glossary

One word per concept, in the interface, the help (`?`), these docs, the
command line, the config file and Python.

| Use | Not | Means |
|---|---|---|
| **view** (saved view) | template | A saved query, filters, sort, column layout and reshape, matched to files (`v`, `V`, `--view`, `[views]`) |
| **query** | search | The `/` prompt. Its modes are **SQL**, **Text** (rows whose text columns hold every word) and **q** |
| **find** | search, locate | Move to a match without changing the rows: `f`, `n`, `N`, find in the hex view and the inspector |
| **filter** | narrow, search | Keep only the matching rows: Sort & Filter (`s`), a query |
| **narrow** | filter | Shrink a picker or a list by typing: pickers, the home screen's filter |
| **search** | find | On the home screen only: look below the current directory for files |
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
| **row** | line | A row of the table; `:` goes to a row. *Line* only for raw text, as in `--skip-lines` |
