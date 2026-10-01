# Copy to the clipboard

Press <kbd>y</kbd> to copy from the current view to the system clipboard. The
dialog picks a scope and a format; the last choices are kept, so repeating a
copy is <kbd>y</kbd> <kbd>Enter</kbd>.

## Copy a table into a note

On **Food nutrition (fast food)**, summarize each chain:

```sql
SELECT restaurant, ROUND(AVG(calories), 0) AS avg_calories,
       ROUND(AVG(protein), 1) AS avg_protein, COUNT(*) AS items
FROM df
GROUP BY restaurant
ORDER BY avg_calories DESC
```

1. Press <kbd>y</kbd>. On **Scope**, press <kbd>Space</kbd>, type `Table` and
   press <kbd>Enter</kbd>.
2. <kbd>Tab</kbd> to **Format**, <kbd>Space</kbd>, type `Markdown`,
   <kbd>Enter</kbd>.
3. Press <kbd>Enter</kbd> to copy. The status line says `Copied 8 rows as Markdown`.

Paste into a note:

```text
| restaurant  | avg_calories | avg_protein | items |
| ----------- | -----------: | ----------: | ----: |
| Mcdonalds   |        640.0 |        40.3 |    57 |
| Sonic       |        632.0 |        29.2 |    53 |
| Burger King |        609.0 |        30.0 |    70 |
| Arbys       |        533.0 |        29.3 |    55 |
| Dairy Queen |        520.0 |        24.8 |    42 |
| Subway      |        503.0 |        30.3 |    96 |
| Taco Bell   |        444.0 |        17.4 |   115 |
| Chick Fil-A |        384.0 |        31.7 |    27 |
```

Values are copied raw, so round them in the query. The data spells McDonald's
`Mcdonalds`.

For a spreadsheet, choose **TSV** instead. **Table** includes all matching
rows; **View** includes only the rows on screen.

| Scope | What it copies |
|---|---|
| Cell | The current row's value in one column, as plain text |
| Row | The current row |
| View | The rows on screen, with every displayed column |
| Table | Everything the view holds, as an export would: rows and columns as queried, filtered and sorted |

| Format | Details |
|---|---|
| TSV | Tab-separated, what spreadsheets expect from a paste |
| CSV | Comma-separated |
| Markdown | A pipe table, padded and aligned, numeric columns right-aligned |

A TSV or CSV copy to the `native` clipboard also carries an HTML table flavor,
so a paste into a spreadsheet or an email keeps its columns while a paste into
a terminal stays plain text. Values are raw, like an export: display formatting
is not applied, a float is copied as stored rather than as the table rounds it,
and a null is an empty field. List and struct cells are JSON in every format,
as in a [CSV export](exporting-data.md#lists-and-structs), and a duration is
[ISO 8601](exporting-data.md#durations) text such as `PT3723.004S`. A binary column is
[base64](exporting-data.md#binary) in a Table copy; Cell, Row and View copies
hold the `‹binary›` placeholder, since the screen never reads the bytes. The **Header**
toggle is on for View and Table and off for Row; a Markdown table always keeps
its header.

A large Table copy asks first, counting binary at its base64 size. A binary
column's size comes from the Parquet footers read to open a local directory of
Parquet files or a single Parquet object in cloud storage. A copy whose size is
not known asks too: the row count is still being read, or no footer gave a
binary column's size, as for a single local file.
Above 200 MiB the copy is refused with a pointer to
[export](exporting-data.md). An `osc52` copy asks only when its cap is over
10 MiB, since it never holds more than the cap.

## Keys

| Key | Action |
|---|---|
| <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Move between rows |
| <kbd>Space</kbd> | Open the focused row's picker; on Header, toggle |
| <kbd>↑</kbd> <kbd>↓</kbd> | Move focus, or the picker cursor |
| <kbd>Enter</kbd> | Copy, from anywhere in the form; in a picker, choose |
| <kbd>?</kbd> | Help |
| <kbd>Esc</kbd> | Close a picker, then the dialog, without copying |

In a picker, typing narrows the list.

## How the copy reaches the clipboard

`[clipboard] backend` in the [configuration](../reference/settings.md#clipboard)
chooses the mechanism:

| Backend | How |
|---|---|
| `auto` (default) | `native` where a display server answers, `osc52` elsewhere |
| `native` | The display server (Wayland, X11, macOS, Windows), with the HTML flavor |
| `osc52` | An escape sequence the terminal applies to the system clipboard |

`osc52` is what works over SSH: no display server is involved, the terminal
you are sitting at does the copy. Caveats terminals impose:

- tmux needs `set-clipboard on` to pass the sequence through.
- Terminals cap the sequence length; datui refuses payloads above
  `osc52_limit_kb` (default 100) rather than sending a copy that arrives
  truncated. A Table copy is read in batches and stops at the first one over
  the cap, so a copy too large is refused without reading the whole table.
  The clipboard keeps what it held. Some terminals disable OSC 52 writes
  entirely by default.
- No HTML flavor: the terminal takes plain text only.

A `native` copy on Wayland or X11 belongs to the datui process: quitting can
drop it unless a clipboard manager keeps copies. datui holds the offer for as
long as it runs.
