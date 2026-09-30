# Quick start

Open a small public dataset, compare three penguin species, and save the result.
[Install datui](installation.md) first.

## 1. Open the data

Download the [Palmer Penguins CSV][penguins] (344 rows, CC0), then open it:

```bash
curl -fL -o penguins.csv https://raw.githubusercontent.com/allisonhorst/palmerpenguins/main/inst/extdata/penguins.csv
datui --null-value NA penguins.csv
```

On Windows, use `curl.exe` for the download. You can also download the file in
your browser and run the second command. `--null-value NA` reads the source's
missing-value marker as null, shown as `∅`.

| Key | Action |
|---|---|
| Arrow keys or <kbd>h</kbd> <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd> | Move around |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Move a page |
| <kbd>i</kbd> | Inspect columns and file details |
| <kbd>?</kbd> | Show help for this screen |
| <kbd>Esc</kbd> | Close a panel or go back |
| <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit |

## 2. Group by species

Which species has the highest average body mass? Press <kbd>/</kbd>; the
prompt opens on **SQL**. Type this, then press <kbd>Enter</kbd>:

```sql
SELECT species, AVG(body_mass_g) AS mean_mass_g
FROM df
GROUP BY species
ORDER BY mean_mass_g DESC
```

You get three rows, one per species, highest first. `AVG` ignores null values;
Gentoo has the highest mean, about 5,076 g.

Know q? Press <kbd>Ctrl</kbd>+<kbd>T</kbd> twice for **q-style**, a subset of
q that evaluates right to left:

```text
select mean_mass_g: avg body_mass_g by species
```

Then press <kbd>s</kbd> to open **Sort & Filter**, select `mean_mass_g` on
**Columns**, and press <kbd>Space</kbd> twice for descending order, then
<kbd>Enter</kbd> to apply.

The loaded table is named `df`. A new query starts a fresh view, clearing
sidebar filters and sort. See [querying](../user-guide/querying-data.md) for
search, expressions and grouped drill-down.

## 3. Plot individual measurements

Press <kbd>R</kbd> to reset to the original rows, then <kbd>c</kbd> for charts.
On the **XY** tab, use <kbd>Tab</kbd> to move between settings:

| Setting | Choose |
|---|---|
| Plot style | Scatter |
| X | `flipper_length_mm` |
| Y | `body_mass_g` |

Use <kbd>Space</kbd> on a column setting to open its picker. Type part of the
name, then select it; on Y, <kbd>Space</kbd> toggles the series and
<kbd>Enter</kbd> closes the picker. All 342 complete measurement pairs fit
within the default 10,000-row chart limit.

Press <kbd>e</kbd> in the chart to save a PNG or EPS. Press <kbd>Esc</kbd>
to return to the table. [More chart options](../user-guide/charting.md).

## 4. Copy or export

Run the three-row summary again, then choose an output:

| Do this | How |
|---|---|
| Copy the summary into a note | <kbd>y</kbd> → Table → Markdown → <kbd>Enter</kbd> |
| Save a data file | <kbd>e</kbd> → type `penguin-summary.csv` in Path → <kbd>Enter</kbd> |
| Reuse the query on another file | <kbd>v</kbd> → <kbd>s</kbd> to save a view |

Export and copy use the current rows and columns. They do not overwrite the
input file unless you explicitly export to that path and confirm.

## Use your own data

```bash
datui flights.parquet
datui ./exports/
datui s3://bucket/events/
datui                        # browse from the home screen
```

Next: [file formats](../user-guide/loading-data.md),
[cloud access](../user-guide/remote-data.md), or
[all keyboard shortcuts](../reference/keyboard-shortcuts.md).

[penguins]: https://allisonhorst.github.io/palmerpenguins/
