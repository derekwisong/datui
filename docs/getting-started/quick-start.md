# Quick start

Open public penguin measurements from the built-in catalog, compare three
species, chart them, and copy the result. [Install datui](installation.md) first.

## 1. Open the data

```bash
datui
```

Type `penguins` to narrow the home screen, select **Palmer penguins** under
**Public datasets**, and press <kbd>Enter</kbd>. A catalog file this small
downloads without a question.

The table has 344 penguins. Empty fields in the file are null, shown as `∅`;
`rownames` is the row number the host, Rdatasets, adds. The data is CC0, from
Palmer Station LTER; credit Horst, Hill and Gorman (2020).

To skip the home screen, give the URL; datui asks before it downloads a URL
you type, and <kbd>Enter</kbd> says yes:

```bash,network
datui https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv
```

| Key | Action |
|---|---|
| Arrow keys or <kbd>h</kbd> <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd> | Move around |
| <kbd>PgUp</kbd> <kbd>PgDn</kbd> | Move a page |
| Wheel, click | Scroll; put the cursor on a cell. A double-click inspects the row |
| <kbd>i</kbd> | Info panel: columns and file details |
| <kbd>?</kbd> | This screen's keys; <kbd>Enter</kbd> on one runs it |
| <kbd>Esc</kbd> | Close a panel or go back |
| <kbd>q</kbd> | Back to the home screen; quits when the table was opened from the shell |
| <kbd>Ctrl</kbd>+<kbd>Q</kbd> | Quit |

## 2. Group by species

Which species is heaviest? Press <kbd>:</kbd>; the command line opens at
`sql:`. Type this, then press <kbd>Enter</kbd>:

```sql,dataset=penguins,network,rows=3
SELECT species, AVG(body_mass_g) AS mean_mass_g, COUNT(*) AS penguins
FROM df
GROUP BY species
ORDER BY mean_mass_g DESC
```

Type it on one line, or press <kbd>Alt</kbd>+<kbd>Enter</kbd> for a new line.

| species | mean_mass_g | penguins |
|---|---:|---:|
| Gentoo | 5076.01626 | 124 |
| Chinstrap | 3733.088235 | 68 |
| Adelie | 3700.662252 | 152 |

`AVG` skips the two penguins with no mass; `COUNT(*)` counts every row. The
table is named `df`. Press <kbd>Enter</kbd> on Gentoo to see its 124 penguins,
and <kbd>Esc</kbd> to come back.

The same summary is one line in **q**, a subset of q that evaluates right
to left. Press <kbd>:</kbd>, then <kbd>Ctrl</kbd>+<kbd>T</kbd> for `q:`:

```q,dataset=penguins,network,rows=3
select mean_mass_g: avg body_mass_g by species
```

A new query starts a fresh view, clearing sidebar filters and sort. See
[querying](../user-guide/querying-data.md) for more SQL and q.

## 3. Chart the measurements

Press <kbd>R</kbd> to return to the original rows, then <kbd>c</kbd> for charts.
On the **XY** tab, <kbd>Tab</kbd> moves between settings:

| Setting | Choose |
|---|---|
| Style | Scatter (<kbd>→</kbd>) |
| X axis | `flipper_length_mm` |
| Y series | `body_mass_g` |

<kbd>Space</kbd> on a column setting opens its picker. Type part of the name;
on **Y series**, <kbd>Space</kbd> toggles a series and <kbd>Enter</kbd> closes
the picker. The two penguins with no measurements are left out.

Now press <kbd>6</kbd> for **Bar**, choose `species` for **Category** and
**Count** for **Value**: Adelie 152, Gentoo 124, Chinstrap 68.

Press <kbd>e</kbd> in the chart to export a PNG or EPS. Press <kbd>Esc</kbd> to
return to the table. [More chart options](../user-guide/charting.md).

## 4. Copy or export

Run the species summary again, then choose an output:

| Do this | How |
|---|---|
| Copy the summary into a note | <kbd>y</kbd> → Scope **Table** → Format **Markdown** → <kbd>Enter</kbd> |
| Export a data file | <kbd>e</kbd> → type `penguin-summary.csv` in **Path** → <kbd>Enter</kbd> |
| Reuse the query on another file | <kbd>v</kbd> → <kbd>s</kbd> to save a view |

Export and copy use the current rows and columns. They do not overwrite the
input file unless you export to that path and confirm.

## Use your own data

Replace `<FILE>`, `<DIRECTORY>` and `<BUCKET>/<PREFIX>` with your own:

```bash,template
datui <FILE>
datui <DIRECTORY>/
datui s3://<BUCKET>/<PREFIX>/
```

`datui` alone starts at the home screen.

Next: [open files and directories](../user-guide/open-files.md), [formats](../formats/index.md),
[cloud access](../user-guide/remote-data.md), or
[all keyboard shortcuts](../reference/keyboard-shortcuts.md).
