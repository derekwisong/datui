# Check data quality

Press <kbd>a</kbd>, choose **Data Quality**, and press <kbd>Enter</kbd>.
Use it to find missing values, repeated values and changes across files or time.

| Source | What opens |
|---|---|
| Local file | Results from the default plan; <kbd>e</kbd> edits the plan |
| Remote source | The plan first; review it and press <kbd>Enter</kbd> to read values |

## Check missing values

Open the [quick-start penguin data](../getting-started/quick-start.md#1-open-the-data)
with `--null-value NA`, then:

1. Open **Data Quality** and press <kbd>e</kbd> to edit the plan.
2. Use **Current view** scope, **dataset** grain and **Full scan** compute.
3. Press <kbd>Enter</kbd> and confirm the full scan.
4. Open **Columns** with <kbd>2</kbd> and inspect `body_mass_g`.

The source has 344 rows and two null body-mass values: a null rate of
`2 / 344`, about **0.58%**. A filter changes the current-view denominator.
Use **whole source** when you want to ignore the query and filters.

## Inspect a finding

On **Overview**, select an observation and press <kbd>Enter</kbd> to see
its definition, row counts and examples. For supported exact observations,
press <kbd>Enter</kbd> again to open the matching rows; <kbd>Esc</kbd> returns.
A sampled observation may require a full profile before this drill-down is available.

| Result tab | Use it for |
|---|---|
| **1 Overview** | Findings and their supporting rows |
| **2 Columns** | Nulls, distinct values, parse rates and ranges |
| **3 Segments** | Compare files, partitions, row chunks or time windows |
| **4 Trends** | Plot changes across ordered segments and inspect time-role latency |

## Compare parts of a dataset

Press <kbd>e</kbd> to edit the plan. Choose the source or row range in
**Scope**, then choose how to divide it in **Grain**. Set **Comparison** to
the previous segment or first-segment baseline and run it.

On **Segments** or **Trends**, <kbd>[</kbd> / <kbd>]</kbd> selects a column,
<kbd>m</kbd> changes the metric and <kbd>b</kbd> uses the selected segment as
the baseline. For time windows, assign a date/datetime column under **Time roles**;
datui does not infer a column's business meaning from its name.

## Check the read size

Press <kbd>p</kbd> for the access plan. Full scans require confirmation.
Sampling has different costs depending on grain:

- **Whole dataset:** samples from a bounded prefix, so later rows may not be represented.
- **Files, partitions, chunks or windows:** reads the entire selected scope and keeps a sample per segment.

Results label the evaluated rows and whether the values are sampled, exact,
or metadata-only. Sampled distinct counts cannot establish uniqueness.
Missing-column and type-conflict findings always describe the loaded source's
footer metadata, even when value checks use a narrower scope.

See the [reference](../reference/data-quality.md) for scope syntax, metric
formulas and budgets, or the [key list](../reference/keyboard-shortcuts.md#analysis).
