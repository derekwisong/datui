# Check data quality

Press <kbd>a</kbd>, choose **Data Quality**, and press <kbd>Enter</kbd>.
Use it to find missing values, repeated values and changes across files or time.

The first time, the pane is the [Sample](analysis-features.md#sampling) form:
which rows, how they are picked, how many. Press <kbd>Enter</kbd> to run with it
as it stands; nothing is read before that. Every analysis tool reads the same
sample, so the next tool you open runs at once.

## Check missing values

Open the [quick-start penguin data](../getting-started/quick-start.md#1-open-the-data)
with `--null-value NA`, then:

1. Press <kbd>a</kbd>, choose **Data Quality** and press <kbd>Enter</kbd>.
2. Press <kbd>Enter</kbd> in the Sample form. The table is smaller than the
   sample size, so every row is read.
3. Open **Columns** with <kbd>2</kbd> and inspect `body_mass_g`.

The source has 344 rows and two null body-mass values: a null rate of
`2 / 344`, about **0.58%**. A filter changes the rows checked; to ignore it,
press <kbd>s</kbd> and set **Rows from** to the unfiltered source.

## Inspect a finding

On **Overview**, select a finding and press <kbd>Enter</kbd>: its numbers, the
evidence and what to check. Press <kbd>Enter</kbd> again to open the matching
rows; on a sampled run these are the sample's rows, the ones the finding
counted. <kbd>Esc</kbd> returns.

| Page | Use it for |
|---|---|
| **Overview** | Problems, notes and clean columns, most important first |
| **Columns** | Each column's findings, nulls, distinct values, parse rates and ranges |
| **Segments** | Compare files, partitions, days or row chunks |
| **Trends** | Each column across the whole range, and the time between dates |
| **Plan** | How the rows are split and compared |

<kbd>←</kbd> <kbd>→</kbd> move between the pages.

## Compare parts of a dataset

Press <kbd>e</kbd> for the plan. Move to **Grain**, press <kbd>Space</kbd> and
choose how to split the rows: by file, by a partition column, by day, week or
month of a date column, or in chunks of rows. Set **Compare** to the segment
before or a baseline the same way, then press <kbd>Enter</kbd> to run.

**Segments** lists each segment's rows, its share of null cells and its
largest change: a row count that halved or doubled, or a column's rate that
moved past sampling noise. <kbd>o</kbd> puts the largest changes first,
<kbd>Enter</kbd> shows a segment's columns beside the one it is compared with,
and <kbd>b</kbd> makes the selected segment the baseline.

**Trends** draws each column across the whole range, the rows per segment
first, pooling consecutive segments into bars; <kbd>m</kbd> changes the
measure. A sample thin per segment, such as 100,000 rows over years of days,
names only large changes on Segments; Trends shows the smaller ones. To judge
single days, sample **Equal per value** of the date, or read every row.

For the time between dates, assign **Time roles** on the plan: datui does not
infer a column's meaning from its name.

## Check the read size

Press <kbd>p</kbd> for the access plan. Reading every row asks for
confirmation first. The header says what each result was measured on:
`Data Quality · sample of 100,000 of 36,839,175 rows`.

Missing-column and type-conflict findings describe the loaded source's
footers, whatever rows the sample covers.

See the [reference](../reference/data-quality.md) for the plan, keys, metric
formulas and budgets, or the [key list](../reference/keyboard-shortcuts.md#analysis).
