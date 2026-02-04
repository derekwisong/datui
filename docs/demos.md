# Demo Gallery

This page showcases interactive demonstrations of Datui's features.

## Navigation

![Basic Navigation Demo](demos/01-basic-navigation.gif)

**What it shows:**
- Loading a Parquet file (`people.parquet`)
- Scrolling through data using `↑`/ `↓` (or `j`/`k`) keys

See [Loading Data](user-guide/loading-data.md) for more information about file formats and options.

## Querying

![Querying Demo](demos/02-querying.gif)

**What it shows:**
- Opening the query input with `/`
- Typing a query: `select first_name, last_name, city, salary where salary > 80000`
- Executing the query and seeing filtered results

See [Querying Data](user-guide/querying-data.md) for detailed query syntax and examples.

## Info Panel

![Info Panel Demo](demos/03-info.gif)

**What it shows:**
- Opening the Info panel with `i`
- Viewing information about the dataset

See [Dataset Info](user-guide/dataset-info.md) for details about the Schema and Resources tabs.

## Pivot

![Pivot Demo](demos/04-pivot.gif)

**What it shows:**
- Opening the Pivot & Melt dialog with `p`
- Selecting index columns and applying a pivot (long → wide)

See [Pivot and Melt](user-guide/reshaping.md) for pivot and melt options.

## Melt

![Melt Demo](demos/05-melt.gif)

**What it shows:**
- Switching to the Melt tab and selecting index columns
- Applying a melt (wide → long)

See [Pivot and Melt](user-guide/reshaping.md) for pivot and melt options.

## Sorting

![Sorting Demo](demos/06-sorting.gif)

**What it shows:**
- Opening the Sort & Filter dialog with `s`
- Selecting a sort column and applying the sort

See [Filtering and Sorting](user-guide/filtering-sorting.md) for sort and filter options.

## Filtering

![Filtering Demo](demos/07-filtering.gif)

**What it shows:**
- Switching to the Filter tab and adding a filter (e.g. `dist_normal > 4`)
- Applying the filter to the table

See [Filtering and Sorting](user-guide/filtering-sorting.md) for sort and filter options.

## Export

![Export Demo](demos/08-export.gif)

**What it shows:**
- Opening export with `e` and entering an output path
- Exporting the current data to Parquet in `/tmp`

See [Exporting Data](user-guide/exporting-data.md) for supported formats and options.

## Correlation Matrix

![Correlation Matrix Demo](demos/09-correlation-matrix.gif)

**What it shows:**
- Opening analysis with `a` and selecting the Correlation Matrix tool
- Scrolling the correlation matrix

See [Analysis Features](user-guide/analysis-features.md) for details.

## Charting

![Charting Demo](demos/10-charting.gif)

**What it shows:**
- Opening the chart with `c`
- Viewing data in a line chart, and then a scatter plot
- Exporting the chart to a PNG file

See [Charting](user-guide/charting.md) for more details.

---

To install and run Datui, see the [Getting Started Guide](getting-started.md).
