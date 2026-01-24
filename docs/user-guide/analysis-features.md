# Analysis Mode

Use Datui to get insights about your data.

> For large datasets, analysis tools operate on sampled data. This means that
> a subset of the data will be drawn. You can resample the data with the `r` key.
>
> You may adjust the size threshold at which the full dataset will not be used
> and instead be sampled. See [Configuration](configuration.md) for details about
> adjusting the threshold.

## Starting Analysis Mode

Open analysis mode using the `a` key.

> See [Keyboard Shortcuts](../reference/keyboard-shortcuts.md) for more key bindings.

You will see a collection of tools on the right. Using the `Tab` key to navigate to the list,
select a tool to analyze your data with. The [Describe](#describe) tool is selected by default.

To exit analysis mode, press the `Esc` key.

## Tools

### Describe

Displays summary statistics about your data, similar to Polars'
[describe](https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.describe.html).

### Distribution Analysis

- Compares your data against a set of hypothetical distributions and suggests the best fit.
- Select a column and press `Enter` on it to view a Q-Q plot and a histogram for the column.

### Correlation Matrix

Discover the relationships in your data with the correlation matrix tool. Colors will be used to note
the degree of correlation.
