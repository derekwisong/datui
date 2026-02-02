# Datui

Explore Polars DataFrames in the terminal.

To learn more, see the [full documentation](https://derekwisong.github.io/datui/).

## Installation

Install Datui with `pip`:

```
pip install datui
```

## Usage

**View a LazyFrame or DataFrame**

```python
import polars as pl
import datui

# From a LazyFrame (e.g. scan)
lf = pl.scan_csv("data.csv")
datui.view(lf)
```

Press `q` to exit Datui and return to Python.

## Run at the Command Line

Run the `datui` command line application:

```bash
datui /path/to/data.parquet
```

For help:

```bash
datui --help
```
