# Python Module

Datui is distributed on [Pypi](https://pypi.org/project/datui/).

Install using **pip**:
```
pip install datui
```

## Viewing a LazyFrame or DataFrame

View your `LazyFrame` (or `DataFrame`) in the terminal:

```
import polars as pl
import datui

# From a LazyFrame (e.g. scan)
lf = pl.scan_csv("data.csv")
datui.view(lf)
```

Press `q` to exit Datui.
