# datui

Explore Polars DataFrames, files and URLs in the terminal.

```bash,install
pip install datui
```

The package installs the `datui` command and the `datui` module.

```python,network
import polars as pl
import datui

url = "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv"
datui.view(pl.scan_csv(url))           # a LazyFrame, a DataFrame, a path or a URL

result = datui.view(pl.scan_csv(url), capture=True)
if result is not None:
    print(result.collect())            # the query, filters and sort you left on screen
```

Press `q` or `Ctrl+Q` to return to Python. With `capture=True`, `view()` returns
the final view as a LazyFrame, or `None` when no dataset was open.

[Python API](https://derekwisong.github.io/datui/latest/reference/python-api.html) ·
[Documentation](https://derekwisong.github.io/datui/latest/) ·
`datui --help` for the command line.
