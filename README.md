# datui

**Explore tabular data without leaving your terminal.**

Open a file, ask a question, and take the result with you. datui reads Parquet,
CSV, JSON, Excel and more, from your disk or cloud storage. Query with SQL or
its short query language; sort, chart, reshape and export with the keyboard.

[![Release](https://img.shields.io/github/v/release/derekwisong/datui?style=flat-square)](https://github.com/derekwisong/datui/releases/latest)
[![CI](https://img.shields.io/github/actions/workflow/status/derekwisong/datui/ci.yml?branch=main&style=flat-square)](https://github.com/derekwisong/datui/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)

[Website][site] · [Documentation][docs] · [Quick start][quick-start] · [All demos][demos]

```bash
datui flights.parquet              # open a file
datui ./exports/                   # open a dataset or browse its files
datui s3://bucket/events/           # S3, GCS and Azure work too
datui                              # find a dataset from the home screen
```

![Filtering public US baby-name data to see one name's history](demos/02-querying.gif)

## Install

On Linux or macOS:

```bash
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
```

| Prefer a package manager? | Command |
|---|---|
| macOS | `brew tap derekwisong/datui && brew trust derekwisong/datui && brew install datui` |
| Windows | `winget install derekwisong.datui` |
| Python | `pip install datui` |
| Rust | `cargo install datui --locked` |
| Arch Linux | `paru -S datui-bin` |

The [installation guide][install-guide] covers apt, RPMs, user-only installs
and building from source. [Prebuilt binaries][latest-release] are also available.

## From rows to an answer

| Do this | Press | Read more |
|---|---|---|
| Query, run SQL, or search text | `/` | [Queries][query-guide] |
| Sort, filter, hide or freeze columns | `s` | [Table controls][filter-guide] |
| Plot a trend or distribution | `c` | [Charts][chart-guide] |
| Check statistics, correlations or data quality | `a` | [Analysis][analysis-guide] |
| Pivot or melt | `p` | [Reshaping][reshape-guide] |
| Copy a result or export a file | `y` / `e` | [Copy][copy-guide] · [Export][export-guide] |
| Save a view to reuse on another file | `v` | [Views][views-guide] |

Press `?` for help on any screen. `Esc` backs out; `Ctrl+Q` quits.
The [quick start][quick-start] walks through a real public dataset, from opening
it to saving a chart and a table.

Parquet and other scan-based formats use [Polars](https://pola.rs) to load rows
as needed. Queries, sorting and analysis can read much more than the visible
page; see [performance tips][performance] for large datasets.

## Use it from Python

`pip install datui` includes both the command and the Python module:

```python
import datui
import polars as pl

datui.view(pl.scan_parquet("flights.parquet"))

# Bring the view you built in the UI back to Python.
result = datui.view("flights.parquet", capture=True)
if result is not None:
    result.collect()
```

[Python guide][python-module] · [Supported formats and cloud access][loading-guide]

## Make it yours

Run `datui --generate-config` to create a TOML config. Set your data directories,
cloud sources, number formatting, and light or dark colors in the
[configuration guide][config-guide].

## Contribute

Found a bug or have an idea? [Open an issue][issues]. For code and documentation
changes, start with the [developer guide][for-developers]. Report security
issues using [SECURITY.md](SECURITY.md).

Built with [Polars](https://pola.rs) and [Ratatui](https://ratatui.rs).
Released under the [MIT license](LICENSE).

[site]: https://derekwisong.github.io/datui/
[docs]: https://derekwisong.github.io/datui/latest/
[quick-start]: https://derekwisong.github.io/datui/latest/getting-started/quick-start.html
[demos]: https://derekwisong.github.io/datui/latest/demos.html
[install-guide]: https://derekwisong.github.io/datui/latest/getting-started/installation.html
[latest-release]: https://github.com/derekwisong/datui/releases/latest
[query-guide]: https://derekwisong.github.io/datui/latest/user-guide/querying-data.html
[filter-guide]: https://derekwisong.github.io/datui/latest/user-guide/filtering-sorting.html
[chart-guide]: https://derekwisong.github.io/datui/latest/user-guide/charting.html
[analysis-guide]: https://derekwisong.github.io/datui/latest/user-guide/analysis-features.html
[reshape-guide]: https://derekwisong.github.io/datui/latest/user-guide/reshaping.html
[copy-guide]: https://derekwisong.github.io/datui/latest/user-guide/copying.html
[export-guide]: https://derekwisong.github.io/datui/latest/user-guide/exporting-data.html
[views-guide]: https://derekwisong.github.io/datui/latest/user-guide/views.html
[performance]: https://derekwisong.github.io/datui/latest/advanced/performance-tips.html
[loading-guide]: https://derekwisong.github.io/datui/latest/user-guide/loading-data.html
[python-module]: https://derekwisong.github.io/datui/latest/user-guide/python-module.html
[config-guide]: https://derekwisong.github.io/datui/latest/user-guide/configuration.html
[for-developers]: https://derekwisong.github.io/datui/latest/for-developers.html
[issues]: https://github.com/derekwisong/datui/issues
