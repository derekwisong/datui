# Datui

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.3.2-orange.svg)
![Rust](https://img.shields.io/badge/rust-1.89.0+-orange.svg)
![Downloads](https://img.shields.io/github/downloads/derekwisong/datui/total?style=flat-square&logo=github&color=blue)
![PyPI Downloads](https://img.shields.io/pypi/dm/datui?style=flat-square&logo=pypi&logoColor=white&color=blue)
![GitHub Stars](https://img.shields.io/github/stars/derekwisong/datui?style=flat-square&logo=github&color=blue)
[![CI](https://img.shields.io/github/actions/workflow/status/derekwisong/datui/ci.yml?branch=main&style=flat-square&logo=githubactions&logoColor=white)](https://github.com/derekwisong/datui/actions)

**Datui** is a terminal UI for looking at tabular data: Parquet, CSV, JSON, Arrow
and more, on disk or in S3, GCS and HTTP, from a few rows to a few billion.

📖 **Documentation**: [Full User Guide][docs].

![Overview Demo](demos/11-overview.gif)

## What it does

- **Opens the file where it is.** Local paths, `s3://`, `gs://` and `https://`
  URLs, and hive-partitioned directories. Parquet is read lazily through
  [Polars](https://pola.rs), so a dataset larger than memory scrolls like a
  small one.
- **Shows you what is around you.** Run `datui` with no arguments for the home
  screen: recent datasets, the current directory, your configured data
  directories, and the buckets your cloud credentials can reach. Each row shows
  rows, columns and size before you open it; the schema is one keystroke away.
  See [The Home Screen][home-screen].
- **Answers questions.** SQL, a short query language (`select a, b where c > 10
  by region`), and fuzzy text search across every column. Sort, filter, freeze
  and hide columns from a sidebar.
- **Summarises.** Describe, distribution fitting with Q-Q plots, and a
  correlation matrix, computed on the data as filtered.
- **Draws.** Line, scatter, bar, histogram, box, KDE and heatmap charts in the
  terminal, exportable as PNG or EPS.
- **Reshapes and saves.** Pivot and melt, export to CSV, Parquet, JSON, NDJSON,
  Arrow or Avro, and templates that replay a query, filters and sort on the next
  dataset with the same shape.
- **Fits in.** Light and dark palettes, every colour configurable, and a theme
  template for [Omarchy][system-theming]. Arrow keys or `h`/`j`/`k`/`l`.

## Quick Start

> See the [Quick Start Guide][quickstart-guide]

Open a file:

```bash
datui data.parquet
datui --hive /path/to/partitioned/dataset
datui s3://bucket/path/file.parquet
datui https://example.com/file.csv
```

Or open nothing and pick from the home screen:

```bash
datui
```

From Python:

```python
import polars as pl
import datui

datui.view(pl.scan_parquet("data.parquet"))
```

Press `?` for the keys. `/` queries, `s` sorts and filters, `a` analyses,
`c` charts, `Ctrl+O` returns to the home screen, `q` quits.

> See [Loading Data][loading-data], [Loading Remote Data][loading-remote] and
> the [Python Module][python-module].

## Installation

> Visit the [Install Guide][install-guide] for additional installation details

### ✨ Quick Install for Linux and macOS

```bash
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
```
**or via cargo:**
```
cargo install datui
```
*Don't like piping to shell? See the alternative methods below.*

### Pre-built Releases

Get the pre-built binary for your platform from the [Latest Release](https://github.com/derekwisong/datui/releases/latest).

### Package Managers

> See [Package Managers][pkg-managers]

- **Arch Linux (AUR)**:
  ```bash
  paru -S datui-bin
  ```
- **macOS (Homebrew)**:
  ```bash
  brew tap derekwisong/datui
  brew trust derekwisong/datui
  brew install datui
  ```
- **Windows (WinGet)**:
  ```powershell
  winget install derekwisong.datui
  ```
- **Pip** (See [Python Module][python-module]):
  ```
  pip install datui
  ```


### From Source

> See [Compiling][compiling]

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release --locked
```

The binary will be available at `target/release/datui`.

## Configuration

> See the [Configuration Guide][config-guide]

Generate a default [TOML](https://toml.io) config file:
```bash
datui --generate-config
```

Data directories for the home screen, S3 endpoints and credentials, number
formatting and the colour theme all live there.

## For Developers

### Setup

See the [Setup Script][setup-script] guide to quickly get configured.

### Contributing

Contributions are welcome! Please see [Contributing][contributing] for more.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

> If you encounter any issues or have feature requests, please
> [open an issue on  GitHub](https://github.com/derekwisong/datui/issues).
>
> Found a security problem? Please report it privately instead. See
> [SECURITY.md](SECURITY.md).

[docs]: https://derekwisong.github.io/datui/
[config-guide]: https://derekwisong.github.io/datui/latest/user-guide/configuration.html
[install-guide]: https://derekwisong.github.io/datui/latest/getting-started/installation.html
[quickstart-guide]: https://derekwisong.github.io/datui/latest/getting-started/quick-start.html
[pkg-managers]: https://derekwisong.github.io/datui/latest/getting-started/installation.html#package-managers
[compiling]: https://derekwisong.github.io/datui/latest/getting-started/installation.html#compiling-from-source
[contributing]: https://derekwisong.github.io/datui/latest/for-developers/contributing.html
[setup-script]: https://derekwisong.github.io/datui/latest/for-developers/setup-script.html
[python-module]: https://derekwisong.github.io/datui/latest/user-guide/python-module.html
[loading-remote]: https://derekwisong.github.io/datui/latest/user-guide/loading-data.html#remote-data-s3-gcs-and-http
[loading-data]: https://derekwisong.github.io/datui/latest/user-guide/loading-data.html
[home-screen]: https://derekwisong.github.io/datui/latest/user-guide/home-screen.html
[system-theming]: https://derekwisong.github.io/datui/latest/user-guide/system-theming.html
