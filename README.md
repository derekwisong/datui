# Datui

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.2.33-orange.svg)
![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)

**Datui** is a high-performance terminal UI for exploring and analyzing datasets. Powered
by the **Polars** engine and written in **Rust**, it brings SQL-like power and Excel-like
visibility to your CLI.

> Datui is in rapid development; features and interfaces are evolving.

📖 **Documentation**: [Full User Guide][docs].

## Demo

![Basic Navigation Demo](demos/01-basic-navigation.gif)

## Why Datui?

- 🚀 **Fast**: Powered by Polars for optimized data handling
- 📁 **Universal**: Supports Parquet, CSV, JSON, Avro, Arrow, ORC, and Excel
- 🔍 **Query Engine**: SQL-like query syntax for selecting, filtering, grouping, and aggregation
- 📊 **Charts**: Render terminal-based charts and export them as images
- 🔬 **Analysis**: Use analytical tools to understand correlations, distributions, and more
- ⚒️ **Transformations**: Sort, filter, pivot, melt, and more
- ⌨️ **Keyboard-Driven**: Arrow keys and Vim-style navigation (`h`/`j`/`k`/`l`)


## Installation

See the [Install Guide][install-guide] for full instructions.

### ✨ Quick Install for Linux and MacOS

```bash
curl -fsSL https://raw.githubusercontent.com/derekwisong/datui/main/scripts/install/install.sh | sh
```

*Don't like piping to shell? See the alternative methods below.*

### Pre-built Releases

Get the pre-built binary for your platform from the [Latest Release](https://github.com/derekwisong/datui/releases/latest).

### From Package Managers

> See [Package Managers][pkg-managers]

- **Arch Linux (AUR)**:
  - `paru -S datui-bin`
  - `yay -S datui-bin`
- **Pip**: `pip install datui`


### From Source

> See [Compiling][compiling]

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release
```

The binary will be available at `target/release/datui`.


## Quick Start

> See the [Quick Start Guide][quickstart-guide]

- 💻 Load a file from the shell and start exploring:
  ```bash
  datui /path/to/data.parquet
  ```
- 🐍 View your data from Python:
  ```python
  import polars as pl
  import datui

  lf = pl.scan_parquet("/path/to/data.parquet")
  datui.view(lf)
  ```
- Use arrow keys or Vim-style keybinds (`h`/`j`/`k`/`l`) to navigate
- Press `q` to exit

> 💡 Use `?` or `F1` to show help


## Configuration

> See the [Configuration Guide][config-guide]

Generate a default [TOML](https://toml.io) config file:
```bash
datui --generate-config
```

## For Developers

### Environment Setup

See the [Setup Script][setup-script] guide to quickly get configured to run the tests,
build docs, demos, and create packages builds.

### Contributing

Contributions are welcome! Please see [Contributing][contributing] for more.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Note**: If you encounter any issues or have feature requests, please [open an issue on GitHub](https://github.com/derekwisong/datui/issues).

[docs]: https://derekwisong.github.io/datui/
[config-guide]: https://derekwisong.github.io/datui/latest/user-guide/configuration.html
[install-guide]: https://derekwisong.github.io/datui/latest/getting-started/installation.html
[quickstart-guide]: https://derekwisong.github.io/datui/latest/getting-started/quick-start.html
[pkg-managers]: https://derekwisong.github.io/datui/latest/getting-started/installation.html#package-managers
[compiling]: https://derekwisong.github.io/datui/latest/getting-started/installation.html#compiling-from-source
[contributing]: https://derekwisong.github.io/datui/latest/for-developers/contributing.html
[setup-script]: https://derekwisong.github.io/datui/latest/for-developers/setup-script.html