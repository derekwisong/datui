# Datui

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.2.26-orange.svg)
![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)

Datui is an interactive terminal user interface (TUI) for exploring and analyzing data files.

> Datui is currently in rapid development—features and interfaces are evolving.

📖 **Documentation**: For comprehensive documentation including user guides, reference, and examples, see the [full documentation][docs].

## Demo

![Basic Navigation Demo](demos/01-basic-navigation.gif)

## Features

- **File Format Support**: Load CSV, Parquet, JSON, Avro, Excel, Arrow, and more
- **Query Engine**: SQL-like query syntax for selecting columns, filtering, grouping, and aggregation
- **Charts**: Create charts right from the terminal and export them for distribution
- **Analysis**: See statistics about various aspects of your data
- **Transformations**: Sort, filter, reorder, and more
- **Keyboard-Driven**: Arrow keys and Vim-style navigation (`h`/`j`/`k`/`l`)
- **Configurable**: Configure Datui to suit your environment and needs
- **Templates**: Save and restore your work as a template (queries, filters, sorts, column orders)

## Installation

See the [Install Guide][install-guide] for full instructions.

### Pre-built Releases

Get a pre-built binary for your platform from the [releases](https://github.com/derekwisong/datui/releases) page.

### From Package Managers

> See [Package Managers][pkg-managers]

- **Pip**: `pip install datui`
- **Arch Linux (AUR)**:
  - `paru -S datui`
  - `yay -S datui`


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

Load a data file and start exploring:

```bash
datui path/to/data.csv
```

Use keyboard shortcuts to navigate:
- Arrow keys (`↑`/`↓`/`←`/`→`) or `h`/`j`/`k`/`l` - Navigate the table
- `/` - Open query input
- `s` - Open Sort & Filter modal (tabs: Sort, Filter)
- `a` - Open analysis tools
- `Esc` - Go back a screen
- `Ctrl+h` - Show help

Press `q` to exit.

## Configuration

> See the [Configuration Guide][config-guide]

Datui supports extensive customization through a configuration file.

Generate a default config:
```bash
datui --generate-config
```


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## For Developers

### Environment Setup

The following script can be used to set up your local repo for development:
```bash
python scripts/setup-dev.py
```

See the [Setup Script Guide](https://derekwisong.github.io/datui/latest/for-developers/contributing.html#setup-script)
for more information about what it does.

### Contributing

Contributions are welcome! Please see [Contributing][contributing] for more.

---

**Note**: If you encounter any issues or have feature requests, please [open an issue on GitHub](https://github.com/derekwisong/datui/issues).

[docs]: https://derekwisong.github.io/datui/
[config-guide]: https://derekwisong.github.io/datui/latest/user-guide/configuration.html
[install-guide]: https://derekwisong.github.io/datui/latest/getting-started/installation.html
[quickstart-guide]: https://derekwisong.github.io/datui/latest/getting-started/quick-start.html
[pkg-managers]: https://derekwisong.github.io/datui/latest/getting-started/installation.html#package-managers
[compiling]: https://derekwisong.github.io/datui/latest/getting-started/installation.html#compiling-from-source
[contributing]: https://derekwisong.github.io/datui/latest/for-developers/contributing.html
