# Datui

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.2.14-orange.svg)
![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)

Datui is an interactive terminal user interface (TUI) for exploring and analyzing data files.

> Datui is currently in rapid development—features and interfaces are evolving.

📖 **Documentation**: For comprehensive documentation including user guides, reference, and examples, see the [full documentation][docs].

## Demo

![Basic Navigation Demo](demos/01-basic-navigation.gif)

## Features

- **File Format Support**: Load CSV, Parquet, JSON, and NDJSON files
- **Query Engine**: SQL-like query syntax for selecting columns, filtering, grouping, and aggregation
- **Analysis**: See statistics about various aspects of your data
- **Transformations**: Sort, filter, reorder, and more
- **Keyboard-Driven**: Arrow keys and Vim-style navigation (`h`/`j`/`k`/`l` for movement) and intuitive shortcuts
- **Templates**: Save and restore data view configurations (queries, filters, sorts, column orders)

## Installation

### From Releases

Download a pre-built binary for your platform from the [releases](https://github.com/derekwisong/datui/releases) page.

### From Source

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release
```

The binary will be available at `target/release/datui`.

## Quick Start

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

Datui supports extensive customization through a configuration file. Generate a default config:

```bash
datui --generate-config
```

This creates `~/.config/datui/config.toml` where you can customize:

- **Colors**: Complete theme customization with hex, named, or indexed colors
- **Display**: Row numbers, page buffering, starting index
- **Performance**: Sampling thresholds, event polling
- **File Loading**: Default delimiters, headers, compression
- **Query History**: History limits and caching
- **Templates**: Auto-apply behavior

**Example config:**
```toml
[display]
row_numbers = true
row_start_index = 0

[theme.colors]
primary = "#00bfff"        # Bright blue keybinds
error = "bright_red"       # Red errors
controls_bg = "dark_gray" # Dark gray bar
```

See the [Configuration Guide][config-guide] for complete documentation.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## For Developers

### Environment Setup

The following script can be used to set up your local repo for development:
```bash
python scripts/setup-dev.py
```

See the [Setup Script Guide](https://derekwisong.github.io/datui/main/for-developers/contributing.html#setup-script)
for more information about what it does.

### Contributing

Contributions are welcome! Please see the [full documentation][docs] for more information.

---

**Note**: If you encounter any issues or have feature requests, please [open an issue on GitHub](https://github.com/derekwisong/datui/issues).

[docs]: https://derekwisong.github.io/datui/
[config-guide]: https://derekwisong.github.io/datui/main/user-guide/configuration.html
