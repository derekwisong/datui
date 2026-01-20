# Datui

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.2.3-orange.svg)
![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)

A fast, memory-efficient terminal user interface (TUI) for exploring and analyzing tabular data files, built with Rust and Polars.

> Datui is currently in rapid development—features and interfaces are evolving as we build. Expect some changes between releases as we work toward a stable foundation.

📖 **Documentation**: For comprehensive documentation including user guides, reference, and examples, see the [full documentation][docs].

## Demo

![Basic Navigation Demo](demos/01-basic-navigation.gif)

## Features

- **File Format Support**: Load CSV, Parquet, JSON, and NDJSON files with customizable parsing options
- **Query Engine**: SQL-like query syntax for selecting columns, filtering, grouping, and aggregation
- **Statistical Analysis**: Column-level and group-level statistics (mean, median, std dev, percentiles, skewness, kurtosis)
- **Distribution Analysis**: Automatic distribution type inference with Q-Q plots
- **Data Transformations**: Sort, filter, reorder, freeze columns, pivot, and melt operations
- **Keyboard-Driven**: Vim-style navigation (`h`/`j`/`k`/`l` for movement) and intuitive shortcuts
- **Memory Efficient**: Built on Polars Lazy API with lazy evaluation for handling large datasets
- **Templates**: Save and restore data view configurations (queries, filters, sorts, column orders)

## Prerequisites

- **Rust**: Version 1.70 or later (for building from source)
- **Terminal**: A terminal emulator that supports ANSI escape codes

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
- `f` - Open filter menu
- `s` - Open sort & column order menu
- `a` - Open statistical analysis
- `Ctrl+h` - Show help

Press `q` or `Esc` to exit.

## Usage

### Basic Usage

```bash
datui [OPTIONS] <PATH>
```

### Examples

Load a CSV file:

```bash
datui data/sales.csv
```

Load a Parquet file with custom options:

```bash
datui data/large_dataset.parquet --skip-lines 1
```

Load a JSON file:

```bash
datui data/config.json
```

Run from source with Cargo:

```bash
cargo run -- data/sample.parquet
```

### Command Line Options

Use `--help` to see all available options:

```bash
datui --help
```

Common options include:
- `--skip-lines <N>` - Skip the first N lines when reading CSV files
- `--clear-cache` - Clear application cache (query history, etc.)
- `--debug` - Enable debug overlay showing query state and performance metrics

## Keyboard Shortcuts

### Navigation
- **Arrow keys** (`h`/`j`/`k`/`l`) - Scroll table
- **PgUp/PgDown** - Scroll pages
- **Home** - Go to top

### Data Operations
- `/` - Open query input
- `f` - Open filter menu
- `s` - Open sort & column order menu
- `a` - Open statistical analysis
- `r` - Reverse sort order
- `R` - Reset table (clear queries, filters, sorts, locks)
- `T` - Apply most relevant template
- `t` - Open template menu

### Display
- `i` - Toggle info view
- `Ctrl+h` - Toggle help

### Exit
- `q` / `Esc` - Quit

## Examples

### Query Data

Type `/` to open the query input, then use SQL-like syntax:

```
select column1, column2 where column3 > 100
```

### Filter and Sort

1. Press `f` to open the filter menu
2. Press `s` to open the sort menu
3. Navigate with arrow keys and select options

### Statistical Analysis

1. Press `a` to open statistical analysis
2. View column statistics, distributions, and correlations
3. Use arrow keys to navigate between statistics

More examples and detailed usage information are available in the [full documentation][docs].

## Supported File Formats

- **CSV** - Comma-separated values with customizable delimiters
- **Parquet** - Apache Parquet columnar storage format
- **JSON** - Standard JSON format
- **NDJSON** - Newline-delimited JSON (JSONL)

All formats support optional parsing options such as skipping lines, custom delimiters, and schema inference.

## Developing

### Building

```bash
cargo build              # Debug build
cargo build --release    # Release build
```

### Testing

```bash
cargo test              # All tests
cargo test --lib        # Unit tests only
cargo test --test integration_test  # Integration tests
```

### Code Quality

```bash
cargo fmt               # Format code
cargo clippy            # Run linter
```

For detailed development information, architecture details, and contribution guidelines, see the **For Developers** section of the [full documentation][docs].

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please see the [full documentation][docs] for development guidelines and architecture information.

---

**Note**: If you encounter any issues or have feature requests, please [open an issue on GitHub](https://github.com/derekwisong/datui/issues).

[docs]: https://derekwisong.github.io/datui/
