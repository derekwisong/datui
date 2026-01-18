# Datui

A terminal user interface (TUI) for exploring and analyzing data tables, built with Rust and Polars.

## Demo

![Basic Navigation Demo](demos/01-basic-navigation.gif)

## Features

- **Exploration**: Browse CSV, Parquet, and JSON files with a responsive TUI
- **Query Engine**: Custom query syntax for grouping, aggregation, and filtering
- **Statistics**: Column-level and group-level statistics (mean, median, std dev, percentiles, etc.)
- **Trasformations**: Sort, filter, reorder, freeze columns, pivot, and melt operations
- **Efficient**: Built on Polars Lazy API for handling large datasets

## Installation

### From Releases

Download a pre-built binary from the [releases](https://github.com/derekwisong/datui/releases) page.

### From Source

```bash
git clone https://github.com/derekwisong/datui.git
cd datui
cargo build --release
```

The binary will be available at `target/release/datui`.

## Usage

```bash
datui path/to/data.csv
```

```bash
cargo run -- path/to/data.parquet
```
