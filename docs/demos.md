# Demo Gallery

This page showcases interactive demonstrations of Datui's features. The
recordings open real data: the [demo datasets](https://github.com/derekwisong/datui/blob/main/demo/DATA-LICENSES.md)
bundled with the repository.

## Overview

![Overview Demo](demos/11-overview.gif)

## Home Screen

![Home Screen Demo](demos/12-home-screen.gif)

**What it shows:**
- Running `datui` with no arguments
- Recents, the current directory, the configured data directory, and directories
  of datasets opened before
- The details pane: source filesystem, rows, columns, size on disk, size once
  decompressed with its codec and ratio, row groups, and hive partition layout
- Typing to filter, with the matched characters underlined
- `Found`, the recursive search of the working directory that typing starts
- Matching on a *column* (`tic` finds every dataset with a `ticker` column)
- Opening a dataset with `Enter`

See [The Home Screen](user-guide/home-screen.md) for the full guide.

## Light Terminals

![Light Theme Demo](demos/13-light-theme.gif)

**What it shows:**
- The light colour set, chosen automatically on a terminal that sets `COLORFGBG`
  (or with `mode = "light"` in the config)
- The home screen, the table and the sort & filter sidebar in that set

See [Theme Mode](user-guide/configuration.md#theme-mode-light-and-dark-terminals).

## Navigation

![Basic Navigation Demo](demos/01-basic-navigation.gif)

**What it shows:**
- Opening the Meteoritical Society's catalogue of 45,716 meteorites
- Scrolling with `↑`/`↓` (or `j`/`k`) and moving across columns with `→` (or `l`)

See [Loading Data](user-guide/loading-data.md) for more information about file formats and options.

## Querying

![Querying Demo](demos/02-querying.gif)

**What it shows:**
- Opening a hive-partitioned directory of US baby names, one file per year
- Opening the query input with `/`
- Typing a query: `select year, name, count, rank where name = "Derek", sex = "M"`
- One name's popularity, year by year

See [Querying Data](user-guide/querying-data.md) for detailed query syntax and examples.

## Info Panel

![Info Panel Demo](demos/03-info.gif)

**What it shows:**
- Opening the Info panel with `i` on every orbital launch attempt since Sputnik
- The schema and the resources tabs

See [Dataset Info](user-guide/dataset-info.md) for details about the Schema and Resources tabs.

## Pivot

![Pivot Demo](demos/04-pivot.gif)

**What it shows:**
- Opening the Pivot & Melt dialog with `p` on the Gapminder country-year data
- Indexing on country and continent and pivoting on year: one column per year of
  life expectancy

See [Pivot and Melt](user-guide/reshaping.md) for pivot and melt options.

## Melt

![Melt Demo](demos/05-melt.gif)

**What it shows:**
- Switching to the Melt tab on 157 years of Central Park daily weather
- Keeping `date` as the index and melting every measurement into `variable` and
  `value` rows

See [Pivot and Melt](user-guide/reshaping.md) for pivot and melt options.

## Sorting

![Sorting Demo](demos/06-sorting.gif)

**What it shows:**
- Opening the Sort & Filter dialog with `s` on every magnitude 6+ earthquake
  since 1900
- Sorting by magnitude, descending: Chile 1960, Alaska 1964, Sumatra 2004

See [Filtering and Sorting](user-guide/filtering-sorting.md) for sort and filter options.

## Filtering

![Filtering Demo](demos/07-filtering.gif)

**What it shows:**
- Switching to the Filter tab on the meteorite catalogue
- Adding `mass_g > 1000000` and applying it: the 52 meteorites heavier than a tonne

See [Filtering and Sorting](user-guide/filtering-sorting.md) for sort and filter options.

## Export

![Export Demo](demos/08-export.gif)

**What it shows:**
- Opening export with `e` on the Palmer penguins and entering an output path
- Exporting the current data to Parquet in `/tmp`

See [Exporting Data](user-guide/exporting-data.md) for supported formats and options.

## Correlation Matrix

![Correlation Matrix Demo](demos/09-correlation-matrix.gif)

**What it shows:**
- Opening analysis with `a` on daily Bitcoin chain statistics and selecting the
  Correlation Matrix tool
- Scrolling the matrix: blocks, addresses, transaction counts, values and fees

See [Analysis Features](user-guide/analysis-features.md) for details.

## Charting

![Charting Demo](demos/10-charting.gif)

**What it shows:**
- Opening the chart with `c` on daily Bitcoin chain statistics
- Transactions per day since the genesis block, as a line chart
- Exporting the chart to a PNG file

See [Charting](user-guide/charting.md) for more details.

## Overview

![Overview Demo](demos/11-overview.gif)

**What it shows:**
- Scrolling through 157 years of Central Park daily weather
- Querying it down to the last decade:
  `select date, tmax_c, tmin_c, precip_mm, snow_mm where date >= 2016.01.01`
- Charting the daily high against the date: ten summers and ten winters
- The Analysis page: Describe, Distribution and Correlation Matrix on the
  queried rows

A single tour of table navigation, querying, charting and analysis.

---

To install and run Datui, see the [Getting Started Guide](getting-started.md).
