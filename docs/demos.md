# Demos

Every recording uses real data: the local
[demo datasets](https://github.com/derekwisong/datui/blob/main/demo/DATA-LICENSES.md)
and the publisher-hosted datasets in datui's public cloud catalog.

## Home and cloud

Start with recent and local files, browse the built-in public cloud catalog,
open Google's `us-states.parquet` without credentials, then return home and
open the local Palmer penguins data.

![Home and Cloud Demo](demos/14-cloud-home.gif)

## Home screen variations

The browse-first cut spends more time on the six publisher-maintained public
datasets and their details, then descends into BigQuery sample data.

![Cloud Browse Demo](demos/15-cloud-home-browse.gif)

The local-first cut starts with populated recents and Central Park weather,
then moves from the same home screen into NOAA's public Parquet folders. See
[The Home Screen](user-guide/home-screen.md).

![Local and Cloud Demo](demos/16-cloud-home-local.gif)

## Light terminals

The light palette, chosen automatically on a terminal that sets `COLORFGBG` or
with `mode = "light"`. See [Light and dark](user-guide/configuration.md#light-and-dark).

![Light Theme Demo](demos/13-light-theme.gif)

## Navigation

The Meteoritical Society's catalog of 45,716 meteorites, with the arrow keys
and <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd>.

![Basic Navigation Demo](demos/01-basic-navigation.gif)

## Querying

A hive-partitioned directory of US baby names, one file per year, and one
name's popularity year by year:
`select year, name, count, rank where name = "Derek", sex = "M"`.
See [Querying Data](user-guide/querying-data.md).

![Querying Demo](demos/02-querying.gif)

## Info panel

<kbd>i</kbd> on every orbital launch attempt since Sputnik: the schema and
resources tabs. See [Dataset Info](user-guide/dataset-info.md).

![Info Panel Demo](demos/03-info.gif)

## Pivot

Gapminder country-year data pivoted on year: one column per year of life
expectancy. See [Pivot and Melt](user-guide/reshaping.md).

![Pivot Demo](demos/04-pivot.gif)

## Melt

Central Park daily weather melted into `variable` and `value` rows, keeping
`date` as the index.

![Melt Demo](demos/05-melt.gif)

## Sorting

Every magnitude 6+ earthquake since 1900, sorted by magnitude descending:
Chile 1960, Alaska 1964, Sumatra 2004. See
[Filtering and Sorting](user-guide/filtering-sorting.md).

![Sorting Demo](demos/06-sorting.gif)

## Filtering

The meteorite catalog with `mass_g > 1000000` applied: the 52 meteorites
heavier than a metric ton.

![Filtering Demo](demos/07-filtering.gif)

## Export

The Palmer penguins written to Parquet. See [Exporting Data](user-guide/exporting-data.md).

![Export Demo](demos/08-export.gif)

## Correlation matrix

Daily Bitcoin chain statistics: blocks, addresses, transaction counts, values
and fees. See [Analysis](user-guide/analysis-features.md).

![Correlation Matrix Demo](demos/09-correlation-matrix.gif)

## Charting

Transactions per day since the genesis block as a line chart, exported to PNG.
See [Charting](user-guide/charting.md).

![Charting Demo](demos/10-charting.gif)
