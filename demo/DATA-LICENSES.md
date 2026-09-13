# Demo data: sources and licences

The datasets under `demo/data` are what the demo GIFs and the home-screen fixture
open. `demo/build.py` downloads the public ones from the URLs below, cleans them
the same way every run, and writes zstd Parquet. Datui's code is MIT; these files
are third-party data under the licences listed here.

## penguins.parquet

- Source: Palmer Station Antarctica LTER, via the `palmerpenguins` R package,
  <https://github.com/allisonhorst/palmerpenguins>
- Licence: CC0 1.0
- Citation requested: Horst, Hill & Gorman (2020), doi:10.5281/zenodo.3960218
- Changes: `NA` read as null. 344 rows, 8 columns.

## gapminder.parquet

- Source: the `gapminder` R package excerpt, <https://github.com/jennybc/gapminder>,
  based on free material from GAPMINDER.ORG
- Licence: CC0 (package); the underlying Gapminder data is CC BY 4.0
- Changes: columns renamed to `life_exp`, `population`, `gdp_per_capita`.
  1,704 rows: 142 countries, every five years from 1952 to 2007.

## earthquakes_m6.parquet

- Source: USGS Earthquake Catalog, magnitude 6 and above since 1900,
  <https://earthquake.usgs.gov/fdsnws/event/1/>
- Licence: US public domain (USGS-produced data)
- Changes: columns renamed, `time` parsed as UTC datetime, sorted by time,
  error and update columns dropped. 14,522 rows.

## central_park_weather.parquet

- Source: NOAA GHCN-Daily, station USW00094728 (New York Central Park),
  <https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/>
- Licence: US public domain (NOAA data)
- Citation requested: Menne et al. (2012), doi:10.7289/V5D21VHZ
- Changes: pivoted from one row per element to one row per day; tenths of a
  degree and tenths of a millimetre converted to `tmax_c`, `tmin_c`, `precip_mm`;
  `snow_mm` and `snow_depth_mm` kept in millimetres. 57,596 days from 1869-01-01.

## meteorites.parquet

- Source: NASA Open Data, Meteorite Landings, compiled by the Meteoritical Society,
  <https://data.nasa.gov/dataset/meteorite-landings>
- Licence: published by NASA as open data; no licence string on the record
- Changes: columns renamed, `GeoLocation` dropped in favour of `latitude` and
  `longitude`, sorted by year and name. 45,716 rows.

## space_launches.parquet

- Source: General Catalog of Artificial Space Objects, launch log, Jonathan McDowell,
  <https://planet4589.org/space/gcat/>
- Licence: CC BY 4.0. Cite as: McDowell, J., 2020, General Catalog of Artificial
  Space Objects, https://planet4589.org/space/gcat
- Changes: orbital launch attempts only (`LaunchCode` starting with `O`), a subset
  of columns renamed, `-` placeholders read as null, `success` derived from the
  launch code. 7,167 rows.

## babynames/year=…/

- Source: US Social Security Administration, names by year of birth,
  <https://www.ssa.gov/oact/babynames/limits.html>, via the `babynames` R package
  (<https://github.com/hadley/babynames>) when the SSA archive is not on hand
- Licence: US public domain (CC0 on the data.gov record)
- Changes: the 1,000 most common names per sex and year, with `rank` added.
  Hive-partitioned by year.

## Snapshots not built from a public source

These cannot be rebuilt by `demo/build.py`. They are copies of the author's own
data, trimmed for the demos.

- `quant-research/`: an extract of a research tree. Daily bars and returns for
  30 tickers, out-of-sample factor information coefficients, an alpha model
  parameter sweep, and a factor covariance history. Hive-partitioned by year.
- `bitcoin_daily.parquet`: daily statistics computed from the public Bitcoin
  blockchain: blocks, addresses, transaction counts, values and fees, from the
  genesis block.
- `fred/series_id=…/`: US government economic series (Treasury yields, the federal
  funds rate, CPI, GDP, money supply) retrieved from FRED, Federal Reserve Bank
  of St. Louis. US public domain.
