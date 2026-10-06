# Demos

Two recordings, each of a dataset from **Example datasets** on the home screen,
opened as its publisher serves it. Every step they show is written out in the
guides below.

## When should I fly out of JFK?

**NYC flights (2013)**: 336,776 departures from New York's three airports.
Sorted by `dep_delay`, inspected, then charted as the mean departure delay by
`hour`, one line per `origin`. At JFK it climbs from 0.5 minutes at 5:00 to
26.1 at 21:00. The data is 2013's; this is not a forecast.

<!-- CAPTURE PLACEHOLDER (#356): ![datui opening NYC flights, sorting by delay and charting the mean delay by hour per airport](demos/teaser.gif) -->

Do it yourself: [Query data](user-guide/querying-data.md#run-a-query) and
[Make a chart](user-guide/charting.md#examples-on-the-built-in-datasets).

## A public bucket, opened in place

**NOAA daily weather (GHCN-D)** on S3, with no login. Central Park's 155 years
of daily highs, from the catalog's bookmark, charted by year; then one year of
the whole bucket, `s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/`, opened as
one table and scrolled to its last row. The waits are real; nothing is
downloaded whole.

<!-- CAPTURE PLACEHOLDER (#356): ![datui charting Central Park highs from NOAA's S3 bucket, then opening and scrolling 38 million rows of 2024](demos/noaa-cloud.gif) -->

Do it yourself: [Connect to cloud storage](user-guide/remote-data.md#examples-on-public-data).

## More examples

| Question | Dataset | Guide |
|---|---|---|
| Which species is heaviest? | Palmer penguins | [Quick start](getting-started/quick-start.md) |
| Which airlines arrive late, and where does AS fly? | NYC flights (2013) | [Query data](user-guide/querying-data.md#run-a-query) |
| Which matches had the most goals? | Premier League (2020-21) | [Dates and messy text](user-guide/querying-data.md#dates-and-messy-text) |
| The heaviest chicken dishes | Food nutrition (fast food) | [Sort, filter and arrange columns](user-guide/filtering-sorting.md) |
| Emma, Jennifer and Olivia by year | US baby names (1880-2017) | [Pivot and melt](user-guide/reshaping.md#pivot) |
| Every chart on the built-in data | Several | [Make a chart](user-guide/charting.md#examples-on-the-built-in-datasets) |
| One query, two years of weather | NOAA daily weather (GHCN-D) | [Save and apply views](user-guide/views.md) |
