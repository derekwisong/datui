# Demos

Two recordings of datasets from **Example datasets** on the home screen, each
opened as its publisher serves it. The guides below walk through every step.

## When should I fly out of JFK?

**NYC flights (2013)**: 336,776 departures from New York's three airports.
The recording sorts by `dep_delay`, inspects the worst row, then charts the mean
departure delay by `hour`, one line per `origin`. At JFK the mean delay climbs
from 0.5 minutes at 5:00 to 26.1 at 21:00. The data is from 2013, so this is
not a forecast.

![datui opening NYC flights, sorting by delay and charting the mean delay by hour per airport](demos/teaser.gif)

Do it yourself: [Query data](user-guide/querying-data.md#run-a-query) and
[Make a chart](user-guide/charting.md#examples-on-the-built-in-datasets).

## A public bucket, opened in place

**NOAA daily weather (GHCN-D)** on S3, with no login. The recording opens
Central Park's 155 years of daily highs from the catalog's bookmark and charts
them by year. Then it opens a whole year of the bucket,
`s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/`, as one table and scrolls to its
last row. The waits are real, and nothing is downloaded whole.

![datui charting Central Park highs from NOAA's S3 bucket, then opening and scrolling 38 million rows of 2024](demos/noaa-cloud.gif)

Recorded 2026-10-05 with datui 0.4.0-dev on an 8-core Ryzen 7 9800X3D, over a
wired home connection, with a cold cache. `YEAR=2024` opened its 135 files as
one table of 38,466,379 rows in about a second. Your times depend on your
connection and the bucket.

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
