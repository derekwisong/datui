# NOAA daily weather (GHCN-D), YEAR=2024 straight from S3: the most common
# measurements over every file of the year. Rolling bucket: caption what the
# frame shows. Page: user-guide/remote-data.md (Examples on public data).

Hide
Type "clear; datui s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/"
Enter
Wait+Screen /1 \/ [0-9]+,[0-9]{3},[0-9]{3}/
Show

Sleep 1s
Type ":"
Sleep 300ms
Type@15ms "SELECT ELEMENT, COUNT(*) AS observations, COUNT(DISTINCT ID) AS stations FROM df GROUP BY ELEMENT ORDER BY observations DESC"
Enter
Wait+Screen /observations▼/
Wait+Screen /PRCP/
Sleep 1s
Screenshot remote-noaa-elements.png
