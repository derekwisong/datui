# Cloud: NOAA daily weather (GHCN-D) on S3, no login. Central Park's 155 years
# from the catalog's bookmark, charted by year; then one year of the bucket,
# YEAR=2024, typed as a path, opened as one table and scrolled to its end.
# Real time: never cut or speed up a wait. The secondary storyboard in
# plans/demo-storyboards.md. About 40 s.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

# 1. A real public bucket, with its bookmarks.
Sleep 1s
Down 3
Sleep 1.5s

# 2. Catalogs carry their documentation.
Ctrl+E
Sleep 3.5s
Escape
Sleep 500ms

# 3. Central Park, NY: 59 files, opened in place.
Down 2
Sleep 800ms
Enter
Wait+Screen /458,003/
Sleep 1.5s

# 4. What is in it, at a glance.
Type "l"
Sleep 300ms
Type "F"
Wait+Screen /Distinct/
Sleep 3s
Escape
Sleep 500ms

# 5-6. SQL on any dataset: tenths of a degree to degrees Celsius. The partial
# 2025 is left out; it would drag the last point down.
Type ":"
Sleep 500ms
Type@25ms "SELECT STRPTIME(DATE, '%Y%m%d') AS day, DATA_VALUE / 10.0 AS high_c FROM df WHERE ELEMENT = 'TMAX' AND DATE < '20250101'"
Sleep 500ms
Enter
Wait+Screen /56,971/
Sleep 1.5s

# 7-8. The mean daily high by year: about 15 °C in the 1870s, 18 °C now.
Type "c"
Sleep 800ms
Down 2
Sleep 300ms
Right@20ms 5
Wait+Screen /mean by year/
Sleep 5s

# 9. Any S3 path. q goes home; ~ types a path.
Escape
Sleep 400ms
Type "q"
Sleep 800ms
Type "~"
Sleep 300ms
Type@30ms "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024"
Wait+Screen /YEAR=2024\//
Sleep 800ms
Enter
Wait+Screen /hive table/
Sleep 2s

# 10. Every partition as one table: the wait is real.
Enter
Wait+Screen /1 \/ [0-9]+,[0-9]{3},[0-9]{3}/
Sleep 1.5s

# 11. Scroll, then to the last of the rows.
PageDown
Sleep 700ms
PageDown
Sleep 700ms
PageDown
Sleep 1s
Type "G"
Wait+Screen /20241231/
Sleep 5s
