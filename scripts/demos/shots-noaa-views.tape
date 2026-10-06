# Save and apply views on NOAA daily weather (GHCN-D): the Central Park query
# on 2024's highs saved as a view, then 2023's highs open and the Views list
# offering it by `same columns`, then applied. Page: user-guide/views.md.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

# 2024's highs, from the catalog's bookmark.
Type "daily highs"
Wait+Screen /1 match/
Sleep 500ms
Enter
Wait+Screen /1 \/ [0-9]/
Sleep 500ms
Type ":"
Sleep 300ms
Type@15ms "SELECT CAST(STRPTIME(DATE, '%Y%m%d') AS DATE) AS day, DATA_VALUE / 10.0 AS high_c FROM df WHERE ID = 'USW00094728' ORDER BY day"
Enter
Wait+Screen /1 \/ 366/

# Saved as "Central Park highs"; the name is typed over the directory's.
Type "v"
Sleep 500ms
Type "s"
Wait+Screen /Save View/
Type "Central Park highs"
Enter
Wait+Screen /Views/
Escape
Sleep 300ms

# 2023's highs by path, then the Views list. Esc clears the home filter.
Type "q"
Wait+Screen /EXAMPLE DATASETS/
Escape
Wait+Screen /narrow and search/
Type "~"
Sleep 300ms
Type@15ms "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/ELEMENT=TMAX"
Sleep 1s
Enter
Wait+Screen /one schema/
Enter
Wait+Screen /1 \/ [0-9]/
Sleep 500ms
Type "v"
Wait+Screen /same columns/
Sleep 1s
Screenshot views-list.png

# Applied: 365 rows of 2023.
Enter
Wait+Screen /1 \/ 365/
Sleep 1s
Screenshot views-applied.png
