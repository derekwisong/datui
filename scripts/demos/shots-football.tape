# Premier League (2020-21): dates and scores parsed out of text in SQL; the
# two nine-goal matches first. Page: user-guide/querying-data.md (Dates and
# messy text). The footer shows at most two lines of a query being typed, so
# the shot is the result, not the multi-line prompt.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

Type "premier"
Wait+Screen /1 match/
Enter
Wait+Screen /1 \/ 380/
Sleep 500ms
Type ":"
Sleep 300ms
Type@15ms `SELECT Round, CAST(STRPTIME(SUBSTR(Date, 1, 15), '%a %b %d %Y') AS DATE) AS match_date, "Team 1" AS home, "Team 2" AS away, CAST(SPLIT_PART(FT, '–', 1) AS INT) + CAST(SPLIT_PART(FT, '–', 2) AS INT) AS goals FROM df ORDER BY goals DESC, match_date, home`
Enter
Wait+Screen /goals▼/
Sleep 1s
Screenshot query-goals.png
