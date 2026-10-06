# NYC flights (2013) charts: JFK's mean delay by hour from the query, the same
# question for all three airports colored by origin, and the mean arrival delay
# per carrier as bars. Page: user-guide/charting.md.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

Type "flights"
Sleep 300ms
Enter
Wait+Screen /336,776/
Sleep 500ms

# JFK delay by hour: the query, then a line of mean_delay over hour.
Type ":"
Sleep 300ms
Type@15ms "SELECT hour, AVG(dep_delay) AS mean_delay, COUNT(dep_delay) AS flights FROM df WHERE origin = 'JFK' GROUP BY hour ORDER BY hour"
Enter
Wait+Screen /1 \/ 19 /
Type "g"
Sleep 200ms
Type "mean_delay"
Enter
Sleep 300ms
Type "c1"
Sleep 800ms
Down
Space
Sleep 200ms
Type "hour"
Sleep 300ms
Enter
Wait+Screen /X +hour/
Sleep 1.5s
Screenshot chart-jfk-hour.png

# Every airport, every row: mean dep_delay by hour, colored by origin.
Escape
Sleep 300ms
Type "R"
Wait+Screen /1 \/ 336,776/
Type "g"
Sleep 200ms
Type "dep_delay"
Enter
Sleep 300ms
Type "c1"
Sleep 800ms
Down
Space
Sleep 200ms
Type "hour"
Sleep 300ms
Enter
Sleep 500ms
Down 2
Right@20ms 4
Wait+Screen /mean by hour/
Down
Space
Sleep 200ms
Type "origin"
Sleep 300ms
Enter
Wait+Screen /colored by origin/
Sleep 1.5s
Screenshot chart-color-origin.png

# Mean arrival delay per carrier: HA and AS left of zero.
Escape
Sleep 300ms
Type "R"
Wait+Screen /1 \/ 336,776/
Type "g"
Sleep 200ms
Type "carrier"
Enter
Sleep 300ms
Type "c"
Wait+Screen /count by carrier/
Down 3
Space
Sleep 200ms
Type "arr_delay"
Sleep 300ms
Enter
Sleep 300ms
Down
Right@20ms 3
Wait+Screen /mean by carrier/
Sleep 1.5s
Screenshot chart-carrier-bar.png
