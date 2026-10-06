# NYC flights (2013) queries: the footer's pipeline and a sorted header, the
# carrier ranking with the cursor on AS, and AS drilled down to its flights.
# Pages: user-guide/table.md (The footer), user-guide/querying-data.md (Run a
# query, Drill down a GROUP BY).

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

# One row per day of 2013, sorted by delay: `query › delay ▼` in the footer.
Type ":"
Sleep 300ms
Type@15ms "SELECT DATE(CONCAT_WS('-', year, month, day)) AS flight_date, COUNT(*) AS flights, AVG(dep_delay) AS delay FROM df GROUP BY flight_date ORDER BY flight_date"
Enter
Wait+Screen /1 \/ 365/
Type "g"
Sleep 200ms
Type "delay"
Enter
Sleep 300ms
Type "]"
Wait+Screen /delay ▼/
Sleep 1s
Screenshot table-footer.png

# Which airlines arrive late? The cursor on AS, last.
Type ":"
Sleep 300ms
Ctrl+U
Type@15ms "SELECT carrier, AVG(arr_delay) AS delay, COUNT(*) AS flights FROM df GROUP BY carrier ORDER BY delay DESC"
Enter
Wait+Screen /1 \/ 16 /
Type "G"
Wait+Screen /16 \/ 16/
Sleep 1s
Screenshot query-carriers.png

# AS's 714 flights, every one Newark to Seattle; origin and dest in view.
Enter
Wait+Screen /Group: carrier=AS/
Type "g"
Sleep 200ms
Type "origin"
Enter
Sleep 1s
Screenshot query-drill.png
