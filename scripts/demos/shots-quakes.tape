# Earthquakes (past month): longitude against latitude, the Pacific Ring of
# Fire. A rolling feed: captions quote no counts. Page: user-guide/charting.md.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

Type "earthquakes"
Wait+Screen /1 match/
Enter
Wait+Screen /1 \/ [0-9]/
Sleep 500ms
Type "g"
Sleep 200ms
Type "latitude"
Enter
Sleep 300ms
Type "c2"
Sleep 800ms
Down
Space
Type "longitude"
Sleep 300ms
Enter
Wait+Screen /longitude/
Sleep 2s
Screenshot chart-quakes.png
