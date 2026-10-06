# Teaser: "When should I fly out of JFK?" NYC flights (2013), from Example
# datasets: sort by delay, inspect the worst, chart the mean delay by hour per
# airport. Storyboard A in plans/demo-storyboards.md. About 27 s.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

# 1. Public data, no login.
Sleep 1s
Type "flights"
Sleep 2s

# 2. Opens at once: the download is on camera.
Enter
Wait+Screen /336,776/
Sleep 1.5s

# 3. Sort by the worst departure delay.
Type "g"
Sleep 300ms
Type "dep_delay"
Sleep 400ms
Enter
Sleep 600ms
Type "]"
Wait+Screen /dep_delay ▼/
Sleep 1.5s

# 4. Every field of that row, documented: HA 51, JFK to HNL, 1301 minutes.
Space
Sleep 3s
Escape
Sleep 600ms

# 5. A line chart; c and 1 together so the histogram never shows.
Type "c1"
Sleep 800ms
Down
Space
Sleep 300ms
Type "hour"
Sleep 500ms
Enter
Sleep 800ms

# 6. Mean of every row, not a sample. The four steps go fast so only mean lands.
Down 2
Sleep 300ms
Right@20ms 4
Wait+Screen /all 336,776 rows/
Sleep 2s

# 7. One line per airport.
Down
Space
Sleep 300ms
Type "origin"
Sleep 400ms
Enter
Wait+Screen /colored by origin/

# 8. The answer: fly early; avoid Newark in the evening.
Sleep 7s
