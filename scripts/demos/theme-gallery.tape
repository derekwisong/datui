# The theme gallery: one screen, recorded once per theme (capture.py sets the
# theme and the terminal colors behind it), composed two by two.
# NYC flights (2013), sorted by dep_delay: header fill, stripes, the selection
# rail, the column cursor, type colors and the footer.
# Page: user-guide/configuration.md (Themes).

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Type "flights"
Wait+Screen /1 match/
Enter
Wait+Screen /1 \/ 336,776/
Type "g"
Sleep 200ms
Type "dep_delay"
Enter
Sleep 300ms
Type "]"
Wait+Screen /dep_delay ▼/
Sleep 1s
Down 2
Wait+Screen /3 \/ 336,776/
Sleep 500ms
Show

Sleep 1s
Screenshot theme.png
