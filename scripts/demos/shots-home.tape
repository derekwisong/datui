# Home screen and catalogs: the details pane with a catalog's column notes, and
# the Documentation view (Ctrl+E) with a column's value legend open.
# Pages: user-guide/home-screen.md (The details pane, Documentation view).

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

# NOAA daily weather (GHCN-D): its details, then COLUMNS. NYC flights' notes
# fall below the pane at 32 lines, so NOAA's carry this shot.
Down 3
Wait+Screen /COLUMNS/
Sleep 1.5s
Screenshot home-details.png

# The whole page, the ELEMENT legend open.
Ctrl+E
Wait+Screen /Documentation/
Sleep 500ms
Down 13
Sleep 300ms
Enter
Wait+Screen /TMAX  Maximum temperature/
Sleep 1s
Screenshot home-docs.png
