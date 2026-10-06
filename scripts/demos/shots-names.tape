# US baby names (1880-2017): Emma, Jennifer and Olivia in the Pivot & Melt
# builder with its live preview, then the pivot charted as three lines.
# Pages: user-guide/reshaping.md (Pivot), user-guide/charting.md.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

Type "baby names"
Wait+Screen /1 match/
Enter
Wait+Screen /1 \/ 1,924,665/
Sleep 500ms
Type ":"
Sleep 300ms
Type@15ms "SELECT year, name, n FROM df WHERE sex = 'F' AND name IN ('Emma', 'Jennifer', 'Olivia')"
Enter
Wait+Screen /1 \/ 376/

# Index year, Columns name, Values n: the preview shows 138 rows.
Type "p"
Wait+Screen /Pivot & Melt/
Down
Space
Type "year"
Space
Enter
Down
Space
Type "name"
Enter
Down
Space
Type "n"
Enter
Wait+Screen /138 rows/
Sleep 1s
Screenshot pivot-builder.png

# Applied, then a line per name over year.
Enter
Wait+Screen /pivoted/
Type "l"
Sleep 200ms
Type "c1"
Sleep 800ms
Down
Space
Type "year"
Sleep 300ms
Enter
Sleep 300ms
Down
Space
Sleep 300ms
Down
Space
Down
Space
Enter
Wait+Screen /Emma, Jennifer, Olivia/
Sleep 1.5s
Screenshot chart-names.png
