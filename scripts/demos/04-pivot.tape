Output demos/04-pivot.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui tests/sample-data/pivot_long.parquet"
Enter
Sleep 0.5s
Show
Sleep 1s

# Open Pivot & Melt (p)
Type "p"
Sleep 1s

# Tab to Index Columns list (skip filter)
Tab
Sleep 0.5s
Tab
Sleep 0.5s

# Add id and date to index (Space toggles selection)
Type " "
Sleep 0.5s
Down
Sleep 0.5s
Type " "
Sleep 0.5s

# Tab to Apply (PivotCol, ValueCol, Aggregation, SortToggle, Apply) — 5 Tabs
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Sleep 2s

# Apply pivot
Enter
Sleep 0.5s

# Hold on pivoted result so user can see what happened
Sleep 5s

# Pause for loop
Sleep 1s
Type "q"
