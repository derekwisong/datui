Output demos/06-sorting.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui tests/sample-data/large_dataset.parquet"
Enter
Sleep 0.5s
Show
Sleep 1s

# Open Sort & Filter (s)
Type "s"
Sleep 1s

# Tab to Column list (Tab -> Filter, Tab -> ColumnList; only Down moves selection, not 'j')
Tab
Sleep 0.5s
Tab
Sleep 0.5s

# Navigate down to dist_normal (id=0 .. timestamp=5, dist_normal=7) using Down key
Down
Sleep 0.5s
Down
Sleep 0.5s
Down
Sleep 0.5s
Down
Sleep 0.5s
Down
Sleep 0.5s
Down
Sleep 0.5s
Down
Sleep 0.5s

# Add dist_normal to sort
Type " "
Sleep 0.5s

# Tab to Apply (ColumnList -> Order -> Apply) — 2 Tabs
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Sleep 2s

# Apply sort
Enter
Sleep 0.5s

# Hold on sorted result
Sleep 5s

# Pause for loop
Sleep 1s
Type "q"
