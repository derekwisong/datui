Output demos/07-filtering.gif
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

# Switch to Filter tab (Right/l on tab bar)
Type "l"
Sleep 0.5s

# Tab to body (Filter: Column)
Tab
Sleep 0.5s

# Cycle column to dist_normal (id=0 .. timestamp=5, dist_normal=6)
Type "l"
Sleep 0.5s
Type "l"
Sleep 0.5s
Type "l"
Sleep 0.5s
Type "l"
Sleep 0.5s
Type "l"
Sleep 0.5s
Type "l"
Sleep 0.5s

# Tab to Operator, cycle to > (Gt is 3rd)
Tab
Sleep 0.5s
Type "l"
Type "l"
Sleep 0.5s

# Tab to Value, type 4
Tab
Sleep 0.5s
Type "4"
Sleep 0.5s

# Add Filter is two tabs after value (Value -> Logical -> Add), then Enter to add to list
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Enter
Sleep 0.5s

# Tab to Apply (Column -> Operator -> Value -> Logical -> Add -> Statements -> Apply) — 5 Tabs
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
Tab
Sleep 2s

# Press Enter to apply filter (must land on Apply, not Cancel)
Enter
Sleep 0.5s

# Hold on filtered result
Sleep 2s

# Pause for loop
Sleep 1s
Type "q"
