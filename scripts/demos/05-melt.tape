Output demos/05-melt.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui tests/sample-data/melt_wide.parquet"
Enter
Sleep 0.5s
Show
Sleep 1s

# Open Pivot & Melt (p)
Type "p"
Sleep 1s

# Switch to Melt tab (Right arrow — only KeyCode::Right switches tab)
Right
Sleep 0.5s

# Tab to Melt Index list (skip filter)
Tab
Sleep 0.5s
Tab
Sleep 0.5s

# Add id and date to index (Space toggles)
Type " "
Sleep 0.5s
Down
Sleep 0.5s
Type " "
Sleep 0.5s

# Tab to Apply (Strategy, Pattern, Type, ExplicitList, VarName, ValName, Apply) — 7 Tabs
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
Sleep 0.5s
Tab
Sleep 0.5s
Sleep 2s

# Apply melt
Enter
Sleep 0.5s

# Hold on melted result
Sleep 5s

# Pause for loop
Sleep 1s
Type "q"
