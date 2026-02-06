Output demos/10-charting.gif
Set FontSize 18
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui tests/sample-data/charting_demo.parquet"
Enter
Sleep 0.5s
Show

# App loaded at main screen
Sleep 2s

# Open charting (c)
Type "c"
Sleep 3s

# Tab to the plot style selector
Tab
Sleep 0.5s

# Tab to X axis filter
Tab
Sleep 0.5s

# Type filter "high" (narrows to high_temp, etc.)
Type "high"
Sleep 1s

# Tab to X column list, high_temp selected
Tab
Sleep 0.5s

# Enter to select high_temp as X axis
Space
Sleep 0.5s

# Tab to Y axis filter
Tab
Sleep 0.5s

# Type filter "temp"
Type "temp"
Sleep 0.5s

# Tab to Y column list
Tab
Sleep 1s

# j to move down in Y list
Type "j"
Sleep 0.5s
Space
Sleep 1s

# Shift+Tab 4 times back to chart type (Y list → Y input → X list → X input → Chart type)
Shift+Tab
Sleep 0.5s
Shift+Tab
Sleep 0.5s
Shift+Tab
Sleep 0.5s
Shift+Tab
Sleep 1s

# l to switch to Scatter chart type
Type "l"
Sleep 2s

# e to open export
Type "e"
Sleep 1s

# Type export path
Type "/tmp/datui_scatter.png"
Sleep 1s

# Enter to export
Enter
Sleep 2s

# Enter to dismiss success dialog
Enter
Sleep 1s

# Escape to close charting (back to main view)
Escape
Sleep 2s
