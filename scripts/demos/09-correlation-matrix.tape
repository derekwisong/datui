Output demos/09-correlation-matrix.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui tests/sample-data/correlation_matrix_demo.parquet"
Enter
Sleep 0.5s
Show
Sleep 1s

# Open analysis tools (a)
Type "a"
Sleep 1.5s

# Down twice to Correlation Matrix (Describe -> Distribution -> Correlation Matrix), then Enter
Down
Sleep 0.5s
Down
Sleep 0.5s

# Select Correlation Matrix
Enter
Sleep 1s

# Scroll right a few times (focus is on main area, correlation matrix view)
Right
Sleep 0.5s
Right
Sleep 0.5s
Right
Sleep 0.5s
Right
Sleep 0.5s

# Scroll back left
Left
Sleep 0.5s
Left
Sleep 0.5s
Left
Sleep 0.5s
Left
Sleep 0.5s

# Pause 3 seconds to let the user see the correlation matrix
Sleep 3s

# Back to main from analysis page
Escape

# Pause before resuming the loop
Sleep 2s
Type "q"
