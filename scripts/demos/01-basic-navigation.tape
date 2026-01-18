Output demos/01-basic-navigation.gif
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "catppuccin-frappe"
Set TypingSpeed 50ms

Type "datui tests/sample-data/people.parquet"
Enter

# Wait for TUI to load and display data
Sleep 3s

# Show initial view
Sleep 1s

# Navigate down (press 'j' multiple times)
Type "j"
Sleep 0.4s
Type "j"
Sleep 0.4s
Type "j"
Sleep 0.4s
Type "j"
Sleep 0.4s

# Navigate up (press 'k')
Type "k"
Sleep 0.5s

# Navigate right (press 'l')
Type "l"
Sleep 0.4s
Type "l"
Sleep 0.4s

# Navigate left (press 'h')
Type "h"
Sleep 0.5s

# Quit
Type "q"
